# Crush fork — READ-ONLY map: workspace-scoped MCP channel-event routing + self-drive agent turn

Repo: `/Users/jgavinray/dev/crush-agent/p3/crush`, module `github.com/charmbracelet/crush`, HEAD `b0115a1b` (approx post-v0.91.0, 2026-08-25). All paths relative to `p3/crush/`. This is a mapping document only — no code was modified.

---

## 0. Executive summary

1. **MCP is process-global, not per-workspace.** One `pubsub.Broker[Event]`, one `sessions`/`states`/`allTools` registry per process, bound to the backend context, not the workspace context. All the channel-event plumbing exists but is deliberately **dead-ended**: a channel notification is rendered into an injection-ready `<channel>` element, published must-deliver to the global broker, and **dropped at the three consumer seams** — `mcp.SubscribeEvents` filters it out (`init.go:238`), the SSE `wrapEvent` refuses to encode it (`server/events.go:40-48`), and the TUI model has no case for it (`ui/model/ui.go:957`). No code injects it into any session anywhere.
2. **The self-drive turn machinery already exists and was engineered for exactly this use.** `sessionAgent.Run` serializes dispatch per session under `dispatchMu`; a busy session **folds** a queued in-process `Run` into the active step (`drainQueueForStep` in `PrepareStep`, `agent.go:828-836`); the comments at `agent.go:585-588` and the test `dispatch_race_test.go:101-113` name "a burst of channel events" as the motivating case. What is missing is the **consumer**: nothing subscribes to channel events and calls `AgentCoordinator.Run(...)` with the rendered message as the prompt.
3. **The blocker for workspace scoping is identity.** `mcp.Event` carries only `Name` (the MCP server name) + `ChannelMessage`. There is no `WorkspaceID`/`SessionID`. The publish site (`channelConn.Read` / `createSession`) has no workspace reference, and each new workspace's `mcp.Initialize` **replaces the process-global session and its channel gate** (last-initialized-wins), so opt-in cannot be attributed to a workspace today.

The rest of this document is the precise map, the seams, and the gaps.

---

## 1. The channel inbound path (client-side of the MCP connection)

Artifacts: `internal/agent/tools/mcp/channel.go` (326 ln), `internal/agent/tools/mcp/init.go` (1321 ln).

### Constants and wire shape — `channel.go`
- `channelCapability = "claude/channel"` (`channel.go:29`) — capability key in `capabilities.experimental`.
- `channelNotificationMethod = "notifications/claude/channel"` (`channel.go:35`).
- Size caps: `maxChannelContentBytes = 64 * 1024` (40), `maxChannelMetaEntries = 32`, `maxChannelMetaValueBytes = 1024`; `metaKeyPattern` (56), `reservedMetaKeys = [source, xmlns, xml]` (61-65).
- `type channelParams struct { Content string \`json:"content"\`; Meta map[string]string \`json:"meta"\` }` (`channel.go:68-71`); `parseChannelParams(raw json.RawMessage) (channelParams, bool)` (78-116) validates, disallows unknown fields, enforces caps.
- `renderChannel(source string, p channelParams) string` (123-149): builds `<channel source="..." attrs...>content</channel>` as escaped XML (sorted meta keys, reserved key filtering).
- `hasChannelCapability(res *mcp.InitializeResult) bool` (153-159): `_, ok := res.Capabilities.Experimental[channelCapability]`.
- `channelEnabled(enabled []string, name string) bool` (165-176): case-insensitive, TrimSpace, matches bare `name` or `server:`+`name` — wired against `cfg.Overrides().EnabledChannels`.
- **`publishChannelMessage(ctx, name, raw)`** (183-194): parse → `broker.PublishMustDeliver(ctx, pubsub.CreatedEvent, Event{Type: EventChannelMessage, Name: name, ChannelMessage: renderChannel(name, p)})`. Must-deliver (bounded-blocking) because inbound channel notifications must not be lost.

### Transport interception — `channel.go`
- `channelGate` state machine (196-275): `stateGateUndecided | stateGateOpen | stateGateClosed`; `newChannelGate()`, `isOpen()`, `resolve(open bool) []json.RawMessage` (drains the pre-decision buffer when opened, discards when closed), `accept(raw json.RawMessage) json.RawMessage`. Begins **undecided** so notifications arriving during capability negotiation are buffered, not lost.
- `channelTransport struct { inner mcp.Transport; name string; gate *channelGate }` (281-297); `Connect` wraps the conn in `channelConn` (291-297); `unwrapTransport()` peels decorators (also in `init.go:974-984`).
- `channelConn.Read(ctx)` (312-326): loops `Connection.Read`; when a `jsonrpc.Request` is **not** a call (`!req.IsCall()`) and `req.Method == channelNotificationMethod` → `if raw := c.gate.accept(req.Params); raw != nil { publishChannelMessage(ctx, c.name, raw) }`; all other messages return to the SDK. This is unconditional interception — the *gate* decides publish vs drop, which is how opt-in is enforced.

### Session construction and gate resolution — `init.go`
- Process-globals (package level, `init.go:71` and nearby): `broker = pubsub.NewBroker[Event]()`, `sessions` (map), `states = csync.NewMap[string, ClientInfo]()`, `authURLs`, `gens`, plus `initOnce/initDone/initMu/initStarted` for `WaitForInit`.
- `createSession(ctx, cfg *config.ConfigStore, name string, m config.MCPConfig, resolver, channelOptIn bool)` (872-969):
  - `mcpCtx, cancel := context.WithCancel(ctx)` (874) — **ctx originates from `app.New`'s arg, i.e. the backend context in server mode, NOT the workspace context** (see §5).
  - `channelGate := newChannelGate()` (901); `transport = &channelTransport{inner: transport, name: name, gate: channelGate}` (902).
  - `client := mcp.NewClient(...)` with `ToolListChangedHandler` / `PromptListChangedHandler` / `ResourceListChangedHandler` each publishing `EventToolsListChanged`/`EventPromptsListChanged`/`EventResourcesListChanged` to `broker` (904-934).
  - `session, err := client.Connect(mcpCtx, transport, nil)` (936); on error `maybeStdioErr` + `StateError` (937-944).
  - **Gate resolution** (954-962): `if channelOptIn && hasChannelCapability(session.InitializeResult()) { buffered := channelGate.resolve(true); for _, raw := range buffered { publishChannelMessage(mcpCtx, name, raw) }; slog.Info("MCP channel enabled", "name", name, "buffered", len(buffered)) } else { channelGate.resolve(false) }`.
- `channelOptIn` at every caller: `channelEnabled(cfg.Overrides().EnabledChannels, name)` at `init.go:376` (`AuthenticateMCP`→`connectAndRegister`), `:469` (`runAuthFlow`), `:505` (`initClient`), `:705` (`getOrRenewClient`→`newSession`).
- `connectAndRegister` (535-584): `createSession` → generation check → `registerSessionTools` (tools.go:139-145) → `getPrompts` → `sessions.Set(name, session)` (576) → `updateState(StateConnected, ...)`. **Always creates a fresh session; it does not reuse an existing one** — see §5 for the multi-workspace consequence.
- `Initialize(ctx, permissions, cfg)` (292-316): for each non-disabled MCP in `cfg.Config().MCP`, `goInitClient` (goroutine with panic recovery, gen captured), `wg.Wait`, `initOnce` closes `initDone`. Called once **per app.New** (per workspace) at `app/app.go:145`.
- `Close(ctx)` (262-289): closes **every** session in `sessions`, closes all OAuth handlers, and calls **`broker.Shutdown()`** — permanently closing the global broker and every subscriber channel. Called from app cleanup (`app/app.go:162`) i.e. on **every** workspace teardown in server mode (see §5).

### Events published, and who consumes them
- `EventType` (`init.go:170-179`): `EventStateChanged, EventToolsListChanged, EventPromptsListChanged, EventResourcesListChanged, EventChannelMessage`.
- `Event struct { Type EventType; Name string; State State; Error error; Counts Counts; ChannelMessage string }` (`init.go:182-191`). `ChannelMessage` set only for `EventChannelMessage`; comment at 176-177: "the rendered, escaped `<channel>...</channel>` element to inject into the session."
- **`SubscribeEvents(ctx) <-chan pubsub.Event[Event]`** (232-249): subscribes to broker, goroutine filters — `for ev := range raw { if ev.Payload.Type == EventChannelMessage { continue } ... }` — returns buffered chan 64. Doc (224-231) states the reason verbatim: channel events carry **no workspace/session identity**, the broker is process-global, so without the filter every workspace subscription would receive every other workspace's channel events (= cross-workspace injection); "Channel events must be routed workspace-scoped before they can re-join this stream" / scoped routing deferred to a later PR.
- `updateState` (815-870) publishes `EventStateChanged`; `init.go:911-928` handlers publish list-changed events. All broker publishes are lossy `Publish` except channel messages (must-deliver).

---

## 2. Event fan-out into the app (local mode) and the SSE boundary (server mode)

### `internal/app/app.go` — the tea.Msg fan-in
- `App` has `events *pubsub.Broker[tea.Msg]` (app.go:73, created at 123).
- **`Events(ctx) <-chan pubsub.Event[tea.Msg]` (203-205)** = `app.events.Subscribe(ctx)`; `SendEvent(msg)` (208-210) publishes `UpdatedEvent`. This is what `backend.SubscribeEvents` returns via the embedded `*app.App`.
- `setupEvents` (585-609) wires per-subscriber **fan-in goroutines** into `app.events`:
  - lossy `setupSubscriber` (611-634) for sessions/messages/history/agent-notifications/**mcp**/lsp/skills;
  - must-deliver `setupSubscriberMustDeliver` (643-666) for permissions (both), question batches+notifications, **run-completions**.
  - The **mcp** fan-in (app.go:597) calls `mcp.SubscribeEvents` → so channel events are filtered *before* they reach `app.events`. Even if not filtered, app.events → wire drops them again (below).
- `App.Shutdown` (739-795): `AgentCoordinator.CancelAll()`, `Messages.FlushAll`, parallel `cleanupFuncs` — one of which is `mcp.Close(ctx)` (app.go:162), plus `event.AppExited()`, shell kill, LSP kill, events teardown (602-608).
- `app.New` (97-189): builds services; `mcp.ArmInit(); go mcp.Initialize(ctx, app.Permissions, store)` (144-145) — the **per-workspace init of the process-global MCP layer**; `InitCoderAgent` at 170 creates the coordinator (see §4).
- `initCoderAgent` (678-703) → `agent.NewCoordinator(ctx, CoordinatorOptions{...})`; `App.Subscribe(program)` (706-736) feeds the TUI.

### `internal/backend/events.go` — the Workspace-facing API (145 ln)
- `func (b *Backend) SubscribeEvents(ctx, workspaceID) (<-chan pubsub.Event[tea.Msg], error)` (16): `GetWorkspace` → `ws.Events(ctx)`. **`*backend.Workspace` gets `Events` by embedding `*app.App`; the `workspace.Workspace` interface has no `Events` method** (workspace.go:117-237) — relevant because `backend.Workspace` is not the same type as `workspace.AppWorkspace`/`ClientWorkspace`.
- MCP passthroughs are workspaceID-agnostic: `MCPGetStates(_ string)` → `mcptools.GetStates()`, `MCPRefreshPrompts(_ string, name)`, `MCPRefreshResources(_ string, name)` — they read/write the **global** MCP registry (also `workspace/app_workspace.go:404-483` delegating straight to `mcptools.*`).
- `MCPPendingAuth(workspaceID)` uses `ws.Cfg`; `MCPAuthenticate(ctx, workspaceID, name)` uses `mcptools.BeginAuth(ws.Cfg, name)` (134-144).

### `internal/server/proto.go` — SSE surface (1215 ln)
- `handleGetWorkspaceEvents` (281-337): `requireClientID` (validates UUID `client_id` query, 148-161); **`events, err := c.backend.SubscribeEvents(r.Context(), id)` at 293** (before `AttachClient(id, clientID)` at 298; `defer DetachClient(id, clientID)` at 302); headers `text/event-stream`, no-cache, keep-alive; returns 200+Flush; loop: on ctx.Done return; on `ev, ok := <-events`: `wrapped := wrapEvent(ev.Payload); if wrapped == nil { continue }; json.Marshal; fmt.Fprintf(w, "data: %s\n\n", data); flusher.Flush()`.
- Agent endpoints: `handlePostWorkspaceAgent` (777-800) decodes `proto.AgentMessage`, calls `c.backend.SendMessage(id, msg)` (795), returns 202; comment: run lifetime detached from HTTP request, only explicit cancel endpoint can end a run. `handlePostWorkspaceAgentInit` (811-828) → `InitAgent(ctx, id, req.Interactive)`; `handlePostWorkspaceAgentUpdate` (839-846) → `UpdateAgent`; `handlePostWorkspaceAgentSessionCancel` (880-888) → `CancelSession(id, sid)`; queued-prompts/clear/summarize/shell handlers (901-984).
- `handleError` (1170-1204): ErrWorkspaceNotFound/LSPClientNotFound→404; ErrAgentNotInitialized/ErrPathRequired/ErrInvalidPermissionAction/ErrUnknownCommand/ErrInvalidClientID→400; ErrClientNotAttached/ErrWorkspaceClosing/ErrServerNotIdle/ErrClientRetired→409; ErrServerShuttingDown→503.

### `internal/server/events.go` — wrapEvent (384 ln)
- `wrapEvent(ev any) *pubsub.Payload` (27-170), `mcpEventTypeToProto` (185-200). For `pubsub.Event[mcp.Event]` (40-58): `pt := mcpEventTypeToProto(e.Payload.Type); if pt == "" { slog.Debug("Dropping unsupported MCP event type for SSE", "type", ...); return nil }` → then `proto.MCPEvent{Type, Name, State, Error, ToolCount}`. `mcpEventTypeToProto` has **no case for `EventChannelMessage`** — returns "" deliberately (196-199: "Unsupported type (e.g. EventChannelMessage). Return empty so callers can drop it rather than coercing to state_changed").
- Guarded by test `events_test.go:212-232` `TestMCPChannelMessageNotWrappedAsStateChange`.
- `proto.MCPEvent` (`proto/mcp.go:85-93`) has fields `Type/Name/State/Error/ToolCount/PromptCount/ResourceCount` — **no ChannelMessage field**. `proto.MCPEventType` consts (67-70) exclude channel.

### `internal/workspace/client_workspace.go` — TUI-side (client/server) consumer (1432 ln)
- `Subscribe(program)` (792-799) → `runSubscription(program.Send)`; loop `evc, err := w.client.SubscribeEvents(w.subCtx, w.workspaceID())` (847); backoff reconnect + `markDegraded` + `recoverWorkspace()` (914-942, re-`CreateWorkspace` from `w.recreateArgs()` — proto.Workspace{Path,DataDir,Debug,YOLO,Channels,Env,Version} at 948-959); after reconnect re-asserts `SetCurrentSession`.
- `consumeEvents` (997-1013): `case pubsub.Event[proto.ConfigChanged]: refreshWorkspace; continue`; otherwise `w.translateEvent(ev)` → `send(translated)`.
- `translateEvent` (1071+): `pubsub.Event[proto.LSPEvent]`/`[proto.MCPEvent]` (via `protoToMCPEventType`) / `[proto.PermissionRequest]` etc. Because the server drops channel events, and `proto.MCPEvent` has no channel field, **no channel message ever reaches the TUI**.
- `Shutdown` (1021-1049): subCancel, awaitSubscription, herdrClient.Close, `client.RetireClient(...)`, fallback `DeleteWorkspace` on `client.ErrUnsupported`.

### TUI renderer — `internal/ui/model/ui.go:957-970`
- `case pubsub.Event[mcp.Event]:` handles `EventStateChanged | EventPromptsListChanged | EventToolsListChanged | EventResourcesListChanged` — **no `EventChannelMessage` case**, and none is ever delivered anyway.

### Local-mode console (`internal/cmd/run.go`, `internal/app/app.go`)
- `crush run` local path: `setupLocalWorkspace` (root.go:331-402, sets `store.Overrides().EnabledChannels = channels` at 350) then `appWs.App().RunNonInteractive(...)` (run.go:157). `RunNonInteractive` (app.go:267-438): `InitCoderAgentNonInteractive`, `mcp.WaitForInit` (319), `UpdateModels` (324), `AutoApproveSession`, `AgentCoordinator.Run` on a goroutine (361), streams `messageEvents` to stdout, exits on `done`.
- Client/server path: run.go:110-128 builds `ClientWorkspace`, `InitCoderAgentNonInteractive` → server; `runNonInteractive` (173-…): `c.UpdateAgent` (229), resolve session, **`events, _ := c.SubscribeEvents(ctx, ws.ID)` (253)**, **mints a per-call `RunID = uuid.New().String()` (263)** and `c.SendMessage(ctx, ws.ID, sess.ID, runID, prompt)` (264); the `runStream` loop exits only on a `proto.RunComplete` whose `RunID` matches (run.go:384-399) — the reliable completion contract.

---

## 3. The workspace/backend ownership model

`internal/backend/backend.go` (1137 ln) + `internal/workspace/workspace.go` (245 ln).

- `Backend` (backend.go:89-137): `workspaces *csync.Map[string,*Workspace]`, `pathIndex map[string]string` (resolved abs path → ws ID), `pending`, `shutdownTimer`, `closing`, `retired map[string]struct{}`, `mu`, `cfg`, `ctx`, `shutdownFn`. Lock order: `b.mu` → `ws.clientsMu`.
- `type Workspace struct` (backend.go:170-211): embeds `*app.App`; `ID`, `Path`, `Cfg *config.ConfigStore`, `Env`, `Skills *skills.Manager`, `resolvedPath`; **`ctx context.Context` + `cancel` = workspace-scoped run context** (comment 183-191: agent runs dispatched on behalf of the workspace are bound to `ws.ctx`; owned by workspace, not the client's HTTP request); `runMu/closing/runWG`, `clientsMu`, `clients map[string]*clientState`, `shutdownFn`.
- `Workspace.Shutdown` (242-257): set `closing` → `ws.cancel()` → if `App != nil && AgentCoordinator != nil { CancelAll() }` → `runWG.Wait()` → `App.Shutdown()`. (Order matters: cancel → CancelAll → wait → app cleanup/DB.)
- `clientState` (161-166): `{streams int; holdTimer *time.Timer; currentSessionID string; released bool}`.
- **`CreateWorkspace` (345-515)**: validate `args.Path`/ClientID; `key = resolveWorkspaceKey(args.Path)` (354, EvalSymlinks/Abs); `b.mu`; `admitLocked`; `cancelShutdownLocked`; **pathIndex dedup** — on existing ws: `if !stringSlicesEqual(ws.Cfg.Overrides().EnabledChannels, args.Channels) { return ..., ErrChannelOptInMismatch }` (375-378; second occurrence 481-485 after the slow-init re-check) + `logFirstWinsMismatch` + `registerClient`; else `b.pending++`, slow init under released mu: `id := uuid.New().String()` (413), `config.Init(...)` (414), **`cfg.Overrides().SkipPermissionRequests = args.YOLO; cfg.Overrides().EnabledChannels = args.Channels` (419-420)**, `createDotCrushDir`, `db.Connect(b.ctx, ...)` (426 — backend ctx), skills `NewManager` **without `WithGlobalMirror`** (431-441, per-workspace to prevent cross-workspace skill crosstalk), **`app.New(b.ctx, conn, cfg, skillsMgr)` (443 — note: backend ctx, not wsCtx)**; `wsCtx, wsCancel := context.WithCancel(b.ctx)` (448); `ws := &Workspace{... ctx: wsCtx ...}` (449-460); re-admit + re-check index under mu; `b.workspaces.Set(id, ws)` + `pathIndex[key]=id` + `registerClient(ws, clientID)` (494-500); version-mismatch warn via `SendEvent` (502-512).
- Errors (31-44): incl. **`ErrChannelOptInMismatch = errors.New("requested channels differ from the existing workspace; channels are an explicit opt-in and are not shared across duplicate creates")`** (43). Defaults: `DefaultCreateGrace=30s`, `DefaultIdleShutdownDelay=60s` (`CRUSH_SERVER_IDLE_TIMEOUT`), `DefaultDetachGrace=10s` (`CRUSH_SERVER_DETACH_GRACE`).
- `AttachClient` (570-598, under `b.mu`), `DetachClient` (606-612 → `detachStream` 765-798 with grace timer), `admitLocked` (617-625), `RetireClient` (637-667 clears claims across workspaces), `registerClient`/`newHeldClient`/`expireHold`/`releaseHoldLocked` (693-763), `teardown` (811-841: re-checks `len(clients)>0`, unindexes, `scheduleShutdownIfIdleLocked` (857-872), `ws.invokeShutdown()` (215-223: `shutdownFn` or `App.Shutdown`)), `maybeShutdown` (890-903).
- `SetCurrentSession(workspaceID, clientID, sessionID)` (924-944): client must be attached (`streams>0`) else `ErrClientNotAttached`; records `cs.currentSessionID`. `AttachedClients(workspaceID, sessionID)` (951-957) → `AttachedClientsForSession` (964-974): counts `streams>0 && currentSessionID==sessionID`. **This is the existing "which session is this client viewing" signal** — the natural target for "which session should a self-drive channel turn attach to."
- `workspaceToProto` (1069-1086): `Channels: ws.Cfg.Overrides().EnabledChannels` (1075).
- `workspace.Workspace` interface (workspace.go:117-237): Sessions/Messages/Agent/Permissions/Questions/FileTracker/History/LSP/Config/MCP ops + `Subscribe(program)` + `Shutdown()`; **no `Events()`** (the server branch uses `*backend.Workspace` which exposes `Events` only via its embedded `*app.App`). `workspace.AppWorkspace` (`app_workspace.go`, 506 ln) delegates to `*app.App` (backend of local mode); `ClientWorkspace` is the HTTP-client-backed type (TUI over server).

---

## 4. The agent turn machinery (the self-drive foundation)

### `internal/agent/coordinator.go` — `Coordinator` (1458+ ln)
- Interface (88-112): `Run(ctx, sessionID, prompt, attachments...)`, `RunAccepted(ctx, accept, sessionID, prompt, attachments...)`, `BeginAccepted(sessionID) *AcceptedRun`, `Cancel`, `CancelAll`, `IsSessionBusy`, `IsBusy`, `QueuedPrompts(s)`, `QueuedPromptsList`, `ClearQueue`, `Summarize`, `Model`, `UpdateModels`, `GenerateTitle`.
- `run` (223-338): `readyWg.Wait()`; non-interactive → `mcp.WaitForInit(ctx)` (243); `UpdateModels` (249); model/provider resolution + `mergeCallOptions`; `OnComplete` coalescing closure (282-288); **`runID := RunIDFromContext(ctx)` (295)** threaded into `SessionAgentCall.RunID`; run closure (296-313) calls `c.currentAgent.Run(ctx, SessionAgentCall{...})` with `Accepted: accept, OnComplete: onComplete, OnAuthRefresh: ...`; after run: `RunCompletePublished`/`MarkRunCompletePublished` context markers (330-336) so `backend.runAgent` can detect an already-published terminal event.
- `buildTools` (679-803): built-ins (bash, crush_info, logs, job_output/kill, download, edit, multi_edit, fetch, glob, grep, ls, sourcegraph, todos, view, write, question-if-interactive), LSP tools, mcp resource tools; **`for _, tool := range tools.GetMCPTools(c.permissions, c.cfg, c.cfg.WorkingDir())` (768)** with `agent.AllowedMCP` filtering; `wrapToolsWithHooks` (800) for the top-level agent only.
- `UpdateModels` (1208-1227) rebuilds models + tools from current config (picks up newly-registered MCP tools).

### `internal/agent/agent.go` — `sessionAgent.Run` (2287 ln)
- `SessionAgentCall` (76-133): `SessionID`, `RunID` (correlator echoed on RunComplete), `Prompt`, `Attachments`, provider knobs, `NonInteractive`, `OnComplete`, `Accepted`, `acceptSeq`, `OnAuthRefresh`.
- `sessionAgent` struct (169-223): `largeModel/smallModel/systemPrompt*` as `csync.Value`, `tools *csync.Slice[fantasy.AgentTool]`, `messageQueue *csync.Map[string, []SessionAgentCall]`, `activeRequests`, `dispatchMu`, `acceptedRuns`, `cancelMark`, `acceptSeqGen`.
- **`Run(ctx, call)` (566-1327)** — the single serialized entry point for every prompt, whether user-typed or (intended) channel-born:
  - `ValidateCall` (556-564; also called from `backend.SendMessage`).
  - **The dispatch decision is serialized under the per-session `dispatchMu`** (588-650). Comment at 583-587 (verbatim intent): "two concurrent in-process callers — **a burst of channel events**, or a channel event racing a typed prompt — cannot both pass the busy check and start two runs on the same session."
  - Cancel-on-entry via `canceledBySeq` (591-618, publishes canceled RunComplete, persists a canceled turn). Busy → `enqueueCall` (620-638, strips OnComplete/Accepted, preserves acceptSeq) and returns nil,nil; the queue drains later. Idle → register `activeRequests[sessionID]` cancel, `context.WithValue(ctx, tools.SessionIDContextKey, ...)` (643), create user message, stream.
  - Builds `fantasy.NewAgent` with system prompt + **`<mcp-instructions>` from connected MCP `InitializeResult.Instructions`** (666-678) + `a.tools` (680-690).
  - `agent.Stream(genCtx, fantasy.AgentStreamCall{...})` (796-1063) with callbacks: `PrepareStep` (808-879) — **`prepared.Tools = a.tools.Copy()` (815); `fold, canceledRunIDs := a.drainQueueForStep(call.SessionID)` (828); `publishCanceledQueueDrops` (829); for each folded queued call: `a.createUserMessage(callContext, queued)` and append `userMessage.ToAIMessage()` to `prepared.Messages` (830-836)** — i.e. **queued prompts are folded into the live turn as user messages at the next step, without starting a competing run.** Also cache-control, promptPrefix prepend, message create per step.
  - `OnReasoningStart/Delta/End`, `OnTextDelta` (908-918), `OnToolInputStart` (919-930), `OnRetry`, `OnAuthRefresh`, `ModelProvider` (943-949), `OnToolCall` (950-966: sanitize + record ToolCall), `OnToolResult` (967-982: create Tool-role message), `OnStepFinish` (983-1036: finish reason mapping incl. `StopTurn → EndTurn`, session usage update), `StopWhen` (1037-1062: auto-summarize threshold + repeated-tool-call loop detection).
  - End-of-turn: `activeRequests.Del` + `cancel()` (1213-1214), `notify.TypeAgentFinished` unless `NonInteractive` (1218-1224), then **queue-drain handoff** (1238-1327): under `dispatchMu` observe `cancelMark` vs queued calls, drop canceled (publishing canceled RunComplete drops), otherwise pop `firstQueuedMessage`, re-acquire a fresh accept (`firstQueuedMessage.Accepted = a.BeginAccepted(...)` 1313), handle `outerOwesRunComplete` (1296-1324), then **`return a.Run(ctx, firstQueuedMessage)` (1326)** — the recursive per-prompt turn loop.
  - Deferred terminal `RunComplete` (749-781): `FlushAll` then `publishRunComplete` (539-548: honors `OnComplete`, else `runComplete.PublishMustDeliver`). `notify.RunComplete{SessionID, RunID, MessageID, Text, Error, Cancelled}`.
- **This is the complete "self-drive" primitive**: any goroutine may call `coordinator.Run(ctx, sessionID, prompt)` (or `sessionAgent.Run` in-process) at any time; if the session is idle it starts a turn, if busy it folds into the running turn as a user message, and a burst serializes correctly. `dispatch_race_test.go` (155 ln): `TestRun_ConcurrentInProcessDispatchStartsOneRun` (113-155) fires 8 concurrent in-process `Run` calls ("the path channel events use", comment 101-106) and asserts `maxSeen == 1`.

### `internal/backend/agent.go` — fire-and-forget dispatch over HTTP (276 ln)
- **`SendMessage(workspaceID, msg proto.AgentMessage) error`** (30-61): `GetWorkspace` → nil-coordinator check (`ErrAgentNotInitialized`) → `agent.ValidateCall` → **`accept := ws.AgentCoordinator.BeginAccepted(msg.SessionID)` (48)** → under `ws.runMu`: refuse if `ws.closing` (`ErrWorkspaceClosing`), `ws.runWG.Add(1)` → `go b.runAgent(ws, msg, accept)` (59). Returns immediately; the run is owned by the workspace.
- **`runAgent(ws, msg, accept)`** (87-122): `defer ws.runWG.Done(); defer accept.Close()`; **`ctx := ws.ctx` (91, the workspace-scoped context)**; `agent.WithRunID(ctx, msg.RunID)` (93) if set; `agent.WithRunCompleteMarker(ctx)` (95); `ws.AgentCoordinator.RunAccepted(ctx, accept, msg.SessionID, msg.Prompt, ...)` (97). On non-cancel error: publish `notify.TypeAgentError` (102-107); reliable terminal fallback `RunComplete{Error}` when `msg.RunID != "" && !agent.RunCompletePublished(ctx)` (112-121).
- `InitAgent` (145-155) → `ws.InitCoderAgent(ctx)` / `InitCoderAgentNonInteractive`; `UpdateAgent` (158-165); `CancelSession` (169-179) → `Coordinator.Cancel(sessionID)`; `SummarizeSession` (182-193); queue helpers (196-235); `RunShellCommand` (249-275).
- `proto.AgentMessage` (`proto/proto.go:144-149`): `SessionID`, `RunID`, `Prompt`, `Attachments`. `proto.RunComplete` (78-85) with `RunID` semantics documented (62-77) — the reliable completion contract.

### Message/session services (SQLite, `message.Service`, `session.Service`)
- `message.Service`: `Create/Update/List/FlushAll/Subscribe`; `message.Message` parts incl. `TextContent`, `ToolCall`, `ToolResult`, `ShellCommand`, `Finish` — channel injection would surface as a `User` message with `TextContent` (exactly what `PrepareStep` folding does).
- `session.Service`: `Create/CreateTaskSession/CreateAgentToolSessionID/ParseAgentToolSessionID/Get/GetLast/Save/Rename/UpdateTitleAndUsage`, `IsAgentToolSession`, todos. Sessions are the dispatch key for `dispatchMu`/queues — one run per session, per-session queue.

---

## 5. Cross-cutting facts that shape the design

### 5.1 MCP is deliberately global; per-workspace isolation is NOT there yet
- `broker`, `sessions`, `states`, `allTools`, `allPrompts`, `allResources`, `authURLs`, `gens` are package globals in `internal/agent/tools/mcp/`.
- `mcp.Initialize` runs once per workspace (`app.New` at `app/app.go:144-145`), but `connectAndRegister` **always creates a fresh client** and `sessions.Set(name, session)` **overwrites the previous workspace's session without closing it** (`init.go:576`). Net effect in a multi-workspace server: **the last workspace to initialize owns each MCP connection and its channel gate**; the previous workspace's session is leaked until some `teardown`/`Close` path closes it. Channel opt-in therefore is **not** per-workspace beyond the moment of the latest init.
- **`mcp.Close` on ANY workspace teardown** (app cleanup `app/app.go:162` → `backend.go:211/220-222` teardown → `App.Shutdown`) closes **all** sessions and calls `broker.Shutdown()`, permanently shutting the global broker — any subsequent workspace's `Subscribe` gets an already-closed channel (broker.go:104-114, 85-102). In practice the current server is single-workspace-viable for MCP, which is exactly why the fork's channel docs defer workspace-scoped routing.
- MCP session contexts derive from the **backend** context (`app.New(b.ctx, ...)` at `backend.go:443`), not `ws.ctx` — so MCP connections outlive and are decoupled from workspace lifecycle today.
- `pubsub.Broker` (broker.go, 236 ln): default per-subscriber buffer **4096** (`bufferSize`, commit 0585f498 raised 64→4096); `Publish` lossy (165-188); `PublishMustDeliver` bounded-blocking, per-subscriber 50 ms timeout (190-236); `Shutdown` closes all subscriber channels (85-102).

### 5.2 Channels opt-in is enforced at exactly four seams
1. Gate opening per MCP connection: `channelOptIn && hasChannelCapability(...)` at `init.go:954` (createSession).
2. Per-workspace flag capture: `cfg.Overrides().EnabledChannels = args.Channels` (`backend.go:420`; local mode `root.go:350`).
3. Duplicate-create consistency: `ErrChannelOptInMismatch` + `logFirstWinsMismatch` (`backend.go:375-378, 481-485`, `test backend_test.go:779-843` — `TestChannelOptInBoundary_DuplicateCreate`).
4. Wire: server drops channel events (`server/events.go:40-48`, `mcpEventTypeToProto` 185-200); TUI has no channel case (`ui/model/ui.go:957`).

### 5.3 CLI/server wiring
- `--channels` = persistent `StringSlice` on rootCmd (`cmd/root.go:63-64`, hidden, `MarkHidden("channels")`; committed as `feat(cmd): add --channels opt-in flag` + `fix(cmd): make --channels a persistent flag so crush run inherits it`). Wired: local `setupLocalWorkspace` (root.go:334→350), client/server `connectToServer` (root.go:461→479 `proto.Workspace.Channels`). `crush run` shares the flag via persistent inheritance (`channels_flag_test.go:9-36`).
- Server (background, client/server mode): spawned by `startDetachedServer` as `crush server [--host ...]` (root.go:916-964) — **`--channels` is intentionally NOT forwarded to the server process**; channels reach the server per-workspace via `proto.Workspace.Channels` at create time. `ensureServer` (563-629) probes/restarts on version mismatch (`restartIfStale` 829-867, `ShutdownServerIfIdle`).

### 5.4 Feature history (fork commits that built this)
`git log --oneline --all -i --grep=channel` shows the PR chain (PR #3345 "upstream/channels-1-core"):
- `a1cf0022 feat(mcp): detect claude/channel capability and receive channel messages`
- `d291d9f2 feat(cmd): add --channels opt-in flag for MCP channel servers`
- `ae9257b9 fix(agent): serialize in-process run dispatch to prevent concurrent turns` (the `dispatchMu` machinery — motivated by channel events)
- `fed86b00 fix(cmd): make --channels a persistent flag`
- `a09cb366 fix(backend): log channel flag mismatch on duplicate workspace creation`
- `f7c53cbc fix(mcp): route channel messages through must-deliver broker path`
- `2cb0bdb2 fix(server): stop mapping channel events to spurious state changes`
- `bc8ff341 fix(mcp): buffer channel notifications during capability negotiation`
- `1f623ec0 fix(mcp): filter channel events from the shared app event stream`
- `14fa1de8 fix(mcp): prevent clients from inheriting channel opt-ins`
- `0585f498 fix(pubsub): raise default per-subscriber buffer (64 -> 4096)`
- `2af939d8 chore(cli): hide --channels`
So the inbound path, opt-in, gate, race-safe dispatch, and the drop-everywhere policy are all deliberate and tested. What remains unimplemented is exactly the scope named in the `SubscribeEvents` doc: **workspace-scoped routing + injecting the message into the session as a (self-driven) agent turn.**

---

## 6. Integration points and gaps (for the two features)

### Feature A — workspace-scoped MCP channel-event routing
Required seams, in dependency order:
1. **Identity on the event.** `mcp.Event` gains a workspace (and maybe session) field, OR a per-workspace subscription API `NotifyEvents(ctx)` next to `SubscribeEvents` (`init.go:232`). The publish site (`channelConn.Read` at `channel.go:312-326`, `publishChannelMessage` at 183-194) runs under `mcpCtx` derived from `b.ctx` (`backend.go:443` → `app.go:145` → `init.go:292/874`); it currently has **no handle on the workspace ID**. Options: (a) thread a `WorkspaceID` through the mcp package (e.g. a `CreateWorkspaceID` scope on `cfg`, or a per-workspace init path that stamps the event), or (b) scope the MCP layer per workspace first (see 5.1: per-workspace `broker`/`sessions`, `mcp.Close` refcounted or moved to server-level shutdown).
2. **Don't drop at the seams.** Remove/make-configurable the filter at `init.go:238`; add a `proto` MCP channel event type + `mcpEventTypeToProto` case (`server/events.go:185-200`) if clients should see it; OR keep them server-internal only (self-drive happens server-side, so SSE visibility may be unnecessary).
3. **Route to the owning workspace.** A per-workspace event loop living where the workspace's `App`/coordinator lives (server: `backend.Workspace`; local: `App`), consuming only events whose source server is in that workspace's `cfg.Overrides().EnabledChannels` (the same predicate as `channelEnabled`, `channel.go:165-176`).

### Feature B — self-drive agent turn (channel message → model turn)
The machinery (§4) is complete; only the caller is missing:
1. **Consumer**: on a routed channel event, resolve the target session — candidates: the attached client's `currentSessionID` (`backend.go:161-166/924-944`), or the workspace's most recent/last session (`session.Service.GetLast`, cf. `resolveSession` at `app.go:238-263`), or the active queued session.
2. **Dispatch**: in-process, call `ws.AgentCoordinator.Run(ctx = ws.ctx, sessionID, prompt = channelMessage)` (the path `dispatch_race_test.go:101-113` names), or via the HTTP surface with `beginAccepted`/`RunAccepted` (`backend/agent.go:30-122`). In-process `Run` is the intended path — the busy-session **fold** (`agent.go:828-836`) turns a channel event arriving mid-turn into a user message in the ongoing step, and the per-session `dispatchMu` serializes bursts.
3. **Presentation**: a channel fold becomes a `User` text message (exactly what `createUserMessage`/`drainQueueForStep` builds, `agent.go:1510-1525`, `828-836`). If users should see it distinctly, the prompt prefix/marker and the `Workspace`/TUI rendering (`ui/model/ui.go:957`) would need a channel-specific message kind; the `<channel>` element produced by `renderChannel` (`channel.go:123-149`) is already the model-facing injection format.
4. **Lifecycle**: the run owns its workspace lifetime via `ws.ctx` (`backend.go:91`, `runAgent`), and `RunComplete`/`RunID` give a reliable per-turn completion signal (`notify.RunComplete`, `agent.go:539-548`, `proto.go:62-85`) if an external driver (or `crush run`-style waiter) must observe completion.

### Key gaps (nothing to reuse as-is)
- No `WorkspaceID`/session attribution on `mcp.Event` — the single biggest blocker for scoping.
- No consumer/publisher wiring between the mcp broker and the coordinator for channel events (the filter at `init.go:238` is the only thing standing between a channel message and the app event stream).
- No proto/SSE representation for channel messages if clients need them.
- Multi-workspace MCP session ownership and `broker.Shutdown` on any workspace teardown (5.1) must be resolved before per-workspace routing is sound.

---

## 7. Test inventory relevant to this work
- `internal/agent/tools/mcp/channel_test.go` — channelParams/renderChannel/gate/channelConn (incl. cap enforcement, XML escaping, must-deliver, closed-gate drop, `channelEnabled` table).
- `internal/agent/tools/mcp/channel_integration_test.go` — full client-side path: capability detection → gate open → `publishChannelMessage` → received `EventChannelMessage`.
- `internal/agent/tools/mcp/lifecycle_test.go:410-435` — every transport wrapped in `channelTransport` before Connect.
- `internal/agent/dispatch_race_test.go` — 8 concurrent in-process `Run` calls → exactly one active stream (the channel-event burst guard).
- `internal/server/events_test.go:212-232` — `TestMCPChannelMessageNotWrappedAsStateChange` (SSE drop).
- `internal/backend/backend_test.go:779-843` — `TestChannelOptInBoundary_DuplicateCreate`.
- `internal/cmd/channels_flag_test.go` — `--channels` persistent flag availability on root/run.
- `internal/server/e2e_agent_test.go`, `agent_runcomplete_test.go`, `multiclient_test.go` — run-complete / SetCurrentSession / attached-clients coverage for the session-identity pieces a self-drive turn would lean on.

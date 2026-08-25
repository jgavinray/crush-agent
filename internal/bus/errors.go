package bus

import "fmt"

// Error is a bus-level error carrying a stable machine-readable code that
// the MCP tool layer surfaces to callers (SPEC §9 "Errors").
type Error struct {
	Code    string `json:"error"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func newError(code, format string, a ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, a...)}
}

// Canonical bus error codes (SPEC §9).
const (
	CodeAgentNotRegistered = "agent_not_registered"
	CodeAgentRemoved       = "agent_removed"
	CodeRecipientUnknown   = "recipient_unknown"
	CodeInReplyToNotFound  = "in_reply_to_not_found"
	CodeNoSuchMailbox      = "no_such_mailbox"
	CodeInvalidArgument    = "invalid_argument"
)

func errAgentNotRegistered(id string) error {
	return newError(CodeAgentNotRegistered, "agent %q has no live register event", id)
}

func errAgentRemoved(id string) error {
	return newError(CodeAgentRemoved, "agent %q's latest membership event is unregister", id)
}

func errRecipientUnknown(ref string) error {
	return newError(CodeRecipientUnknown, "no registered agent matches recipient reference %q", ref)
}

func errInReplyToNotFound(id int, agent string) error {
	return newError(CodeInReplyToNotFound, "message id %d not found in mailbox %q", id, agent)
}

func errNoSuchMailbox(id string) error {
	return newError(CodeNoSuchMailbox, "agent %q is not registered", id)
}

func errInvalidArgument(format string, a ...any) error {
	return newError(CodeInvalidArgument, format, a...)
}

package stream

import (
	"fmt"
	"strings"

	"go.temporal.io/api/serviceerror"
)

// The reason tokens of the Brook Stream crate. Each error message starts with one, as in
// "stream_closed: ...", so an SDK maps a server error to the same error class as a Store error.
const (
	ReasonNotFound      = "stream_not_found"
	ReasonExpired       = "stream_expired"
	ReasonClosed        = "stream_closed"
	ReasonFenced        = "stream_fenced"
	ReasonSequence      = "stream_sequence"
	ReasonAlreadyExists = "stream_already_exists"
	ReasonOffset        = "stream_offset"
	ReasonTooLarge      = "stream_too_large"
	ReasonInvalid       = "stream_invalid"
	ReasonStorage       = "stream_storage"
	ReasonUnsupported   = "stream_unsupported"
)

var reasons = []string{
	ReasonNotFound,
	ReasonExpired,
	ReasonClosed,
	ReasonFenced,
	ReasonSequence,
	ReasonAlreadyExists,
	ReasonOffset,
	ReasonTooLarge,
	ReasonInvalid,
	ReasonStorage,
	ReasonUnsupported,
}

// NewError builds the service error of a reason, with the status code the crate gives it.
func NewError(reason string, format string, args ...any) error {
	message := reason + ": " + fmt.Sprintf(format, args...)
	switch reason {
	case ReasonNotFound, ReasonExpired:
		return serviceerror.NewNotFound(message)
	case ReasonClosed, ReasonFenced, ReasonSequence:
		return serviceerror.NewFailedPrecondition(message)
	case ReasonAlreadyExists:
		return serviceerror.NewAlreadyExists(message)
	case ReasonOffset, ReasonTooLarge, ReasonInvalid:
		return serviceerror.NewInvalidArgument(message)
	case ReasonUnsupported:
		return serviceerror.NewUnimplemented(message)
	default:
		return serviceerror.NewUnavailable(message)
	}
}

// ReasonOf gives the reason token that starts the message of err, or "" when it has none.
func ReasonOf(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	for _, reason := range reasons {
		if strings.HasPrefix(message, reason+": ") {
			return reason
		}
	}
	return ""
}

func invalid(format string, args ...any) error {
	return NewError(ReasonInvalid, format, args...)
}

func notFound() error {
	return NewError(ReasonNotFound, "Nobody created the Stream")
}

func expired() error {
	return NewError(ReasonExpired, "The Records at the offset expired")
}

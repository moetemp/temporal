package stream

import (
	"fmt"
	"strings"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	ReasonNotOwner      = "stream_not_owner"
	ReasonSeqDivergent  = "seq_divergent"
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
	ReasonNotOwner,
	ReasonSeqDivergent,
}

// NewError builds the service error of a reason, with the status code the crate gives it and
// StreamErrorDetails in the status details.
func NewError(reason string, format string, args ...any) error {
	st := status.New(reasonCode(reason), reason+": "+fmt.Sprintf(format, args...))
	detailed, err := st.WithDetails(&streamservice.StreamErrorDetails{Reason: reason})
	if err == nil {
		st = detailed
	}
	return serviceerror.FromStatus(st)
}

func reasonCode(reason string) codes.Code {
	switch reason {
	case ReasonNotFound, ReasonExpired:
		return codes.NotFound
	case ReasonClosed, ReasonFenced, ReasonSequence, ReasonNotOwner, ReasonSeqDivergent:
		return codes.FailedPrecondition
	case ReasonAlreadyExists:
		return codes.AlreadyExists
	case ReasonOffset, ReasonTooLarge, ReasonInvalid:
		return codes.InvalidArgument
	case ReasonUnsupported:
		return codes.Unimplemented
	default:
		return codes.Unavailable
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

package stream

import (
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	sdkconverter "go.temporal.io/server/common/sdk"
)

var errCanceled = errors.New("the read was canceled")

// ParkedRead is a Workflow read that found no Record. It holds the completion callback of the
// read's Nexus operation, and the Stream completes it when the read can answer.
type ParkedRead struct {
	chasm.UnimplementedComponent

	*streampb.ParkedReadState

	Callbacks chasm.Map[string, *callback.Callback]
}

var _ callback.Host = (*ParkedRead)(nil)

func (r *ParkedRead) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	if r.GetResolved() {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

func (r *ParkedRead) CompletionCallbacks() chasm.Map[string, *callback.Callback] {
	if r.Callbacks == nil {
		r.Callbacks = make(chasm.Map[string, *callback.Callback])
	}
	return r.Callbacks
}

func (r *ParkedRead) CompletionCallbackMetadata() *callbackspb.CallbackMetadata {
	if r.CallbackMetadata == nil {
		r.CallbackMetadata = &callbackspb.CallbackMetadata{}
	}
	return r.CallbackMetadata
}

// GetNexusCompletion gives the page as the result, or the error as the failure of the
// operation.
func (r *ParkedRead) GetNexusCompletion(ctx chasm.Context, _ string) (nexusrpc.CompleteOperationOptions, error) {
	opts := nexusrpc.CompleteOperationOptions{StartTime: r.GetStartTime().AsTime()}
	if r.GetFailure() == nil {
		result, err := SystemPayload(r.GetResponse())
		if err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		opts.Result = result
		return opts, nil
	}
	state := nexus.OperationStateFailed
	if r.GetFailure().GetCanceledFailureInfo() != nil {
		state = nexus.OperationStateCanceled
	}
	nf, err := commonnexus.TemporalFailureToNexusFailure(r.GetFailure())
	if err != nil {
		return nexusrpc.CompleteOperationOptions{}, err
	}
	opErr := &nexus.OperationError{
		State:   state,
		Message: r.GetFailure().GetMessage(),
		Cause:   &nexus.FailureError{Failure: nf},
	}
	if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
		return nexusrpc.CompleteOperationOptions{}, err
	}
	opts.Error = opErr
	return opts, nil
}

func (r *ParkedRead) resolve(ctx chasm.MutableContext, resp *streamservice.ReadRecordsResponse, err error) {
	r.Resolved = true
	r.CreateDeadline = nil
	if err != nil {
		r.Failure = FailureOf(err)
	} else {
		r.Response = resp
	}
	if scheduleErr := callback.ScheduleCompletionCallbacks(ctx, r); scheduleErr != nil {
		ctx.Logger().Error("failed to schedule the callbacks of a Stream read")
	}
}

// FailureOf gives the failure of a Stream error. The application failure type is the reason
// token, so a Workflow sees the same error class as a client.
func FailureOf(err error) *failurepb.Failure {
	if errors.Is(err, errCanceled) {
		return &failurepb.Failure{
			Message:     "The read was canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{}},
		}
	}
	return &failurepb.Failure{
		Message: err.Error(),
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         ReasonOf(err),
			NonRetryable: true,
		}},
	}
}

func errorOfFailure(failure *failurepb.Failure) error {
	reason := failure.GetApplicationFailureInfo().GetType()
	if reason == "" {
		return errCanceled
	}
	return NewError(reason, "%s", trimReason(failure.GetMessage(), reason))
}

func trimReason(message, reason string) string {
	if len(message) > len(reason)+2 && message[:len(reason)+2] == reason+": " {
		return message[len(reason)+2:]
	}
	return message
}

// SystemPayload encodes a response as the System Nexus endpoint answers, as protobuf with the
// system payload flag.
func SystemPayload(resp *streamservice.ReadRecordsResponse) (*commonpb.Payload, error) {
	payloads, err := sdkconverter.PreferProtoDataConverter.ToPayloads(resp)
	if err != nil {
		return nil, err
	}
	payload := payloads.GetPayloads()[0]
	payload.Metadata[commonnexus.SystemPayloadMetadataKey] = []byte("true")
	return payload, nil
}

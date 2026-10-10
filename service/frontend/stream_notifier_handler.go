package frontend

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
)

// The stream notifier RPCs are declared by the API this server pins but not served yet, so they
// answer Unimplemented rather than leaving the service without them.

func (wh *WorkflowHandler) AttachStreamCallback(
	context.Context, *workflowservice.AttachStreamCallbackRequest,
) (*workflowservice.AttachStreamCallbackResponse, error) {
	return nil, serviceerror.NewUnimplemented("AttachStreamCallback is not implemented")
}

func (wh *WorkflowHandler) DetachStreamCallback(
	context.Context, *workflowservice.DetachStreamCallbackRequest,
) (*workflowservice.DetachStreamCallbackResponse, error) {
	return nil, serviceerror.NewUnimplemented("DetachStreamCallback is not implemented")
}

func (wh *WorkflowHandler) NotifyStream(
	context.Context, *workflowservice.NotifyStreamRequest,
) (*workflowservice.NotifyStreamResponse, error) {
	return nil, serviceerror.NewUnimplemented("NotifyStream is not implemented")
}

func (wh *WorkflowHandler) DescribeStreamNotifier(
	context.Context, *workflowservice.DescribeStreamNotifierRequest,
) (*workflowservice.DescribeStreamNotifierResponse, error) {
	return nil, serviceerror.NewUnimplemented("DescribeStreamNotifier is not implemented")
}

package frontend

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
)

// PollStreamNotifier is declared by the API this server pins but not served yet, so it answers
// Unimplemented rather than leaving the service without it.
func (wh *WorkflowHandler) PollStreamNotifier(
	context.Context, *workflowservice.PollStreamNotifierRequest,
) (*workflowservice.PollStreamNotifierResponse, error) {
	return nil, serviceerror.NewUnimplemented("PollStreamNotifier is not implemented")
}

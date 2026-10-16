package frontend

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

// SyncNexusChecks are the checks the Nexus HTTP route runs before a dispatch, so that
// ExecuteSyncNexusOperation admits the same calls.
type SyncNexusChecks struct {
	Authorizer interface {
		Authorize(
			ctx context.Context,
			claims *authorization.Claims,
			target *authorization.CallTarget,
		) (*commonpb.Principal, error)
	}
	StateChecker interface {
		ValidateState(ns *namespace.Namespace, fullMethod string, businessID string) error
	}
	CountLimiter interface {
		Allow(
			ns namespace.Name,
			methodName string,
			handler metrics.Handler,
			req any,
		) (func(), error)
	}
}

package stream

import (
	"context"

	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// grpcHandler serves the frontend's Stream calls on the history shard of the Stream.
type grpcHandler struct {
	streampb.UnimplementedStreamServiceServer

	handler *handler
}

func newGRPCHandler(h *handler) *grpcHandler {
	return &grpcHandler{handler: h}
}

func (g *grpcHandler) CreateStream(ctx context.Context, req *streampb.CreateStreamRequest) (*streampb.CreateStreamResponse, error) {
	resp, err := g.handler.CreateStream(ctx, req.GetFrontendRequest(), Caller{})
	if err != nil {
		return nil, err
	}
	return &streampb.CreateStreamResponse{FrontendResponse: resp}, nil
}

func (g *grpcHandler) AppendRecords(ctx context.Context, req *streampb.AppendRecordsRequest) (*streampb.AppendRecordsResponse, error) {
	resp, err := g.handler.AppendRecords(ctx, req.GetFrontendRequest(), Caller{})
	if err != nil {
		return nil, err
	}
	return &streampb.AppendRecordsResponse{FrontendResponse: resp}, nil
}

func (g *grpcHandler) CloseStream(ctx context.Context, req *streampb.CloseStreamRequest) (*streampb.CloseStreamResponse, error) {
	resp, err := g.handler.CloseStream(ctx, req.GetFrontendRequest(), Caller{})
	if err != nil {
		return nil, err
	}
	return &streampb.CloseStreamResponse{FrontendResponse: resp}, nil
}

func (g *grpcHandler) ReadRecords(ctx context.Context, req *streampb.ReadRecordsRequest) (*streampb.ReadRecordsResponse, error) {
	resp, err := g.handler.ReadRecords(ctx, req.GetFrontendRequest())
	if err != nil {
		return nil, err
	}
	return &streampb.ReadRecordsResponse{FrontendResponse: resp}, nil
}

func (g *grpcHandler) DescribeStream(ctx context.Context, req *streampb.DescribeStreamRequest) (*streampb.DescribeStreamResponse, error) {
	resp, err := g.handler.DescribeStream(ctx, req.GetFrontendRequest())
	if err != nil {
		return nil, err
	}
	return &streampb.DescribeStreamResponse{FrontendResponse: resp}, nil
}

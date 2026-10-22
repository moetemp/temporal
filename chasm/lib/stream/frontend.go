package stream

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/streamservice/v1"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/namespace"
)

// FrontendHandler serves the StreamService on the frontend, for Activities and clients. The
// namespace comes from the request. Each call goes to the history shard of the Stream.
type FrontendHandler struct {
	streamservice.UnimplementedStreamServiceServer

	client            streampb.StreamServiceClient
	config            *Config
	namespaceRegistry namespace.Registry
}

func NewFrontendHandler(
	client streampb.StreamServiceClient,
	config *Config,
	namespaceRegistry namespace.Registry,
) *FrontendHandler {
	return &FrontendHandler{client: client, config: config, namespaceRegistry: namespaceRegistry}
}

// RedirectableMethods lists the StreamService methods that a cell forwards to the namespace's
// active cell, with the response each one answers. The Stream is in the active cell's
// mutable state, so a call served elsewhere would read a replica or write a copy.
func RedirectableMethods() map[string]func() any {
	return map[string]func() any{
		streamservice.StreamService_CreateStream_FullMethodName:   func() any { return &streamservice.CreateStreamResponse{} },
		streamservice.StreamService_AppendRecords_FullMethodName:  func() any { return &streamservice.AppendRecordsResponse{} },
		streamservice.StreamService_CloseStream_FullMethodName:    func() any { return &streamservice.CloseStreamResponse{} },
		streamservice.StreamService_ReadRecords_FullMethodName:    func() any { return &streamservice.ReadRecordsResponse{} },
		streamservice.StreamService_DescribeStream_FullMethodName: func() any { return &streamservice.DescribeStreamResponse{} },
	}
}

func (h *FrontendHandler) namespaceID(namespaceName, streamID string) (string, error) {
	if namespaceName == "" {
		return "", serviceerror.NewInvalidArgument("Namespace is not set on request")
	}
	if !h.config.Enabled(namespaceName) {
		return "", serviceerror.NewUnimplemented("The Stream service is disabled")
	}
	if err := CheckStreamID(streamID); err != nil {
		return "", err
	}
	id, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(namespaceName))
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func (h *FrontendHandler) CreateStream(
	ctx context.Context,
	req *streamservice.CreateStreamRequest,
) (*streamservice.CreateStreamResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespace(), req.GetStreamId())
	if err != nil {
		return nil, err
	}
	resp, err := h.client.CreateStream(ctx, &streampb.CreateStreamRequest{NamespaceId: namespaceID, FrontendRequest: req})
	return resp.GetFrontendResponse(), err
}

func (h *FrontendHandler) AppendRecords(
	ctx context.Context,
	req *streamservice.AppendRecordsRequest,
) (*streamservice.AppendRecordsResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	resp, err := h.client.AppendRecords(ctx, &streampb.AppendRecordsRequest{NamespaceId: namespaceID, FrontendRequest: req})
	return resp.GetFrontendResponse(), err
}

func (h *FrontendHandler) CloseStream(
	ctx context.Context,
	req *streamservice.CloseStreamRequest,
) (*streamservice.CloseStreamResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	resp, err := h.client.CloseStream(ctx, &streampb.CloseStreamRequest{NamespaceId: namespaceID, FrontendRequest: req})
	return resp.GetFrontendResponse(), err
}

func (h *FrontendHandler) ReadRecords(
	ctx context.Context,
	req *streamservice.ReadRecordsRequest,
) (*streamservice.ReadRecordsResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	resp, err := h.client.ReadRecords(ctx, &streampb.ReadRecordsRequest{NamespaceId: namespaceID, FrontendRequest: req})
	return resp.GetFrontendResponse(), err
}

func (h *FrontendHandler) DescribeStream(
	ctx context.Context,
	req *streamservice.DescribeStreamRequest,
) (*streamservice.DescribeStreamResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespace(), req.GetStream().GetId())
	if err != nil {
		return nil, err
	}
	resp, err := h.client.DescribeStream(ctx, &streampb.DescribeStreamRequest{NamespaceId: namespaceID, FrontendRequest: req})
	return resp.GetFrontendResponse(), err
}

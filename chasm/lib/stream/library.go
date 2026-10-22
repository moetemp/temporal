package stream

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/grpc"
)

const (
	libraryName   = "stream"
	componentName = "stream"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type library struct {
	chasm.UnimplementedLibrary

	config  *Config
	handler *handler
	grpc    *grpcHandler
	timer   *timerTaskHandler
	chain   *closeChainTaskHandler
	readers *readersCheckTaskHandler
}

func newLibrary(
	config *Config,
	handler *handler,
	grpcHandler *grpcHandler,
	timer *timerTaskHandler,
	chain *closeChainTaskHandler,
	readers *readersCheckTaskHandler,
) *library {
	return &library{config: config, handler: handler, grpc: grpcHandler, timer: timer, chain: chain, readers: readers}
}

// NewNilLibrary creates a Library with nil handlers, for readers that only decode a Stream.
func NewNilLibrary() chasm.Library {
	return &library{}
}

func (l *library) Name() string {
	return libraryName
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Stream](
			componentName,
			chasm.WithBusinessIDAlias("StreamId"),
		),
		chasm.NewRegistrableComponent[*ParkedRead]("parkedRead"),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask("timer", l.timer),
		chasm.NewRegistrableSideEffectTask("closeChain", l.chain),
		chasm.NewRegistrableSideEffectTask("readersCheck", l.readers),
	}
}

func (l *library) NexusServices() []*nexus.Service {
	if l.handler == nil {
		return nil
	}
	return []*nexus.Service{newNexusService(l.handler)}
}

func (l *library) NexusServiceProcessors() []*chasm.NexusServiceProcessor {
	if l.config == nil {
		return nil
	}
	return []*chasm.NexusServiceProcessor{newNexusServiceProcessor(l.config)}
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&streampb.StreamService_ServiceDesc, l.grpc)
}

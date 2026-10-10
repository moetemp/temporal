package streamnotifier

import (
	"go.temporal.io/server/chasm"
	streamnotifierpb "go.temporal.io/server/chasm/lib/streamnotifier/gen/streamnotifierpb/v1"
	"google.golang.org/grpc"
)

const (
	libraryName   = "streamnotifier"
	componentName = "notifier"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

// componentOnlyLibrary registers the component alone, for a service that only needs to name it.
type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
}

func newComponentOnlyLibrary() *componentOnlyLibrary {
	return &componentOnlyLibrary{}
}

func (l *componentOnlyLibrary) Name() string {
	return libraryName
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*StreamNotifier](componentName),
	}
}

type library struct {
	componentOnlyLibrary

	handler           *handler
	expiryTaskHandler *expiryTaskHandler
}

func newLibrary(handler *handler, expiryTaskHandler *expiryTaskHandler) *library {
	return &library{handler: handler, expiryTaskHandler: expiryTaskHandler}
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&streamnotifierpb.StreamNotifierService_ServiceDesc, l.handler)
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask("expiry", l.expiryTaskHandler),
	}
}

// NewNilLibrary creates a library with nil handlers, for registration-only contexts such as tdbg
// that decode persisted trees without running tasks.
func NewNilLibrary() chasm.Library {
	return &library{}
}

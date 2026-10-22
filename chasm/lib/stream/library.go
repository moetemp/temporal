package stream

import (
	"go.temporal.io/server/chasm"
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

	timer *timerTaskHandler
}

func newLibrary(timer *timerTaskHandler) *library {
	return &library{timer: timer}
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
	}
}

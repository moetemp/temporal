package stream

import (
	"slices"
	"strings"

	"go.temporal.io/api/streamservice/v1"
	"go.temporal.io/server/chasm"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

// The Producers, the runs and the reader floors are data nodes, one per key, so an append writes
// the entries it touches and not the whole table. A Get in a write marks its node written, so a
// transition gets only the entries it needs.

// producer is the state of a Producer, nil when the Stream keeps none.
func (s *Stream) producer(ctx chasm.Context, key string) *streampb.ProducerState {
	if field, ok := s.Producers[key]; ok {
		if state, ok := field.TryGet(ctx); ok {
			return state
		}
	}
	return nil
}

func (s *Stream) setProducer(ctx chasm.MutableContext, key string, state *streampb.ProducerState) {
	if known := s.producer(ctx, key); known != nil {
		known.Epoch, known.Sequence = state.GetEpoch(), state.GetSequence()
		known.FirstOffset, known.LastOffset = state.GetFirstOffset(), state.GetLastOffset()
		known.Holds, known.Digest = state.GetHolds(), state.GetDigest()
		return
	}
	if s.Producers == nil {
		s.Producers = make(chasm.Map[string, *streampb.ProducerState])
	}
	s.Producers[key] = chasm.NewDataField(ctx, state)
}

// run is the task facts of a run, nil when the Stream keeps none.
func (s *Stream) run(ctx chasm.Context, runID string) *streampb.RunTasks {
	if field, ok := s.Runs[runID]; ok {
		if run, ok := field.TryGet(ctx); ok {
			return run
		}
	}
	return nil
}

// runForWrite is the task facts of a run, made when the Stream keeps none.
func (s *Stream) runForWrite(ctx chasm.MutableContext, runID string) *streampb.RunTasks {
	if run := s.run(ctx, runID); run != nil {
		return run
	}
	if s.Runs == nil {
		s.Runs = make(chasm.Map[string, *streampb.RunTasks])
	}
	run := &streampb.RunTasks{}
	s.Runs[runID] = chasm.NewDataField(ctx, run)
	return run
}

// reader is the floor of a reader run, nil when it has none.
func (s *Stream) reader(ctx chasm.Context, runID string) *streampb.ReaderFloor {
	if field, ok := s.Readers[runID]; ok {
		if floor, ok := field.TryGet(ctx); ok {
			return floor
		}
	}
	return nil
}

// setReader keeps a run's floor. The lowest floor is in the root, so a trim reads no floor node.
func (s *Stream) setReader(ctx chasm.MutableContext, run ReaderRun, offset int64) {
	if floor := s.reader(ctx, run.RunID); floor != nil {
		floor.Offset = offset
	} else {
		if s.Readers == nil {
			s.Readers = make(chasm.Map[string, *streampb.ReaderFloor])
		}
		floor := &streampb.ReaderFloor{WorkflowId: run.WorkflowID, Offset: offset}
		s.Readers[run.RunID] = chasm.NewDataField(ctx, floor)
	}
	if s.Floor == 0 || offset < s.Floor {
		s.Floor = offset
	}
}

// dropReaders drops the floors and the waiting reads of runs that closed, and finds the lowest
// floor of the rest. A closed run takes no answer, and its wake would hold a slot for days.
func (s *Stream) dropReaders(ctx chasm.MutableContext, runIDs []string) {
	for _, runID := range runIDs {
		delete(s.Readers, runID)
	}
	if len(runIDs) > 0 {
		s.resolveReads(ctx, func(read *ParkedRead) (readOutcome, bool) {
			return readOutcome{err: errCanceled}, slices.Contains(runIDs, read.GetReaderRunId())
		})
	}
	s.Floor = 0
	for runID := range s.Readers {
		floor := s.reader(ctx, runID)
		if floor != nil && (s.Floor == 0 || floor.GetOffset() < s.Floor) {
			s.Floor = floor.GetOffset()
		}
	}
}

// limitProducers drops Producers past the cap. A Local Activity call writes only while it runs,
// so its entry goes first. Then the Producer with the oldest last batch goes, with its fence.
func (s *Stream) limitProducers(ctx chasm.MutableContext, maxProducers int) {
	localPrefix := streamservice.PRODUCER_KIND_LOCAL_ACTIVITY.String() + "|"
	for maxProducers > 0 && len(s.Producers) > maxProducers {
		victim := s.oldestProducer(ctx, func(key string) bool {
			return strings.HasPrefix(key, localPrefix)
		})
		if victim == "" {
			victim = s.oldestProducer(ctx, func(string) bool { return true })
		}
		delete(s.Producers, victim)
	}
}

func (s *Stream) oldestProducer(ctx chasm.MutableContext, match func(string) bool) string {
	oldestKey := ""
	var oldest int64
	for key := range s.Producers {
		if !match(key) {
			continue
		}
		if state := s.producer(ctx, key); oldestKey == "" || state.GetLastOffset() < oldest {
			oldestKey, oldest = key, state.GetLastOffset()
		}
	}
	return oldestKey
}

// DropActivityProducers drops the entries of an Activity of a run that closed. It never writes
// again, so its fence and dedupe state can go.
func (s *Stream) DropActivityProducers(runID, activityID string) {
	prefix := streamservice.PRODUCER_KIND_ACTIVITY.String() + "|act:" + encodePart(runID) + ":" +
		encodePart(activityID) + ":"
	for key := range s.Producers {
		if strings.HasPrefix(key, prefix) {
			delete(s.Producers, key)
		}
	}
}

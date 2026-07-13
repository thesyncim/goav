package pipeline

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// errSourceFatal is the sentinel a stress source returns to model a fatal node
// failure.
var errSourceFatal = errors.New("stress: fatal source error")

// signalBlockSink closes entered when its first Handle begins, then blocks
// until release closes (or the context cancels), so a test can verify the
// "consumer is holding a message" precondition instead of guessing with time.
type signalBlockSink struct {
	name    string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *signalBlockSink) Name() string { return s.name }

func (s *signalBlockSink) Handle(ctx context.Context, _ *Message) error {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return nil
}

func (s *signalBlockSink) Close() error { return nil }

// waitForStalledEmits waits until the producer's completed-emit counter stops
// advancing across consecutive observations — on a full DropBlock queue that
// means the next Emit has parked in enqueueBlocking. It fails the test if the
// counter never stalls before the deadline.
func waitForStalledEmits(t *testing.T, emits *atomic.Int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := emits.Load()
	stable := 0
	for stable < 10 {
		if time.Now().After(deadline) {
			t.Fatal("producer emit counter never stalled; DropBlock never parked the producer")
		}
		time.Sleep(100 * time.Microsecond)
		now := emits.Load()
		if now == last {
			stable++
		} else {
			stable = 0
			last = now
		}
	}
}

// directErrorSource fails immediately from Start.
type directErrorSource struct {
	name string
	err  error
}

func (s *directErrorSource) Name() string                         { return s.name }
func (s *directErrorSource) Start(context.Context, Emitter) error { return s.err }
func (s *directErrorSource) Close() error                         { return nil }

// loopEventSource emits events in a tight loop until ctx is cancelled or stop is
// closed: an event storm for the publish/close race.
type loopEventSource struct {
	name  string
	event av.Event
	stop  <-chan struct{}
	msg   Message
}

func (s *loopEventSource) Name() string { return s.name }

func (s *loopEventSource) Start(ctx context.Context, emitter Emitter) error {
	for {
		select {
		case <-s.stop:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		s.msg = Message{Kind: MessageEvent, Event: &s.event}
		if err := emitter.Emit(ctx, &s.msg); err != nil {
			return err
		}
	}
}

func (s *loopEventSource) Close() error { return nil }

// blockUntilStage blocks in Handle until released or cancelled, modelling a
// slow stage whose Handle is in flight during a mutation.
type blockUntilStage struct {
	name    string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockUntilStage) Name() string { return s.name }

func (s *blockUntilStage) Handle(ctx context.Context, msg *Message, emitter Emitter) error {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return emitter.Emit(ctx, msg)
}

func (s *blockUntilStage) Close() error { return nil }

// TestStressDirectMultiSourceErrorCancellation proves the direct runner cancels
// sibling sources on the first fatal error instead of waiting for them to end
// on their own.
func TestStressDirectMultiSourceErrorCancellation(t *testing.T) {
	for i := 0; i < 50; i++ {
		failing := &directErrorSource{name: "failing", err: errSourceFatal}
		started := make(chan struct{})
		blocking := &directBlockingStartSource{name: "blocking", started: started, release: make(chan struct{})}

		graph, err := NewGraph(GraphConfig{Name: "stress-multi-source"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := graph.AddSource(failing, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if _, err := graph.AddSource(blocking, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}

		done := make(chan error, 1)
		go func() { done <- graph.Run(context.Background()) }()

		select {
		case runErr := <-done:
			// The blocking sibling must have been cancelled, so Run returns the
			// fatal error promptly rather than hanging on the blocked source.
			if !errors.Is(runErr, errSourceFatal) {
				t.Fatalf("Run err = %v, want errSourceFatal", runErr)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return: sibling source was not cancelled on first fatal error")
		}
		// The blocking source must have actually entered Start and been cancelled.
		select {
		case <-started:
		default:
			// It may not have started before the failure cancelled the context;
			// that is also a valid prompt-cancellation outcome.
		}
		_ = graph.Close()
	}
}

// TestStressBufferedDropBlockCloseWhileProducerBlocked proves Close completes
// promptly when a DropBlock producer is blocked on capacity: the producer must
// not hold node.queueMutex while parked, or the close path would wedge.
func TestStressBufferedDropBlockCloseWhileProducerBlocked(t *testing.T) {
	for i := 0; i < 25; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		packet := &av.Packet{StreamID: "live", Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
		stop := make(chan struct{})
		source := &directLoopSource{name: "src", packet: packet, stop: stop}
		sink := &signalBlockSink{name: "sink", entered: make(chan struct{}), release: make(chan struct{})}

		graph, err := NewGraph(GraphConfig{
			Name:             "stress-dropblock-close",
			Buffer:           BufferPolicy{Capacity: 1, Drop: DropBlock},
			CloseWaitTimeout: 25 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		sourceRef, err := graph.AddSource(source, BufferPolicy{})
		if err != nil {
			t.Fatal(err)
		}
		sinkRef, err := graph.AddSink(sink, BufferPolicy{Capacity: 1, Drop: DropBlock})
		if err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(Route{From: sourceRef.String(), To: []string{sinkRef.String()}, Policy: RouteAll}); err != nil {
			t.Fatal(err)
		}

		runErr := make(chan error, 1)
		go func() { runErr <- graph.Run(ctx) }()

		// Wait for the observable precondition instead of a timed guess: the
		// sink holds the first message, the producer has completed exactly one
		// more Emit (filling the capacity-1 queue), and its emit counter has
		// stalled — the next Emit is parked in enqueueBlocking.
		select {
		case <-sink.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("sink never received the first message")
		}
		waitForStalledEmits(t, &source.emits, 5*time.Second)

		closed := make(chan error, 1)
		go func() { closed <- graph.Close() }()
		select {
		case <-closed:
			// Close returned promptly: the parked producer did not wedge the
			// queue mutex. (It may carry a CloseWaitError for the sink still in
			// Handle; what matters is that it did not hang on the producer.)
		case <-time.After(5 * time.Second):
			t.Fatal("Close hung: a blocked DropBlock producer wedged the close path")
		}

		close(stop)
		cancel()
		select {
		case <-runErr:
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return after Close")
		}
	}
}

// TestStressEventStormDuringClose proves the event channel close is safe against
// a concurrent lock-free publish storm: no send-on-closed panic (run with -race
// for the data-race check).
func TestStressEventStormDuringClose(t *testing.T) {
	for i := 0; i < 25; i++ {
		stop := make(chan struct{})
		source := &loopEventSource{name: "storm", event: av.Event{Type: av.EventType("stress")}, stop: stop}
		sink := &directTestSink{name: "sink"}

		graph, err := NewGraph(GraphConfig{Name: "stress-event-storm", Buffer: BufferPolicy{Capacity: 4, Drop: DropOldest}})
		if err != nil {
			t.Fatal(err)
		}
		sourceRef, err := graph.AddSource(source, BufferPolicy{})
		if err != nil {
			t.Fatal(err)
		}
		sinkRef, err := graph.AddSink(sink, BufferPolicy{Capacity: 4, Drop: DropOldest})
		if err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(Route{From: sourceRef.String(), To: []string{sinkRef.String()}, Policy: RouteAll}); err != nil {
			t.Fatal(err)
		}

		// Drain the event stream so the publish path keeps doing real sends,
		// counting deliveries so the test can see the storm is live.
		var delivered atomic.Int64
		var drain sync.WaitGroup
		drain.Add(1)
		go func() {
			defer drain.Done()
			for range graph.Events() {
				delivered.Add(1)
			}
		}()

		runErr := make(chan error, 1)
		go func() { runErr <- graph.Run(context.Background()) }()

		// Wait until at least one event has flowed through publishEvent, so
		// Close genuinely races an active storm rather than a warming-up one.
		deadline := time.Now().Add(5 * time.Second)
		for delivered.Load() == 0 {
			if time.Now().After(deadline) {
				t.Fatal("event storm never delivered an event")
			}
			runtime.Gosched()
		}
		// Close while the source is still storming events through publishEvent.
		if err := graph.Close(); err != nil && !errors.Is(err, ErrCloseWait) {
			t.Fatalf("Close err = %v", err)
		}
		close(stop)
		<-runErr
		drain.Wait()
	}
}

// TestStressRemoveWhileStageHandleInFlight proves removing a node while its
// Handle is in flight is race-free and does not deadlock (run with -race).
func TestStressRemoveWhileStageHandleInFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	packet := &av.Packet{StreamID: "live", Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	stop := make(chan struct{})
	source := &directLoopSource{name: "src", packet: packet, stop: stop}
	stage := &blockUntilStage{name: "slow", entered: make(chan struct{}), release: make(chan struct{})}
	sink := &directTestSink{name: "sink"}

	graph, err := NewGraph(GraphConfig{
		Name:             "stress-remove-handle",
		Buffer:           BufferPolicy{Capacity: 2, Drop: DropBlock},
		CloseWaitTimeout: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceRef, err := graph.AddSource(source, BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	stageRef, err := graph.AddStage(stage, BufferPolicy{Capacity: 1, Drop: DropBlock})
	if err != nil {
		t.Fatal(err)
	}
	sinkRef, err := graph.AddSink(sink, BufferPolicy{Capacity: 1, Drop: DropBlock})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: sourceRef.String(), To: []string{stageRef.String()}, Policy: RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: stageRef.String(), To: []string{sinkRef.String()}, Policy: RouteAll}); err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()

	select {
	case <-stage.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("stage did not enter Handle")
	}

	// Remove the stage while its Handle is blocked in flight.
	removed := make(chan error, 1)
	go func() { removed <- graph.Remove(stageRef) }()
	select {
	case <-removed:
	case <-time.After(5 * time.Second):
		t.Fatal("Remove hung while stage Handle was in flight")
	}

	close(stage.release)
	close(stop)
	cancel()
	_ = graph.Close()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
		t.Fatalf("Run err = %v", err)
	}
}

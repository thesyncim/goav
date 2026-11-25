package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// TestConcurrentRunRejected pins that a concurrent second Run on an already
// executing graph returns ErrAlreadyRunning on both runners, and a subsequent
// Run after completion succeeds.
func TestConcurrentRunRejected(t *testing.T) {
	policies := map[string]BufferPolicy{
		"direct":   {},
		"buffered": {Capacity: 4},
	}
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			g, err := NewGraph(GraphConfig{Name: "concurrent-run", Buffer: policy})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()

			src := &reusableHoldSource{name: "src"}
			src.arm()
			if _, err := g.AddSource(src, policy); err != nil {
				t.Fatal(err)
			}
			if _, err := g.AddSink(&benchSink{name: "sink"}, policy); err != nil {
				t.Fatal(err)
			}
			if err := g.Connect(route("src", "sink")); err != nil {
				t.Fatal(err)
			}

			runDone := make(chan error, 1)
			go func() { runDone <- g.Run(context.Background()) }()
			<-src.ready

			if err := g.Run(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
				t.Fatalf("concurrent Run = %v, want ErrAlreadyRunning", err)
			}

			close(src.release)
			if err := <-runDone; err != nil {
				t.Fatalf("first Run = %v", err)
			}

			// For buffered graphs, opened must be reset when Run finishes.
			if br, ok := g.(*bufferedRunner); ok {
				br.mu.RLock()
				for _, n := range br.nodes {
					if n.opened {
						br.mu.RUnlock()
						t.Fatalf("node %q still marked opened after Run returned", n.name)
					}
				}
				br.mu.RUnlock()
			}

			// A second sequential Run after the first completes must succeed.
			src.arm()
			runDone2 := make(chan error, 1)
			go func() { runDone2 <- g.Run(context.Background()) }()
			<-src.ready
			close(src.release)
			if err := <-runDone2; err != nil {
				t.Fatalf("second sequential Run = %v", err)
			}
		})
	}
}

type reusableHoldSource struct {
	name    string
	ready   chan struct{}
	release chan struct{}
	emitter Emitter
}

func (s *reusableHoldSource) arm() {
	s.ready = make(chan struct{})
	s.release = make(chan struct{})
}

func (s *reusableHoldSource) Name() string { return s.name }
func (s *reusableHoldSource) Start(_ context.Context, emitter Emitter) error {
	s.emitter = emitter
	close(s.ready)
	<-s.release
	return nil
}
func (s *reusableHoldSource) Close() error { return nil }

type controllableHoldSource struct {
	name     string
	ready    chan struct{}
	emitter  Emitter
	controls atomic.Int32
}

func (s *controllableHoldSource) Name() string { return s.name }
func (s *controllableHoldSource) Start(ctx context.Context, emitter Emitter) error {
	s.emitter = emitter
	close(s.ready)
	<-ctx.Done()
	return ctx.Err()
}
func (s *controllableHoldSource) Control(_ context.Context, _ *Message) error {
	s.controls.Add(1)
	return nil
}
func (s *controllableHoldSource) Close() error { return nil }

// TestBufferedDrainingRejectsInjectAndSourceControl verifies that once Run
// enters runDraining, Inject, InjectSource, and CanControlSource all return
// ErrDynamicGraphUnsupported instead of touching closed queues or exited sources.
func TestBufferedDrainingRejectsInjectAndSourceControl(t *testing.T) {
	g, err := NewGraph(GraphConfig{Name: "draining", Buffer: BufferPolicy{Capacity: 4}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	src := &controllableHoldSource{name: "src", ready: make(chan struct{})}
	sink := &recoveringSink{entered: make(chan struct{}), release: make(chan struct{})}
	if _, err := g.AddSource(src, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddSink(sink, BufferPolicy{Capacity: 4}); err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(route("src", sink.Name())); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(runCtx) }()
	<-src.ready

	pkt := immutablePacket(1)
	if err := src.emitter.Emit(context.Background(), &Message{Kind: MessagePacket, Packet: &pkt}); err != nil {
		t.Fatal(err)
	}
	<-sink.entered

	// Canceling runCtx causes src.Start to exit and pending.wait to return
	// immediately, moving the runner into runDraining while sink.Handle holds
	// the worker open.
	cancel()

	br := g.(*bufferedRunner)
	deadline := time.Now().Add(2 * time.Second)
	for {
		br.mu.RLock()
		phase := br.phase
		br.mu.RUnlock()
		if phase == runDraining {
			break
		}
		if time.Now().After(deadline) {
			close(sink.release)
			t.Fatal("bufferedRunner never entered runDraining")
		}
		time.Sleep(time.Millisecond)
	}

	ctl := &Message{Kind: MessageEvent, Event: &av.Event{Type: av.EventSeek}}
	if err := br.CanControlSource("src"); !errors.Is(err, ErrDynamicGraphUnsupported) {
		t.Fatalf("CanControlSource during draining = %v, want ErrDynamicGraphUnsupported", err)
	}
	if err := br.InjectSource(context.Background(), "src", ctl); !errors.Is(err, ErrDynamicGraphUnsupported) {
		t.Fatalf("InjectSource during draining = %v, want ErrDynamicGraphUnsupported", err)
	}
	if src.controls.Load() != 0 {
		t.Fatalf("Control called %d times during draining, want 0", src.controls.Load())
	}
	if err := br.Inject(context.Background(), NodeRef(sink.Name()), ctl); !errors.Is(err, ErrDynamicGraphUnsupported) {
		t.Fatalf("Inject during draining = %v, want ErrDynamicGraphUnsupported", err)
	}

	close(sink.release)
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
}

// TestReadinessDynamicSinkDoesNotPrematurelyFire verifies that adding (and
// feeding or removing) a sink while the graph is running does not decrement
// the readiness countdown for unfed sinks that were present at Run start.
func TestReadinessDynamicSinkDoesNotPrematurelyFire(t *testing.T) {
	policies := map[string]BufferPolicy{
		"direct":   {},
		"buffered": {Capacity: 8},
	}
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			g, source, runDone := readinessGraph(t, policy, "a", "b")

			// Dynamically add sink "c" while "a" and "b" are both unfed.
			if _, err := g.AddSink(&benchSink{name: "c"}, policy); err != nil {
				t.Fatal(err)
			}
			if err := g.Connect(Route{From: "src", To: []string{"c"}, Policy: RouteByStream, Label: "c"}); err != nil {
				t.Fatal(err)
			}
			// Dynamically add and remove sink "d" before it sees media.
			if _, err := g.AddSink(&benchSink{name: "d"}, policy); err != nil {
				t.Fatal(err)
			}
			if err := g.Remove("d"); err != nil {
				t.Fatal(err)
			}

			// Feed "c" and "a"; "b" is still unfed, so readiness must NOT fire.
			if err := source.emitter.Emit(ctx, readinessPacket("c")); err != nil {
				t.Fatal(err)
			}
			waitSinkDelivered(t, g, "c", 1)
			if err := source.emitter.Emit(ctx, readinessPacket("a")); err != nil {
				t.Fatal(err)
			}
			waitSinkDelivered(t, g, "a", 1)
			if got := drainTaskReady(g.Events()); got != 0 {
				t.Fatalf("readiness fired prematurely with unfed sink b (count %d)", got)
			}

			// Feeding "b" completes readiness.
			if err := source.emitter.Emit(ctx, readinessPacket("b")); err != nil {
				t.Fatal(err)
			}
			waitTaskReady(t, g.Events())

			close(source.release)
			if err := <-runDone; err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestReadinessRearmsOnSecondRun verifies that readiness.arm resets fired so
// a second Run on the same graph publishes EventTaskReady again.
func TestReadinessRearmsOnSecondRun(t *testing.T) {
	policies := map[string]BufferPolicy{
		"direct":   {},
		"buffered": {Capacity: 8},
	}
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			g, err := NewGraph(GraphConfig{Name: "rearm", Buffer: policy})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()

			src := &reusableHoldSource{name: "src"}
			if _, err := g.AddSource(src, policy); err != nil {
				t.Fatal(err)
			}
			if _, err := g.AddSink(&benchSink{name: "a"}, policy); err != nil {
				t.Fatal(err)
			}
			if err := g.Connect(Route{From: "src", To: []string{"a"}, Policy: RouteByStream, Label: "a"}); err != nil {
				t.Fatal(err)
			}

			// First Run: feed "a", observe EventTaskReady.
			src.arm()
			runDone := make(chan error, 1)
			go func() { runDone <- g.Run(context.Background()) }()
			<-src.ready
			if err := src.emitter.Emit(context.Background(), readinessPacket("a")); err != nil {
				t.Fatal(err)
			}
			waitTaskReady(t, g.Events())
			close(src.release)
			if err := <-runDone; err != nil {
				t.Fatal(err)
			}

			// Add a new unfed sink "b" before the second Run.
			if _, err := g.AddSink(&benchSink{name: "b"}, policy); err != nil {
				t.Fatal(err)
			}
			if err := g.Connect(Route{From: "src", To: []string{"b"}, Policy: RouteByStream, Label: "b"}); err != nil {
				t.Fatal(err)
			}

			// Second Run: readiness must wait for "b" and then fire again.
			src.arm()
			runDone2 := make(chan error, 1)
			go func() { runDone2 <- g.Run(context.Background()) }()
			<-src.ready
			if got := drainTaskReady(g.Events()); got != 0 {
				t.Fatalf("second Run fired readiness before new sink b was fed (count %d)", got)
			}
			if err := src.emitter.Emit(context.Background(), readinessPacket("b")); err != nil {
				t.Fatal(err)
			}
			waitTaskReady(t, g.Events())
			close(src.release)
			if err := <-runDone2; err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestDropUntilSyncFlushClearsStaleSyncWait verifies that FlushDownstream
// clears droppingUntilSync so post-flush non-keyframe packets are admitted.
func TestDropUntilSyncFlushClearsStaleSyncWait(t *testing.T) {
	policy := BufferPolicy{Capacity: 1, Drop: DropUntilSync}
	g, err := NewGraph(GraphConfig{Name: "flush-drop-sync", Buffer: policy})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	src := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
	sink := &gateableCountingSink{name: "sink", entered: make(chan struct{}), release: make(chan struct{})}
	if _, err := g.AddSource(src, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddSink(sink, policy); err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(route("src", "sink")); err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(context.Background()) }()
	<-src.ready

	inter := func() *Message {
		return &Message{Kind: MessagePacket, Packet: &av.Packet{
			Type:    av.MediaVideo,
			Payload: av.Buffer{Bytes: []byte{1}},
		}}
	}

	// 1st packet enters sink.Handle and blocks.
	if err := src.emitter.Emit(context.Background(), inter()); err != nil {
		t.Fatal(err)
	}
	<-sink.entered
	// 2nd packet fills the capacity-1 queue.
	if err := src.emitter.Emit(context.Background(), inter()); err != nil {
		t.Fatal(err)
	}
	// 3rd non-keyframe packet overflows and sets droppingUntilSync = true.
	if err := src.emitter.Emit(context.Background(), inter()); err != nil {
		t.Fatal(err)
	}

	br := g.(*bufferedRunner)
	if err := br.FlushDownstream("src"); err != nil {
		t.Fatal(err)
	}
	close(sink.release)

	// Wait for the in-flight packet to finish, then emit a non-keyframe packet.
	// Because flush cleared droppingUntilSync, it must be admitted and delivered.
	if err := src.emitter.Emit(context.Background(), inter()); err != nil {
		t.Fatal(err)
	}
	waitSinkDelivered(t, g, "sink", 2)

	close(src.release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}

type gateableCountingSink struct {
	name    string
	entered chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (s *gateableCountingSink) Name() string { return s.name }
func (s *gateableCountingSink) Handle(_ context.Context, _ *Message) error {
	if s.once.CompareAndSwap(false, true) {
		close(s.entered)
		<-s.release
	}
	return nil
}
func (s *gateableCountingSink) Close() error { return nil }

// TestDropControllerUntilSyncFullSyncKeepsWaiting verifies that a sync message
// arriving on a full queue while droppingUntilSync is true returns
// bufferDropOldest without clearing droppingUntilSync until it is admitted.
func TestDropControllerUntilSyncFullSyncKeepsWaiting(t *testing.T) {
	c := newDropController(BufferPolicy{Capacity: 1, Drop: DropUntilSync})
	inter := Message{Kind: MessagePacket, Packet: &av.Packet{Type: av.MediaVideo}}
	key := Message{Kind: MessagePacket, Packet: &av.Packet{Type: av.MediaVideo, Keyframe: true}}

	if action := c.decide(true, &inter); action != bufferDropIncoming {
		t.Fatalf("inter on full = %v, want bufferDropIncoming", action)
	}
	if !c.droppingUntilSync.Load() {
		t.Fatal("expected droppingUntilSync to be true")
	}

	// Keyframe sees full queue: drops oldest, must NOT clear droppingUntilSync yet.
	if action := c.decide(true, &key); action != bufferDropOldest {
		t.Fatalf("key on full = %v, want bufferDropOldest", action)
	}
	if !c.droppingUntilSync.Load() {
		t.Fatal("droppingUntilSync cleared before keyframe was admitted")
	}

	// Concurrent non-keyframe racing into the freed slot must still be dropped.
	if action := c.decide(false, &inter); action != bufferDropIncoming {
		t.Fatalf("racing inter on non-full = %v, want bufferDropIncoming", action)
	}

	// Now the keyframe retries on the non-full queue, is admitted, and clears state.
	if action := c.decide(false, &key); action != bufferAdmit {
		t.Fatalf("key on non-full = %v, want bufferAdmit", action)
	}
	if c.droppingUntilSync.Load() {
		t.Fatal("droppingUntilSync should be false after keyframe admission")
	}
}

// TestSpecAndStatsDoNotCollideDrainingAndReplacementNode verifies that when a
// buffered node is detached/draining and a new node with the same name is added,
// Spec() lists the name once and Stats().Nodes[name] reflects the active node.
func TestSpecAndStatsDoNotCollideDrainingAndReplacementNode(t *testing.T) {
	policy := BufferPolicy{Capacity: 4}
	g, err := NewGraph(GraphConfig{Name: "draining-name-reuse", Buffer: policy, CloseWaitTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	src := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
	oldSink := &recoveringSink{entered: make(chan struct{}), release: make(chan struct{})}
	if _, err := g.AddSource(src, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddSink(oldSink, policy); err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(route("src", oldSink.Name())); err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(context.Background()) }()
	<-src.ready

	pkt := immutablePacket(1)
	if err := src.emitter.Emit(context.Background(), &Message{Kind: MessagePacket, Packet: &pkt}); err != nil {
		t.Fatal(err)
	}
	<-oldSink.entered

	// Start removing oldSink; it detaches from index and blocks draining in Handle.
	removeDone := make(chan error, 1)
	go func() { removeDone <- g.Remove(NodeRef(oldSink.Name())) }()

	// Wait until oldSink is detached so adding a replacement with the same name succeeds.
	newSink := &benchSink{name: oldSink.Name()}
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := g.AddSink(newSink, policy)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrNodeExists) {
			close(oldSink.release)
			t.Fatalf("AddSink replacement = %v", err)
		}
		if time.Now().After(deadline) {
			close(oldSink.release)
			t.Fatal("oldSink never detached")
		}
		time.Sleep(time.Millisecond)
	}

	spec := g.Spec()
	matches := 0
	for _, n := range spec.Nodes {
		if n.Name == oldSink.Name() {
			matches++
		}
	}
	if matches != 1 {
		close(oldSink.release)
		t.Fatalf("Spec() has %d nodes named %q, want 1", matches, oldSink.Name())
	}

	stats := g.Stats()
	if stats.PendingCleanup != 1 {
		close(oldSink.release)
		t.Fatalf("PendingCleanup = %d, want 1", stats.PendingCleanup)
	}
	if got := stats.Nodes[oldSink.Name()].InMessages; got != 0 {
		close(oldSink.release)
		t.Fatalf("Stats().Nodes[%q].InMessages = %d, want 0 (replacement node collided with draining node)", oldSink.Name(), got)
	}

	close(oldSink.release)
	if err := <-removeDone; err != nil {
		t.Fatalf("Remove = %v", err)
	}
	if got := g.Stats().PendingCleanup; got != 0 {
		t.Fatalf("PendingCleanup after Remove = %d, want 0", got)
	}

	close(src.release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}

// TestRetireFaultDomainSetsBufferedRemoved verifies that retireFaultDomain
// marks buffered members with removed.Store(true) consistently.
func TestRetireFaultDomainSetsBufferedRemoved(t *testing.T) {
	policy := BufferPolicy{Capacity: 4}
	g, err := NewGraph(GraphConfig{Name: "fault-removed", Buffer: policy})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	src := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
	boom := errors.New("sink boom")
	failSink := &faultingSink{name: "bad", err: boom}
	if _, err := g.AddSource(src, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	ref, err := g.AddSink(failSink, policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(route("src", "bad")); err != nil {
		t.Fatal(err)
	}
	iso := g.(FaultIsolator)
	domain, err := iso.IsolateFaults("branch-bad", []NodeRef{ref})
	if err != nil {
		t.Fatal(err)
	}

	br := g.(*bufferedRunner)
	br.mu.RLock()
	badNode := br.nodes[br.index["bad"]]
	br.mu.RUnlock()

	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(context.Background()) }()
	<-src.ready

	pkt := immutablePacket(1)
	if err := src.emitter.Emit(context.Background(), &Message{Kind: MessagePacket, Packet: &pkt}); err != nil {
		t.Fatal(err)
	}
	if err := domain.Wait(context.Background()); err != nil {
		t.Fatalf("domain.Wait = %v", err)
	}
	if !badNode.removed.Load() {
		t.Fatal("retired fault-domain bufferedNode does not have removed set")
	}

	close(src.release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}

type faultingSink struct {
	name string
	err  error
}

func (s *faultingSink) Name() string                           { return s.name }
func (s *faultingSink) Handle(context.Context, *Message) error { return s.err }
func (s *faultingSink) Close() error                           { return nil }

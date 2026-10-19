package pipeline

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// seqSource emits count packets tagged with its producer index and a
// sequence number, in the payload and in the PTS. A plain source publishes
// fresh bytes each time; a pooled one (the default) writes each packet into
// a pooled buffer and releases its own reference as soon as Emit returns, so
// a queued delivery that dropped its reference early would read bytes the
// pool already handed to the next packet, and the sink sees the mismatch. It
// emits through EmitDelivery and counts the targets that shed each message.
type seqSource struct {
	name     string
	producer int
	count    int
	keyEvery int
	plain    bool
	shed     atomic.Int64
}

func (s *seqSource) Name() string { return s.name }
func (s *seqSource) Close() error { return nil }

func (s *seqSource) Start(ctx context.Context, emitter Emitter) error {
	delivery, _ := emitter.(DeliveryEmitter)
	for seq := range s.count {
		payload := av.Buffer{Bytes: make([]byte, 8)}
		if !s.plain {
			payload = av.NewBuffer(8)
		}
		binary.LittleEndian.PutUint32(payload.Bytes[0:], uint32(s.producer))
		binary.LittleEndian.PutUint32(payload.Bytes[4:], uint32(seq))
		packet := av.Packet{
			StreamID: "seq",
			PTS:      av.Timestamp{Value: int64(seq), Base: av.TimeBase{Num: 1, Den: 1000}},
			Keyframe: s.keyEvery > 0 && seq%s.keyEvery == 0,
			Payload:  payload,
		}
		msg := Message{Kind: MessagePacket, Packet: &packet}
		d, err := delivery.EmitDelivery(ctx, &msg)
		packet.Release()
		if err != nil {
			return err
		}
		s.shed.Add(int64(d.Shed))
	}
	return nil
}

// seqSink checks what it is handed: one Handle at a time, every producer's
// messages in the order emitted, each at most once, and the payload still
// the one its PTS names. Delivery is counted per producer.
type seqSink struct {
	name      string
	producers int
	slow      bool
	inside    atomic.Int32
	failures  atomic.Int64
	firstFail atomic.Pointer[string]
	last      []int64
	got       []int64
	rng       *rand.Rand
}

func newSeqSink(name string, producers int, slow bool) *seqSink {
	s := &seqSink{name: name, producers: producers, slow: slow, last: make([]int64, producers), got: make([]int64, producers), rng: rand.New(rand.NewPCG(uint64(len(name)), 7))}
	for i := range s.last {
		s.last[i] = -1
	}
	return s
}

func (s *seqSink) Name() string { return s.name }
func (s *seqSink) Close() error { return nil }

func (s *seqSink) fail(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	s.firstFail.CompareAndSwap(nil, &msg)
	s.failures.Add(1)
}

func (s *seqSink) Handle(_ context.Context, msg *Message) error {
	if n := s.inside.Add(1); n != 1 {
		s.fail("%s: %d Handles at once", s.name, n)
	}
	defer s.inside.Add(-1)
	if msg.Kind != MessagePacket || msg.Packet == nil {
		return nil
	}
	payload := msg.Packet.Payload.Bytes
	if len(payload) != 8 {
		s.fail("%s: payload of %d bytes", s.name, len(payload))
		return nil
	}
	producer := int(binary.LittleEndian.Uint32(payload[0:]))
	seq := int64(binary.LittleEndian.Uint32(payload[4:]))
	if producer >= s.producers || seq != msg.Packet.PTS.Value {
		s.fail("%s: payload (%d,%d) does not match PTS %d", s.name, producer, seq, msg.Packet.PTS.Value)
		return nil
	}
	if seq <= s.last[producer] {
		s.fail("%s: producer %d seq %d after %d", s.name, producer, seq, s.last[producer])
	}
	s.last[producer] = seq
	s.got[producer]++
	if s.slow {
		switch s.rng.IntN(8) {
		case 0:
			time.Sleep(time.Duration(s.rng.IntN(50)) * time.Microsecond)
		case 1, 2:
			runtime.Gosched()
		}
	}
	return nil
}

func (s *seqSink) delivered() int64 {
	var n int64
	for _, v := range s.got {
		n += v
	}
	return n
}

func (s *seqSink) check(t *testing.T) {
	t.Helper()
	if n := s.failures.Load(); n > 0 {
		t.Fatalf("%d violations, first: %s", n, *s.firstFail.Load())
	}
}

// TestStressBufferedExactlyOnceInOrder runs eight producers fanning out to
// five consumers with different drop policies, small queues, and a slow
// consumer, so every policy sheds while producers race each other on the
// same queues. Each consumer sees every producer's messages in order and
// each at most once, never two Handles at once, and every message is
// accounted exactly once: delivered, or counted as a drop on that consumer.
// Pooled payloads, shared by reference across the fanout and released by the
// producer after each Emit, must still read as the message that carried them.
func TestStressBufferedExactlyOnceInOrder(t *testing.T) {
	const producers, count = 8, 3000
	for _, mode := range []struct {
		name  string
		plain bool
	}{{"pooled", false}, {"plain", true}} {
		t.Run(mode.name, func(t *testing.T) {
			graph, err := NewGraph(GraphConfig{Name: "stress-exactly-once", Buffer: BufferPolicy{Capacity: 4, Drop: DropOldest}})
			if err != nil {
				t.Fatal(err)
			}
			policies := []BufferPolicy{
				{Capacity: 4, Drop: DropOldest},
				{Capacity: 2, Drop: DropNewest},
				{Capacity: 3, Drop: DropBlock},
				{Capacity: 4, Drop: DropUntilSync},
				{Capacity: 8, Drop: DropOldest, MaxBytes: 3 * 8},
			}
			sinks := make([]*seqSink, len(policies))
			names := make([]string, len(policies))
			for i, policy := range policies {
				names[i] = "sink-" + strconv.Itoa(i)
				sinks[i] = newSeqSink(names[i], producers, i == 0 || i == 2)
				if _, err := graph.AddSink(sinks[i], policy); err != nil {
					t.Fatal(err)
				}
			}
			sources := make([]*seqSource, producers)
			for p := range sources {
				sources[p] = &seqSource{name: "producer-" + strconv.Itoa(p), producer: p, count: count, keyEvery: 16, plain: mode.plain}
				if _, err := graph.AddSource(sources[p], BufferPolicy{}); err != nil {
					t.Fatal(err)
				}
				if err := graph.Connect(Route{From: sources[p].name, To: names}); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := graph.Run(ctx); err != nil {
				t.Fatal(err)
			}
			stats := graph.Stats()
			if err := graph.Close(); err != nil {
				t.Fatal(err)
			}
			const emitted = producers * count
			for i, sink := range sinks {
				sink.check(t)
				delivered, dropped := sink.delivered(), int64(stats.Nodes[names[i]].Dropped)
				if delivered+dropped != emitted {
					t.Errorf("%s (%s): delivered %d + dropped %d = %d, want every one of %d messages accounted once",
						names[i], policies[i].Drop, delivered, dropped, delivered+dropped, emitted)
				}
				if policies[i].Drop == DropBlock && delivered != emitted {
					t.Errorf("%s (block): delivered %d of %d", names[i], delivered, emitted)
				}
			}
		})
	}
}

// directSeqStage passes messages through one at a time and records whether
// a delivery ever queued behind it (the waiter path ran).
type directSeqStage struct {
	name   string
	node   func() *directNode
	inside atomic.Int32
	failed atomic.Int64
	queued atomic.Bool
	held   atomic.Bool
	spin   int
	work   int
}

func (s *directSeqStage) Name() string { return s.name }
func (s *directSeqStage) Close() error { return nil }

func (s *directSeqStage) Handle(ctx context.Context, msg *Message, emitter Emitter) error {
	if s.inside.Add(1) != 1 {
		s.failed.Add(1)
	}
	if n := s.node(); n != nil {
		if !s.held.Swap(true) {
			// Hold the first message until another producer queues behind
			// it, so the waiter path runs on any core count.
			for deadline := time.Now().Add(10 * time.Second); n.gate.Load()&^nodeClosingBit < 2 && time.Now().Before(deadline); {
				runtime.Gosched()
			}
		}
		if n.gate.Load()&^nodeClosingBit > 1 {
			s.queued.Store(true)
		}
	}
	for i := range s.spin {
		s.work += i // a Handle that takes a moment, so producers collide
	}
	s.inside.Add(-1)
	return emitter.Emit(ctx, msg)
}

// TestStressDirectExactlyOnceInOrder runs eight producers on their own
// goroutines into one shared direct stage and a fanout behind it, plus one
// sink every producer feeds directly and that is paused and resumed at
// random. The shared nodes are serialized by ownership handoff: no two
// Handles overlap, every producer's messages arrive in order, each exactly
// once (the paused sink's sheds are reported to the producers), and a
// producer reusing its payload after Emit returns proves Emit returned only
// after its message was handled.
func TestStressDirectExactlyOnceInOrder(t *testing.T) {
	const producers, count = 8, 4000
	graph, err := NewGraph(GraphConfig{Name: "stress-direct"})
	if err != nil {
		t.Fatal(err)
	}
	runner := graph.(*directRunner)
	stage := &directSeqStage{name: "shared", spin: 200}
	stage.node = func() *directNode {
		snap := runner.snap.Load()
		if snap == nil {
			return nil
		}
		return snap.nodes[runner.index["shared"]].node
	}
	if _, err := graph.AddStage(stage, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	sinkNames := []string{"after-0", "after-1", "after-2"}
	sinks := make([]*seqSink, len(sinkNames))
	for i, name := range sinkNames {
		sinks[i] = newSeqSink(name, producers, i == 0)
		if _, err := graph.AddSink(sinks[i], BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	tap := newSeqSink("tap", producers, true)
	if _, err := graph.AddSink(tap, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "shared", To: sinkNames}); err != nil {
		t.Fatal(err)
	}
	sources := make([]*seqSource, producers)
	for p := range sources {
		sources[p] = &seqSource{name: "producer-" + strconv.Itoa(p), producer: p, count: count}
		if _, err := graph.AddSource(sources[p], BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(Route{From: sources[p].name, To: []string{"shared", "tap"}}); err != nil {
			t.Fatal(err)
		}
	}
	stop := make(chan struct{})
	var toggles sync.WaitGroup
	toggles.Go(func() {
		rng := rand.New(rand.NewPCG(1, 2))
		for paused := false; ; paused = !paused {
			select {
			case <-stop:
				_ = graph.(NodePauser).SetNodePaused("tap", false)
				return
			case <-time.After(time.Duration(rng.IntN(200)) * time.Microsecond):
			}
			_ = graph.(NodePauser).SetNodePaused("tap", paused)
		}
	})
	runErr := graph.Run(context.Background())
	close(stop)
	toggles.Wait()
	if runErr != nil {
		t.Fatal(runErr)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if n := stage.failed.Load(); n > 0 {
		t.Fatalf("shared stage ran %d Handles concurrently", n)
	}
	if !stage.queued.Load() {
		t.Fatal("no delivery ever queued behind the shared stage's owner; the waiter path was not exercised")
	}
	const emitted = producers * count
	for _, sink := range sinks {
		sink.check(t)
		if got := sink.delivered(); got != emitted {
			t.Errorf("%s: delivered %d, want exactly %d", sink.name, got, emitted)
		}
	}
	tap.check(t)
	var shed int64
	for _, source := range sources {
		shed += source.shed.Load()
	}
	if got := tap.delivered(); got+shed != emitted {
		t.Errorf("tap: delivered %d + shed while paused %d = %d, want exactly %d", got, shed, got+shed, emitted)
	}
	if shed == 0 {
		t.Log("tap was never paused during a delivery; drops not exercised this run")
	}
}

// contendedDataPlaneLocks reads the mutex profile and returns, per stack,
// how many contended sync.Mutex or sync.RWMutex unlocks ran on the
// pipeline's message path (emit, deliver, enqueue, the worker loop).
// Runtime-internal locks (channel locks) are not sync mutexes and do not
// count.
func contendedDataPlaneLocks(t *testing.T) map[string]int64 {
	t.Helper()
	var buf bytes.Buffer
	if err := pprof.Lookup("mutex").WriteTo(&buf, 1); err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int64)
	var count int64
	var frames []string
	flush := func() {
		if len(frames) == 0 {
			return
		}
		stack := strings.Join(frames, " < ")
		if isSyncMutexStack(frames) && onMessagePath(frames) {
			counts[stack] += count
		}
		frames = frames[:0]
	}
	scanner := bufio.NewScanner(&buf)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case strings.Contains(line, " @ "):
			flush()
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				count, _ = strconv.ParseInt(fields[1], 10, 64)
			}
		case strings.HasPrefix(line, "#"):
			fields := strings.Fields(line)
			if len(fields) >= 3 {
				frames = append(frames, fields[2])
			}
		default:
			flush()
		}
	}
	flush()
	return counts
}

// isSyncMutexStack reports whether a contended unlock was a sync mutex, not
// counting the one inside sync.Pool's pinSlow: when a slot drops the last
// reference to a pooled payload, av returns the bytes to its sync.Pool, and
// the pool takes its global lock once per P after each GC, not per message.
func isSyncMutexStack(frames []string) bool {
	mutex := false
	for _, f := range frames {
		if strings.Contains(f, "sync.(*Pool).pinSlow") {
			return false
		}
		if strings.Contains(f, "sync.(*Mutex)") || strings.Contains(f, "sync.(*RWMutex)") {
			mutex = true
		}
	}
	return mutex
}

func onMessagePath(frames []string) bool {
	for _, f := range frames {
		if !strings.Contains(f, "goav/pipeline.") {
			continue
		}
		for _, fn := range []string{".Emit", ".EmitDelivery", ".emitDelivery", ".deliver", ".enqueue", ".runNode", ".own", ".queue", ".release", ".handle", ".enqueueBlocking", ".waitSlot", ".waitSpace", ".pumpSlots", ".releaseSlot", ".yield"} {
			if strings.Contains(f, fn+"+") || strings.HasSuffix(f, fn) {
				return true
			}
		}
	}
	return false
}

// TestDataPlaneNoContendedMutex runs both runners in their most contended
// steady state, with the mutex profile recording every contended unlock:
// eight producers into shared direct nodes, and eight producers into shared
// buffered queues with a blocking consumer (parked producers) and dropping
// ones. Not one contended sync.Mutex or RWMutex may come from the message
// path. (TestDataPlaneTakesNoMutex pins that none is taken at all.)
func TestDataPlaneNoContendedMutex(t *testing.T) {
	prev := runtime.SetMutexProfileFraction(1)
	defer runtime.SetMutexProfileFraction(prev)
	before := contendedDataPlaneLocks(t)

	for range 3 {
		TestStressDirectExactlyOnceInOrder(t)
		TestStressBufferedExactlyOnceInOrder(t)
	}

	after := contendedDataPlaneLocks(t)
	for stack, n := range after {
		if n > before[stack] {
			t.Errorf("%d contended lock(s) on the message path: %s", n-before[stack], stack)
		}
	}
}

// contendedSink takes a moment per message and records whether the
// contention under test happened while it held one: a delivery queued
// behind its owner (direct), or a producer parked on its queue (buffered).
type contendedSink struct {
	name    string
	spin    int
	work    int
	inside  atomic.Int32
	overlap atomic.Int64
	saw     atomic.Bool
	probe   func() bool
	// hold, when set, makes the next Handle wait until probe holds, so the
	// contention under test happens on any core count.
	hold atomic.Bool
}

func (s *contendedSink) Name() string { return s.name }
func (s *contendedSink) Close() error { return nil }

func (s *contendedSink) Handle(context.Context, *Message) error {
	if s.inside.Add(1) != 1 {
		s.overlap.Add(1)
	}
	for i := range s.spin {
		s.work += i
	}
	if s.probe != nil && s.hold.Swap(false) {
		for deadline := time.Now().Add(10 * time.Second); !s.probe() && time.Now().Before(deadline); {
			runtime.Gosched()
		}
	}
	if s.probe != nil && s.probe() {
		s.saw.Store(true)
	}
	s.inside.Add(-1)
	return nil
}

// producerPool runs emits from goroutines started once, so a measured round
// spawns nothing: each round hands every producer a token and waits for all
// of them to finish their emits.
type producerPool struct {
	start []chan struct{}
	done  chan error
}

func newProducerPool(emitters []Emitter, perRound int) *producerPool {
	p := &producerPool{start: make([]chan struct{}, len(emitters)), done: make(chan error, len(emitters))}
	for i, emitter := range emitters {
		p.start[i] = make(chan struct{})
		packet := av.Packet{StreamID: "contended", Payload: av.Buffer{Bytes: make([]byte, 64)}}
		msg := Message{Kind: MessagePacket, Packet: &packet}
		go func() {
			ctx := context.Background()
			for range p.start[i] {
				var err error
				for range perRound {
					if err = emitter.Emit(ctx, &msg); err != nil {
						break
					}
				}
				p.done <- err
			}
		}()
	}
	return p
}

func (p *producerPool) round(t *testing.T) {
	t.Helper()
	for _, start := range p.start {
		start <- struct{}{}
	}
	for range p.start {
		if err := <-p.done; err != nil {
			t.Fatal(err)
		}
	}
}

func (p *producerPool) stop() {
	for _, start := range p.start {
		close(start)
	}
}

// mallocsOver counts the heap allocations of rounds after one warm-up round;
// measure runs between the two, just before the count starts.
func mallocsOver(t *testing.T, pool *producerPool, rounds int, measure func()) uint64 {
	t.Helper()
	pool.round(t)
	measure()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range rounds {
		pool.round(t)
	}
	runtime.ReadMemStats(&after)
	return after.Mallocs - before.Mallocs
}

// TestGraphDirectContendedEmitAllocs pins the direct runner's contended path
// at zero allocations: four producers hammer one sink, so deliveries keep
// queuing behind its owner and being served from the queue, and each
// producer's waiter record is the one its emitter made at attach.
func TestGraphDirectContendedEmitAllocs(t *testing.T) {
	const producers, perRound, rounds = 4, 2000, 4
	graph, err := NewGraph(GraphConfig{Name: "contended-direct"})
	if err != nil {
		t.Fatal(err)
	}
	runner := graph.(*directRunner)
	sink := &contendedSink{name: "sink", spin: 300}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	sources := make([]*benchCaptureSource, producers)
	for i := range sources {
		sources[i] = &benchCaptureSource{name: "producer-" + strconv.Itoa(i)}
		if _, err := graph.AddSource(sources[i], BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(route(sources[i].name, "sink")); err != nil {
			t.Fatal(err)
		}
	}
	if err := graph.Run(context.Background()); err != nil { // captures the emitters
		t.Fatal(err)
	}
	defer graph.Close()
	node := runner.nodes[runner.index["sink"]]
	sink.probe = func() bool { return node.gate.Load() > 1 }
	emitters := make([]Emitter, producers)
	for i := range sources {
		emitters[i] = sources[i].emitter
	}
	pool := newProducerPool(emitters, perRound)
	defer pool.stop()
	mallocs := mallocsOver(t, pool, rounds, func() { sink.saw.Store(false); sink.hold.Store(true) })
	t.Logf("%d allocations over %d messages", mallocs, producers*perRound*rounds)
	if n := sink.overlap.Load(); n > 0 {
		t.Fatalf("sink ran %d Handles concurrently", n)
	}
	if !sink.saw.Load() {
		t.Fatal("no delivery queued behind the sink's owner; the waiter path was not exercised")
	}
	if mallocs > 16 {
		t.Fatalf("contended direct emits allocated %d times over %d messages, want none", mallocs, producers*perRound*rounds)
	}
}

// TestGraphBufferedParkedProducersAllocs pins DropBlock parking at zero
// allocations with several producers: four producers outpace a slow sink on
// a one-slot blocking queue, so each emit parks in the node's parking lot on
// the producer's own wake channel and is woken by the worker.
func TestGraphBufferedParkedProducersAllocs(t *testing.T) {
	const producers, perRound, rounds = 4, 300, 4
	graph, err := NewGraph(GraphConfig{Name: "parked", Buffer: BufferPolicy{Capacity: 1, Drop: DropBlock}})
	if err != nil {
		t.Fatal(err)
	}
	runner := graph.(*bufferedRunner)
	sink := &contendedSink{name: "sink", spin: 2000}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	sources := make([]*holdSource, producers)
	for i := range sources {
		sources[i] = &holdSource{name: "producer-" + strconv.Itoa(i), ready: make(chan struct{}), release: make(chan struct{})}
		if _, err := graph.AddSource(sources[i], BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(route(sources[i].name, "sink")); err != nil {
			t.Fatal(err)
		}
	}
	runDone := make(chan error, 1)
	go func() { runDone <- graph.Run(context.Background()) }()
	emitters := make([]Emitter, producers)
	for i, source := range sources {
		<-source.ready
		emitters[i] = source.emitter
	}
	node := runner.nodes[runner.index["sink"]]
	sink.probe = func() bool { return node.waiting.Load() != 0 }
	pool := newProducerPool(emitters, perRound)
	mallocs := mallocsOver(t, pool, rounds, func() { sink.saw.Store(false); sink.hold.Store(true) })
	t.Logf("%d allocations over %d messages", mallocs, producers*perRound*rounds)
	pool.stop()
	for _, source := range sources {
		close(source.release)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if !sink.saw.Load() {
		t.Fatal("no producer ever parked on the full queue; the parking path was not exercised")
	}
	if mallocs > 16 {
		t.Fatalf("parked producers allocated %d times over %d messages, want none", mallocs, producers*perRound*rounds)
	}
}

// atomicCountSink counts deliveries without a lock.
type atomicCountSink struct {
	name string
	n    atomic.Int64
}

func (s *atomicCountSink) Name() string                           { return s.name }
func (s *atomicCountSink) Close() error                           { return nil }
func (s *atomicCountSink) Handle(context.Context, *Message) error { s.n.Add(1); return nil }

// TestBufferedBlockingDiamondCompletes pins that blocking queues in a
// diamond, S -> {A, B} and B -> A, never wait on themselves: B's worker
// emitting into a full A waits for A's slots, and A's worker frees them as
// it drains, because a slot holds references, not a slot of anyone else's.
// Every message reaches A twice and the run ends cleanly.
func TestBufferedBlockingDiamondCompletes(t *testing.T) {
	const count = 2000
	for attempt := range 20 {
		policy := BufferPolicy{Capacity: 2, Drop: DropBlock}
		graph, err := NewGraph(GraphConfig{Name: "diamond", Buffer: policy})
		if err != nil {
			t.Fatal(err)
		}
		a := &atomicCountSink{name: "a"}
		if _, err := graph.AddSink(a, policy); err != nil {
			t.Fatal(err)
		}
		if _, err := graph.AddStage(&passStage{name: "b"}, policy); err != nil {
			t.Fatal(err)
		}
		source := &seqSource{name: "s", count: count}
		if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(Route{From: "s", To: []string{"a", "b"}}); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(route("b", "a")); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- graph.Run(context.Background()) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("attempt %d: Run err = %v", attempt, err)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("attempt %d: blocking diamond hung", attempt)
		}
		if got := a.n.Load(); got != 2*count {
			t.Fatalf("attempt %d: A received %d, want %d", attempt, got, 2*count)
		}
		_ = graph.Close()
	}
}

// TestParkLinesServeInJoinOrder pins the order a buffered node serves its
// waiting producers in, deterministically: released slots are handed to the
// slot line in exactly the order its producers joined, skipping one that
// gave up, and the space line wakes its oldest first. A producer that joins
// behind k others is served after exactly k grants.
func TestParkLinesServeInJoinOrder(t *testing.T) {
	node := &bufferedNode{}
	node.free.reset(8)
	slots := make([]bufferedMessage, 4)
	in := make([]*parkedProducer, 5)
	for i := range in {
		in[i] = newParkedProducer()
		node.waiting.Add(slotWaiter)
		node.slotLine.join(in[i])
	}
	if !node.slotLine.cancel(in[2]) {
		t.Fatal("cancel of a producer still in line reported a handoff")
	}
	node.waiting.Add(-slotWaiter)
	served := []int{}
	for i := range slots {
		node.free.pushWait(&slots[i])
		node.pumpSlots()
		for j, p := range in {
			if len(p.wake) == 1 {
				<-p.wake
				if p.handed() != &slots[i] {
					t.Fatalf("producer %d woke without slot %d", j, i)
				}
				node.waiting.Add(-slotWaiter)
				served = append(served, j)
			}
		}
	}
	if want := []int{0, 1, 3, 4}; !slices.Equal(served, want) {
		t.Fatalf("slots went to producers %v, want %v (join order, the cancelled one skipped)", served, want)
	}

	node.queue.reset(1)
	space := make([]*parkedProducer, 3)
	for i := range space {
		space[i] = newParkedProducer()
		node.spaceLine.join(space[i])
	}
	for i := range space {
		node.spaceLine.wakeOne()
		for j, p := range space {
			if got := len(p.wake); (j == i) != (got == 1) {
				t.Fatalf("wakeup %d: producer %d holds %d tokens, want the oldest woken first", i, j, got)
			}
		}
		<-space[i].wake
	}
}

// gatedSleepSink holds every Handle until gate closes, then sleeps a moment
// per message. Sleeping (not spinning) parks the worker on a timer: a
// worker and the producer it just woke cannot then keep handing the P to
// each other through the scheduler's runnext slot and starve the producers
// queued behind it, so the pace below measures the pipeline's order, not
// the scheduler's.
type gatedSleepSink struct {
	name  string
	gate  chan struct{}
	delay time.Duration
}

func (s *gatedSleepSink) Name() string { return s.name }
func (s *gatedSleepSink) Close() error { return nil }

func (s *gatedSleepSink) Handle(context.Context, *Message) error {
	<-s.gate
	time.Sleep(s.delay)
	return nil
}

// TestParkedProducersAllProgress pins that producers parked on one node are
// served in turn, on any number of CPUs. 24 DropBlock producers feed a slow
// sink through a one-slot queue; the sink holds its first message until all
// 24 are waiting in the node's lines, so the measured phase starts with
// every producer in line. From there a slot goes to whoever waited longest,
// and a producer that got through queues behind the rest for its next one
// (TestParkLinesServeInJoinOrder pins that order), so they take turns: by
// the time the first producer has done 30 more emits, every one has done at
// least three quarters as many. A producer that ran ahead of waiting ones,
// or one left waiting while others came round, fails it. Every producer also
// completes.
func TestParkedProducersAllProgress(t *testing.T) {
	const producers, perProducer, measured = 24, 60, 30
	graph, err := NewGraph(GraphConfig{Name: "fair-park", Buffer: BufferPolicy{Capacity: 1, Drop: DropBlock}})
	if err != nil {
		t.Fatal(err)
	}
	sink := &gatedSleepSink{name: "sink", gate: make(chan struct{}), delay: 5 * time.Microsecond}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	sources := make([]*holdSource, producers)
	for i := range sources {
		sources[i] = &holdSource{name: "producer-" + strconv.Itoa(i), ready: make(chan struct{}), release: make(chan struct{})}
		if _, err := graph.AddSource(sources[i], BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(route(sources[i].name, "sink")); err != nil {
			t.Fatal(err)
		}
	}
	runDone := make(chan error, 1)
	go func() { runDone <- graph.Run(context.Background()) }()
	for _, source := range sources {
		<-source.ready
	}
	node := graph.(*bufferedRunner).nodes[0]

	progress := make([]atomic.Int64, producers)
	var base []int64
	first := make(chan []int64, 1)
	measuring := make(chan struct{})
	var wg sync.WaitGroup
	for i, source := range sources {
		wg.Go(func() {
			packet := av.Packet{StreamID: "fair", Payload: av.Buffer{Bytes: make([]byte, 64)}}
			msg := Message{Kind: MessagePacket, Packet: &packet}
			reported := false
			for range perProducer {
				if err := source.emitter.Emit(context.Background(), &msg); err != nil {
					t.Error(err)
					return
				}
				n := progress[i].Add(1)
				select {
				case <-measuring:
					if !reported && n-base[i] >= measured {
						reported = true
						snapshot := make([]int64, producers)
						for j := range progress {
							snapshot[j] = progress[j].Load() - base[j]
						}
						select {
						case first <- snapshot:
						default:
						}
					}
				default:
				}
			}
		})
	}
	// The sink holds its first message: wait until every producer waits in
	// a line, then start measuring and let the sink go.
	for deadline := time.Now().Add(10 * time.Second); slotWaiters(node.waiting.Load())+spaceWaiters(node.waiting.Load()) < producers; {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d producers ever waited", slotWaiters(node.waiting.Load())+spaceWaiters(node.waiting.Load()), producers)
		}
		runtime.Gosched()
	}
	base = make([]int64, producers)
	for i := range progress {
		base[i] = progress[i].Load()
	}
	close(measuring)
	close(sink.gate)
	wg.Wait()
	for _, source := range sources {
		close(source.release)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	for i := range progress {
		if got := progress[i].Load(); got != perProducer {
			t.Fatalf("producer %d completed %d of %d emits", i, got, perProducer)
		}
	}
	snapshot := <-first
	slowest := slices.Min(snapshot)
	t.Logf("slowest producer at %d when the first had done %d more emits", slowest, measured)
	if slowest < measured*3/4 {
		t.Fatalf("when the first producer had done %d more emits, the slowest had %d: parked producers are not served in turn (%v)", measured, slowest, snapshot)
	}
}

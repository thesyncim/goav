package pipeline

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// countingOwner is an av.BufferOwner that counts references, so a test can
// prove every reference a runner took was given back.
type countingOwner struct {
	refs     atomic.Int32
	retained atomic.Int64
}

// countedBuffer wraps bytes in a counting owner holding the caller's one
// reference.
func countedBuffer(bytes []byte) (av.Buffer, *countingOwner) {
	owner := &countingOwner{}
	owner.refs.Store(1)
	return av.Buffer{Bytes: bytes, Owner: owner}, owner
}

func (o *countingOwner) Retain() {
	o.retained.Add(1)
	if o.refs.Add(1) <= 1 {
		panic("countingOwner: Retain after the last Release")
	}
}

func (o *countingOwner) Release() {
	if o.refs.Add(-1) < 0 {
		panic("countingOwner: released more often than retained")
	}
}

func (o *countingOwner) Refs() int { return int(o.refs.Load()) }

// payloadModes are the two runners every payload test runs on.
var payloadModes = []struct {
	name   string
	buffer BufferPolicy
}{
	{name: "direct"},
	{name: "buffered", buffer: BufferPolicy{Capacity: 4, Drop: DropBlock}},
}

// recordSink notes where each delivered payload's bytes live and what they
// held at delivery.
type recordSink struct {
	name   string
	mu     sync.Mutex
	ptrs   []*byte
	values []byte
}

func (s *recordSink) Name() string { return s.name }
func (s *recordSink) Close() error { return nil }
func (s *recordSink) Handle(_ context.Context, msg *Message) error {
	var bytes []byte
	switch {
	case msg.Packet != nil:
		bytes = msg.Packet.Payload.Bytes
	case msg.Frame != nil && len(msg.Frame.Planes) != 0:
		bytes = msg.Frame.Planes[0].Buffer.Bytes
	}
	if len(bytes) == 0 {
		return nil
	}
	s.mu.Lock()
	s.ptrs = append(s.ptrs, &bytes[0])
	s.values = append(s.values, bytes[0])
	s.mu.Unlock()
	return nil
}

// runOneMessage runs a graph whose source emits msg once to sinks.
func runOneMessage(t *testing.T, buffer BufferPolicy, msg *Message, sinks ...Sink) {
	t.Helper()
	graph, err := NewGraph(GraphConfig{Name: "payload", Buffer: buffer})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(&directTestSource{name: "source", msg: msg}, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(sinks))
	for i, sink := range sinks {
		names[i] = sink.Name()
		if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := graph.Connect(route("source", names...)); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestFanoutSharesOnePayload is the "no copy per edge" proof: every branch of
// a fanout reads the very bytes the producer published, in both runners, and
// every reference a queue took is given back once the run ends.
func TestFanoutSharesOnePayload(t *testing.T) {
	for _, mode := range payloadModes {
		t.Run(mode.name+"/packet", func(t *testing.T) {
			payload, owner := countedBuffer([]byte{7, 7, 7})
			msg := &Message{Kind: MessagePacket, Packet: &av.Packet{StreamID: "v", Payload: payload}}
			sinks := []*recordSink{{name: "a"}, {name: "b"}, {name: "c"}}
			runOneMessage(t, mode.buffer, msg, sinks[0], sinks[1], sinks[2])
			for _, sink := range sinks {
				if len(sink.ptrs) != 1 || sink.ptrs[0] != &payload.Bytes[0] {
					t.Fatalf("sink %s read %v, want the producer's bytes at %p", sink.name, sink.ptrs, &payload.Bytes[0])
				}
			}
			assertProducerRefOnly(t, owner, mode.buffer, len(sinks))
		})
		t.Run(mode.name+"/frame", func(t *testing.T) {
			var planes []av.Plane
			var owners []*countingOwner
			for _, size := range []int{16, 4, 4} {
				buffer, owner := countedBuffer(make([]byte, size))
				planes = append(planes, av.Plane{Buffer: buffer, Stride: 4})
				owners = append(owners, owner)
			}
			frame := &av.Frame{StreamID: "v", Type: av.MediaVideo, Video: &av.VideoFrame{Width: 4, Height: 4, PixelFormat: av.PixelFormatI420}, Planes: planes}
			sinks := []*recordSink{{name: "a"}, {name: "b"}}
			runOneMessage(t, mode.buffer, &Message{Kind: MessageFrame, Frame: frame}, sinks[0], sinks[1])
			for _, sink := range sinks {
				if len(sink.ptrs) != 1 || sink.ptrs[0] != &planes[0].Buffer.Bytes[0] {
					t.Fatalf("sink %s read %v, want the producer's luma at %p", sink.name, sink.ptrs, &planes[0].Buffer.Bytes[0])
				}
			}
			for _, owner := range owners {
				assertProducerRefOnly(t, owner, mode.buffer, len(sinks))
			}
		})
	}
}

// assertProducerRefOnly checks that only the producer's own reference is left
// and that a buffered graph took one per target.
func assertProducerRefOnly(t *testing.T, owner *countingOwner, buffer BufferPolicy, targets int) {
	t.Helper()
	if refs := owner.Refs(); refs != 1 {
		t.Fatalf("payload holds %d references after the run, want the producer's 1", refs)
	}
	want := int64(0)
	if !buffer.IsDirect() {
		want = int64(targets)
	}
	if got := owner.retained.Load(); got != want {
		t.Fatalf("runner took %d references, want %d", got, want)
	}
}

// TestBufferedFrameHeadersAreSlotCopies pins what a queued slot copies: the
// frame header (planes, Video and Audio) is the slot's own, so a producer
// reusing its header cannot reach a queued message; the plane bytes are the
// producer's, shared, never copied.
func TestBufferedFrameHeadersAreSlotCopies(t *testing.T) {
	audio := &av.AudioFrame{SampleRate: 48000, Channels: 2, SampleFormat: "s16", Samples: 2}
	video := &av.VideoFrame{Width: 2, Height: 2, PixelFormat: "i420"}
	planeBytes := []byte{1, 2, 3, 4}
	frame := &av.Frame{
		Type:   av.MediaAudio,
		Audio:  audio,
		Video:  video,
		Planes: []av.Plane{{Buffer: av.Buffer{Bytes: planeBytes}, Stride: 4}},
	}
	slot := bufferedMessage{planes: make([]av.Plane, 0, slotPlanes)}
	if err := slot.bind(&Message{Kind: MessageFrame, Frame: frame}, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	audio.SampleRate = 44100
	video.Width = 9
	frame.Planes[0].Stride = 99

	got := slot.message.Frame
	if got.Audio == audio || got.Video == video || got.Audio.SampleRate != 48000 || got.Video.Width != 2 {
		t.Fatalf("slot header = audio %+v video %+v, want copies of the producer's", got.Audio, got.Video)
	}
	if got.Planes[0].Stride != 4 {
		t.Fatalf("plane stride = %d, want the slot's copy 4", got.Planes[0].Stride)
	}
	if &got.Planes[0].Buffer.Bytes[0] != &planeBytes[0] {
		t.Fatal("slot copied the plane bytes; it must share them")
	}
	slot.release()
}

// TestBufferedFrameWithManyPlanes pins that a frame with more planes than a
// slot reserves still queues: the slot grows instead of refusing it.
func TestBufferedFrameWithManyPlanes(t *testing.T) {
	planes := make([]av.Plane, 10)
	for i := range planes {
		planes[i] = av.Plane{Buffer: av.Buffer{Bytes: []byte{byte(i + 1)}}, Stride: 1}
	}
	sink := &recordSink{name: "sink"}
	frame := &av.Frame{StreamID: "v", Type: av.MediaVideo, Planes: planes}
	runOneMessage(t, BufferPolicy{Capacity: 2, Drop: DropBlock}, &Message{Kind: MessageFrame, Frame: frame}, sink)
	if len(sink.values) != 1 || sink.values[0] != 1 {
		t.Fatalf("sink saw %v, want the 10-plane frame", sink.values)
	}
}

// mutatingSink writes its delivery the legal way: it takes its own reference
// (Ref) and makes it writable, which copies because others hold the bytes.
type mutatingSink struct {
	name    string
	mutated chan struct{}
}

func (s *mutatingSink) Name() string { return s.name }
func (s *mutatingSink) Close() error { return nil }
func (s *mutatingSink) Handle(_ context.Context, msg *Message) error {
	defer close(s.mutated)
	switch {
	case msg.Frame != nil:
		own := msg.Frame.Ref()
		own.MakeWritable()
		for i := range own.Planes {
			for j := range own.Planes[i].Buffer.Bytes {
				own.Planes[i].Buffer.Bytes[j] = 0xEE
			}
		}
		own.Release()
	case msg.Packet != nil:
		own := msg.Packet.Ref()
		own.Payload.MakeWritable()
		for j := range own.Payload.Bytes {
			own.Payload.Bytes[j] = 0xEE
		}
		own.Release()
	}
	return nil
}

// observingSink reads its delivery after the mutator is done with its own.
type observingSink struct {
	name  string
	wait  <-chan struct{}
	bytes []byte
}

func (s *observingSink) Name() string { return s.name }
func (s *observingSink) Close() error { return nil }
func (s *observingSink) Handle(ctx context.Context, msg *Message) error {
	select {
	case <-s.wait:
	case <-time.After(5 * time.Second):
		return errors.New("mutator never ran")
	case <-ctx.Done():
		return ctx.Err()
	}
	switch {
	case msg.Frame != nil:
		s.bytes = append([]byte(nil), msg.Frame.Planes[0].Buffer.Bytes...)
	case msg.Packet != nil:
		s.bytes = append([]byte(nil), msg.Packet.Payload.Bytes...)
	}
	return nil
}

// TestGraphBufferedFanoutMutatingSinkCannotCorruptSibling is the runner-level
// proof of north-star acceptance #22 under shared payloads: a frame or packet
// fans out to two sinks by reference, and the one that writes its delivery
// through MakeWritable gets a private copy, so it can corrupt neither its
// sibling's view nor the producer's bytes. The direct runner, where both
// sinks run inside the producer's Emit, keeps the same guarantee.
func TestGraphBufferedFanoutMutatingSinkCannotCorruptSibling(t *testing.T) {
	original := []byte{1, 2, 3, 4}
	for _, mode := range payloadModes {
		for _, kind := range []string{"frame", "packet"} {
			for _, pooled := range []bool{false, true} {
				name := mode.name + "/" + kind + "/ordinary"
				if pooled {
					name = mode.name + "/" + kind + "/pooled"
				}
				t.Run(name, func(t *testing.T) {
					payload := av.Buffer{Bytes: append([]byte(nil), original...)}
					if pooled {
						payload = av.NewBuffer(len(original))
						copy(payload.Bytes, original)
					}
					msg := &Message{Kind: MessagePacket, Packet: &av.Packet{StreamID: "v", Payload: payload}}
					if kind == "frame" {
						msg = &Message{Kind: MessageFrame, Frame: &av.Frame{StreamID: "v", Type: av.MediaVideo,
							Planes: []av.Plane{{Buffer: payload, Stride: 4}}}}
					}
					mutator := &mutatingSink{name: "mutator", mutated: make(chan struct{})}
					observer := &observingSink{name: "observer", wait: mutator.mutated}
					if mode.buffer.IsDirect() {
						// Inline delivery runs the mutator first, then the
						// observer, on the producer's goroutine.
						observer.wait = closedChan()
					}
					runOneMessage(t, mode.buffer, msg, mutator, observer)
					if !equalBytes(observer.bytes, original) {
						t.Fatalf("sibling saw %v, want original %v after the mutator wrote 0xEE", observer.bytes, original)
					}
					if !equalBytes(payload.Bytes, original) {
						t.Fatalf("producer bytes = %v, want untouched %v", payload.Bytes, original)
					}
					payload.Release()
				})
			}
		}
	}
}

func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// pooledFrameSource emits n new pooled frames, frame i filled with byte(i),
// and releases its own reference after each Emit, as a decoder stage does.
type pooledFrameSource struct {
	name string
	n    int
	size int
}

func (s *pooledFrameSource) Name() string { return s.name }
func (s *pooledFrameSource) Close() error { return nil }
func (s *pooledFrameSource) Start(ctx context.Context, emitter Emitter) error {
	for i := range s.n {
		frame := av.Frame{StreamID: "v", Type: av.MediaVideo, Planes: []av.Plane{{Buffer: av.NewBuffer(s.size), Stride: s.size}}}
		for j := range frame.Planes[0].Buffer.Bytes {
			frame.Planes[0].Buffer.Bytes[j] = byte(i)
		}
		err := emitter.Emit(ctx, &Message{Kind: MessageFrame, Frame: &frame})
		frame.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

// keepingSink keeps every frame it receives (Ref), and a reader goroutine
// checks the kept frames while deliveries continue.
type keepingSink struct {
	name string
	mu   sync.Mutex
	kept []*av.Frame
}

func (s *keepingSink) Name() string { return s.name }
func (s *keepingSink) Close() error { return nil }
func (s *keepingSink) Handle(_ context.Context, msg *Message) error {
	if msg.Frame == nil {
		return nil
	}
	ref := msg.Frame.Ref()
	s.mu.Lock()
	s.kept = append(s.kept, ref)
	s.mu.Unlock()
	return nil
}

// corrupt reports the first kept frame whose bytes changed since delivery.
func (s *keepingSink) corrupt() (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, frame := range s.kept {
		for _, b := range frame.Planes[0].Buffer.Bytes {
			if b != byte(i) {
				return i, true
			}
		}
	}
	return 0, false
}

// TestRetainedFramesStayStableAcrossPushes is the C4 proof: a sink that keeps
// the frames it is handed sees each one exactly as delivered, while the
// producer keeps emitting new pooled frames and releasing its own references.
// The pool never hands a kept frame's memory to a later frame. A concurrent
// reader checks the kept frames during the run (meaningful under -race), and
// a sibling sink that keeps nothing shares the same deliveries.
func TestRetainedFramesStayStableAcrossPushes(t *testing.T) {
	const frames = 300
	for _, mode := range payloadModes {
		t.Run(mode.name, func(t *testing.T) {
			graph, err := NewGraph(GraphConfig{Name: "keep", Buffer: mode.buffer})
			if err != nil {
				t.Fatal(err)
			}
			keeper := &keepingSink{name: "keeper"}
			for _, add := range []func() error{
				func() error {
					_, err := graph.AddSource(&pooledFrameSource{name: "source", n: frames, size: 4096}, BufferPolicy{})
					return err
				},
				func() error { _, err := graph.AddSink(keeper, BufferPolicy{}); return err },
				func() error { _, err := graph.AddSink(&recordSink{name: "passer"}, BufferPolicy{}); return err },
				func() error { return graph.Connect(route("source", "keeper", "passer")) },
			} {
				if err := add(); err != nil {
					t.Fatal(err)
				}
			}
			stop := make(chan struct{})
			readerDone := make(chan error, 1)
			go func() {
				for {
					if i, bad := keeper.corrupt(); bad {
						readerDone <- errors.New("kept frame " + strconv.Itoa(i) + " changed during the run")
						return
					}
					select {
					case <-stop:
						readerDone <- nil
						return
					default:
					}
				}
			}()
			runErr := graph.Run(context.Background())
			close(stop)
			if err := <-readerDone; err != nil {
				t.Fatal(err)
			}
			if runErr != nil {
				t.Fatal(runErr)
			}
			if len(keeper.kept) != frames {
				t.Fatalf("kept %d frames, want %d", len(keeper.kept), frames)
			}
			if i, bad := keeper.corrupt(); bad {
				t.Fatalf("kept frame %d no longer holds byte(%d)", i, i)
			}
			for _, frame := range keeper.kept {
				frame.Release()
			}
			if err := graph.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// blockedSink blocks its first delivery until release closes, so a test can
// pile messages up in its queue.
type blockedSink struct {
	name    string
	started chan struct{}
	release chan struct{}
	once    sync.Once
	fail    error
}

func (s *blockedSink) Name() string { return s.name }
func (s *blockedSink) Close() error { return nil }
func (s *blockedSink) Handle(ctx context.Context, _ *Message) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return s.fail
}

// TestBufferedQueueReleasesEveryReference pins that a queued slot gives its
// payload reference back on every path out of the queue, not only after
// delivery: shed by DropOldest, shed by DropNewest, shed as stale, shed after
// a failure, flushed by a seek, and dropped at Close.
func TestBufferedQueueReleasesEveryReference(t *testing.T) {
	cases := []struct {
		name   string
		policy BufferPolicy
		fail   error
		flush  bool
	}{
		{name: "drop-oldest", policy: BufferPolicy{Capacity: 1, Drop: DropOldest}},
		{name: "drop-newest", policy: BufferPolicy{Capacity: 1, Drop: DropNewest}},
		{name: "stale", policy: BufferPolicy{Capacity: 8, Drop: DropOldest, MaxLatency: time.Nanosecond}},
		{name: "failure", policy: BufferPolicy{Capacity: 8, Drop: DropOldest}, fail: errors.New("sink failed")},
		{name: "flush", policy: BufferPolicy{Capacity: 8, Drop: DropOldest}, flush: true},
		{name: "close", policy: BufferPolicy{Capacity: 8, Drop: DropOldest}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			graph, err := NewGraph(GraphConfig{Name: tc.name, Buffer: tc.policy})
			if err != nil {
				t.Fatal(err)
			}
			source := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
			sink := &blockedSink{name: "sink", started: make(chan struct{}), release: make(chan struct{}), fail: tc.fail}
			if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			if err := graph.Connect(route("src", "sink")); err != nil {
				t.Fatal(err)
			}
			runDone := make(chan error, 1)
			go func() { runDone <- graph.Run(ctx) }()
			<-source.ready

			var owners []*countingOwner
			emit := func() {
				payload, owner := countedBuffer([]byte{1})
				owners = append(owners, owner)
				packet := av.Packet{StreamID: "v", Payload: payload}
				_ = source.emitter.Emit(ctx, &Message{Kind: MessagePacket, Packet: &packet})
				packet.Release()
			}
			emit()
			if tc.policy.MaxLatency == 0 {
				<-sink.started // the first message is in Handle; the rest queue
			}
			for range 5 {
				emit()
			}
			if tc.flush {
				if err := graph.(*bufferedRunner).FlushDownstream("src"); err != nil {
					t.Fatal(err)
				}
			}
			if tc.name == "close" {
				cancel()
			}
			close(sink.release)
			close(source.release)
			<-runDone
			if err := graph.Close(); err != nil {
				t.Fatal(err)
			}
			for i, owner := range owners {
				if refs := owner.Refs(); refs != 0 {
					t.Fatalf("payload %d holds %d references after the run, want 0", i, refs)
				}
			}
		})
	}
}

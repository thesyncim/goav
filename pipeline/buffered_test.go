package pipeline

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

type bufferedPacketSource struct {
	name       string
	packets    []av.Packet
	afterFirst chan struct{}
	done       chan struct{}
	message    Message
}

func (s *bufferedPacketSource) Name() string {
	return s.name
}

func (s *bufferedPacketSource) Start(ctx context.Context, emitter Emitter) error {
	defer close(s.done)
	for i := range s.packets {
		if i == 1 && s.afterFirst != nil {
			select {
			case <-s.afterFirst:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		s.message.Kind = MessagePacket
		s.message.Packet = &s.packets[i]
		s.message.Frame = nil
		s.message.Event = nil
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	return nil
}

func (s *bufferedPacketSource) Close() error {
	return nil
}

type bufferedFrameSource struct {
	name    string
	frame   av.Frame
	done    chan struct{}
	message Message
}

func (s *bufferedFrameSource) Name() string {
	return s.name
}

func (s *bufferedFrameSource) Start(ctx context.Context, emitter Emitter) error {
	defer close(s.done)
	s.message.Kind = MessageFrame
	s.message.Packet = nil
	s.message.Frame = &s.frame
	s.message.Event = nil
	return emitter.Emit(ctx, &s.message)
}

func (s *bufferedFrameSource) Close() error {
	return nil
}

type bufferedBlockingSink struct {
	name        string
	started     chan struct{}
	release     chan struct{}
	once        sync.Once
	mu          sync.Mutex
	values      []byte
	frameValues []byte
}

func (s *bufferedBlockingSink) Name() string {
	return s.name
}

func (s *bufferedBlockingSink) Handle(ctx context.Context, msg *Message) error {
	s.once.Do(func() {
		close(s.started)
	})
	if len(s.values) == 0 && s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if msg != nil && msg.Packet != nil && len(msg.Packet.Payload.Bytes) != 0 {
		s.mu.Lock()
		s.values = append(s.values, msg.Packet.Payload.Bytes[0])
		s.mu.Unlock()
	}
	if msg != nil && msg.Frame != nil && len(msg.Frame.Planes) != 0 && len(msg.Frame.Planes[0].Buffer.Bytes) != 0 {
		s.mu.Lock()
		s.frameValues = append(s.frameValues, msg.Frame.Planes[0].Buffer.Bytes[0])
		s.mu.Unlock()
	}
	return nil
}

func (s *bufferedBlockingSink) Close() error {
	return nil
}

func TestNewGraphBuildsBufferedExecutionForBufferPolicy(t *testing.T) {
	graph, err := NewGraph(GraphConfig{
		Name:   "buffered",
		Buffer: BufferPolicy{Capacity: 2, Drop: DropOldest},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := graph.(*bufferedRunner); !ok {
		t.Fatalf("graph = %T, want *bufferedRunner", graph)
	}
}

func TestGraphBufferedRejectsAddAfterClose(t *testing.T) {
	graph, err := NewGraph(GraphConfig{Name: "buffered", Buffer: BufferPolicy{Capacity: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(&directTestSource{name: "source"}, BufferPolicy{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("AddSource err = %v, want ErrClosed", err)
	}
	if _, err := graph.AddStage(&directPassStage{name: "stage"}, BufferPolicy{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("AddStage err = %v, want ErrClosed", err)
	}
	if _, err := graph.AddSink(&directTestSink{name: "sink"}, BufferPolicy{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("AddSink err = %v, want ErrClosed", err)
	}
}

func TestGraphBufferedPassThroughImmutablePacket(t *testing.T) {
	source := &bufferedPacketSource{
		name:    "source",
		packets: []av.Packet{immutablePacket(7)},
		done:    make(chan struct{}),
	}
	sink := &bufferedBlockingSink{name: "sink", started: make(chan struct{})}

	graph, err := NewGraph(GraphConfig{Name: "pass", Buffer: BufferPolicy{Capacity: 2, Drop: DropOldest}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "sink")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.values) != 1 || sink.values[0] != 7 {
		t.Fatalf("values = %v", sink.values)
	}
}

func TestGraphBufferedAddsSinkWhileRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	afterFirst := make(chan struct{})
	source := &bufferedPacketSource{
		name:       "source",
		packets:    []av.Packet{immutablePacket(1), immutablePacket(2)},
		afterFirst: afterFirst,
		done:       make(chan struct{}),
	}
	base := &bufferedBlockingSink{name: "base", started: make(chan struct{})}
	late := &bufferedBlockingSink{name: "late", started: make(chan struct{})}

	graph, err := NewGraph(GraphConfig{Name: "dynamic-buffered", Buffer: BufferPolicy{Capacity: 2, Drop: DropOldest}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(base, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "base")); err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() {
		runErr <- graph.Run(ctx)
	}()
	select {
	case <-base.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := graph.AddSink(late, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "late")); err != nil {
		t.Fatal(err)
	}
	close(afterFirst)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if !equalBytes(base.values, []byte{1, 2}) {
		t.Fatalf("base values = %v, want [1 2]", base.values)
	}
	if !equalBytes(late.values, []byte{2}) {
		t.Fatalf("late values = %v, want [2]", late.values)
	}
}

func TestGraphBufferedRejectsBorrowedPacketPayload(t *testing.T) {
	packet := immutablePacket(1)
	packet.Payload.Ownership = av.BufferBorrowed
	source := &bufferedPacketSource{
		name:    "source",
		packets: []av.Packet{packet},
		done:    make(chan struct{}),
	}
	sink := &bufferedBlockingSink{name: "sink", started: make(chan struct{})}

	graph, err := NewGraph(GraphConfig{Name: "unsafe", Buffer: BufferPolicy{Capacity: 1, Drop: DropOldest}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "sink")); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(context.Background()); !errors.Is(err, ErrBufferedMessageUnsafe) {
		t.Fatalf("err = %v, want ErrBufferedMessageUnsafe", err)
	}
}

func TestGraphBufferedCopiesBorrowedPacketPayload(t *testing.T) {
	afterFirst := make(chan struct{})
	source := &bufferedPacketSource{
		name: "source",
		packets: []av.Packet{{
			Payload: av.Buffer{
				Bytes:     []byte{7},
				Ownership: av.BufferBorrowed,
			},
		}},
		done: make(chan struct{}),
	}
	sink := &bufferedBlockingSink{
		name:    "sink",
		started: make(chan struct{}),
		release: afterFirst,
	}

	graph, err := NewGraph(GraphConfig{
		Name: "copy-packet",
		Buffer: BufferPolicy{
			Capacity:        1,
			Drop:            DropOldest,
			CopyPacketBytes: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "sink")); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 1)
	go func() {
		errs <- graph.Run(context.Background())
	}()
	<-sink.started
	<-source.done
	source.packets[0].Payload.Bytes[0] = 9
	close(afterFirst)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if !equalBytes(sink.values, []byte{7}) {
		t.Fatalf("values = %v, want copied payload 7", sink.values)
	}
}

func TestBufferedMessageKeepsPacketCopyBackingAfterImmutableReuse(t *testing.T) {
	var slot bufferedMessage
	slot.init(BufferPolicy{CopyPacketBytes: 1})

	immutablePayload := []byte{1}
	immutable := Message{
		Kind: MessagePacket,
		Packet: &av.Packet{
			Payload: av.Buffer{
				Bytes:     immutablePayload,
				Ownership: av.BufferImmutable,
			},
		},
	}
	if err := slot.bind(&immutable, BufferPolicy{CopyPacketBytes: 1}); err != nil {
		t.Fatal(err)
	}
	slot.Reset()

	borrowedPayload := []byte{5}
	borrowed := Message{
		Kind: MessagePacket,
		Packet: &av.Packet{
			Payload: av.Buffer{
				Bytes:     borrowedPayload,
				Ownership: av.BufferBorrowed,
			},
		},
	}
	if err := slot.bind(&borrowed, BufferPolicy{CopyPacketBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if immutablePayload[0] != 1 {
		t.Fatalf("immutable payload was reused as copy backing: %v", immutablePayload)
	}
	borrowedPayload[0] = 9
	if got := slot.message.Packet.Payload.Bytes[0]; got != 5 {
		t.Fatalf("copied payload = %d, want 5", got)
	}
}

func TestGraphBufferedRejectsOversizedBorrowedPacketPayload(t *testing.T) {
	source := &bufferedPacketSource{
		name: "source",
		packets: []av.Packet{{
			Payload: av.Buffer{
				Bytes:     []byte{1, 2},
				Ownership: av.BufferBorrowed,
			},
		}},
		done: make(chan struct{}),
	}
	sink := &bufferedBlockingSink{name: "sink", started: make(chan struct{})}

	graph, err := NewGraph(GraphConfig{
		Name: "copy-packet-too-small",
		Buffer: BufferPolicy{
			Capacity:        1,
			Drop:            DropOldest,
			CopyPacketBytes: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "sink")); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(context.Background()); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("err = %v, want ErrMessageTooLarge", err)
	}
}

func TestGraphBufferedCopiesBorrowedFramePlane(t *testing.T) {
	release := make(chan struct{})
	source := &bufferedFrameSource{
		name: "source",
		frame: av.Frame{
			Type: av.MediaVideo,
			Planes: []av.Plane{{
				Buffer: av.Buffer{
					Bytes:     []byte{4},
					Ownership: av.BufferBorrowed,
				},
				Stride: 1,
			}},
		},
		done: make(chan struct{}),
	}
	sink := &bufferedBlockingSink{
		name:    "sink",
		started: make(chan struct{}),
		release: release,
	}

	graph, err := NewGraph(GraphConfig{
		Name: "copy-frame",
		Buffer: BufferPolicy{
			Capacity:       1,
			Drop:           DropOldest,
			CopyFrameBytes: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "sink")); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 1)
	go func() {
		errs <- graph.Run(context.Background())
	}()
	<-sink.started
	<-source.done
	source.frame.Planes[0].Buffer.Bytes[0] = 8
	close(release)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if !equalBytes(sink.frameValues, []byte{4}) {
		t.Fatalf("frame values = %v, want copied plane 4", sink.frameValues)
	}
}

func TestBufferedMessageOwnsFrameHeaders(t *testing.T) {
	var slot bufferedMessage
	slot.init(BufferPolicy{CopyFrameBytes: 4})

	audio := &av.AudioFrame{SampleRate: 48000, Channels: 2, SampleFormat: "s16", Samples: 2}
	video := &av.VideoFrame{Width: 2, Height: 2, PixelFormat: "i420"}
	frame := &av.Frame{
		Type:  av.MediaAudio,
		Audio: audio,
		Video: video,
		Planes: []av.Plane{{
			Buffer: av.Buffer{Bytes: []byte{1, 2, 3, 4}, Ownership: av.BufferBorrowed},
			Stride: 4,
		}},
	}
	msg := Message{Kind: MessageFrame, Frame: frame}
	if err := slot.bind(&msg, BufferPolicy{CopyFrameBytes: 4}); err != nil {
		t.Fatal(err)
	}
	audio.SampleRate = 44100
	audio.Channels = 1
	video.Width = 9
	frame.Planes[0].Stride = 99
	frame.Planes[0].Buffer.Bytes[0] = 9

	got := slot.message.Frame
	if got.Audio == audio || got.Video == video {
		t.Fatal("buffered frame aliases producer audio/video header pointers")
	}
	if got.Audio.SampleRate != 48000 || got.Audio.Channels != 2 {
		t.Fatalf("audio header = %+v, want copied 48kHz stereo", got.Audio)
	}
	if got.Video.Width != 2 {
		t.Fatalf("video header = %+v, want copied width 2", got.Video)
	}
	if got.Planes[0].Stride != 4 {
		t.Fatalf("plane stride = %d, want copied stride 4", got.Planes[0].Stride)
	}
	if got.Planes[0].Buffer.Bytes[0] != 1 {
		t.Fatalf("plane byte = %d, want copied payload byte 1", got.Planes[0].Buffer.Bytes[0])
	}
}

// TestGraphBufferedCopyAlwaysCopiesImmutablePacketPayload proves CopyAlways is
// defensive: even a payload declared av.BufferImmutable is copied into
// graph-owned backing, so a producer that lies about immutability and mutates
// its bytes after Emit cannot reach the delivered copy.
func TestGraphBufferedCopyAlwaysCopiesImmutablePacketPayload(t *testing.T) {
	release := make(chan struct{})
	source := &bufferedPacketSource{
		name: "source",
		packets: []av.Packet{{
			Payload: av.Buffer{
				Bytes:     []byte{7},
				Ownership: av.BufferImmutable,
			},
		}},
		done: make(chan struct{}),
	}
	sink := &bufferedBlockingSink{
		name:    "sink",
		started: make(chan struct{}),
		release: release,
	}

	graph, err := NewGraph(GraphConfig{
		Name: "copy-always-packet",
		Buffer: BufferPolicy{
			Capacity:        1,
			Drop:            DropOldest,
			CopyPacketBytes: 1,
			CopyAlways:      true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "sink")); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 1)
	go func() {
		errs <- graph.Run(context.Background())
	}()
	<-sink.started
	<-source.done
	source.packets[0].Payload.Bytes[0] = 9
	close(release)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if !equalBytes(sink.values, []byte{7}) {
		t.Fatalf("values = %v, want defensively copied payload 7", sink.values)
	}
}

// TestGraphBufferedCopyAlwaysWithoutBoundsRejectsImmutablePayload pins the
// fail-closed half of CopyAlways: without copy backing nothing can be copied,
// so even immutable payloads are refused instead of shared by reference.
func TestGraphBufferedCopyAlwaysWithoutBoundsRejectsImmutablePayload(t *testing.T) {
	source := &bufferedPacketSource{
		name:    "source",
		packets: []av.Packet{immutablePacket(1)},
		done:    make(chan struct{}),
	}
	sink := &bufferedBlockingSink{name: "sink", started: make(chan struct{})}

	graph, err := NewGraph(GraphConfig{
		Name:   "copy-always-unbounded",
		Buffer: BufferPolicy{Capacity: 1, Drop: DropOldest, CopyAlways: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "sink")); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(context.Background()); !errors.Is(err, ErrBufferedMessageUnsafe) {
		t.Fatalf("err = %v, want ErrBufferedMessageUnsafe", err)
	}
}

type bufferedMutatingFrameSink struct {
	name    string
	mutated chan struct{}
}

func (s *bufferedMutatingFrameSink) Name() string {
	return s.name
}

func (s *bufferedMutatingFrameSink) Handle(_ context.Context, msg *Message) error {
	if msg == nil || msg.Kind != MessageFrame || msg.Frame == nil {
		return nil
	}
	for i := range msg.Frame.Planes {
		bytes := msg.Frame.Planes[i].Buffer.Bytes
		for j := range bytes {
			bytes[j] = 0xEE
		}
	}
	close(s.mutated)
	return nil
}

func (s *bufferedMutatingFrameSink) Close() error {
	return nil
}

type bufferedObservingFrameSink struct {
	name  string
	wait  <-chan struct{}
	bytes []byte
}

func (s *bufferedObservingFrameSink) Name() string {
	return s.name
}

func (s *bufferedObservingFrameSink) Handle(ctx context.Context, msg *Message) error {
	select {
	case <-s.wait:
	case <-ctx.Done():
		return ctx.Err()
	}
	if msg != nil && msg.Frame != nil && len(msg.Frame.Planes) != 0 {
		s.bytes = append([]byte(nil), msg.Frame.Planes[0].Buffer.Bytes...)
	}
	return nil
}

func (s *bufferedObservingFrameSink) Close() error {
	return nil
}

// TestGraphBufferedFanoutMutatingSinkCannotCorruptSibling is the runner-level
// proof of north-star acceptance #22: a mutable (av.BufferOwned) frame fans out
// to two nodes, each of which binds its own slot copy, so one consumer
// mutating its delivered plane bytes can corrupt neither the sibling's copy
// nor the producer's backing array.
func TestGraphBufferedFanoutMutatingSinkCannotCorruptSibling(t *testing.T) {
	source := &bufferedFrameSource{
		name: "source",
		frame: av.Frame{
			Type: av.MediaVideo,
			Planes: []av.Plane{{
				Buffer: av.Buffer{
					Bytes:     []byte{1, 2, 3, 4},
					Ownership: av.BufferOwned,
				},
				Stride: 4,
			}},
		},
		done: make(chan struct{}),
	}
	mutator := &bufferedMutatingFrameSink{name: "mutator", mutated: make(chan struct{})}
	observer := &bufferedObservingFrameSink{name: "observer", wait: mutator.mutated}

	graph, err := NewGraph(GraphConfig{
		Name: "fanout-isolation",
		Buffer: BufferPolicy{
			Capacity:       2,
			Drop:           DropOldest,
			CopyFrameBytes: 16,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(mutator, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(observer, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "mutator", "observer")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !equalBytes(observer.bytes, []byte{1, 2, 3, 4}) {
		t.Fatalf("sibling frame bytes = %v, want original 1 2 3 4 after mutator wrote 0xEE", observer.bytes)
	}
	if !equalBytes(source.frame.Planes[0].Buffer.Bytes, []byte{1, 2, 3, 4}) {
		t.Fatalf("source backing = %v, want untouched original", source.frame.Planes[0].Buffer.Bytes)
	}
}

func TestGraphBufferedDropOldest(t *testing.T) {
	values, err := runBufferedBurst(BufferPolicy{Capacity: 1, Drop: DropOldest})
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{1, 3}; !equalBytes(values, want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
}

func TestGraphBufferedDropStats(t *testing.T) {
	values, stats, err := runBufferedBurstWithStats(BufferPolicy{Capacity: 1, Drop: DropOldest})
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{1, 3}; !equalBytes(values, want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
	if stats.Messages != 3 || stats.Packets != 3 || stats.Delivered != 2 ||
		stats.Dropped != 1 || stats.DropReasons[DropOldest] != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if got := stats.Nodes["source"]; got.OutPackets != 3 || got.InPackets != 0 {
		t.Fatalf("source stats = %+v", got)
	}
	if got := stats.Nodes["sink"]; got.InPackets != 2 || got.OutPackets != 0 ||
		got.Dropped != 1 || got.DropReasons[DropOldest] != 1 {
		t.Fatalf("sink stats = %+v", got)
	}
}

func TestGraphBufferedDropNewest(t *testing.T) {
	values, err := runBufferedBurst(BufferPolicy{Capacity: 1, Drop: DropNewest})
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{1, 2}; !equalBytes(values, want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
}

func TestGraphBufferedBackpressure(t *testing.T) {
	values, err := runBufferedBurst(BufferPolicy{Capacity: 1, Drop: DropNever})
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("err = %v, want ErrBackpressure", err)
	}
	if want := []byte{1, 2}; !equalBytes(values, want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
}

func TestGraphBufferedFullEventChannelDoesNotFailRun(t *testing.T) {
	source := &directEventSource{
		name: "source",
		events: []av.Event{
			{Type: av.EventPacketLoss},
			{Type: av.EventBackpressure},
		},
	}
	sink := &directTestSink{name: "sink"}
	graph, err := NewGraph(GraphConfig{
		Name:          "events",
		EventCapacity: 1,
		Buffer:        BufferPolicy{Capacity: 2, Drop: DropNever},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "sink")); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if sink.count != 2 {
		t.Fatalf("sink count = %d, want 2", sink.count)
	}
	stats := graph.Stats()
	if stats.Dropped != 1 || stats.DropReasons[DropObserver] != 1 {
		t.Fatalf("stats dropped=%d reasons=%+v, want one observer drop", stats.Dropped, stats.DropReasons)
	}
	sourceStats := stats.Nodes["source"]
	if sourceStats.Dropped != 1 || sourceStats.DropReasons[DropObserver] != 1 {
		t.Fatalf("source stats = %+v, want one observer drop", sourceStats)
	}
}

type countingSink struct {
	name  string
	delay time.Duration
	mu    sync.Mutex
	count int
}

func (s *countingSink) Name() string { return s.name }

func (s *countingSink) Handle(context.Context, *Message) error {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	s.count++
	s.mu.Unlock()
	return nil
}

func (s *countingSink) Close() error { return nil }

func (s *countingSink) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// TestGraphBufferedBlockingAppliesBackpressure proves the DropBlock fix: a slow
// consumer paces the source (true backpressure) without tearing the pipeline down
// or dropping, and a fast sibling still receives every message.
func TestGraphBufferedBlockingAppliesBackpressure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const n = 30
	source := &benchDrainSource{name: "src", n: n, msg: benchPacketMessage(64, true)}
	slow := &countingSink{name: "slow", delay: time.Millisecond}
	fast := &countingSink{name: "fast"}

	graph, err := NewGraph(GraphConfig{Name: "blocking", Buffer: BufferPolicy{Capacity: 2, Drop: DropBlock}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(slow, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(fast, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "src", To: []string{"slow", "fast"}}); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(ctx); err != nil {
		t.Fatalf("blocking pipeline tore down under backpressure: %v", err)
	}
	if got := slow.total(); got != n {
		t.Fatalf("slow sink delivered %d, want all %d (blocking must not drop)", got, n)
	}
	if got := fast.total(); got != n {
		t.Fatalf("fast sibling delivered %d, want all %d (slow consumer must not abort siblings)", got, n)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestGraphBufferedBlockingCancelUnblocks proves a permanently stuck Blocking
// consumer releases the source on ctx cancellation (the accepted escape).
func TestGraphBufferedBlockingCancelUnblocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	source := &benchDrainSource{name: "src", n: 1000, msg: benchPacketMessage(64, true)}
	stuck := &blockUntilSink{name: "stuck", release: make(chan struct{})}

	graph, err := NewGraph(GraphConfig{Name: "stuck", Buffer: BufferPolicy{Capacity: 1, Drop: DropBlock}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(stuck, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "src", To: []string{"stuck"}}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- graph.Run(ctx) }()
	cancel() // unblock the paced source
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking source did not release on ctx cancel")
	}
	close(stuck.release)
	_ = graph.Close()
}

// TestGraphBufferedShedsStaleUnderMaxLatency proves MaxLatency sheds messages
// that waited too long behind a slow consumer instead of delivering them late.
func TestGraphBufferedShedsStaleUnderMaxLatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const n = 20
	source := &benchDrainSource{name: "src", n: n, msg: benchPacketMessage(64, true)}
	slow := &countingSink{name: "slow", delay: 5 * time.Millisecond}

	graph, err := NewGraph(GraphConfig{
		Name:   "stale",
		Buffer: BufferPolicy{Capacity: n, Drop: DropOldest, MaxLatency: 2 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(slow, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "src", To: []string{"slow"}}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}

	delivered := slow.total()
	if delivered == 0 {
		t.Fatal("expected at least the first fresh message to be delivered")
	}
	if delivered >= n {
		t.Fatalf("delivered %d of %d; expected stale shedding to drop late messages", delivered, n)
	}
	stats := graph.Stats()
	if stats.DropReasons[DropStale] == 0 {
		t.Fatalf("expected stale drops recorded, got %+v", stats.DropReasons)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestGraphBufferedShedsOverflowUnderMaxBytes proves MaxBytes caps the queued
// payload bytes: behind a slow consumer, messages that would exceed the byte
// budget are shed (DropOverflow) instead of queued past the bound.
func TestGraphBufferedShedsOverflowUnderMaxBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const n = 20
	const packetBytes = 64
	source := &benchDrainSource{name: "src", n: n, msg: benchPacketMessage(packetBytes, true)}
	slow := &countingSink{name: "slow", delay: 5 * time.Millisecond}

	graph, err := NewGraph(GraphConfig{
		Name:   "overflow",
		Buffer: BufferPolicy{Capacity: n, Drop: DropOldest, MaxBytes: 2 * packetBytes},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(slow, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "src", To: []string{"slow"}}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}

	delivered := slow.total()
	if delivered == 0 {
		t.Fatal("expected at least one message delivered")
	}
	if delivered >= n {
		t.Fatalf("delivered %d of %d; expected byte-budget shedding to drop messages", delivered, n)
	}
	stats := graph.Stats()
	if stats.DropReasons[DropOverflow] == 0 {
		t.Fatalf("expected overflow drops recorded, got %+v", stats.DropReasons)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

type twoPhaseSource struct {
	name   string
	msg    *Message
	n      int
	phase1 chan struct{}
	resume chan struct{}
}

func (s *twoPhaseSource) Name() string { return s.name }

func (s *twoPhaseSource) Start(ctx context.Context, e Emitter) error {
	for i := 0; i < s.n; i++ {
		if err := e.Emit(ctx, s.msg); err != nil {
			return err
		}
	}
	close(s.phase1)
	<-s.resume
	for i := 0; i < s.n; i++ {
		if err := e.Emit(ctx, s.msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *twoPhaseSource) Close() error { return nil }

// TestGraphBufferedPauseAndResumeBranch proves SetNodePaused stops delivery to one
// branch without affecting the source or a sibling, and that resume restores it.
func TestGraphBufferedPauseAndResumeBranch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const n = 5
	src := &twoPhaseSource{name: "src", msg: benchPacketMessage(64, true), n: n, phase1: make(chan struct{}), resume: make(chan struct{})}
	a := &countingSink{name: "a"}
	b := &countingSink{name: "b"}

	graph, err := NewGraph(GraphConfig{Name: "pause", Buffer: BufferPolicy{Capacity: 4 * n, Drop: DropOldest}})
	if err != nil {
		t.Fatal(err)
	}
	for _, add := range []func() error{
		func() error { _, e := graph.AddSource(src, BufferPolicy{}); return e },
		func() error { _, e := graph.AddSink(a, BufferPolicy{}); return e },
		func() error { _, e := graph.AddSink(b, BufferPolicy{}); return e },
		func() error { return graph.Connect(Route{From: "src", To: []string{"a", "b"}}) },
	} {
		if err := add(); err != nil {
			t.Fatal(err)
		}
	}
	pauser, ok := graph.(NodePauser)
	if !ok {
		t.Fatal("buffered graph should implement NodePauser")
	}
	if err := pauser.SetNodePaused("a", true); err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()

	select {
	case <-src.phase1:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := pauser.SetNodePaused("a", false); err != nil {
		t.Fatal(err)
	}
	close(src.resume)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}

	if got := a.total(); got != n {
		t.Fatalf("paused branch a delivered %d, want %d (phase-1 skipped, phase-2 resumed)", got, n)
	}
	if got := b.total(); got != 2*n {
		t.Fatalf("sibling b delivered %d, want %d (unaffected by a's pause)", got, 2*n)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestGraphBufferedFanoutIsolatesFullTarget proves a full DropNever target no
// longer tears down the source: it drops for itself while the fast sibling keeps
// receiving every message and Run completes without error.
func TestGraphBufferedFanoutIsolatesFullTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const n = 20
	source := &benchDrainSource{name: "src", n: n, msg: benchPacketMessage(64, true)}
	slow := &countingSink{name: "slow", delay: 2 * time.Millisecond}
	fast := &countingSink{name: "fast"}

	graph, err := NewGraph(GraphConfig{Name: "isolate", Buffer: BufferPolicy{Capacity: 1, Drop: DropNever}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	// slow keeps the strict DropNever default and a tiny queue, so it backs up.
	if _, err := graph.AddSink(slow, BufferPolicy{Capacity: 1, Drop: DropNever}); err != nil {
		t.Fatal(err)
	}
	// fast has room and drops-oldest, so it never blocks.
	if _, err := graph.AddSink(fast, BufferPolicy{Capacity: 4 * n, Drop: DropOldest}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "src", To: []string{"slow", "fast"}}); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(ctx); err != nil {
		t.Fatalf("a full DropNever fanout target must not tear down the source: %v", err)
	}
	if got := fast.total(); got != n {
		t.Fatalf("fast sibling received %d, want %d (isolated from slow's backpressure)", got, n)
	}
	if got := slow.total(); got >= n {
		t.Fatalf("slow DropNever target received %d; expected it to drop under load", got)
	}
	if stats := graph.Stats(); stats.Dropped == 0 {
		t.Fatal("expected drops recorded for the backed-up target")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

type blockUntilSink struct {
	name    string
	release chan struct{}
}

func (s *blockUntilSink) Name() string { return s.name }

func (s *blockUntilSink) Handle(ctx context.Context, _ *Message) error {
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return nil
}

func (s *blockUntilSink) Close() error { return nil }

func runBufferedBurst(policy BufferPolicy) ([]byte, error) {
	values, _, err := runBufferedBurstWithStats(policy)
	return values, err
}

func runBufferedBurstWithStats(policy BufferPolicy) ([]byte, GraphStats, error) {
	afterFirst := make(chan struct{})
	source := &bufferedPacketSource{
		name:       "source",
		packets:    []av.Packet{immutablePacket(1), immutablePacket(2), immutablePacket(3)},
		afterFirst: afterFirst,
		done:       make(chan struct{}),
	}
	sink := &bufferedBlockingSink{
		name:    "sink",
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	graph, err := NewGraph(GraphConfig{Name: "burst", Buffer: policy})
	if err != nil {
		return nil, GraphStats{}, err
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		return nil, GraphStats{}, err
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		return nil, GraphStats{}, err
	}
	if err := graph.Connect(route("source", "sink")); err != nil {
		return nil, GraphStats{}, err
	}

	errs := make(chan error, 1)
	go func() {
		errs <- graph.Run(context.Background())
	}()

	<-sink.started
	close(afterFirst)
	<-source.done
	close(sink.release)
	err = <-errs

	sink.mu.Lock()
	values := append([]byte(nil), sink.values...)
	sink.mu.Unlock()
	return values, graph.Stats(), err
}

func immutablePacket(value byte) av.Packet {
	return av.Packet{
		Payload: av.Buffer{
			Bytes:     []byte{value},
			Ownership: av.BufferImmutable,
		},
	}
}

func equalBytes(a []byte, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestGraphBufferedRemoveDrainsInFlightDeliveries is the buffered sibling of
// the direct runner's live-remove pin: removing a sink while a source floods
// the graph must close the sink's queue without erroring the producer, drain
// the sink's worker before Close (no Handle after Close), and leave the rest
// of the graph delivering. The add -> connect -> traffic -> remove cycle is
// repeated so producers holding a routing snapshot from before the removal —
// including ones inside the blocking DropBlock enqueue while the queue is torn
// down — race the teardown on every iteration.
func TestGraphBufferedRemoveDrainsInFlightDeliveries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	graph, err := NewGraph(GraphConfig{Name: "buffered-live-remove", Buffer: BufferPolicy{Capacity: 2, Drop: DropBlock}})
	if err != nil {
		t.Fatal(err)
	}
	defer graph.Close()

	packet := &av.Packet{StreamID: "live", Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	stop := make(chan struct{})
	source := &directLoopSource{name: "src", packet: packet, stop: stop}
	sourceRef, err := graph.AddSource(source, BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	base := &removableTestSink{name: "base"}
	baseRef, err := graph.AddSink(base, BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: sourceRef.String(), To: []string{baseRef.String()}, Policy: RouteAll}); err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()
	for i := 0; i < 100; i++ {
		sink := &removableTestSink{name: fmt.Sprintf("tap-%d", i)}
		ref, err := graph.AddSink(sink, BufferPolicy{Capacity: 1, Drop: DropBlock})
		if err != nil {
			t.Fatalf("iteration %d: add: %v", i, err)
		}
		if err := graph.Connect(Route{From: sourceRef.String(), To: []string{ref.String()}, Policy: RouteAll}); err != nil {
			t.Fatalf("iteration %d: connect: %v", i, err)
		}
		for sink.delivered.Load() == 0 {
			if err := ctx.Err(); err != nil {
				t.Fatalf("iteration %d: %v", i, err)
			}
			runtime.Gosched()
		}
		if err := graph.Remove(ref); err != nil {
			t.Fatalf("iteration %d: remove: %v", i, err)
		}
		if got := sink.afterClosed.Load(); got != 0 {
			t.Fatalf("iteration %d: deliveries after close = %d, want 0 (remove must drain the worker before close)", i, got)
		}
		if !sink.closed.Load() {
			t.Fatalf("iteration %d: removed sink was not closed", i)
		}
	}
	close(stop)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if base.delivered.Load() == 0 {
		t.Fatal("base sink received nothing, want continuous delivery across removals")
	}
}

func TestGraphBufferedRemoveReportsStuckNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	graph, err := NewGraph(GraphConfig{Name: "buffered-stuck-remove", Buffer: BufferPolicy{Capacity: 1, Drop: DropBlock}, CloseWaitTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	packet := &av.Packet{StreamID: "live", Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	stop := make(chan struct{})
	source := &directLoopSource{name: "src", packet: packet, stop: stop}
	sourceRef, err := graph.AddSource(source, BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	sink := &stuckRemoveSink{name: "stuck", entered: make(chan struct{}), release: make(chan struct{})}
	sinkRef, err := graph.AddSink(sink, BufferPolicy{Capacity: 1, Drop: DropBlock})
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: sourceRef.String(), To: []string{sinkRef.String()}, Policy: RouteAll}); err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("sink did not enter Handle")
	}

	err = graph.Remove(sinkRef)
	var waitErr *CloseWaitError
	if !errors.Is(err, ErrCloseWait) || !errors.As(err, &waitErr) {
		t.Fatalf("Remove err = %v, want CloseWaitError", err)
	}
	if waitErr.Operation != "remove" || waitErr.Node != "stuck" {
		t.Fatalf("wait err = %+v, want remove/stuck", waitErr)
	}
	close(sink.release)
	close(stop)
	cancel()
	_ = graph.Close()
	// Shutdown can surface either context.Canceled (the source observes the
	// cancel first) or ErrClosed (Close wins the race while the source is still
	// emitting); both are valid teardown signals. The source no longer wedges
	// node.queueMutex while blocked, so the close path is not gated on it.
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
		t.Fatalf("Run err = %v", err)
	}
}

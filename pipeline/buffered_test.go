package pipeline

import (
	"context"
	"errors"
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

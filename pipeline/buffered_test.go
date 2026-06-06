package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"

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

type bufferedBlockingSink struct {
	name    string
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	values  []byte
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
	return nil
}

func (s *bufferedBlockingSink) Close() error {
	return nil
}

func TestDirectFactoryBuildsBufferedGraphForBufferPolicy(t *testing.T) {
	graph, err := NewDirectFactory().NewGraph(context.Background(), GraphConfig{
		Name:   "buffered",
		Buffer: BufferPolicy{Capacity: 2, Drop: DropOldest},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := graph.(*BufferedGraph); !ok {
		t.Fatalf("graph = %T, want *BufferedGraph", graph)
	}
}

func TestBufferedGraphPassThroughImmutablePacket(t *testing.T) {
	source := &bufferedPacketSource{
		name:    "source",
		packets: []av.Packet{immutablePacket(7)},
		done:    make(chan struct{}),
	}
	sink := &bufferedBlockingSink{name: "sink", started: make(chan struct{})}

	graph, err := NewBufferedGraph(GraphConfig{Name: "pass", Buffer: BufferPolicy{Capacity: 2, Drop: DropOldest}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("source", "sink")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.values) != 1 || sink.values[0] != 7 {
		t.Fatalf("values = %v", sink.values)
	}
}

func TestBufferedGraphRejectsBorrowedPacketPayload(t *testing.T) {
	packet := immutablePacket(1)
	packet.Payload.Ownership = av.BufferBorrowed
	source := &bufferedPacketSource{
		name:    "source",
		packets: []av.Packet{packet},
		done:    make(chan struct{}),
	}
	sink := &bufferedBlockingSink{name: "sink", started: make(chan struct{})}

	graph, err := NewBufferedGraph(GraphConfig{Name: "unsafe", Buffer: BufferPolicy{Capacity: 1, Drop: DropOldest}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Connect("source", "sink")); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(context.Background()); !errors.Is(err, ErrBufferedMessageUnsafe) {
		t.Fatalf("err = %v, want ErrBufferedMessageUnsafe", err)
	}
}

func TestBufferedGraphDropOldest(t *testing.T) {
	values, err := runBufferedBurst(BufferPolicy{Capacity: 1, Drop: DropOldest})
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{1, 3}; !equalBytes(values, want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
}

func TestBufferedGraphDropNewest(t *testing.T) {
	values, err := runBufferedBurst(BufferPolicy{Capacity: 1, Drop: DropNewest})
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{1, 2}; !equalBytes(values, want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
}

func TestBufferedGraphBackpressure(t *testing.T) {
	values, err := runBufferedBurst(BufferPolicy{Capacity: 1, Drop: DropNever})
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("err = %v, want ErrBackpressure", err)
	}
	if want := []byte{1, 2}; !equalBytes(values, want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
}

func runBufferedBurst(policy BufferPolicy) ([]byte, error) {
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

	graph, err := NewBufferedGraph(GraphConfig{Name: "burst", Buffer: policy})
	if err != nil {
		return nil, err
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		return nil, err
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		return nil, err
	}
	if err := graph.Connect(Connect("source", "sink")); err != nil {
		return nil, err
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
	return values, err
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

package rtpav

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func TestVideoFrameAssemblerTruncatedFrameInterFrameDropsAndRequestsSync(t *testing.T) {
	ctx := context.Background()
	stream := videoTestStream(av.CodecVP8)
	d := NewVP8Depacketizer(stream, WithMaxVideoFrameSize(64))
	pc := PayloadCodec{ClockRate: 90000}
	out := DepacketizeResult{
		Packets: make([]av.Packet, 0, 2),
		Events:  make([]av.Event, 0, 2),
	}

	// Start frame 1 (keyframe) without end marker.
	if err := d.PushInto(ctx, &rtp.Packet{
		Header:  rtp.Header{Timestamp: 1000, Marker: false},
		Payload: []byte{0x10, 0x00, 0xaa},
	}, pc, &out); err != nil {
		t.Fatal(err)
	}
	if d.assembler.state != asmAssembling {
		t.Fatalf("state = %v, want asmAssembling", d.assembler.state)
	}

	// New inter-frame starts at ts=2000 before frame 1 ended: must drop and request sync once.
	out.Reset()
	if err := d.PushInto(ctx, &rtp.Packet{
		Header:  rtp.Header{Timestamp: 2000, Marker: true},
		Payload: []byte{0x10, 0x01, 0xbb},
	}, pc, &out); err != nil {
		t.Fatal(err)
	}
	if d.assembler.state != asmDropping {
		t.Fatalf("state = %v, want asmDropping", d.assembler.state)
	}
	if len(out.Packets) != 0 {
		t.Fatalf("emitted %d corrupted inter-frame packets, want 0", len(out.Packets))
	}
	if len(out.Events) != 1 || out.Events[0].Type != av.EventKeyframeRequired {
		t.Fatalf("events = %+v, want 1 EventKeyframeRequired", out.Events)
	}

	// Continuation fragment at same timestamp while dropping must not flood EventKeyframeRequired.
	out.Reset()
	if err := d.PushInto(ctx, &rtp.Packet{
		Header:  rtp.Header{Timestamp: 2000, Marker: true},
		Payload: []byte{0x00, 0xcc},
	}, pc, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Events) != 0 {
		t.Fatalf("unexpected repeated EventKeyframeRequired: %+v", out.Events)
	}

	// Keyframe at ts=3000 recovers with LossBefore=true.
	out.Reset()
	if err := d.PushInto(ctx, &rtp.Packet{
		Header:  rtp.Header{Timestamp: 3000, Marker: true},
		Payload: []byte{0x10, 0x00, 0xdd},
	}, pc, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 || !out.Packets[0].Keyframe || !out.Packets[0].LossBefore {
		t.Fatalf("recovery packets = %+v", out.Packets)
	}
}

func TestVideoFrameAssemblerErrResultFullPreservesState(t *testing.T) {
	ctx := context.Background()
	stream := videoTestStream(av.CodecVP8)
	d := NewVP8Depacketizer(stream, WithMaxVideoFrameSize(64))
	pc := PayloadCodec{ClockRate: 90000}

	// 1. Single-packet frame with zero-cap out.Packets must return ErrResultFull and stay asmIdle.
	fullOut := DepacketizeResult{Packets: make([]av.Packet, 0, 0)}
	pkt := &rtp.Packet{Header: rtp.Header{Timestamp: 100, Marker: true}, Payload: []byte{0x10, 0x00, 0x11}}
	if err := d.PushInto(ctx, pkt, pc, &fullOut); !errors.Is(err, ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
	if d.assembler.state != asmIdle {
		t.Fatalf("state = %v after ErrResultFull on single packet, want asmIdle", d.assembler.state)
	}

	// 2. Multi-packet frame: first fragment buffers, final fragment gets ErrResultFull, retry succeeds.
	okOut := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}
	if err := d.PushInto(ctx, &rtp.Packet{
		Header:  rtp.Header{Timestamp: 200, Marker: false},
		Payload: []byte{0x10, 0x00, 0xaa},
	}, pc, &okOut); err != nil {
		t.Fatal(err)
	}
	lastFrag := &rtp.Packet{Header: rtp.Header{Timestamp: 200, Marker: true}, Payload: []byte{0x00, 0xbb}}
	if err := d.PushInto(ctx, lastFrag, pc, &fullOut); !errors.Is(err, ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
	if d.assembler.state != asmAssembling {
		t.Fatalf("state = %v after ErrResultFull on final fragment, want asmAssembling", d.assembler.state)
	}
	okOut.Reset()
	if err := d.PushInto(ctx, lastFrag, pc, &okOut); err != nil {
		t.Fatalf("retry after ErrResultFull failed: %v", err)
	}
	if len(okOut.Packets) != 1 || !bytes.Equal(okOut.Packets[0].Payload.Bytes, []byte{0x00, 0xaa, 0xbb}) {
		t.Fatalf("retried packets = %+v", okOut.Packets)
	}
}

func TestH264DepacketizerSyncsOnKeyframeOrSPSPPSAfterTruncatedFrame(t *testing.T) {
	ctx := context.Background()
	stream := videoTestStream(av.CodecH264)
	pc := PayloadCodec{ClockRate: 90000}

	t.Run("direct IDR at new timestamp", func(t *testing.T) {
		d := NewH264Depacketizer(stream, WithMaxVideoFrameSize(64))
		out := DepacketizeResult{Packets: make([]av.Packet, 0, 2), Events: make([]av.Event, 0, 2)}

		// Partial inter-frame at ts=1000 (Marker=false).
		if err := d.PushInto(ctx, &rtp.Packet{
			Header:  rtp.Header{Timestamp: 1000, Marker: false},
			Payload: []byte{0x61, 0xaa},
		}, pc, &out); err != nil {
			t.Fatal(err)
		}

		// New IDR keyframe arrives at ts=2000: must NOT be discarded!
		out.Reset()
		if err := d.PushInto(ctx, &rtp.Packet{
			Header:  rtp.Header{Timestamp: 2000, Marker: true},
			Payload: []byte{0x65, 0xbb},
		}, pc, &out); err != nil {
			t.Fatal(err)
		}
		if len(out.Packets) != 1 || !out.Packets[0].Keyframe || !out.Packets[0].LossBefore {
			t.Fatalf("packets = %+v, want 1 keyframe with LossBefore", out.Packets)
		}
		want := []byte{0, 0, 0, 1, 0x65, 0xbb}
		if !bytes.Equal(out.Packets[0].Payload.Bytes, want) {
			t.Fatalf("payload = %x, want %x", out.Packets[0].Payload.Bytes, want)
		}
	})

	t.Run("SPS+PPS then IDR at new timestamp", func(t *testing.T) {
		d := NewH264Depacketizer(stream, WithMaxVideoFrameSize(64))
		out := DepacketizeResult{Packets: make([]av.Packet, 0, 2), Events: make([]av.Event, 0, 2)}

		// Partial FU-A at ts=1000.
		if err := d.PushInto(ctx, &rtp.Packet{
			Header:  rtp.Header{Timestamp: 1000, Marker: false},
			Payload: []byte{0x7c, 0x81, 0xaa},
		}, pc, &out); err != nil {
			t.Fatal(err)
		}

		// SPS at ts=2000 (Marker=false), then PPS (Marker=false), then IDR (Marker=true).
		out.Reset()
		if err := d.PushInto(ctx, &rtp.Packet{
			Header:  rtp.Header{Timestamp: 2000, Marker: false},
			Payload: []byte{0x67, 0x42, 0xe0},
		}, pc, &out); err != nil {
			t.Fatal(err)
		}
		if err := d.PushInto(ctx, &rtp.Packet{
			Header:  rtp.Header{Timestamp: 2000, Marker: false},
			Payload: []byte{0x68, 0xce},
		}, pc, &out); err != nil {
			t.Fatal(err)
		}
		if err := d.PushInto(ctx, &rtp.Packet{
			Header:  rtp.Header{Timestamp: 2000, Marker: true},
			Payload: []byte{0x65, 0x88},
		}, pc, &out); err != nil {
			t.Fatal(err)
		}
		if len(out.Packets) != 1 || !out.Packets[0].Keyframe || !out.Packets[0].LossBefore {
			t.Fatalf("packets = %+v, want 1 keyframe with LossBefore", out.Packets)
		}
		want := []byte{
			0, 0, 0, 1, 0x67, 0x42, 0xe0,
			0, 0, 0, 1, 0x68, 0xce,
			0, 0, 0, 1, 0x65, 0x88,
		}
		if !bytes.Equal(out.Packets[0].Payload.Bytes, want) {
			t.Fatalf("payload = %x, want %x", out.Packets[0].Payload.Bytes, want)
		}
	})

	t.Run("no keyframe-required flood on FU-A continuations while dropping", func(t *testing.T) {
		d := NewH264Depacketizer(stream, WithMaxVideoFrameSize(64))
		out := DepacketizeResult{Packets: make([]av.Packet, 0, 2), Events: make([]av.Event, 0, 2)}
		if err := d.HandleEvent(ctx, &av.Event{Type: av.EventPacketLoss, StreamID: stream.ID}); err != nil {
			t.Fatal(err)
		}
		// Start of non-IDR FU-A at ts=5000 emits 1 EventKeyframeRequired.
		if err := d.PushInto(ctx, &rtp.Packet{
			Header:  rtp.Header{Timestamp: 5000, Marker: false},
			Payload: []byte{0x7c, 0x81, 0xaa},
		}, pc, &out); err != nil {
			t.Fatal(err)
		}
		if len(out.Events) != 1 {
			t.Fatalf("events on FU-A start = %d, want 1", len(out.Events))
		}
		// Middle and end FU-A fragments at ts=5000 must NOT emit additional EventKeyframeRequired.
		for _, payload := range [][]byte{{0x7c, 0x01, 0xbb}, {0x7c, 0x41, 0xcc}} {
			out.Reset()
			if err := d.PushInto(ctx, &rtp.Packet{
				Header:  rtp.Header{Timestamp: 5000, Marker: false},
				Payload: payload,
			}, pc, &out); err != nil {
				t.Fatal(err)
			}
			if len(out.Events) != 0 {
				t.Fatalf("unexpected EventKeyframeRequired flood on FU-A continuation: %+v", out.Events)
			}
		}
	})
}

func TestAV1DepacketizerSyncsOnKeyframeAfterTruncatedFrame(t *testing.T) {
	ctx := context.Background()
	stream := videoTestStream(av.CodecAV1)
	d := NewAV1Depacketizer(stream, WithMaxVideoFrameSize(64))
	pc := PayloadCodec{ClockRate: 90000}
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 2), Events: make([]av.Event, 0, 2)}

	// Partial temporal unit at ts=1000 (Y=1, Marker=false).
	if err := d.PushInto(ctx, &rtp.Packet{
		Header:  rtp.Header{Timestamp: 1000, Marker: false},
		Payload: []byte{0x50, 0x30, 0xaa},
	}, pc, &out); err != nil {
		t.Fatal(err)
	}

	// New AV1 keyframe (N=1, Z=0, W=1) at ts=2000 must sync immediately!
	out.Reset()
	if err := d.PushInto(ctx, &rtp.Packet{
		Header:  rtp.Header{Timestamp: 2000, Marker: true},
		Payload: []byte{0x18, 0x30, 0xbb},
	}, pc, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 || !out.Packets[0].Keyframe || !out.Packets[0].LossBefore {
		t.Fatalf("packets = %+v, want 1 keyframe with LossBefore", out.Packets)
	}
	want := []byte{0x32, 0x01, 0xbb}
	if !bytes.Equal(out.Packets[0].Payload.Bytes, want) {
		t.Fatalf("payload = %x, want %x", out.Packets[0].Payload.Bytes, want)
	}
}

func TestNewHistoryZeroReturnsNil(t *testing.T) {
	if h := newHistory(0, 1200); h != nil {
		t.Fatalf("newHistory(0, 1200) allocated %d slots, want nil", len(h))
	}
	if h := newHistory(-5, 1200); h != nil {
		t.Fatalf("newHistory(-5, 1200) allocated %d slots, want nil", len(h))
	}
}

type noopPacketWriter struct {
	readCalls atomic.Int32
	closed    chan struct{}
	closeOnce sync.Once
}

func newNoopPacketWriter() *noopPacketWriter {
	return &noopPacketWriter{closed: make(chan struct{})}
}

func (w *noopPacketWriter) WriteRTP(context.Context, *rtp.Packet) error { return nil }
func (w *noopPacketWriter) WriteRTCP(context.Context, []rtcp.Packet) error {
	return nil
}
func (w *noopPacketWriter) ReadRTCP(ctx context.Context) ([]rtcp.Packet, error) {
	w.readCalls.Add(1)
	select {
	case <-w.closed:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (w *noopPacketWriter) Close() error {
	w.closeOnce.Do(func() { close(w.closed) })
	return nil
}

func TestSenderCloseBeforeOpenAndHandleAfterClose(t *testing.T) {
	w := newNoopPacketWriter()
	s := Send(w, WithHistory(0), WithReportInterval(0)).(*sender)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	stream := av.Stream{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, ClockRate: 48000}}
	if err := s.OpenStreams(context.Background(), []av.Stream{stream}); !errors.Is(err, ErrClosed) {
		t.Fatalf("OpenStreams after Close = %v, want ErrClosed", err)
	}
	if w.readCalls.Load() != 0 {
		t.Fatalf("feedback goroutine spawned after Close: readCalls=%d", w.readCalls.Load())
	}

	// Handle after Close on an opened sender returns ErrClosed.
	w2 := newNoopPacketWriter()
	s2 := Send(w2, WithHistory(0), WithReportInterval(0)).(*sender)
	if err := s2.OpenStreams(context.Background(), []av.Stream{stream}); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	pkt := av.Packet{StreamID: "a", PTS: av.RTPToTimestamp(960, 48000), Payload: av.Buffer{Bytes: []byte{1, 2}}}
	if err := s2.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &pkt}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Handle after Close = %v, want ErrClosed", err)
	}
}

func TestSenderConcurrentOpenStreamsAndCloseRaceFree(t *testing.T) {
	stream := av.Stream{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, ClockRate: 48000}}
	for range 20 {
		w := newNoopPacketWriter()
		s := Send(w, WithHistory(0), WithReportInterval(0)).(*sender)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = s.OpenStreams(context.Background(), []av.Stream{stream})
		}()
		go func() {
			defer wg.Done()
			_ = s.Close()
		}()
		wg.Wait()
		_ = s.Close()
	}
}

func TestJitterRingSSRCChangeErrResultFullDoesNotLoseDiscontinuity(t *testing.T) {
	ctx := context.Background()
	jitter, err := NewJitterRing(JitterConfig{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	okOut := JitterResult{Ready: make([]*rtp.Packet, 0, 2), Events: make([]av.Event, 0, 1)}
	if err := jitter.PushInto(ctx, &rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 10}}, &okOut); err != nil {
		t.Fatal(err)
	}

	// Push SSRC=2 with zero-cap Events: must return ErrResultFull and NOT mutate ssrc/expected.
	fullEventsOut := JitterResult{Ready: make([]*rtp.Packet, 0, 2), Events: make([]av.Event, 0, 0)}
	p2 := &rtp.Packet{Header: rtp.Header{SSRC: 2, SequenceNumber: 20}}
	if err := jitter.PushInto(ctx, p2, &fullEventsOut); !errors.Is(err, ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
	if stats := jitter.Stats(); stats.SSRC != 1 || stats.Expected != 11 {
		t.Fatalf("state mutated on ErrResultFull: %+v", stats)
	}

	// Retry with room in Events: must emit EventDiscontinuity and switch to SSRC=2.
	okOut.Reset()
	if err := jitter.PushInto(ctx, p2, &okOut); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if len(okOut.Events) != 1 || okOut.Events[0].Type != av.EventDiscontinuity {
		t.Fatalf("events = %+v, want 1 EventDiscontinuity", okOut.Events)
	}
	if len(okOut.Ready) != 1 || okOut.Ready[0] != p2 || !okOut.State.Discontinuous {
		t.Fatalf("ready=%+v state=%+v", okOut.Ready, okOut.State)
	}
}

type blockingPacketReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingPacketReader) Streams(context.Context) ([]av.Stream, error) { return nil, nil }
func (r *blockingPacketReader) PayloadMap() PayloadMap                       { return NewStaticPayloadMap(1, nil) }
func (r *blockingPacketReader) Events() <-chan av.Event                      { return nil }
func (r *blockingPacketReader) ReadRTP(ctx context.Context) (*rtp.Packet, *av.Event, PayloadMap, error) {
	r.once.Do(func() { close(r.started) })
	select {
	case <-r.release:
		return nil, nil, nil, io.EOF
	case <-ctx.Done():
		return nil, nil, nil, ctx.Err()
	}
}
func (r *blockingPacketReader) Close() error { return nil }

func TestSourceRejectsConcurrentStartAndStartAfterClose(t *testing.T) {
	reader := &blockingPacketReader{started: make(chan struct{}), release: make(chan struct{})}
	src, err := NewSource(SourceConfig{Receiver: reader})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- src.Start(context.Background(), testEmitter(func(context.Context, *pipeline.Message) error { return nil }))
	}()
	<-reader.started

	if err := src.Start(context.Background(), testEmitter(func(context.Context, *pipeline.Message) error { return nil })); err == nil {
		t.Fatal("concurrent Start succeeded, want error")
	}
	close(reader.release)
	if err := <-done; err != nil {
		t.Fatalf("first Start err = %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if err := src.Start(context.Background(), testEmitter(func(context.Context, *pipeline.Message) error { return nil })); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Close = %v, want ErrClosed", err)
	}
}

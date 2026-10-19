package rtpav

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// udpReceiver is a loopback RTP receiver: RTP on one port, RTCP on the next.
type udpReceiver struct {
	rtp, rtcp *net.UDPConn
}

func listenUDPPair(t *testing.T) *udpReceiver {
	t.Helper()
	for range 20 {
		rtpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		port := rtpConn.LocalAddr().(*net.UDPAddr).Port
		rtcpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port + 1})
		if err != nil {
			rtpConn.Close()
			continue
		}
		t.Cleanup(func() { rtpConn.Close(); rtcpConn.Close() })
		return &udpReceiver{rtp: rtpConn, rtcp: rtcpConn}
	}
	t.Fatal("no free port pair")
	return nil
}

func (r *udpReceiver) address() string {
	return "127.0.0.1:" + strconv.Itoa(r.rtp.LocalAddr().(*net.UDPAddr).Port)
}

func (r *udpReceiver) readRTP(t *testing.T) *rtp.Packet {
	t.Helper()
	buf := make([]byte, 1500)
	r.rtp.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := r.rtp.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	var packet rtp.Packet
	if err := packet.Unmarshal(buf[:n]); err != nil {
		t.Fatal(err)
	}
	return &packet
}

// readRTCP returns the next RTCP packet of type T and the address it came
// from.
func readRTCP[T rtcp.Packet](t *testing.T, r *udpReceiver) (T, *net.UDPAddr) {
	t.Helper()
	buf := make([]byte, 1500)
	r.rtcp.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		n, from, err := r.rtcp.ReadFromUDP(buf)
		if err != nil {
			t.Fatal(err)
		}
		packets, err := rtcp.Unmarshal(buf[:n])
		if err != nil {
			t.Fatal(err)
		}
		for _, packet := range packets {
			if p, ok := packet.(T); ok {
				return p, from
			}
		}
	}
}

// controls records the controls feedback sends.
type controls struct {
	mu  sync.Mutex
	got []control.Control
	hit chan struct{}
}

func (c *controls) send(_ context.Context, ctrl control.Control) error {
	c.mu.Lock()
	c.got = append(c.got, ctrl)
	c.mu.Unlock()
	c.hit <- struct{}{}
	return nil
}

func (c *controls) wait(t *testing.T) control.Control {
	t.Helper()
	select {
	case <-c.hit:
	case <-time.After(5 * time.Second):
		t.Fatal("no control")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.got[len(c.got)-1]
}

func sendPacket(t *testing.T, sink pipeline.Sink, stream av.StreamID, i int, payload []byte) {
	t.Helper()
	packet := &av.Packet{StreamID: stream, Keyframe: i == 0, Payload: av.Buffer{Bytes: payload}, PTS: av.Timestamp{Value: int64(i) * 3000, Base: av.RTPTimeBase(90000)}}
	if err := sink.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket, Packet: packet}); err != nil {
		t.Fatal(err)
	}
}

// TestSendOverUDP runs Send against a loopback receiver: VP8 frames arrive
// and depacketize, a sender report maps the stream to the wall clock, a NACK
// gets its packet again at once, with no packet to send behind it, PLI asks for a keyframe, REMB retargets the
// bitrate, and Close says BYE.
func TestSendOverUDP(t *testing.T) {
	receiver := listenUDPPair(t)
	writer, err := DialUDP(receiver.address())
	if err != nil {
		t.Fatal(err)
	}
	sink := Send(writer, WithReportInterval(time.Millisecond), WithSSRC("v", 0x1234))
	if got := sink.(interface{ InputShapes() shape.Set }).InputShapes(); len(got) != 5 || got[0].Codec != av.CodecOpus || got[1].Codec != av.CodecVP8 {
		t.Fatalf("input shapes = %v", got)
	}
	ctl := &controls{hit: make(chan struct{}, 8)}
	sink.(interface {
		UseControl(func(context.Context, control.Control) error)
	}).UseControl(ctl.send)
	stream := videoTestStream(av.CodecVP8)
	stream.ID = "v"
	if err := sink.(interface {
		OpenStreams(context.Context, []av.Stream) error
	}).OpenStreams(context.Background(), []av.Stream{stream}); err != nil {
		t.Fatal(err)
	}
	depacketizer := NewVP8Depacketizer(stream)
	frame := bytes.Repeat([]byte{0x9c, 0x01}, 1000)
	var first *rtp.Packet
	for i := range 3 {
		sendPacket(t, sink, "v", i, frame)
		for {
			packet := receiver.readRTP(t)
			if packet.SSRC != 0x1234 || packet.PayloadType != 96 {
				t.Fatalf("packet = %v", packet.Header)
			}
			if first == nil {
				first = packet
			}
			result := DepacketizeResult{Packets: make([]av.Packet, 0, 1), Events: make([]av.Event, 0, 1)}
			if err := depacketizer.PushInto(context.Background(), packet, PayloadCodec{ClockRate: 90000}, &result); err != nil {
				t.Fatal(err)
			}
			if len(result.Packets) == 1 {
				if !bytes.Equal(result.Packets[0].Payload.Bytes, frame) {
					t.Fatalf("frame %d differs", i)
				}
				break
			}
		}
	}
	sr, from := readRTCP[*rtcp.SenderReport](t, receiver)
	if sr.SSRC != 0x1234 || sr.PacketCount == 0 || sr.NTPTime == 0 {
		t.Fatalf("sender report = %+v", sr)
	}

	nack, _ := rtcp.Marshal([]rtcp.Packet{&rtcp.TransportLayerNack{MediaSSRC: 0x1234, Nacks: []rtcp.NackPair{{PacketID: first.SequenceNumber}}}})
	if _, err := receiver.rtcp.WriteToUDP(nack, from); err != nil {
		t.Fatal(err)
	}
	// No packet follows: the retransmission answers the NACK itself.
	resent := receiver.readRTP(t)
	if resent.SequenceNumber != first.SequenceNumber || !bytes.Equal(resent.Payload, first.Payload) {
		t.Fatalf("after NACK got seq %d, want the retransmitted %d", resent.SequenceNumber, first.SequenceNumber)
	}

	pli, _ := rtcp.Marshal([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: 0x1234}})
	receiver.rtcp.WriteToUDP(pli, from)
	if got := ctl.wait(t); got.Type() != control.KeyframeType || got.StreamID() != "v" {
		t.Fatalf("PLI control = %v %v", got.Type(), got.StreamID())
	}
	remb, _ := rtcp.Marshal([]rtcp.Packet{&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: 750_000, SSRCs: []uint32{0x1234}}})
	receiver.rtcp.WriteToUDP(remb, from)
	if got := ctl.wait(t); got.Type() != control.BitrateType || got.Bitrate() != 750_000 {
		t.Fatalf("REMB control = %v %d", got.Type(), got.Bitrate())
	}

	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if bye, _ := readRTCP[*rtcp.Goodbye](t, receiver); len(bye.Sources) != 1 || bye.Sources[0] != 0x1234 {
		t.Fatalf("bye = %+v", bye)
	}
}

// TestSendRefusals pins Send's refusals: a codec it cannot packetize, a
// packet before OpenStreams, and a packet for a stream it was not opened
// with.
func TestSendRefusals(t *testing.T) {
	sink := Send(discardWriter{})
	open := sink.(interface {
		OpenStreams(context.Context, []av.Stream) error
	})
	if err := sink.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{}}); err == nil {
		t.Fatal("packet before OpenStreams")
	}
	if err := open.OpenStreams(context.Background(), []av.Stream{{ID: "a", Codec: av.CodecParameters{ID: av.CodecAAC}}}); err == nil {
		t.Fatal("aac accepted")
	}
	sink = Send(discardWriter{})
	open = sink.(interface {
		OpenStreams(context.Context, []av.Stream) error
	})
	two := []av.Stream{videoTestStream(av.CodecVP8), {ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus}}}
	if err := open.OpenStreams(context.Background(), two); err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	err := sink.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: "x", Payload: av.Buffer{Bytes: []byte{1}}}})
	if err == nil {
		t.Fatal("packet for an unknown stream")
	}
}

// discardWriter is a PacketWriter that drops everything and hears nothing.
type discardWriter struct{}

func (discardWriter) WriteRTP(context.Context, *rtp.Packet) error    { return nil }
func (discardWriter) WriteRTCP(context.Context, []rtcp.Packet) error { return nil }
func (discardWriter) ReadRTCP(ctx context.Context) ([]rtcp.Packet, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (discardWriter) Close() error { return nil }

// BenchmarkSend measures Send's data path per access unit: packetize, stamp,
// remember for NACK, write. Steady state allocates nothing.
func BenchmarkSend(b *testing.B) {
	sink := Send(discardWriter{}, WithReportInterval(0))
	stream := videoTestStream(av.CodecVP8)
	if err := sink.(interface {
		OpenStreams(context.Context, []av.Stream) error
	}).OpenStreams(context.Background(), []av.Stream{stream}); err != nil {
		b.Fatal(err)
	}
	defer sink.Close()
	frame := bytes.Repeat([]byte{0x9c, 0x01}, 6000)
	packet := &av.Packet{StreamID: stream.ID, Payload: av.Buffer{Bytes: frame}, PTS: av.Timestamp{Value: 1, Base: av.RTPTimeBase(90000)}}
	msg := &pipeline.Message{Kind: pipeline.MessagePacket, Packet: packet}
	ctx := context.Background()
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	for b.Loop() {
		if err := sink.Handle(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}
}

// TestSendToAnAbsentReceiver pins that RTP to a port nobody listens on yet
// keeps sending: the ICMP refusals a connected socket reports are not send
// failures.
func TestSendToAnAbsentReceiver(t *testing.T) {
	gone := listenUDPPair(t)
	address := gone.address()
	gone.rtp.Close()
	gone.rtcp.Close()
	writer, err := DialUDP(address)
	if err != nil {
		t.Fatal(err)
	}
	sink := Send(writer, WithReportInterval(time.Millisecond))
	defer sink.Close()
	if err := sink.(interface {
		OpenStreams(context.Context, []av.Stream) error
	}).OpenStreams(context.Background(), []av.Stream{videoTestStream(av.CodecVP8)}); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		sendPacket(t, sink, av.StreamID(av.CodecVP8), i, []byte{0x9c, 0x01, 0x02})
		time.Sleep(time.Millisecond)
	}
}

// rtcpFedWriter is a PacketWriter fed RTCP by the test; it counts the RTP
// it is handed, from any goroutine.
type rtcpFedWriter struct {
	rtcp  chan []rtcp.Packet
	mu    sync.Mutex
	sent  []uint16
	close sync.Once
	done  chan struct{}
}

func newRTCPFedWriter() *rtcpFedWriter {
	return &rtcpFedWriter{rtcp: make(chan []rtcp.Packet), done: make(chan struct{})}
}

func (w *rtcpFedWriter) WriteRTP(_ context.Context, packet *rtp.Packet) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sent = append(w.sent, packet.SequenceNumber)
	return nil
}

func (w *rtcpFedWriter) WriteRTCP(context.Context, []rtcp.Packet) error { return nil }

func (w *rtcpFedWriter) ReadRTCP(ctx context.Context) ([]rtcp.Packet, error) {
	select {
	case packets := <-w.rtcp:
		return packets, nil
	case <-w.done:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w *rtcpFedWriter) Close() error {
	w.close.Do(func() { close(w.done) })
	return nil
}

// TestSendCapsBitrateEstimates pins the REMB retarget bounds: an estimate
// above the recipe's configured bitrate asks for that bitrate, one below it
// for itself, one within a tenth of the last target for nothing, and one
// below the floor for the floor; a stream with no configured bitrate stops
// at 2.5 Mb/s.
func TestSendCapsBitrateEstimates(t *testing.T) {
	writer := newRTCPFedWriter()
	sink := Send(writer, WithSSRC("hd", 1), WithSSRC("sd", 2))
	ctl := &controls{hit: make(chan struct{}, 8)}
	sink.(interface {
		UseControl(func(context.Context, control.Control) error)
	}).UseControl(ctl.send)
	hd := videoTestStream(av.CodecVP8)
	hd.ID, hd.Metadata = "hd", av.Metadata{av.MetadataBitrate: "500000"}
	sd := videoTestStream(av.CodecVP8)
	sd.ID = "sd"
	if err := sink.(interface {
		OpenStreams(context.Context, []av.Stream) error
	}).OpenStreams(context.Background(), []av.Stream{hd, sd}); err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	remb := func(bitrate float32, ssrc uint32) {
		writer.rtcp <- []rtcp.Packet{&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: bitrate, SSRCs: []uint32{ssrc}}}
	}
	expect := func(stream av.StreamID, bitrate int) {
		t.Helper()
		if got := ctl.wait(t); got.Type() != control.BitrateType || got.StreamID() != stream || got.Bitrate() != bitrate {
			t.Fatalf("control = %v %q %d, want %q at %d", got.Type(), got.StreamID(), got.Bitrate(), stream, bitrate)
		}
	}
	remb(5_000_000, 1)
	expect("hd", 500_000)
	remb(300_000, 1)
	expect("hd", 300_000)
	remb(290_000, 1) // within a tenth: no control
	remb(10, 1)
	expect("hd", minEstimateBitrate)
	remb(50_000_000, 2)
	expect("sd", defaultMaxEstimateBitrate)
	select {
	case <-ctl.hit:
		t.Fatalf("an extra control: %v", ctl.got)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestSendRetransmitsWhileItSends races the data path against NACKs for
// the packets it just sent: under -race the history's handoff holds, and
// every NACK is answered from the feedback loop.
func TestSendRetransmitsWhileItSends(t *testing.T) {
	writer := newRTCPFedWriter()
	sink := Send(writer, WithSSRC("v", 7), WithReportInterval(0))
	stream := videoTestStream(av.CodecVP8)
	stream.ID = "v"
	if err := sink.(interface {
		OpenStreams(context.Context, []av.Stream) error
	}).OpenStreams(context.Background(), []av.Stream{stream}); err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	sendPacket(t, sink, "v", 0, []byte{0x9c, 0x01, 0x02, 0x03})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 400 {
			writer.mu.Lock()
			seq := writer.sent[len(writer.sent)-1]
			writer.mu.Unlock()
			writer.rtcp <- []rtcp.Packet{&rtcp.TransportLayerNack{MediaSSRC: 7, Nacks: []rtcp.NackPair{{PacketID: seq - uint16(i%8), LostPackets: 0x3}}}}
		}
	}()
	for i := 1; ; i++ {
		select {
		case <-done:
			writer.mu.Lock()
			defer writer.mu.Unlock()
			if len(writer.sent) <= i {
				t.Fatalf("%d writes for %d packets: no retransmission", len(writer.sent), i)
			}
			return
		default:
		}
		sendPacket(t, sink, "v", i, []byte{0x9c, 0x01, 0x02, 0x03})
	}
}

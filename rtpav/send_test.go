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
// gets its packet again, PLI asks for a keyframe, REMB retargets the
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
	time.Sleep(50 * time.Millisecond)
	sendPacket(t, sink, "v", 3, frame)
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

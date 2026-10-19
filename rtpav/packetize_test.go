package rtpav

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/thesyncim/goav/av"
)

// sendTrip packetizes access units with p, checks every packet against the
// MTU and the marker rule, and feeds them in order to d, returning what d
// reassembled.
func sendTrip(t testing.TB, p Packetizer, d Depacketizer, mtu int, units [][]byte, keyframes []bool) [][]byte {
	t.Helper()
	ctx := context.Background()
	clock := uint32(90000)
	if p.Codec() == av.CodecOpus {
		clock = 48000
	}
	payload := PayloadCodec{PayloadType: 96, ClockRate: clock}
	var out PacketizeResult
	var got [][]byte
	var seq uint16
	for i, unit := range units {
		packet := &av.Packet{Payload: av.Buffer{Bytes: unit}, PTS: av.Timestamp{Value: int64(i) * 3000, Base: av.RTPTimeBase(clock)}}
		if keyframes != nil {
			packet.Keyframe = keyframes[i]
		}
		out.Reset()
		if err := p.PacketizeInto(ctx, packet, payload, &out); err != nil {
			t.Fatalf("unit %d: %v", i, err)
		}
		if len(out.Packets) == 0 {
			t.Fatalf("unit %d: no packets", i)
		}
		for j := range out.Packets {
			rp := out.Packets[j]
			if size := rp.MarshalSize(); size > mtu {
				t.Fatalf("unit %d packet %d: %d bytes over the %d MTU", i, j, size, mtu)
			}
			last := j == len(out.Packets)-1
			if p.Codec() != av.CodecOpus && rp.Marker != last {
				t.Fatalf("unit %d packet %d: marker=%v, want it on the last packet only", i, j, rp.Marker)
			}
			if rp.Timestamp != uint32(i)*3000 || rp.PayloadType != 96 {
				t.Fatalf("unit %d packet %d: ts=%d pt=%d", i, j, rp.Timestamp, rp.PayloadType)
			}
			rp.SequenceNumber = seq
			seq++
			var result DepacketizeResult
			result.Packets = make([]av.Packet, 0, 2)
			result.Events = make([]av.Event, 0, 2)
			if err := d.PushInto(ctx, &rp, payload, &result); err != nil {
				t.Fatalf("unit %d packet %d: depacketize: %v", i, j, err)
			}
			for _, frame := range result.Packets {
				got = append(got, append([]byte(nil), frame.Payload.Bytes...))
			}
		}
	}
	return got
}

// noZeroRuns fills b with bytes 1..255, so random media never fakes a start
// code.
func noZeroRuns(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(1 + r.IntN(255))
	}
	return b
}

func annexB(nalus ...[]byte) []byte {
	var out []byte
	for _, nalu := range nalus {
		out = append(out, 0, 0, 0, 1)
		out = append(out, nalu...)
	}
	return out
}

func h264NALU(r *rand.Rand, nalType byte, size int) []byte {
	nalu := noZeroRuns(r, size)
	nalu[0] = byte(r.IntN(4))<<5 | nalType
	return nalu
}

// TestH264PacketizerRoundTrips sends random access units — parameter sets,
// SEI, and slices from a few bytes (STAP-A) to many MTUs (FU-A) — through
// the H264 depacketizer at several MTUs: every access unit comes back whole.
func TestH264PacketizerRoundTrips(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, mtu := range []int{minMTU, 200, 1200, 1500} {
		var units, want [][]byte
		for i := range 120 {
			var nalus [][]byte
			if i%10 == 0 {
				nalus = append(nalus, h264NALU(r, 7, 4+r.IntN(20)), h264NALU(r, 8, 2+r.IntN(6)))
			}
			if r.IntN(3) == 0 {
				nalus = append(nalus, h264NALU(r, 6, 1+r.IntN(40)))
			}
			sliceType := byte(1)
			if i%10 == 0 {
				sliceType = 5
			}
			for range 1 + r.IntN(3) {
				nalus = append(nalus, h264NALU(r, sliceType, 1+r.IntN(4000)))
			}
			units = append(units, annexB(nalus...))
			want = append(want, annexB(nalus...))
		}
		p, err := NewPacketizer(videoTestStream(av.CodecH264), WithMTU(mtu))
		if err != nil {
			t.Fatal(err)
		}
		got := sendTrip(t, p, NewH264Depacketizer(videoTestStream(av.CodecH264)), mtu, units, nil)
		if len(got) != len(want) {
			t.Fatalf("mtu %d: %d access units back, want %d", mtu, len(got), len(want))
		}
		for i := range want {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("mtu %d unit %d: round trip differs (%d vs %d bytes)", mtu, i, len(got[i]), len(want[i]))
			}
		}
	}
}

// TestH264PacketizerSendsParameterSetsBeforeIDR pins SPS/PPS before every
// IDR: from the stream's avcC before any arrive in band, then the latest in
// band ones; delimiters and filler are dropped, and 3-byte start codes read.
func TestH264PacketizerSendsParameterSetsBeforeIDR(t *testing.T) {
	sps := []byte{0x67, 0x42, 0xc0, 0x1f, 0x8c}
	pps := []byte{0x68, 0xce, 0x3c, 0x80}
	avcc := append([]byte{1, 0x42, 0xc0, 0x1f, 0xff, 0xe1, 0, byte(len(sps))}, sps...)
	avcc = append(avcc, 1, 0, byte(len(pps)))
	avcc = append(avcc, pps...)
	stream := videoTestStream(av.CodecH264)
	stream.Codec.ExtraData = av.Buffer{Bytes: avcc}
	p, err := NewPacketizer(stream)
	if err != nil {
		t.Fatal(err)
	}
	idr := []byte{0x65, 0x88, 0x84}
	aud := []byte{0x09, 0xf0}
	sps2 := []byte{0x67, 0x64, 0x00, 0x28, 0xac}
	units := [][]byte{
		append([]byte{0, 0, 1}, append(append(aud, 0, 0, 1), idr...)...),
		annexB(sps2, pps, idr),
		annexB(idr),
		annexB([]byte{0x41, 0x9a, 0x02}),
	}
	got := sendTrip(t, p, NewH264Depacketizer(stream), defaultMTU, units, nil)
	want := [][]byte{annexB(sps, pps, idr), annexB(sps2, pps, idr), annexB(sps2, pps, idr), annexB([]byte{0x41, 0x9a, 0x02})}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("unit %d = % x, want % x", i, got[i], want[i])
		}
	}
	var out PacketizeResult
	if err := p.PacketizeInto(context.Background(), &av.Packet{Payload: av.Buffer{Bytes: annexB([]byte{0x78, 1})}, PTS: av.Timestamp{Base: av.RTPTimeBase(90000)}}, PayloadCodec{}, &out); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("aggregation NAL type in an access unit: %v, want ErrInvalidPayload", err)
	}
}

// TestVP8AndVP9PacketizersRoundTrip sends random frames through the VP8 and
// VP9 depacketizers: frames come back byte for byte, keyframes flagged.
func TestVP8AndVP9PacketizersRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for _, codec := range []av.CodecID{av.CodecVP8, av.CodecVP9} {
		for _, mtu := range []int{minMTU, 600, 1200} {
			var frames [][]byte
			var keys []bool
			for i := range 80 {
				frame := noZeroRuns(r, 1+r.IntN(9000))
				key := i%20 == 0
				if codec == av.CodecVP8 {
					frame[0] &^= 1
					if !key {
						frame[0] |= 1
					}
				} else {
					frame = vp9TestFrame(frame, key, 320, 180)
				}
				frames = append(frames, frame)
				keys = append(keys, key)
			}
			p, err := NewPacketizer(videoTestStream(codec), WithMTU(mtu))
			if err != nil {
				t.Fatal(err)
			}
			var d Depacketizer = NewVP8Depacketizer(videoTestStream(codec))
			if codec == av.CodecVP9 {
				d = NewVP9Depacketizer(videoTestStream(codec))
			}
			got := sendTrip(t, p, d, mtu, frames, keys)
			if len(got) != len(frames) {
				t.Fatalf("%s mtu %d: %d frames back, want %d", codec, mtu, len(got), len(frames))
			}
			for i := range frames {
				if !bytes.Equal(got[i], frames[i]) {
					t.Fatalf("%s mtu %d frame %d differs", codec, mtu, i)
				}
			}
		}
	}
}

// vp9TestFrame prefixes body with a profile-0 VP9 uncompressed header: a
// keyframe of the given size, or an inter frame.
func vp9TestFrame(body []byte, key bool, width, height int) []byte {
	var w bitWriter
	w.put(2, 2) // frame_marker
	w.put(0, 2) // profile 0
	w.put(0, 1) // show_existing_frame
	if !key {
		w.put(1, 1)
		w.put(1, 2)
		return append(w.bytes(), body...)
	}
	w.put(0, 1) // frame_type: key
	w.put(1, 1) // show_frame
	w.put(0, 1) // error_resilient_mode
	w.put(0x498342, 24)
	w.put(1, 3) // color_space BT.601
	w.put(0, 1) // color_range
	w.put(uint32(width-1), 16)
	w.put(uint32(height-1), 16)
	return append(w.bytes(), body...)
}

type bitWriter struct {
	out  []byte
	bits int
}

func (w *bitWriter) put(v uint32, n int) {
	for i := n - 1; i >= 0; i-- {
		if w.bits%8 == 0 {
			w.out = append(w.out, 0)
		}
		w.out[len(w.out)-1] |= byte(v>>i&1) << (7 - w.bits%8)
		w.bits++
	}
}

func (w *bitWriter) bytes() []byte { return w.out }

// TestVP9PacketizerSendsScalabilityStructure pins the non-flexible
// descriptor a VP9 keyframe opens with: the resolution its header carries.
func TestVP9PacketizerSendsScalabilityStructure(t *testing.T) {
	p, _ := NewPacketizer(videoTestStream(av.CodecVP9))
	var out PacketizeResult
	frame := vp9TestFrame([]byte{1, 2, 3}, true, 1280, 720)
	if err := p.PacketizeInto(context.Background(), &av.Packet{Payload: av.Buffer{Bytes: frame}, PTS: av.Timestamp{Base: av.RTPTimeBase(90000)}}, PayloadCodec{}, &out); err != nil {
		t.Fatal(err)
	}
	payload := out.Packets[0].Payload
	if payload[0] != 0x8f || payload[3] != 0x18 || int(payload[4])<<8|int(payload[5]) != 1280 || int(payload[6])<<8|int(payload[7]) != 720 {
		t.Fatalf("descriptor = % x", payload[:11])
	}
}

// av1OBU encodes one OBU with its size field.
func av1OBU(obuType int, extension bool, payload []byte) []byte {
	header := byte(obuType<<3) | 0x02
	out := []byte{header}
	if extension {
		out[0] |= 0x04
		out = append(out, 0x28)
	}
	out = appendLEB128(out, len(payload))
	return append(out, payload...)
}

// TestAV1PacketizerRoundTrips sends random temporal units — a sequence
// header on keyframes, frame OBUs from empty to many MTUs, some with an
// extension header — through the AV1 depacketizer: every OBU comes back,
// temporal delimiters dropped, and keyframes open with N set.
func TestAV1PacketizerRoundTrips(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	for _, mtu := range []int{minMTU, 300, 1200} {
		var units, want [][]byte
		var keys []bool
		for i := range 100 {
			key := i%25 == 0
			unit := av1OBU(av1OBUTemporalDelimiter, false, nil)
			var expect []byte
			if key {
				seq := av1OBU(av1OBUSequenceHeader, false, noZeroRuns(r, 12))
				unit = append(unit, seq...)
				expect = append(expect, seq...)
			}
			for range 1 + r.IntN(4) {
				obu := av1OBU(6, r.IntN(3) == 0, noZeroRuns(r, r.IntN(5000)))
				unit = append(unit, obu...)
				expect = append(expect, obu...)
			}
			units, want, keys = append(units, unit), append(want, expect), append(keys, key)
		}
		p, err := NewPacketizer(videoTestStream(av.CodecAV1), WithMTU(mtu))
		if err != nil {
			t.Fatal(err)
		}
		got := sendTrip(t, p, NewAV1Depacketizer(videoTestStream(av.CodecAV1)), mtu, units, keys)
		if len(got) != len(want) {
			t.Fatalf("mtu %d: %d units back, want %d", mtu, len(got), len(want))
		}
		for i := range want {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("mtu %d unit %d differs (%d vs %d bytes)", mtu, i, len(got[i]), len(want[i]))
			}
		}
	}
}

// TestAV1PacketizerSendsSequenceHeaderWithKeyframes pins the av1C sequence
// header ahead of a keyframe that carries none, behind an N packet.
func TestAV1PacketizerSendsSequenceHeaderWithKeyframes(t *testing.T) {
	seq := av1OBU(av1OBUSequenceHeader, false, []byte{0x0a, 0x0b, 0x0c})
	stream := videoTestStream(av.CodecAV1)
	stream.Codec.ExtraData = av.Buffer{Bytes: append([]byte{0x81, 0, 0, 0}, seq...)}
	p, _ := NewPacketizer(stream)
	frame := av1OBU(6, false, []byte{1, 2, 3})
	var out PacketizeResult
	if err := p.PacketizeInto(context.Background(), &av.Packet{Keyframe: true, Payload: av.Buffer{Bytes: frame}, PTS: av.Timestamp{Base: av.RTPTimeBase(90000)}}, PayloadCodec{}, &out); err != nil {
		t.Fatal(err)
	}
	payload := out.Packets[0].Payload
	if len(out.Packets) != 1 || payload[0] != av1NMask|2<<4 {
		t.Fatalf("aggregation header = %08b over %d packets", payload[0], len(out.Packets))
	}
	got := sendTrip(t, p, NewAV1Depacketizer(stream), defaultMTU, [][]byte{frame}, []bool{true})
	if want := append(append([]byte(nil), seq...), frame...); !bytes.Equal(got[0], want) {
		t.Fatalf("unit = % x, want % x", got[0], want)
	}
}

// TestOpusPacketizerRoundTrips sends Opus packets one per RTP packet and
// refuses one larger than the MTU allows.
func TestOpusPacketizerRoundTrips(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	stream := av.Stream{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus}}
	p, _ := NewPacketizer(stream)
	var units [][]byte
	for range 50 {
		units = append(units, noZeroRuns(r, 1+r.IntN(defaultMTU-rtpHeaderSize)))
	}
	got := sendTrip(t, p, NewOpusDepacketizer(stream), defaultMTU, units, nil)
	for i := range units {
		if !bytes.Equal(got[i], units[i]) {
			t.Fatalf("packet %d differs", i)
		}
	}
	var out PacketizeResult
	if err := p.PacketizeInto(context.Background(), &av.Packet{Payload: av.Buffer{Bytes: make([]byte, defaultMTU)}, PTS: av.Timestamp{Base: av.RTPTimeBase(48000)}}, PayloadCodec{}, &out); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized opus: %v", err)
	}
	if _, err := NewPacketizer(av.Stream{Codec: av.CodecParameters{ID: av.CodecAAC}}); !errors.Is(err, ErrPacketizerNotFound) {
		t.Fatalf("aac packetizer: %v", err)
	}
	if err := p.PacketizeInto(context.Background(), &av.Packet{Payload: av.Buffer{Bytes: []byte{1}}}, PayloadCodec{}, &out); !errors.Is(err, ErrNoTimestamp) {
		t.Fatalf("no timestamp: %v", err)
	}
}

// FuzzPacketizers packetizes arbitrary media with every packetizer at an
// arbitrary MTU. Nothing may panic or exceed the MTU, and whatever packetizes
// round-trips: VP8, VP9, and Opus byte for byte, H264 and AV1 to an access
// unit that packetizes to the same packets again.
func FuzzPacketizers(f *testing.F) {
	f.Add(uint8(0), uint16(100), []byte{0x10, 0x00, 0x11, 0x22})
	f.Add(uint8(1), uint16(1200), vp9TestFrame([]byte{9, 9}, true, 64, 64))
	f.Add(uint8(2), uint16(64), av1OBU(6, true, bytes.Repeat([]byte{7}, 300)))
	f.Add(uint8(3), uint16(80), annexB([]byte{0x67, 1, 2}, []byte{0x68, 3}, bytes.Repeat([]byte{0x65}, 500)))
	f.Add(uint8(4), uint16(1200), []byte{0xfc, 1, 2})
	f.Fuzz(func(t *testing.T, kind uint8, mtu uint16, data []byte) {
		codecs := []av.CodecID{av.CodecVP8, av.CodecVP9, av.CodecAV1, av.CodecH264, av.CodecOpus}
		codec := codecs[int(kind)%len(codecs)]
		stream := videoTestStream(codec)
		if codec == av.CodecOpus {
			stream = av.Stream{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus}}
		}
		size := max(int(mtu)%2000, minMTU)
		p, err := NewPacketizer(stream, WithMTU(size))
		if err != nil {
			t.Fatal(err)
		}
		packet := &av.Packet{Keyframe: kind&0x80 != 0, Payload: av.Buffer{Bytes: data}, PTS: av.Timestamp{Base: av.RTPTimeBase(90000)}}
		var first PacketizeResult
		if err := p.PacketizeInto(context.Background(), packet, PayloadCodec{}, &first); err != nil {
			return
		}
		var d Depacketizer
		switch codec {
		case av.CodecVP8:
			d = NewVP8Depacketizer(stream)
		case av.CodecVP9:
			d = NewVP9Depacketizer(stream)
		case av.CodecAV1:
			d = NewAV1Depacketizer(stream)
		case av.CodecH264:
			d = NewH264Depacketizer(stream)
		default:
			d = NewOpusDepacketizer(stream)
		}
		var units [][]byte
		for i := range first.Packets {
			rp := first.Packets[i]
			if rp.MarshalSize() > size {
				t.Fatalf("packet %d: %d bytes over the %d MTU", i, rp.MarshalSize(), size)
			}
			rp.SequenceNumber = uint16(i)
			result := DepacketizeResult{Packets: make([]av.Packet, 0, 2), Events: make([]av.Event, 0, 2)}
			if err := d.PushInto(context.Background(), &rp, PayloadCodec{ClockRate: 90000}, &result); err != nil {
				t.Fatalf("packet %d: depacketize: %v", i, err)
			}
			for _, frame := range result.Packets {
				units = append(units, append([]byte(nil), frame.Payload.Bytes...))
			}
		}
		if len(units) != 1 {
			t.Fatalf("%d units back from %d packets, want one", len(units), len(first.Packets))
		}
		switch codec {
		case av.CodecVP8, av.CodecVP9, av.CodecOpus:
			if !bytes.Equal(units[0], data) {
				t.Fatalf("round trip differs")
			}
			return
		}
		again, err := NewPacketizer(stream, WithMTU(size))
		if err != nil {
			t.Fatal(err)
		}
		var second PacketizeResult
		packet.Payload.Bytes = units[0]
		if err := again.PacketizeInto(context.Background(), packet, PayloadCodec{}, &second); err != nil {
			t.Fatalf("repacketize: %v", err)
		}
		if len(second.Packets) != len(first.Packets) {
			t.Fatalf("repacketized to %d packets, want %d", len(second.Packets), len(first.Packets))
		}
		for i := range first.Packets {
			if !bytes.Equal(first.Packets[i].Payload, second.Packets[i].Payload) {
				t.Fatalf("packet %d differs after a round trip", i)
			}
		}
	})
}

// BenchmarkPacketize measures one access unit through each packetizer with
// a reused result: steady state allocates nothing.
func BenchmarkPacketize(b *testing.B) {
	r := rand.New(rand.NewPCG(9, 10))
	h264 := annexB(h264NALU(r, 7, 12), h264NALU(r, 8, 4), h264NALU(r, 5, 40000))
	av1 := append(av1OBU(av1OBUSequenceHeader, false, noZeroRuns(r, 12)), av1OBU(6, false, noZeroRuns(r, 40000))...)
	vp8 := noZeroRuns(r, 40000)
	vp8[0] &^= 1
	cases := []struct {
		codec av.CodecID
		unit  []byte
	}{
		{av.CodecH264, h264},
		{av.CodecVP8, vp8},
		{av.CodecVP9, vp9TestFrame(noZeroRuns(r, 40000), true, 1280, 720)},
		{av.CodecAV1, av1},
		{av.CodecOpus, noZeroRuns(r, 160)},
	}
	for _, c := range cases {
		b.Run(string(c.codec), func(b *testing.B) {
			stream := videoTestStream(c.codec)
			if c.codec == av.CodecOpus {
				stream = av.Stream{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus}}
			}
			p, _ := NewPacketizer(stream)
			packet := &av.Packet{Keyframe: true, Payload: av.Buffer{Bytes: c.unit}, PTS: av.Timestamp{Value: 1, Base: av.RTPTimeBase(90000)}}
			var out PacketizeResult
			ctx := context.Background()
			b.SetBytes(int64(len(c.unit)))
			b.ReportAllocs()
			for b.Loop() {
				out.Reset()
				if err := p.PacketizeInto(ctx, packet, PayloadCodec{}, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

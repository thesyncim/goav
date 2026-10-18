package mp4

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// muxPacket is a packet a test writes and expects back.
type muxPacket struct {
	stream   av.StreamID
	pts, dts time.Duration
	key      bool
	payload  []byte
}

func opusHead(preSkip uint16) []byte {
	head := make([]byte, 19)
	copy(head, "OpusHead")
	head[8], head[9] = 1, 2
	binary.LittleEndian.PutUint16(head[10:], preSkip)
	binary.LittleEndian.PutUint32(head[12:], 48000)
	return head
}

// vp9Keyframe is a VP9 profile 0 keyframe header for a 64x48 frame.
func vp9Keyframe() []byte {
	// frame_marker 2, profile 0, show_existing 0, key 0, show 1, resilient 0,
	// sync code, color space 0 + range 0, width-1 and height-1 (16 bits each).
	bits := "10" + "00" + "0" + "0" + "1" + "0" + "010010011000001101000010" + "000" + "0" +
		"0000000000111111" + "0000000000101111"
	out := make([]byte, (len(bits)+7)/8+4)
	for i, b := range bits {
		if b == '1' {
			out[i/8] |= 0x80 >> (i % 8)
		}
	}
	return out
}

func avPackets(t *testing.T) ([]av.Stream, []muxPacket) {
	t.Helper()
	streams := []av.Stream{
		{ID: "v", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP9, Type: av.MediaVideo, Width: 64, Height: 48}, TimeBase: av.TimeBase{Num: 1, Den: 90000}},
		{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio, SampleRate: 48000, Channels: 2, ExtraData: av.Buffer{Bytes: opusHead(312)}}},
	}
	var packets []muxPacket
	// 3 s of 25 fps video, a keyframe every 10 frames, and 20 ms Opus frames
	// starting at the pre-skip before zero, interleaved by time.
	frame := 40 * time.Millisecond
	preSkip := 6500 * time.Microsecond
	v, a := 0, 0
	for v < 75 || a < 150 {
		vt := time.Duration(v) * frame
		at := time.Duration(a)*20*time.Millisecond - preSkip
		if v < 75 && (a >= 150 || vt <= at) {
			payload := []byte{byte(v), 1, 2, 3}
			if v%10 == 0 {
				payload = append(vp9Keyframe(), byte(v))
			}
			packets = append(packets, muxPacket{stream: "v", pts: vt, dts: vt, key: v%10 == 0, payload: payload})
			v++
			continue
		}
		packets = append(packets, muxPacket{stream: "a", pts: at, dts: at, key: true, payload: []byte{0xfc, byte(a), byte(a >> 8)}})
		a++
	}
	return streams, packets
}

func writeMP4(t *testing.T, out io.Writer, streams []av.Stream, packets []muxPacket, durations bool) {
	t.Helper()
	ctx := context.Background()
	m := &FormatMuxer{}
	if err := m.Open(ctx, format.Output{Writer: out}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	ns := av.TimeBase{Num: 1, Den: 1_000_000_000}
	for _, p := range packets {
		packet := av.Packet{
			StreamID: p.stream, Keyframe: p.key,
			PTS: av.Timestamp{Value: int64(p.pts), Base: ns}, DTS: av.Timestamp{Value: int64(p.dts), Base: ns},
			Payload: av.Buffer{Bytes: p.payload},
		}
		if durations && p.stream == "a" {
			packet.Duration = av.Duration{Value: int64(20 * time.Millisecond), Base: ns}
		}
		if err := m.Write(ctx, &packet, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func readMP4(t *testing.T, data []byte) ([]av.Stream, []muxPacket) {
	t.Helper()
	ctx := context.Background()
	d := &FormatDemuxer{}
	if err := d.Open(ctx, format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var got []muxPacket
	for {
		packet := av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 1024)}}
		err := d.ReadInto(ctx, &format.ReadResult{Packet: &packet})
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		pts, _ := packet.PTS.ToDuration()
		dts, _ := packet.DTS.ToDuration()
		got = append(got, muxPacket{stream: packet.StreamID, pts: pts, dts: dts, key: packet.Keyframe, payload: append([]byte(nil), packet.Payload.Bytes...)})
	}
	return d.Streams(), got
}

// TestMuxRoundTrips writes VP9 and Opus progressive (to a seekable output)
// and fragmented (to a pipe), and reads every sample back with its time,
// keyframe flag, and bytes, the Opus pre-skip still before zero.
func TestMuxRoundTrips(t *testing.T) {
	streams, packets := avPackets(t)
	progressive := &sliceWriteSeeker{}
	writeMP4(t, progressive, streams, packets, false)
	var fragmented bytes.Buffer
	writeMP4(t, &fragmented, streams, packets, true)
	if bytes.Contains(progressive.data, []byte("moof")) || !bytes.Contains(fragmented.Bytes(), []byte("moof")) {
		t.Fatal("a seekable output got fragments, or a pipe got none")
	}
	for name, data := range map[string][]byte{"progressive": progressive.data, "fragmented": fragmented.Bytes()} {
		t.Run(name, func(t *testing.T) {
			gotStreams, got := readMP4(t, data)
			if len(gotStreams) != 2 || gotStreams[0].Codec.ID != av.CodecVP9 || gotStreams[0].Codec.Width != 64 || gotStreams[1].Codec.ID != av.CodecOpus {
				t.Fatalf("streams = %+v", gotStreams)
			}
			if head := gotStreams[1].Codec.ExtraData.Bytes; !bytes.Equal(head, opusHead(312)) {
				t.Fatalf("OpusHead = % x", head)
			}
			byStream := func(ps []muxPacket, id av.StreamID) []muxPacket {
				var out []muxPacket
				for _, p := range ps {
					if p.stream == id {
						out = append(out, p)
					}
				}
				return out
			}
			for _, id := range []av.StreamID{"v", "a"} {
				want, have := byStream(packets, id), byStream(got, gotStreams[map[av.StreamID]int{"v": 0, "a": 1}[id]].ID)
				if len(have) != len(want) {
					t.Fatalf("%s: %d samples back, want %d", id, len(have), len(want))
				}
				for i := range want {
					if d := have[i].pts - want[i].pts; d > time.Millisecond || d < -time.Millisecond {
						t.Fatalf("%s sample %d at %v, want %v", id, i, have[i].pts, want[i].pts)
					}
					if have[i].key != want[i].key || !bytes.Equal(have[i].payload, want[i].payload) {
						t.Fatalf("%s sample %d = key %v % x, want key %v % x", id, i, have[i].key, have[i].payload, want[i].key, want[i].payload)
					}
				}
			}
		})
	}
}

// TestMuxH264AndAV1Samples pins the sample formats: H.264 Annex B is
// stored length-prefixed with an avcC from its parameter sets, AV1 without
// temporal delimiters with an av1C from its sequence header, and both read
// back as goav carries them.
func TestMuxH264AndAV1Samples(t *testing.T) {
	sps := []byte{0x67, 0x42, 0xc0, 0x1e, 0xda, 0x02, 0x80, 0xbf}
	pps := []byte{0x68, 0xce, 0x3c, 0x80}
	annexB := func(nals ...[]byte) []byte {
		var out []byte
		for _, nal := range nals {
			out = append(append(out, 0, 0, 0, 1), nal...)
		}
		return out
	}
	td := []byte{0x12, 0x00}
	seq := []byte{0x0a, 0x06, 0x19, 0x5d, 0xc3, 0xc3, 0xda, 0x44}
	frame := []byte{0x32, 0x02, 0xaa, 0xbb} // an OBU_FRAME with two payload bytes
	streams := []av.Stream{
		{ID: "h", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo, Width: 320, Height: 240}},
		{ID: "o", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecAV1, Type: av.MediaVideo, Width: 320, Height: 240}},
	}
	packets := []muxPacket{
		{stream: "h", key: true, payload: annexB(sps, pps, []byte{0x65, 0x88, 0x84})},
		{stream: "o", key: true, payload: append(append(append([]byte(nil), td...), seq...), frame...)},
		{stream: "h", pts: 40 * time.Millisecond, dts: 40 * time.Millisecond, payload: annexB([]byte{0x41, 0x9a})},
		{stream: "o", pts: 40 * time.Millisecond, dts: 40 * time.Millisecond, payload: append(append([]byte(nil), td...), frame...)},
	}
	out := &sliceWriteSeeker{}
	writeMP4(t, out, streams, packets, false)
	if !bytes.Contains(out.data, []byte("avcC")) || !bytes.Contains(out.data, append([]byte("av1C"), 0x81)) {
		t.Fatal("the file lacks its avcC or av1C")
	}
	_, got := readMP4(t, out.data)
	want := map[av.StreamID][][]byte{
		"h": {packets[0].payload, packets[2].payload},
		"o": {append(append([]byte(nil), seq...), frame...), frame},
	}
	for _, p := range got {
		id := av.StreamID(map[string]string{"1": "h", "2": "o"}[string(p.stream)])
		if id == "" {
			id = p.stream
		}
		if len(want[id]) == 0 || !bytes.Equal(p.payload, want[id][0]) {
			t.Fatalf("%s sample = % x, want % x", p.stream, p.payload, want[id])
		}
		want[id] = want[id][1:]
	}
}

func TestMuxRefusesWhatMP4CannotCarry(t *testing.T) {
	m := &FormatMuxer{}
	err := m.Open(context.Background(), format.Output{Writer: &bytes.Buffer{}},
		[]av.Stream{{ID: "s", Type: av.MediaSubtitle, Codec: av.CodecParameters{ID: av.CodecTextUTF8}}}, format.OpenOptions{})
	if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), "Matroska (.mkv)") {
		t.Fatalf("err = %v", err)
	}
}

// TestFragmentsStartAtKeyframes: every fragment of a video file opens with a
// sync sample, so a player can start at any fragment.
func TestFragmentsStartAtKeyframes(t *testing.T) {
	streams, packets := avPackets(t)
	var out bytes.Buffer
	writeMP4(t, &out, streams, packets, true)
	data := out.Bytes()
	fragments := 0
	for at := 0; at+8 <= len(data); {
		size := int(binary.BigEndian.Uint32(data[at:]))
		if string(data[at+4:at+8]) == "moof" {
			fragments++
		}
		at += size
	}
	// 75 frames, a keyframe every 10: 8 fragments.
	if fragments != 8 {
		t.Fatalf("%d fragments, want 8", fragments)
	}
}

// TestMuxAudioConfigurations: FLAC's STREAMINFO rides in dfLa, and AAC sent
// as ADTS is stored raw with the AudioSpecificConfig its header implies.
func TestMuxAudioConfigurations(t *testing.T) {
	streamInfo := append([]byte{0x80, 0, 0, 34}, make([]byte, 34)...) // last block, STREAMINFO
	adts := []byte{0xff, 0xf1, 0x50, 0x80, 0x01, 0x7f, 0xfc}          // AAC LC, 44.1 kHz, stereo, no CRC
	streams := []av.Stream{
		{ID: "f", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecFLAC, Type: av.MediaAudio, SampleRate: 44100, Channels: 2, ExtraData: av.Buffer{Bytes: append([]byte("fLaC"), streamInfo...)}}},
		{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecAAC, Type: av.MediaAudio, SampleRate: 44100, Channels: 2}},
	}
	packets := []muxPacket{
		{stream: "f", key: true, payload: []byte{0xff, 0xf8, 1}},
		{stream: "a", key: true, payload: append(append([]byte(nil), adts...), 0x21, 0x10)},
		{stream: "f", key: true, pts: 100 * time.Millisecond, dts: 100 * time.Millisecond, payload: []byte{0xff, 0xf8, 2}},
		{stream: "a", key: true, pts: 23 * time.Millisecond, dts: 23 * time.Millisecond, payload: append(append([]byte(nil), adts...), 0x21, 0x11)},
	}
	out := &sliceWriteSeeker{}
	writeMP4(t, out, streams, packets, false)
	gotStreams, got := readMP4(t, out.data)
	if len(gotStreams) != 2 || gotStreams[0].Codec.ID != av.CodecFLAC || gotStreams[1].Codec.ID != av.CodecAAC {
		t.Fatalf("streams = %+v", gotStreams)
	}
	if extra := gotStreams[0].Codec.ExtraData.Bytes; !bytes.Contains(extra, streamInfo) {
		t.Fatalf("FLAC extradata = % x, want the STREAMINFO block", extra)
	}
	if asc := gotStreams[1].Codec.ExtraData.Bytes; !bytes.Equal(asc, []byte{0x12, 0x10}) {
		t.Fatalf("AAC config = % x, want 12 10 (LC, 44.1 kHz, stereo)", asc)
	}
	for _, p := range got {
		if p.stream == gotStreams[1].ID && (len(p.payload) != 2 || p.payload[0] != 0x21) {
			t.Fatalf("AAC sample = % x, want the frame without its ADTS header", p.payload)
		}
	}
}

// TestFragmentDuration: asked for fragments of at most 200ms, the muxer cuts
// 25 fps video into five-frame fragments, and still at every keyframe.
func TestFragmentDuration(t *testing.T) {
	streams, packets := avPackets(t)
	var out bytes.Buffer
	m := &FormatMuxer{}
	ctx := context.Background()
	if err := m.Open(ctx, format.Output{Writer: &out, Fragment: 200 * time.Millisecond}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	ns := av.TimeBase{Num: 1, Den: 1_000_000_000}
	for _, p := range packets {
		packet := av.Packet{StreamID: p.stream, Keyframe: p.key, PTS: av.Timestamp{Value: int64(p.pts), Base: ns}, DTS: av.Timestamp{Value: int64(p.dts), Base: ns}, Payload: av.Buffer{Bytes: p.payload}}
		if err := m.Write(ctx, &packet, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	// 75 frames: 15 fragments of five, every second one a keyframe's.
	data := out.Bytes()
	fragments := 0
	for at := 0; at+8 <= len(data); at += int(binary.BigEndian.Uint32(data[at:])) {
		if string(data[at+4:at+8]) == "moof" {
			fragments++
		}
	}
	if fragments != 15 {
		t.Fatalf("%d fragments, want 15 of 200ms", fragments)
	}
}

// TestSingleOpusSampleKeepsPreSkip: an Opus track of one packet, without a
// duration, still reads back with its pre-skip before zero.
func TestSingleOpusSampleKeepsPreSkip(t *testing.T) {
	streams := []av.Stream{{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio, SampleRate: 48000, Channels: 2, ExtraData: av.Buffer{Bytes: opusHead(312)}}}}
	packets := []muxPacket{{stream: "a", pts: -6500 * time.Microsecond, dts: -6500 * time.Microsecond, key: true, payload: []byte{0xfc, 1, 2}}}
	out := &sliceWriteSeeker{}
	writeMP4(t, out, streams, packets, false)
	_, got := readMP4(t, out.data)
	if len(got) != 1 || got[0].pts != -6500*time.Microsecond {
		t.Fatalf("samples = %+v, want one at -6.5ms", got)
	}
}

package codectest

import (
	"context"
	"math"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

const (
	minSNR      = 20 // dB, a lossy codec against the source tone
	minMatchSNR = 40 // dB, two decoders of one bitstream
	audioRate   = 128_000
)

func audioStream(id av.CodecID) av.Stream {
	return av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: sampleRate},
		Codec:    av.CodecParameters{ID: id, Type: av.MediaAudio, SampleRate: sampleRate, ClockRate: sampleRate, Channels: channels, ChannelLayout: "stereo", SampleFormat: av.SampleFormatS16},
	}
}

func audioEncodeConfig(b backend, settings codec.CodecSettings) codec.EncodeConfig {
	stream := audioStream("")
	settings.Backend = b.desc.Backend.Name
	return codec.EncodeConfig{
		Stream:     stream,
		Parameters: av.CodecParameters{ID: b.desc.ID, Type: av.MediaAudio, SampleRate: sampleRate, ClockRate: sampleRate, Channels: channels, ChannelLayout: "stereo"},
		Settings:   settings,
	}
}

func runAudio(t *testing.T, b backend, decoders []backend) {
	if b.mode == codec.ModeEncode {
		t.Run("roundtrip", func(t *testing.T) { audioRoundTrip(t, b, decoders) })
		t.Run("mux", func(t *testing.T) { audioMux(t, b, decoders) })
		t.Run("knobs", func(t *testing.T) { encoderKnobs(t, b) })
		t.Run("close", func(t *testing.T) { encoderClose(t, b, audioEncodeConfig(b, codec.CodecSettings{}), tone(0)) })
		return
	}
	t.Run("reference", func(t *testing.T) { audioReference(t, b, decoders) })
	t.Run("close", func(t *testing.T) { decoderClose(t, b, audioStream(b.desc.ID)) })
}

func encodeAudio(t *testing.T, b backend) (encoded, bool) {
	t.Helper()
	encoder := openEncoder(t, b, audioEncodeConfig(b, codec.CodecSettings{Bitrate: audioRate}))
	if encoder == nil {
		return encoded{}, false
	}
	defer encoder.Close()
	out := encoded{stream: audioStream(b.desc.ID)}
	if reporter, ok := encoder.(codec.ParameterReporter); ok {
		out.stream.Codec = merged(out.stream.Codec, reporter.EncodedParameters())
	}
	result := newEncodeResult(av.MediaAudio)
	take := func() { drainPackets(&result, func(p av.Packet) { out.packets = append(out.packets, p) }) }
	ctx := context.Background()
	for n := range audioFrames {
		if err := encoder.EncodeInto(ctx, tone(n), &result); err != nil {
			t.Fatalf("%s: encode frame %d: %v", b.name(), n, err)
		}
		take()
	}
	if err := encoder.FlushInto(ctx, &result); err != nil {
		t.Fatalf("%s: flush: %v", b.name(), err)
	}
	take()
	return out, true
}

func sourceTimeline() map[int64]int16 {
	frames := make([]*av.Frame, audioFrames)
	for n := range frames {
		frames[n] = tone(n)
	}
	return timeline(frames)
}

func audioRoundTrip(t *testing.T, b backend, decoders []backend) {
	out, ok := encodeAudio(t, b)
	if !ok {
		return
	}
	if len(out.packets) < audioFrames-2 {
		t.Fatalf("%s: %d packets for %d frames of input", b.name(), len(out.packets), audioFrames)
	}
	for i := 1; i < len(out.packets); i++ {
		if out.packets[i].PTS.Value <= out.packets[i-1].PTS.Value {
			t.Fatalf("%s: packet %d PTS %d not after %d", b.name(), i, out.packets[i].PTS.Value, out.packets[i-1].PTS.Value)
		}
	}
	if b.desc.ID == av.CodecAAC && len(out.stream.Codec.ExtraData.Bytes) < 2 {
		t.Errorf("%s: no AudioSpecificConfig in the stream's extradata", b.name())
	}
	want := sourceTimeline()
	for _, d := range decoders {
		t.Run("decode/"+d.desc.Backend.Name, func(t *testing.T) {
			checkAudio(t, d, want, decodeAll(t, d, out.stream, out.packets), minSNR)
		})
	}
}

// checkAudio pins decoded audio against want by timestamp: SNR over the
// samples both cover, and coverage of most of the source.
func checkAudio(t *testing.T, d backend, want map[int64]int16, frames []*av.Frame, floor float64) {
	t.Helper()
	if frames == nil {
		return
	}
	ratio, covered := snr(want, timeline(frames), 2048*channels)
	if covered < len(want)*8/10 {
		t.Fatalf("%s: decoded audio covers %d of %d source samples", d.name(), covered, len(want))
	}
	if ratio < floor || math.IsNaN(ratio) {
		t.Errorf("%s: SNR %.1f dB, want at least %d", d.name(), ratio, int(floor))
	}
	t.Logf("%s: SNR %.1f dB over %d samples", d.name(), ratio, covered)
}

func audioMux(t *testing.T, b backend, decoders []backend) {
	if !muxable(b.desc.ID) {
		t.Skipf("the MP4 muxer does not write %s", b.desc.ID)
	}
	out, ok := encodeAudio(t, b)
	if !ok {
		return
	}
	packets := make([]*av.Packet, len(out.packets))
	for i := range out.packets {
		packets[i] = &out.packets[i]
	}
	file, err := muxMP4(out.stream, packets)
	if err != nil {
		t.Fatalf("%s: mux: %v", b.name(), err)
	}
	stream, demuxed, err := demux(file, av.MediaAudio)
	if err != nil {
		t.Fatalf("%s: demux: %v", b.name(), err)
	}
	if len(demuxed) != len(out.packets) || stream.Codec.SampleRate != sampleRate || stream.Codec.Channels != channels {
		t.Fatalf("%s: demuxed %d of %d packets at %d Hz x%d", b.name(), len(demuxed), len(out.packets), stream.Codec.SampleRate, stream.Codec.Channels)
	}
	// An MP4 presents nothing before zero: the demuxer lifts a track that
	// starts with codec priming by that much, the same for every packet.
	lift := demuxed[0].PTS.Value - out.packets[0].PTS.Value
	for i := range demuxed {
		if got := demuxed[i].PTS.Value - out.packets[i].PTS.Value; got != lift {
			t.Fatalf("%s: demuxed packet %d moved %d samples, packet 0 moved %d", b.name(), i, got, lift)
		}
	}
	want := sourceTimeline()
	for _, d := range decoders {
		t.Run("decode/"+d.desc.Backend.Name, func(t *testing.T) {
			frames := decodeAll(t, d, stream, demuxed)
			for _, frame := range frames {
				frame.PTS.Value -= lift
			}
			checkAudio(t, d, want, frames, minSNR)
		})
	}
}

// audioReference decodes an AAC stream FFmpeg's encoder made and matches it
// against the pure-Go decoder.
func audioReference(t *testing.T, b backend, decoders []backend) {
	if b.desc.ID != av.CodecAAC {
		t.Skipf("no reference bitstream for %s", b.desc.ID)
	}
	stream, packets, err := demuxFixture("h264_high_aac.mp4", av.MediaAudio)
	if err != nil {
		t.Fatal(err)
	}
	got := decodeAll(t, b, stream, packets)
	if got == nil {
		return
	}
	samples := 0
	for _, frame := range got {
		samples += len(pcm(frame))
	}
	if samples < len(packets)*1024*channels*9/10 {
		t.Fatalf("%s: decoded %d samples from %d packets", b.name(), samples, len(packets))
	}
	for _, d := range decoders {
		if d.desc.Backend.Kind != codec.KindGo || d.desc.Backend.Name == b.desc.Backend.Name {
			continue
		}
		want := decodeAll(t, d, stream, packets)
		if want == nil {
			continue
		}
		checkAudio(t, b, timeline(want), got, minMatchSNR)
	}
}

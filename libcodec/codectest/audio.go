package codectest

import (
	"context"
	"math"
	"testing"
	"time"

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
	if b.desc.Backend.Kind != codec.KindGo {
		t.Run("transitions", func(t *testing.T) { audioTransitions(t, b) })
	}
	t.Run("close", func(t *testing.T) { decoderClose(t, b, audioStream(b.desc.ID)) })
}

func encodeAudio(t *testing.T, b backend) (encoded, bool) {
	t.Helper()
	encoder := openEncoder(t, b, audioEncodeConfig(b, withBitrate(b, audioRate)))
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
	file, err := muxMP4(out.stream, packets, false)
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

// audioTransitions alternates independent AAC configurations, including a
// non-sample input timebase, and exercises seek/reset on the actual backend.
func audioTransitions(t *testing.T, b backend) {
	if b.desc.ID != av.CodecAAC {
		t.Skipf("no transition fixture for %s", b.desc.ID)
	}
	var fixtures []encoded
	for _, name := range []string{"h264_high_aac.mp4", "aac_44100_mono.mp4"} {
		stream, packets, err := demuxFixture(name, av.MediaAudio)
		if err != nil {
			t.Fatal(err)
		}
		fixtures = append(fixtures, encoded{stream: stream, packets: packets})
	}
	initial := fixtures[0].stream
	initial.Epoch = 1
	decoder := openDecoder(t, b, initial)
	if decoder == nil {
		return
	}
	defer decoder.Close()
	out := newDecodeResult(av.MediaAudio)
	for cycle := range 4 {
		fixture := fixtures[(cycle+1)%2]
		stream := fixture.stream
		stream.ID = "transition-audio"
		stream.Codec.ChannelLayout = "stereo"
		if stream.Codec.Channels == 1 {
			stream.Codec.ChannelLayout = "mono"
		}
		stream.Epoch = av.Epoch(cycle + 2)
		stream.TimeBase = av.TimeBase{Num: 1, Den: 90000}
		if err := decoder.HandleEvent(t.Context(), &av.Event{Type: av.EventCodecChanged, Stream: &stream, Epoch: stream.Epoch}); err != nil {
			t.Fatal(err)
		}
		if err := decoder.DecodeInto(t.Context(), &fixture.packets[0], &out); err != nil {
			t.Fatal(err)
		}
		drainFrames(&out, nil)
		if err := decoder.HandleEvent(t.Context(), &av.Event{Type: av.EventDiscontinuity}); err != nil {
			t.Fatal(err)
		}
		frames := 0
		check := func(f *av.Frame) {
			frames++
			if f.StreamID != stream.ID || f.CodecEpoch != stream.Epoch || f.Audio.SampleRate != stream.Codec.SampleRate || f.Audio.Channels != stream.Codec.Channels || f.Audio.ChannelLayout != stream.Codec.ChannelLayout {
				t.Errorf("%s: stale audio configuration: epoch=%d audio=%+v", b.name(), f.CodecEpoch, f.Audio)
			}
			seconds, _ := f.PTS.ToDuration()
			if seconds < time.Second {
				t.Errorf("pre-seek audio: %s", seconds)
			}
		}
		for _, packet := range fixture.packets {
			packet.PTS.Value += int64(stream.Codec.SampleRate)
			packet.PTS, _ = packet.PTS.Rescale(stream.TimeBase)
			packet.Duration, _ = packet.Duration.Rescale(stream.TimeBase)
			packet.CodecEpoch = stream.Epoch
			if err := decoder.DecodeInto(t.Context(), &packet, &out); err != nil {
				t.Fatal(err)
			}
			drainFrames(&out, check)
		}
		if err := decoder.FlushInto(t.Context(), &out); err != nil {
			t.Fatal(err)
		}
		drainFrames(&out, check)
		if frames < len(fixture.packets)-1 {
			t.Fatalf("%s: %d frames for %d packets", b.name(), frames, len(fixture.packets))
		}
	}
}

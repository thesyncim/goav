package codectest

import (
	"context"
	"errors"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

const (
	minPSNR       = 30 // dB, luma, at 1 Mb/s for 320x240
	testBitrate   = 1_000_000
	testGOP       = 10
	forcedKeyAt   = 15
	reorderWindow = 16
)

func videoStream(id av.CodecID) av.Stream {
	return av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: videoBase,
		Codec:    av.CodecParameters{ID: id, Type: av.MediaVideo, Width: videoWidth, Height: videoHeight, PixelFormat: av.PixelFormatI420, ClockRate: 90000},
	}
}

func encodeConfig(b backend, settings codec.CodecSettings) codec.EncodeConfig {
	stream := videoStream(b.desc.ID)
	stream.Codec.ID = ""
	settings.Backend = b.desc.Backend.Name
	return codec.EncodeConfig{
		Stream:     stream,
		Parameters: av.CodecParameters{ID: b.desc.ID, Type: av.MediaVideo, Width: videoWidth, Height: videoHeight, ClockRate: 90000},
		Settings:   settings,
	}
}

// encoded is one encoder's output: the stream it reports and its packets.
type encoded struct {
	stream  av.Stream
	packets []av.Packet
}

// encodeVideo encodes the test sequence, asking for a keyframe before frame
// forcedKeyAt, and flushes.
func encodeVideo(t *testing.T, b backend, settings codec.CodecSettings) (encoded, bool) {
	t.Helper()
	encoder := openEncoder(t, b, encodeConfig(b, settings))
	if encoder == nil {
		return encoded{}, false
	}
	defer encoder.Close()
	out := encoded{stream: videoStream(b.desc.ID)}
	if reporter, ok := encoder.(codec.ParameterReporter); ok {
		out.stream.Codec = merged(out.stream.Codec, reporter.EncodedParameters())
	}
	result := newEncodeResult(av.MediaVideo)
	take := func() { drainPackets(&result, func(p av.Packet) { out.packets = append(out.packets, p) }) }
	ctx := context.Background()
	for n := range videoFrames {
		if n == forcedKeyAt {
			if err := encoder.HandleEvent(ctx, &av.Event{Type: av.EventKeyframeRequired}); err != nil {
				t.Fatalf("%s: keyframe request: %v", b.name(), err)
			}
		}
		if err := encoder.EncodeInto(ctx, pattern(n), &result); err != nil {
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

func runVideo(t *testing.T, b backend, decoders []backend) {
	if b.mode == codec.ModeEncode {
		t.Run("roundtrip", func(t *testing.T) { videoRoundTrip(t, b, decoders) })
		t.Run("mux", func(t *testing.T) { videoMux(t, b, decoders) })
		t.Run("knobs", func(t *testing.T) { encoderKnobs(t, b) })
		t.Run("bitrate", func(t *testing.T) { videoBitrate(t, b) })
		t.Run("close", func(t *testing.T) { encoderClose(t, b, encodeConfig(b, codec.CodecSettings{}), pattern(0)) })
		return
	}
	t.Run("reference", func(t *testing.T) { videoReference(t, b, decoders) })
	t.Run("loss", func(t *testing.T) { videoLoss(t, b) })
	if b.desc.Backend.Kind != codec.KindGo {
		t.Run("transitions", func(t *testing.T) { videoTransitions(t, b) })
	}
	t.Run("close", func(t *testing.T) { decoderClose(t, b, videoStream(b.desc.ID)) })
}

// videoTransitions runs the same seek/configuration contract on real native
// sessions as the deterministic kit tests. The high-profile fixture includes
// delayed pictures, and replay starts from an independent keyframe.
func videoTransitions(t *testing.T, b backend) {
	if b.desc.ID != av.CodecH264 {
		t.Skipf("no transition fixture for %s", b.desc.ID)
	}
	stream, packets, err := demuxFixture("h264_high_aac.mp4", av.MediaVideo)
	if err != nil {
		t.Fatal(err)
	}
	stream.Epoch = 1
	decoder := openDecoder(t, b, stream)
	if decoder == nil {
		return
	}
	defer decoder.Close()
	result := newDecodeResult(av.MediaVideo)
	for cycle := range 3 {
		for _, packet := range packets[:min(3, len(packets))] {
			if err := decoder.DecodeInto(t.Context(), &packet, &result); err != nil {
				t.Fatal(err)
			}
			drainFrames(&result, nil)
		}
		if err := decoder.HandleEvent(t.Context(), &av.Event{Type: av.EventDiscontinuity, StreamID: stream.ID}); err != nil {
			t.Fatal(err)
		}
		stream.Epoch = av.Epoch(cycle + 2)
		if err := decoder.HandleEvent(t.Context(), &av.Event{Type: av.EventCodecChanged, Stream: &stream, Epoch: stream.Epoch}); err != nil {
			t.Fatal(err)
		}
		offset := int64(cycle+1) * 1_000_000
		frames := 0
		check := func(frame *av.Frame) {
			frames++
			if frame.PTS.Value < offset || frame.CodecEpoch != stream.Epoch {
				t.Errorf("%s: stale frame after transition: PTS=%d epoch=%d, want PTS >= %d epoch=%d", b.name(), frame.PTS.Value, frame.CodecEpoch, offset, stream.Epoch)
			}
		}
		for _, packet := range packets {
			packet.PTS.Value += offset
			packet.DTS.Value += offset
			packet.CodecEpoch = stream.Epoch
			if err := decoder.DecodeInto(t.Context(), &packet, &result); err != nil {
				t.Fatal(err)
			}
			drainFrames(&result, check)
		}
		if err := decoder.FlushInto(t.Context(), &result); err != nil {
			t.Fatal(err)
		}
		drainFrames(&result, check)
		if frames != len(packets) {
			t.Fatalf("%s: %d frames after transition, want %d", b.name(), frames, len(packets))
		}
	}
}

// videoRoundTrip checks the encoder's timestamps and keyframes, then
// decodes its output with every decoder of the codec.
func videoRoundTrip(t *testing.T, b backend, decoders []backend) {
	settings := withBitrate(b, testBitrate)
	_, gop := b.desc.Knob("gop")
	if gop || b.desc.Knobs == nil {
		settings.KeyframeInterval = testGOP
	}
	out, ok := encodeVideo(t, b, settings)
	if !ok {
		return
	}
	checkPackets(t, b, out.packets, gop)
	if b.desc.ID == av.CodecH264 && len(out.stream.Codec.ExtraData.Bytes) != 0 {
		if extra := out.stream.Codec.ExtraData.Bytes; extra[0] != 1 && !strings.HasPrefix(string(extra), "\x00\x00") {
			t.Errorf("%s: extradata is neither an avcC record nor Annex B: % x", b.name(), extra[:min(8, len(extra))])
		}
	}
	for _, d := range decoders {
		t.Run("decode/"+d.desc.Backend.Name, func(t *testing.T) {
			checkDecoded(t, d, decodeAll(t, d, out.stream, out.packets), minPSNR)
		})
	}
}

// checkPackets pins one packet per frame with the frame's timestamps, DTS in
// decode order never after PTS, and keyframes where they were asked for.
func checkPackets(t *testing.T, b backend, packets []av.Packet, gop bool) {
	t.Helper()
	if len(packets) != videoFrames {
		t.Fatalf("%s: %d packets for %d frames", b.name(), len(packets), videoFrames)
	}
	seen := map[int64]bool{}
	keyframes := map[int64]bool{}
	for i, p := range packets {
		if p.PTS.Base != videoBase || p.PTS.Value%frameTicks != 0 || p.PTS.Value < 0 || p.PTS.Value >= videoFrames*frameTicks || seen[p.PTS.Value] {
			t.Fatalf("%s: packet %d PTS %+v is not one of the frames'", b.name(), i, p.PTS)
		}
		seen[p.PTS.Value] = true
		if p.Duration.Value != frameTicks {
			t.Errorf("%s: packet %d duration %+v, want %d", b.name(), i, p.Duration, frameTicks)
		}
		if p.DTS.Value > p.PTS.Value {
			t.Errorf("%s: packet %d DTS %d after PTS %d", b.name(), i, p.DTS.Value, p.PTS.Value)
		}
		if i > 0 && p.DTS.Value < packets[i-1].DTS.Value {
			t.Errorf("%s: packet %d DTS %d goes back from %d", b.name(), i, p.DTS.Value, packets[i-1].DTS.Value)
		}
		if p.Keyframe {
			keyframes[p.PTS.Value/frameTicks] = true
		}
		if len(p.Payload.Bytes) == 0 {
			t.Errorf("%s: packet %d is empty", b.name(), i)
		}
	}
	if !keyframes[0] {
		t.Errorf("%s: the first frame is not a keyframe", b.name())
	}
	if !keyframes[forcedKeyAt] {
		t.Errorf("%s: frame %d is not a keyframe after a keyframe request", b.name(), forcedKeyAt)
	}
	if gop {
		last := int64(0)
		for n := int64(1); n < videoFrames; n++ {
			if keyframes[n] {
				last = n
			} else if n-last >= testGOP {
				t.Errorf("%s: no keyframe within %d frames before frame %d (gop %d)", b.name(), testGOP, n, testGOP)
				break
			}
		}
	}
}

// checkDecoded pins every source frame decoded once, in presentation order,
// at minimum luma PSNR.
func checkDecoded(t *testing.T, d backend, frames []*av.Frame, floor float64) {
	t.Helper()
	if frames == nil {
		return
	}
	if len(frames) != videoFrames {
		t.Fatalf("%s: decoded %d frames, want %d", d.name(), len(frames), videoFrames)
	}
	worst, sum := math.Inf(1), 0.0
	for i, frame := range frames {
		if frame.PTS.Value != int64(i)*frameTicks {
			t.Fatalf("%s: frame %d PTS %d, want %d in presentation order", d.name(), i, frame.PTS.Value, int64(i)*frameTicks)
		}
		psnr := lumaPSNR(pattern(i), frame)
		worst, sum = min(worst, psnr), sum+min(psnr, 99)
	}
	mean := sum / float64(len(frames))
	if mean < floor || worst < floor-6 {
		t.Errorf("%s: luma PSNR mean %.1f dB, worst %.1f dB, want a mean of at least %d and no frame below %d", d.name(), mean, worst, int(floor), int(floor-6))
	}
	t.Logf("%s: luma PSNR mean %.1f dB, worst %.1f dB over %d frames", d.name(), mean, worst, len(frames))
}

// videoMux writes the encoder's output into an MP4 and decodes it back.
func videoMux(t *testing.T, b backend, decoders []backend) {
	if !muxable(b.desc.ID) {
		t.Skipf("the MP4 muxer does not write %s", b.desc.ID)
	}
	out, ok := encodeVideo(t, b, withBitrate(b, testBitrate))
	if !ok {
		return
	}
	packets := make([]*av.Packet, len(out.packets))
	for i := range out.packets {
		packets[i] = &out.packets[i]
	}
	for _, fragmented := range []bool{false, true} {
		layout := map[bool]string{false: "progressive", true: "fragmented"}[fragmented]
		t.Run(layout, func(t *testing.T) {
			file, err := muxMP4(out.stream, packets, fragmented)
			if err != nil {
				t.Fatalf("%s: mux: %v", b.name(), err)
			}
			stream, demuxed, err := demux(file, av.MediaVideo)
			if err != nil {
				t.Fatalf("%s: demux: %v", b.name(), err)
			}
			if len(demuxed) != videoFrames || stream.Codec.Width != videoWidth || stream.Codec.Height != videoHeight {
				t.Fatalf("%s: demuxed %d packets of %dx%d", b.name(), len(demuxed), stream.Codec.Width, stream.Codec.Height)
			}
			// The sample entry needs the codec configuration even when the
			// encoder learns it only with its first packet.
			if b.desc.ID == av.CodecH264 && len(stream.Codec.ExtraData.Bytes) == 0 {
				t.Fatalf("%s: the %s MP4 has no avcC", b.name(), layout)
			}
			for _, d := range decoders {
				t.Run("decode/"+d.desc.Backend.Name, func(t *testing.T) {
					checkDecoded(t, d, decodeAll(t, d, stream, demuxed), minPSNR)
				})
			}
		})
	}
}

// encoderKnobs pins that declared knobs refuse a misspelled name, with a
// suggestion, and a value outside a knob's range.
func encoderKnobs(t *testing.T, b backend) {
	if b.desc.Knobs == nil {
		t.Skipf("%s declares no knobs", b.name())
	}
	if len(b.desc.Knobs) == 0 {
		return
	}
	name := b.desc.Knobs[0].Name
	typo := name[:len(name)-1] + "x"
	_, err := b.encoder.NewEncoder(context.Background(), configFor(b, codec.CodecSettings{Custom: av.Metadata{typo: "1"}}))
	if !errors.Is(err, codec.ErrInvalidSetting) || !strings.Contains(err.Error(), typo) {
		t.Errorf("%s: misspelled knob %q: err = %v, want codec.ErrInvalidSetting naming it", b.name(), typo, err)
	}
	for _, knob := range b.desc.Knobs {
		if knob.Type != codec.KnobInt || knob.Max == 0 {
			continue
		}
		_, err := b.encoder.NewEncoder(context.Background(), configFor(b, codec.CodecSettings{Custom: av.Metadata{knob.Name: "99999999999"}}))
		if !errors.Is(err, codec.ErrInvalidSetting) {
			t.Errorf("%s: %s out of range: err = %v, want codec.ErrInvalidSetting", b.name(), knob.Name, err)
		}
		break
	}
}

func configFor(b backend, settings codec.CodecSettings) codec.EncodeConfig {
	if b.desc.Type == av.MediaAudio {
		return audioEncodeConfig(b, settings)
	}
	return encodeConfig(b, settings)
}

// videoBitrate changes the bitrate mid-stream: the encoder must apply it,
// live or by restarting on a keyframe, never ignore it.
func videoBitrate(t *testing.T, b backend) {
	if _, ok := b.desc.Knob("bitrate"); !ok && b.desc.Knobs != nil {
		t.Skipf("%s takes no bitrate", b.name())
	}
	encoder := openEncoder(t, b, encodeConfig(b, withBitrate(b, testBitrate)))
	if encoder == nil {
		return
	}
	defer encoder.Close()
	ctx := context.Background()
	result := newEncodeResult(av.MediaVideo)
	packets := 0
	for n := range videoFrames {
		if n == 10 {
			event := av.Event{Type: av.EventBitrateChanged, Metadata: codec.BitrateMetadata(testBitrate / 4)}
			if err := encoder.HandleEvent(ctx, &event); err != nil {
				t.Fatalf("%s: bitrate change: %v", b.name(), err)
			}
		}
		if err := encoder.EncodeInto(ctx, pattern(n), &result); err != nil {
			t.Fatalf("%s: encode frame %d after the change: %v", b.name(), n, err)
		}
		packets += drainPackets(&result, nil)
	}
	if err := encoder.FlushInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	packets += drainPackets(&result, nil)
	if packets != videoFrames {
		t.Errorf("%s: %d packets across a bitrate change, want %d", b.name(), packets, videoFrames)
	}
}

// encoderClose pins idempotent Close, ErrClosed after it, and no goroutine
// left behind by many sessions.
func encoderClose(t *testing.T, b backend, config codec.EncodeConfig, frame *av.Frame) {
	before := runtime.NumGoroutine()
	for range 8 {
		encoder := openEncoder(t, b, config)
		if encoder == nil {
			return
		}
		result := newEncodeResult(b.desc.Type)
		if err := encoder.EncodeInto(context.Background(), frame, &result); err != nil {
			t.Fatalf("%s: encode: %v", b.name(), err)
		}
		drainPackets(&result, nil)
		if err := encoder.Close(); err != nil {
			t.Fatalf("%s: close: %v", b.name(), err)
		}
		if err := encoder.Close(); err != nil {
			t.Fatalf("%s: second close: %v", b.name(), err)
		}
		if err := encoder.EncodeInto(context.Background(), frame, &result); !errors.Is(err, codec.ErrClosed) {
			t.Fatalf("%s: encode after close: err = %v, want codec.ErrClosed", b.name(), err)
		}
	}
	settleGoroutines(t, b, before)
}

func decoderClose(t *testing.T, b backend, stream av.Stream) {
	before := runtime.NumGoroutine()
	for range 8 {
		decoder := openDecoder(t, b, stream)
		if decoder == nil {
			return
		}
		if err := decoder.Close(); err != nil {
			t.Fatalf("%s: close: %v", b.name(), err)
		}
		if err := decoder.Close(); err != nil {
			t.Fatalf("%s: second close: %v", b.name(), err)
		}
		result := newDecodeResult(b.desc.Type)
		packet := av.Packet{Payload: av.Buffer{Bytes: []byte{0, 0, 0, 1, 0x65}}}
		if err := decoder.DecodeInto(context.Background(), &packet, &result); !errors.Is(err, codec.ErrClosed) {
			t.Fatalf("%s: decode after close: err = %v, want codec.ErrClosed", b.name(), err)
		}
	}
	settleGoroutines(t, b, before)
}

func settleGoroutines(t *testing.T, b backend, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("%s: %d goroutines after closing every session, %d before", b.name(), after, before)
	}
}

// videoReference decodes bitstreams independent encoders made and matches
// every picture against the pure-Go decoder: conforming H.264 decoders agree
// sample for sample.
func videoReference(t *testing.T, b backend, decoders []backend) {
	// Constrained Baseline must decode sample for sample; on High with
	// B-frames OpenH264 drifts slightly from the reference decoders, so that
	// stream is held to a high PSNR instead.
	files := map[av.CodecID][]struct {
		name  string
		floor float64
	}{av.CodecH264: {{"h264_baseline.mp4", math.Inf(1)}, {"h264_high_aac.mp4", 40}}}[b.desc.ID]
	if len(files) == 0 {
		t.Skipf("no reference bitstream for %s", b.desc.ID)
	}
	var reference backend
	for _, d := range decoders {
		if d.desc.Backend.Kind == codec.KindGo && d.desc.Backend.Name != b.desc.Backend.Name {
			reference = d
		}
	}
	for _, file := range files {
		t.Run(strings.TrimSuffix(file.name, ".mp4"), func(t *testing.T) {
			stream, packets, err := demuxFixture(file.name, av.MediaVideo)
			if err != nil {
				t.Fatal(err)
			}
			got := decodeAll(t, b, stream, packets)
			if got == nil {
				return
			}
			if len(got) != len(packets) {
				t.Fatalf("%s: decoded %d frames from %d packets", b.name(), len(got), len(packets))
			}
			for i := 1; i < len(got); i++ {
				if got[i].PTS.Value <= got[i-1].PTS.Value {
					t.Fatalf("%s: frame %d PTS %d not after %d: not presentation order", b.name(), i, got[i].PTS.Value, got[i-1].PTS.Value)
				}
			}
			if reference.decoder == nil {
				return
			}
			want := decodeAll(t, reference, stream, packets)
			worst := math.Inf(1)
			for i := range min(len(want), len(got)) {
				worst = min(worst, lumaPSNR(want[i], got[i]))
			}
			if len(want) != len(got) || worst < file.floor {
				t.Errorf("%s vs %s: %d and %d frames, worst luma PSNR %.1f dB", b.name(), reference.name(), len(got), len(want), worst)
			}
			t.Logf("%s matches %s: worst luma PSNR %.1f dB over %d frames", b.name(), reference.name(), worst, len(got))
		})
	}
}

// videoLoss feeds a stream with a gap: the decoder must survive it, ask for
// a keyframe, and decode again from the next one.
func videoLoss(t *testing.T, b backend) {
	if b.desc.ID != av.CodecH264 {
		t.Skipf("no reference bitstream for %s", b.desc.ID)
	}
	stream, packets, err := demuxFixture("h264_baseline.mp4", av.MediaVideo)
	if err != nil {
		t.Fatal(err)
	}
	decoder := openDecoder(t, b, stream)
	if decoder == nil {
		return
	}
	defer decoder.Close()
	result := newDecodeResult(av.MediaVideo)
	requests, frames := 0, 0
	for i := range packets {
		if i == 3 {
			continue // lost
		}
		packet := packets[i]
		packet.LossBefore = i == 4
		if err := decoder.DecodeInto(context.Background(), &packet, &result); err != nil {
			t.Fatalf("%s: decode after loss: %v", b.name(), err)
		}
		f, r := drainFrames(&result, nil)
		frames, requests = frames+f, requests+r
	}
	if requests == 0 {
		t.Errorf("%s: no keyframe request after a loss", b.name())
	}
	if frames == 0 {
		t.Errorf("%s: decoded nothing around a loss", b.name())
	}
}

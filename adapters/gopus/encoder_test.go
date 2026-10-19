package gopus

import (
	"context"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	gopuslib "github.com/thesyncim/gopus"
)

func TestOpusEncoderBitrateChangedRetargetsLive(t *testing.T) {
	ctx := context.Background()
	encoder := &Encoder{}
	if err := encoder.Open(ctx, opusEncodeConfig()); err != nil {
		t.Fatal(err)
	}

	// 96_000 bits per second retargets the live Opus encoder, up from the
	// 64 kbps it was opened with; libopus applies it from the next frame.
	event := av.Event{Type: av.EventBitrateChanged, StreamID: "encoded-audio", Metadata: codec.BitrateMetadata(96_000)}
	if err := encoder.HandleEvent(ctx, &event); err != nil {
		t.Fatal(err)
	}
	if got := encoder.encoder.Bitrate(); got != 96_000 {
		t.Fatalf("encoder bitrate = %d, want 96000", got)
	}

	// The retargeted encoder keeps encoding.
	result := opusEncodeResult(1, 4000)
	frame := opusTestFrame()
	if err := encoder.EncodeInto(ctx, &frame, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Packets) != 1 || len(result.Packets[0].Payload.Bytes) == 0 {
		t.Fatalf("packets after retarget = %+v, want one non-empty packet", result.Packets)
	}
}

func TestOpusEncoderBitrateChangedRejectsMalformedEvent(t *testing.T) {
	ctx := context.Background()
	encoder, err := NewEncoderFactory().NewEncoder(ctx, opusEncodeConfig())
	if err != nil {
		t.Fatal(err)
	}

	for name, event := range map[string]av.Event{
		"no metadata":   {Type: av.EventBitrateChanged, StreamID: "encoded-audio"},
		"garbage rate":  {Type: av.EventBitrateChanged, StreamID: "encoded-audio", Metadata: av.Metadata{av.MetadataBitrate: "loud"}},
		"negative rate": {Type: av.EventBitrateChanged, StreamID: "encoded-audio", Metadata: av.Metadata{av.MetadataBitrate: "-1"}},
	} {
		err := encoder.HandleEvent(ctx, &event)
		if err == nil || !strings.Contains(err.Error(), av.MetadataBitrate) {
			t.Fatalf("%s: err = %v, want a clear %s rejection", name, err, av.MetadataBitrate)
		}
	}
}

// TestOpusEncoderReframesArbitraryFrameSizes pins re-framing: AAC decoders
// deliver 1024-sample frames and resamplers deliver whatever the rate ratio
// gives, while Opus encodes only fixed durations. 1024-sample input becomes
// 960-sample (20 ms) packets whose timestamps advance by exactly 960, and end
// of stream flushes the partial tail with its real duration.
func TestOpusEncoderReframesArbitraryFrameSizes(t *testing.T) {
	ctx := context.Background()
	encoder := &Encoder{}
	if err := encoder.Open(ctx, opusEncodeConfig()); err != nil {
		t.Fatal(err)
	}
	const inputFrames, frameSamples = 10, 1024
	var packets []av.Packet
	collect := func(result *codec.EncodeResult) {
		for i := range result.Packets {
			packet := result.Packets[i]
			packet.Payload.Bytes = append([]byte(nil), packet.Payload.Bytes...)
			packets = append(packets, packet)
		}
	}
	for i := range inputFrames {
		frame := opusTestFrame()
		pcm := make([]byte, frameSamples*2)
		for s := range frameSamples {
			pcm[s*2] = byte(s)
		}
		frame.Planes[0].Buffer.Bytes = pcm
		frame.Audio.Samples = frameSamples
		frame.PTS = av.Timestamp{Value: int64(i * frameSamples), Base: av.TimeBase{Num: 1, Den: 48000}}
		frame.Duration = av.SamplesDuration(frameSamples, 48000)
		result := opusEncodeResult(8, 4000)
		if err := encoder.EncodeInto(ctx, &frame, &result); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		collect(&result)
	}
	result := opusEncodeResult(8, 4000)
	if err := encoder.FlushInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	collect(&result)

	total := inputFrames * frameSamples // 10240 = 10 whole 960-sample packets + 640
	if want := total/960 + 1; len(packets) != want {
		t.Fatalf("packets = %d, want %d", len(packets), want)
	}
	for i, packet := range packets {
		if want := int64(i*960 - 312); packet.PTS.Value != want || packet.PTS.Base.Den != 48000 {
			t.Fatalf("packet %d pts = %+v, want %d at 1/48000", i, packet.PTS, want)
		}
		if len(packet.Payload.Bytes) == 0 {
			t.Fatalf("packet %d is empty", i)
		}
	}
	// The tail packet carries the last 640 input samples plus the 312-sample
	// lookahead the flush drains, so its 952 decoded samples all carry input.
	if last := packets[len(packets)-1]; last.Duration != av.SamplesDuration(total%960+312, 48000) {
		t.Fatalf("tail duration = %+v, want %d samples", last.Duration, total%960+312)
	}
}

// TestOpusEncoderControlIsTyped pins codec.Control's typed form: the adapter
// hands the callback its *gopus.Encoder, and a callback for another type
// fails the open naming both types.
func TestOpusEncoderControlIsTyped(t *testing.T) {
	ctx := context.Background()
	config := opusEncodeConfig()
	var got int
	codec.Control(func(e *gopuslib.Encoder) error {
		if err := e.SetComplexity(3); err != nil {
			return err
		}
		got = e.Complexity()
		return nil
	})(&config.Settings)
	if err := (&Encoder{}).Open(ctx, config); err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("complexity = %d after Control, want 3", got)
	}

	config = opusEncodeConfig()
	codec.Control(func(*strings.Builder) error { return nil })(&config.Settings)
	err := (&Encoder{}).Open(ctx, config)
	if err == nil || !strings.Contains(err.Error(), "Control wants *strings.Builder") {
		t.Fatalf("open with a mistyped Control: err = %v, want the type mismatch", err)
	}
}

// TestOpusEncodeAllocs pins the steady-state encode path: a 20 ms frame
// encodes into a caller-owned result without allocating.
func TestOpusEncodeAllocs(t *testing.T) {
	ctx := context.Background()
	encoder := &Encoder{}
	if err := encoder.Open(ctx, opusEncodeConfig()); err != nil {
		t.Fatal(err)
	}
	frame := opusTestFrame()
	frame.Planes[0].Buffer.Bytes = make([]byte, 960*2)
	frame.Audio.Samples = 960
	frame.Duration = av.SamplesDuration(960, 48000)
	result := opusEncodeResult(8, 4000)
	n := int64(0)
	if allocs := testing.AllocsPerRun(1000, func() {
		frame.PTS = av.Timestamp{Value: n * 960, Base: av.TimeBase{Num: 1, Den: 48000}}
		n++
		result.Packets, result.Events = result.Packets[:0], result.Events[:0]
		if err := encoder.EncodeInto(ctx, &frame, &result); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("encode allocs = %v, want 0", allocs)
	}
}

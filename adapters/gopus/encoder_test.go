package gopus

import (
	"context"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
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
		event := event
		err := encoder.HandleEvent(ctx, &event)
		if err == nil || !strings.Contains(err.Error(), av.MetadataBitrate) {
			t.Fatalf("%s: err = %v, want a clear %s rejection", name, err, av.MetadataBitrate)
		}
	}
}

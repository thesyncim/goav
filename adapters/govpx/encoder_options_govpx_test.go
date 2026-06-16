package govpx

import (
	"testing"

	"github.com/thesyncim/goav/codec"
	govpxlib "github.com/thesyncim/govpx"
)

func TestVP8EncoderOptionsRealtimePreset(t *testing.T) {
	rt := vp8EncoderOptions(codec.EncodeConfig{
		Realtime: true,
		Settings: codec.CodecSettings{Bitrate: 600_000, KeyframeInterval: 120},
	}, 640, 360)

	if rt.RateControlMode != govpxlib.RateControlCBR {
		t.Errorf("rate control = %v, want CBR", rt.RateControlMode)
	}
	if rt.Deadline != govpxlib.DeadlineRealtime {
		t.Errorf("deadline = %v, want realtime", rt.Deadline)
	}
	if rt.CpuUsed != -3 {
		t.Errorf("cpu used = %d, want -3", rt.CpuUsed)
	}
	if !rt.ErrorResilient {
		t.Error("realtime VP8 must be error resilient for packet loss")
	}
	if rt.MinQuantizer != 4 || rt.MaxQuantizer != 56 {
		t.Errorf("quantizer range = %d..%d, want 4..56", rt.MinQuantizer, rt.MaxQuantizer)
	}
	if rt.KeyFrameInterval != 120 {
		t.Errorf("keyframe interval = %d, want 120 from CodecSettings", rt.KeyFrameInterval)
	}
	if rt.BufferSizeMs != 1000 || rt.BufferInitialSizeMs != 500 || rt.BufferOptimalSizeMs != 600 {
		t.Errorf("buffer model = %d/%d/%d, want 1000/500/600", rt.BufferSizeMs, rt.BufferInitialSizeMs, rt.BufferOptimalSizeMs)
	}
	if !rt.DropFrameAllowed {
		t.Error("realtime CBR should allow frame drops to hold bitrate")
	}

	vbr := vp8EncoderOptions(codec.EncodeConfig{Settings: codec.CodecSettings{Bitrate: 600_000}}, 640, 360)
	if vbr.Deadline == govpxlib.DeadlineRealtime || vbr.ErrorResilient {
		t.Error("non-realtime VP8 must not force the realtime preset")
	}
}

func TestVP9EncoderOptionsRealtimePreset(t *testing.T) {
	rt := vp9EncoderOptions(codec.EncodeConfig{
		Realtime: true,
		Settings: codec.CodecSettings{Bitrate: 600_000, KeyframeInterval: 90},
	}, 640, 360)

	if !rt.RateControlModeSet || rt.RateControlMode != govpxlib.RateControlCBR {
		t.Errorf("rate control = %v (set=%v), want CBR", rt.RateControlMode, rt.RateControlModeSet)
	}
	if rt.Deadline != govpxlib.DeadlineRealtime {
		t.Errorf("deadline = %v, want realtime", rt.Deadline)
	}
	if rt.CpuUsed != 9 {
		t.Errorf("cpu used = %d, want 9 (aligned with the govpx webrtc-vp9 sample)", rt.CpuUsed)
	}
	if rt.Threads != pickVP9Threads(640, 360) {
		t.Errorf("threads = %d, want the tile-column worker count %d", rt.Threads, pickVP9Threads(640, 360))
	}
	if rt.MinQuantizer != 4 || rt.MaxQuantizer != 56 {
		t.Errorf("quantizer range = %d..%d, want 4..56", rt.MinQuantizer, rt.MaxQuantizer)
	}
	if rt.MaxKeyframeInterval != 90 {
		t.Errorf("max keyframe interval = %d, want 90 from CodecSettings", rt.MaxKeyframeInterval)
	}
	if rt.BufferSizeMs != 1000 || rt.BufferInitialSizeMs != 500 || rt.BufferOptimalSizeMs != 600 {
		t.Errorf("buffer model = %d/%d/%d, want 1000/500/600", rt.BufferSizeMs, rt.BufferInitialSizeMs, rt.BufferOptimalSizeMs)
	}

	// Non-realtime keeps plain CBR without the realtime tuning.
	plain := vp9EncoderOptions(codec.EncodeConfig{Settings: codec.CodecSettings{Bitrate: 600_000}}, 640, 360)
	if plain.Deadline == govpxlib.DeadlineRealtime || plain.CpuUsed == 9 || plain.Threads != 0 {
		t.Error("non-realtime VP9 must not force the realtime preset")
	}
}

func TestPickVP9ThreadsMatchesTileColumns(t *testing.T) {
	if got := maxVP9TileColumns(640); got != 2 {
		t.Errorf("maxVP9TileColumns(640) = %d, want 2", got)
	}
	if got := maxVP9TileColumns(1280); got != 4 {
		t.Errorf("maxVP9TileColumns(1280) = %d, want 4", got)
	}
	// Thread count never exceeds the legal tile columns or the CPU count.
	for _, w := range []int{160, 640, 1280, 1920} {
		threads := pickVP9Threads(w, w*9/16)
		if threads < 1 {
			t.Fatalf("pickVP9Threads(%d) = %d, want >= 1", w, threads)
		}
		if threads > maxVP9TileColumns(w) {
			t.Errorf("pickVP9Threads(%d) = %d exceeds tile columns %d", w, threads, maxVP9TileColumns(w))
		}
	}
}

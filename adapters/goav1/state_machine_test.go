package goav1

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	backend "github.com/thesyncim/goav1"
)

func TestEnsureDecoderPreservesLiveDecoderOnBadSequenceHeader(t *testing.T) {
	state, err := NewDecoderState(DecoderStateConfig{
		Bounds: codec.DecodeBounds{MaxWidth: 64, MaxHeight: 64},
		Format: backend.FrameFormat{
			Width:        16,
			Height:       16,
			BitDepth:     8,
			MonoChrome:   true,
			SubsamplingX: true,
			SubsamplingY: true,
			Align:        defaultAlign,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	if err := state.ensureDecoder([][]byte{testRTPPayload()}); err != nil {
		t.Fatalf("initial ensureDecoder: %v", err)
	}
	live := state.dec
	if live == nil {
		t.Fatal("expected live backend decoder")
	}

	// Bad seed payload must fail without destroying the live decoder.
	if err := state.ensureDecoder([][]byte{{0x00, 0x01, 0xff}}); err == nil {
		t.Fatal("expected error for invalid seed payload")
	}
	if state.dec != live {
		t.Fatal("failed ensureDecoder destroyed live decoder")
	}
}

func TestDecoderDroppingBlocksContinuesPreviousAndBoundsInitBuf(t *testing.T) {
	ctx := context.Background()
	state, err := NewDecoderState(DecoderStateConfig{
		Bounds: codec.DecodeBounds{MaxWidth: 64, MaxHeight: 64},
		Format: backend.FrameFormat{
			Width:        16,
			Height:       16,
			BitDepth:     8,
			MonoChrome:   true,
			SubsamplingX: true,
			SubsamplingY: true,
			Align:        defaultAlign,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	dec := &Decoder{}
	if err := dec.Open(ctx, codec.DecodeConfig{
		Stream:      av.Stream{ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecAV1, Width: 16, Height: 16, PixelFormat: av.PixelFormatGray8}},
		OpaqueState: state,
		Resilience:  codec.ResiliencePolicy{RequestKeyframes: true},
	}); err != nil {
		t.Fatal(err)
	}
	defer dec.Close()

	// Enter Dropping state via packet loss.
	if err := dec.HandleEvent(ctx, &av.Event{Type: av.EventPacketLoss}); err != nil {
		t.Fatal(err)
	}
	if dec.lifecycle != goav1DecoderDropping {
		t.Fatalf("lifecycle = %d, want goav1DecoderDropping", dec.lifecycle)
	}

	// Stray ContinuesPrevious (Z=1 = 0x80) without StartsNewCodedVideoSequence (N=0)
	// must NOT bypass Dropping state.
	stray := av.Packet{
		Payload: av.Buffer{Bytes: []byte{0x80, 0x10, 0x00}},
	}
	out := codec.DecodeResult{
		Frames:   make([]av.Frame, 0, 1),
		Requests: make([]codec.ControlRequest, 0, 2),
	}
	if err := dec.DecodeRTPPayloadInto(ctx, &stray, &out); err != nil {
		t.Fatalf("stray packet err = %v", err)
	}
	if len(out.Frames) != 0 {
		t.Fatalf("stray packet decoded %d frames, want 0", len(out.Frames))
	}
	if len(out.Requests) != 1 || out.Requests[0].Type != codec.ControlRequestKeyframe {
		t.Fatalf("requests = %+v, want 1 keyframe request", out.Requests)
	}
	if len(state.initPayloads) != 0 {
		t.Fatalf("stray packet appended to initPayloads (%d)", len(state.initPayloads))
	}

	// Bound initPayloads growth when ContinuesNext fragments arrive endlessly.
	dec.lifecycle = goav1DecoderAwaitingSeqHeader
	frag := av.Packet{
		Payload: av.Buffer{Bytes: []byte{0x40, 0x78}}, // Y=1 (ContinuesNext), padding OBU
	}
	var hitLimit bool
	for range maxAV1PayloadEvents + 2 {
		out.Reset()
		if err := dec.DecodeRTPPayloadInto(ctx, &frag, &out); err != nil {
			hitLimit = true
			break
		}
	}
	if !hitLimit {
		t.Fatal("expected initPayloads bound to trigger error")
	}
	if len(state.initPayloads) != 0 || len(state.initBuf) != 0 {
		t.Fatalf("init fragments not reset after bound exceeded: payloads=%d buf=%d", len(state.initPayloads), len(state.initBuf))
	}
}

func TestEncoderFailedResetPreservesStateAndBitrate(t *testing.T) {
	ctx := context.Background()
	controlErr := errors.New("control fail")

	enc := &Encoder{}
	cfg := codec.EncodeConfig{
		Parameters: av.CodecParameters{ID: av.CodecAV1, Width: 64, Height: 64},
		Settings:   codec.Settings{Bitrate: 250_000},
	}
	if err := enc.Open(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer enc.Close()

	// Make Control fail on next resetEncoder (triggered by EventBitrateChanged).
	enc.config.Settings.Control = func(any) error { return controlErr }
	live := enc.encoder
	err := enc.HandleEvent(ctx, &av.Event{
		Type:     av.EventBitrateChanged,
		Metadata: codec.BitrateMetadata(500_000),
	})
	if !errors.Is(err, controlErr) {
		t.Fatalf("HandleEvent err = %v, want %v", err, controlErr)
	}
	if enc.config.Settings.Bitrate != 250_000 {
		t.Fatalf("bitrate mutated to %d on failed reset, want 250000", enc.config.Settings.Bitrate)
	}
	if enc.encoder != live {
		t.Fatal("live encoder replaced on failed reset")
	}

	// Close, then failed Open must keep state goav1EncoderClosed.
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Settings.Control = func(any) error { return controlErr }
	if err := enc.Open(ctx, cfg); !errors.Is(err, controlErr) {
		t.Fatalf("Open after close err = %v, want %v", err, controlErr)
	}
	if enc.state != goav1EncoderClosed {
		t.Fatalf("state = %d after failed Open, want goav1EncoderClosed", enc.state)
	}
}

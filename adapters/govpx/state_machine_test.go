package govpx

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func TestUnopenedHandleEventDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	events := []av.Event{
		{Type: av.EventPacketLoss},
		{Type: av.EventDiscontinuity},
		{Type: av.EventCodecChanged},
		{Type: av.EventKeyframeRequired},
		{Type: av.EventBitrateChanged, Metadata: codec.BitrateMetadata(500_000)},
	}

	vp8Dec := &Decoder{}
	vp9Dec := &VP9Decoder{}
	vp8Enc := &VP8Encoder{}
	vp9Enc := &VP9Encoder{}

	for _, ev := range events {
		if err := vp8Dec.HandleEvent(ctx, &ev); !errors.Is(err, codec.ErrUnsupportedFormat) {
			t.Fatalf("VP8 Decoder.HandleEvent(%v) = %v, want ErrUnsupportedFormat", ev.Type, err)
		}
		if err := vp9Dec.HandleEvent(ctx, &ev); !errors.Is(err, codec.ErrUnsupportedFormat) {
			t.Fatalf("VP9Decoder.HandleEvent(%v) = %v, want ErrUnsupportedFormat", ev.Type, err)
		}
		if err := vp8Enc.HandleEvent(ctx, &ev); !errors.Is(err, codec.ErrUnsupportedFormat) {
			t.Fatalf("VP8Encoder.HandleEvent(%v) = %v, want ErrUnsupportedFormat", ev.Type, err)
		}
		if err := vp9Enc.HandleEvent(ctx, &ev); !errors.Is(err, codec.ErrUnsupportedFormat) {
			t.Fatalf("VP9Encoder.HandleEvent(%v) = %v, want ErrUnsupportedFormat", ev.Type, err)
		}
	}
}

func TestDoubleOpenAndFailedControlLifecycle(t *testing.T) {
	ctx := context.Background()
	controlErr := errors.New("control rejected")

	// VP8 decoder double open.
	vp8Dec := &Decoder{}
	if err := vp8Dec.Open(ctx, codec.DecodeConfig{Stream: av.Stream{Codec: av.CodecParameters{ID: av.CodecVP8, Width: 64, Height: 64}}}); err != nil {
		t.Fatal(err)
	}
	firstVP8Dec := vp8Dec.decoder
	if err := vp8Dec.Open(ctx, codec.DecodeConfig{Stream: av.Stream{Codec: av.CodecParameters{ID: av.CodecVP8, Width: 64, Height: 64}}}); err != nil {
		t.Fatal(err)
	}
	if vp8Dec.decoder == nil || vp8Dec.decoder == firstVP8Dec || vp8Dec.state != vpxDropping {
		t.Fatalf("VP8 double open did not cleanly replace handle (state=%d)", vp8Dec.state)
	}
	_ = vp8Dec.Close()

	// VP9 decoder double open.
	vp9Dec := &VP9Decoder{}
	if err := vp9Dec.Open(ctx, codec.DecodeConfig{Stream: av.Stream{Codec: av.CodecParameters{ID: av.CodecVP9, Width: 64, Height: 64}}}); err != nil {
		t.Fatal(err)
	}
	firstVP9Dec := vp9Dec.decoder
	if err := vp9Dec.Open(ctx, codec.DecodeConfig{Stream: av.Stream{Codec: av.CodecParameters{ID: av.CodecVP9, Width: 64, Height: 64}}}); err != nil {
		t.Fatal(err)
	}
	if vp9Dec.decoder == nil || vp9Dec.decoder == firstVP9Dec || vp9Dec.state != vpxDropping {
		t.Fatalf("VP9 double open did not cleanly replace handle (state=%d)", vp9Dec.state)
	}
	_ = vp9Dec.Close()

	// VP8 encoder failed Control must not attach half-configured handle or transition to open.
	vp8Enc := &VP8Encoder{}
	vp8Cfg := codec.EncodeConfig{
		Parameters: av.CodecParameters{ID: av.CodecVP8, Width: 64, Height: 64},
		Settings:   codec.Settings{Control: func(any) error { return controlErr }},
	}
	if err := vp8Enc.Open(ctx, vp8Cfg); !errors.Is(err, controlErr) {
		t.Fatalf("VP8Encoder.Open err = %v, want %v", err, controlErr)
	}
	if vp8Enc.state != vpxUnopened || vp8Enc.encoder != nil {
		t.Fatalf("VP8Encoder after failed control: state=%d encoder=%v", vp8Enc.state, vp8Enc.encoder)
	}
	// Successful open, then failed re-open must preserve live handle.
	vp8Cfg.Settings.Control = nil
	if err := vp8Enc.Open(ctx, vp8Cfg); err != nil {
		t.Fatal(err)
	}
	liveVP8 := vp8Enc.encoder
	vp8Cfg.Settings.Control = func(any) error { return controlErr }
	if err := vp8Enc.Open(ctx, vp8Cfg); !errors.Is(err, controlErr) {
		t.Fatalf("VP8Encoder re-open err = %v, want %v", err, controlErr)
	}
	if vp8Enc.state != vpxOpen || vp8Enc.encoder != liveVP8 {
		t.Fatalf("VP8Encoder live handle clobbered on failed re-open")
	}
	_ = vp8Enc.Close()

	// VP9 encoder failed Control must not attach half-configured handle or transition to open.
	vp9Enc := &VP9Encoder{}
	vp9Cfg := codec.EncodeConfig{
		Parameters: av.CodecParameters{ID: av.CodecVP9, Width: 64, Height: 64},
		Settings:   codec.Settings{Control: func(any) error { return controlErr }},
	}
	if err := vp9Enc.Open(ctx, vp9Cfg); !errors.Is(err, controlErr) {
		t.Fatalf("VP9Encoder.Open err = %v, want %v", err, controlErr)
	}
	if vp9Enc.state != vpxUnopened || vp9Enc.encoder != nil {
		t.Fatalf("VP9Encoder after failed control: state=%d encoder=%v", vp9Enc.state, vp9Enc.encoder)
	}
	vp9Cfg.Settings.Control = nil
	if err := vp9Enc.Open(ctx, vp9Cfg); err != nil {
		t.Fatal(err)
	}
	liveVP9 := vp9Enc.encoder
	vp9Cfg.Settings.Control = func(any) error { return controlErr }
	if err := vp9Enc.Open(ctx, vp9Cfg); !errors.Is(err, controlErr) {
		t.Fatalf("VP9Encoder re-open err = %v, want %v", err, controlErr)
	}
	if vp9Enc.state != vpxOpen || vp9Enc.encoder != liveVP9 {
		t.Fatalf("VP9Encoder live handle clobbered on failed re-open")
	}
	_ = vp9Enc.Close()
}

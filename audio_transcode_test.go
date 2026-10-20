package goav_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
)

// TestOpusRefusesUnsupportedRateWithResampleFix pins the open-time descriptor
// check: a 44.1 kHz stream into Opus (8/12/16/24/48 kHz only) is refused as a
// typed BuildError naming the exact .Resample(...) to insert, not the
// adapter's bare unsupported-format sentinel. (The AAC-to-Opus transcode leg
// lives with the opt-in AAC decoder, in the gplcodec module.)
func TestOpusRefusesUnsupportedRateWithResampleFix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := offlineRuntime().Run(ctx, goavtest.Audio(44100, 1, make([]int16, 1024)).
		Audio().
		Encode(codec.Opus()).
		To(goav.Write("out.webm", &bytes.Buffer{})))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %v, want a *goav.BuildError", err)
	}
	if buildErr.Code != errcode.Code("encode_adapter_incompatible") {
		t.Fatalf("code = %q, want encode_adapter_incompatible\nerr = %v", buildErr.Code, err)
	}
	if !strings.Contains(err.Error(), "insert .Resample(48000, 1) before .Encode(...)") {
		t.Fatalf("refusal does not name the resample fix:\n%v", err)
	}
}

// TestBundleAACDecodeNamesTheOptIns pins the refusal a default bundle gives
// AAC: no pure-Go AAC decoder is bundled (the only one is GPL), so decoding
// AAC is refused with both opt-ins named — Apple's decoder through libcodec,
// or the gplcodec module.
func TestBundleAACDecodeNamesTheOptIns(t *testing.T) {
	aac := goavtest.Packets(av.CodecAAC, av.Packet{Keyframe: true, Payload: av.Buffer{Bytes: []byte{1}}})
	_, err := offlineRuntime().Build(context.Background(), aac.Audio().Decode().To(goavtest.NewCollector().Sink()))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.Code("decode_adapter_missing") {
		t.Fatalf("err = %v, want decode_adapter_missing", err)
	}
	for _, fix := range []string{"bundle.New(libcodec.AudioToolbox())", "bundle.New(gplcodec.AAC())"} {
		if !strings.Contains(err.Error(), fix) {
			t.Errorf("refusal does not name %s:\n%v", fix, err)
		}
	}
}

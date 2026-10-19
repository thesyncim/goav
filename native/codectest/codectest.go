// Package codectest is the conformance suite for codec backends. One call,
// codectest.Run(t, option), checks every encoder and decoder the option
// registers — native or pure Go — against goav's reference codecs and
// against each other.
package codectest

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/native/kit"
	"github.com/thesyncim/goav/runconfig"
)

// backend is one registered encoder or decoder.
type backend struct {
	desc    codec.Descriptor
	mode    codec.Mode
	encoder codec.EncoderFactory
	decoder codec.DecoderFactory
}

func (b backend) name() string {
	return string(b.desc.ID) + "/" + b.desc.Backend.Name
}

// Run checks every backend the options register. Each encoder: a round trip
// through every decoder of its codec (luma PSNR or audio SNR against the
// source), frame counts, timestamps, keyframes on request and on the GOP,
// decode-order timestamps, the codec configuration and an MP4 mux that
// demuxes back, its knobs refusing bad settings, a live bitrate change, and
// Close. Each decoder: reference bitstreams from independent encoders,
// matched against the pure-Go decoder, loss recovery, and Close. Then the
// matrix: every encoder's output through every decoder of the same codec.
//
// A backend that cannot run here (its library is missing) is skipped with
// the fix, unless the GOAV_NATIVE_REQUIRE environment variable names it
// (backend names, comma-separated, or "all"), which fails the test instead.
func Run(t *testing.T, options ...runconfig.Option) {
	t.Helper()
	under, err := discover(options)
	if err != nil {
		t.Fatalf("codectest: %v", err)
	}
	missing := notInstalled(options)
	for _, desc := range missing {
		t.Run(string(desc.ID)+"/"+desc.Backend.Name, func(t *testing.T) {
			reason := desc.Backend.Status
			if err := kit.Probe(options...); err != nil {
				reason = err.Error()
			}
			if required(desc.Backend.Name) {
				t.Fatalf("%s/%s is required (GOAV_NATIVE_REQUIRE) but %s", desc.ID, desc.Backend.Name, reason)
			}
			t.Skipf("%s/%s cannot run here: %s", desc.ID, desc.Backend.Name, reason)
		})
	}
	if len(under) == 0 {
		if len(missing) == 0 {
			t.Fatal("codectest: the options register no encoder or decoder")
		}
		return
	}
	refs, err := discover(referenceOptions())
	if err != nil {
		t.Fatalf("codectest: reference codecs: %v", err)
	}
	for _, b := range under {
		t.Run(b.name()+"/"+string(b.mode), func(t *testing.T) {
			if b.desc.Type == av.MediaAudio {
				runAudio(t, b, decodersOf(b.desc.ID, under, refs))
			} else {
				runVideo(t, b, decodersOf(b.desc.ID, under, refs))
			}
		})
	}
	t.Run("matrix", func(t *testing.T) { runMatrix(t, under, refs) })
}

// discover lists the encoders and decoders the options register.
func discover(options []runconfig.Option) ([]backend, error) {
	config, err := runconfig.NewConfig(options...)
	if err != nil {
		return nil, err
	}
	var out []backend
	seen := map[string]bool{}
	for _, desc := range config.Codecs.Descriptors() {
		for _, mode := range desc.Modes {
			choice, err := config.Codecs.Select(codec.Need{ID: desc.ID, Mode: mode, Backend: desc.Backend.Name})
			key := string(desc.ID) + "/" + desc.Backend.Name + "/" + string(mode)
			if err != nil || seen[key] || !runnable(choice.Descriptor) {
				continue
			}
			seen[key] = true
			out = append(out, backend{desc: choice.Descriptor, mode: mode, encoder: choice.Encoder, decoder: choice.Decoder})
		}
	}
	return out, nil
}

// decodersOf lists every decoder of id, the backends under test first.
func decodersOf(id av.CodecID, sets ...[]backend) []backend {
	var out []backend
	seen := map[string]bool{}
	for _, set := range sets {
		for _, b := range set {
			if b.desc.ID == id && b.mode == codec.ModeDecode && !seen[b.desc.Backend.Name] {
				seen[b.desc.Backend.Name] = true
				out = append(out, b)
			}
		}
	}
	return out
}

// required reports whether GOAV_NATIVE_REQUIRE names the backend.
func required(name string) bool {
	for _, want := range strings.Split(os.Getenv("GOAV_NATIVE_REQUIRE"), ",") {
		if want = strings.TrimSpace(want); want != "" && (want == "all" || strings.EqualFold(want, name)) {
			return true
		}
	}
	return false
}

// unavailable skips (or, when required, fails) a backend that cannot run
// here, and reports whether it did.
func unavailable(t *testing.T, b backend, err error) bool {
	t.Helper()
	if !errors.Is(err, codec.ErrUnavailable) {
		return false
	}
	if required(b.desc.Backend.Name) {
		t.Fatalf("%s is required (GOAV_NATIVE_REQUIRE) but unavailable: %v", b.name(), err)
	}
	t.Skipf("%s unavailable: %v", b.name(), err)
	return true
}

func openEncoder(t *testing.T, b backend, config codec.EncodeConfig) codec.Encoder {
	t.Helper()
	encoder, err := b.encoder.NewEncoder(context.Background(), config)
	if unavailable(t, b, err) {
		return nil
	}
	if err != nil {
		t.Fatalf("%s: open encoder: %v", b.name(), err)
	}
	return encoder
}

func openDecoder(t *testing.T, b backend, stream av.Stream) codec.Decoder {
	t.Helper()
	config, err := decodeConfig(context.Background(), b, stream)
	if err != nil {
		t.Fatalf("%s: decoder state: %v", b.name(), err)
	}
	decoder, err := b.decoder.NewDecoder(context.Background(), config)
	if unavailable(t, b, err) {
		return nil
	}
	if err != nil {
		t.Fatalf("%s: open decoder: %v", b.name(), err)
	}
	return decoder
}

// decodeAll decodes packets in order, then flushes, and returns the frames
// with their payload references held.
func decodeAll(t *testing.T, b backend, stream av.Stream, packets []av.Packet) []*av.Frame {
	t.Helper()
	decoder := openDecoder(t, b, stream)
	if decoder == nil {
		return nil
	}
	defer decoder.Close()
	var frames []*av.Frame
	collect := func(result *codec.DecodeResult) {
		drainFrames(result, func(f *av.Frame) { frames = append(frames, f) })
	}
	result := newDecodeResult(stream.Type)
	for i := range packets {
		if err := decoder.DecodeInto(context.Background(), &packets[i], &result); err != nil {
			t.Fatalf("%s: decode packet %d: %v", b.name(), i, err)
		}
		collect(&result)
	}
	if err := decoder.FlushInto(context.Background(), &result); err != nil {
		t.Fatalf("%s: flush: %v", b.name(), err)
	}
	collect(&result)
	return frames
}

// notInstalled lists the backends the options register descriptor-only
// because they cannot run here, once each.
func notInstalled(options []runconfig.Option) []codec.Descriptor {
	config, err := runconfig.NewConfig(options...)
	if err != nil {
		return nil
	}
	var out []codec.Descriptor
	seen := map[string]bool{}
	for _, desc := range config.Codecs.Descriptors() {
		key := string(desc.ID) + "/" + desc.Backend.Name
		if runnable(desc) || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, desc)
	}
	return out
}

// runnable reports a backend whose status says it can run here; one that
// registered with a factory but cannot (goaac off arm64, unsupported-target)
// is skipped with its status like one registered descriptor-only.
func runnable(desc codec.Descriptor) bool {
	switch desc.Backend.Status {
	case "", "active", "active-build-tagged":
		return true
	}
	return false
}

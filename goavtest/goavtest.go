// Package goavtest provides utilities for testing goav pipelines, in the
// spirit of net/http/httptest: deterministic sources, a recording sink, a
// fake clock, and passthrough codec and container fakes. Nothing here couples
// to *testing.T and nothing asserts — every helper returns a real grammar
// value (goav.InputSpec, goav.Destination, goav.Option, goav.Runtime), so
// test code is pipeline code.
//
// A complete pipeline test is three values and one Run:
//
//	out := goavtest.NewCollector()
//	task, _ := goav.Mix(
//	    goav.From(goavtest.Audio(48000, 1, []int16{100}, []int16{200})).Audio(),
//	    goav.From(goavtest.Audio(48000, 1, []int16{50}, []int16{-50})).Audio(),
//	).To(out.Sink()).UseRuntime(goavtest.Runtime()).Build(ctx)
//	_ = task.Run(ctx)
//	// out.S16() == [[150] [150]]
//
// The sources (Audio, Video, Packets, LiveAudio) are PTS-stamped with
// coherent durations and declare their shape, so the planner solves
// conversions exactly as it would for production inputs. The Collector
// records safe copies of everything a destination receives. Runtime is the
// one-liner deterministic runtime: standard filters, a passthrough codec for
// every well-known codec id, a byte-faithful container for every well-known
// container format id, and a fake clock so realtime pacing never sleeps for
// real.
package goavtest

import (
	"fmt"
	"sync/atomic"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
)

// sourceSeq numbers every constructed goavtest source and collector so each
// gets a distinct name (and therefore a distinct stream id): two Audio inputs
// can stand as Mix arms without colliding.
var sourceSeq atomic.Uint64

func nextName(kind string) string {
	return fmt.Sprintf("%s-%d", kind, sourceSeq.Add(1))
}

// runtimeCodecs are the codec ids Runtime registers the passthrough codec
// for: every well-known av.Codec* id.
var runtimeCodecs = []av.CodecID{
	av.CodecOpus, av.CodecVorbis, av.CodecFLAC, av.CodecAAC,
	av.CodecVP8, av.CodecVP9, av.CodecH264, av.CodecAV1,
	av.CodecPCM, av.CodecTextUTF8,
}

// runtimeFormats are the container ids Runtime registers the byte-faithful
// fake container for: every well-known byte-container av.Format* id (the
// transport framings rtp and webrtc are not byte containers and are skipped).
var runtimeFormats = []av.FormatID{
	av.FormatFLV, av.FormatOgg, av.FormatIVF, av.FormatAnnexB,
	av.FormatMatroska, av.FormatWebM, av.FormatMP4,
}

// Runtime builds the deterministic test runtime. It contains exactly:
//
//   - the standard frame filters (resample, resize), so shape-solver
//     conversions work;
//   - Codec(id) for every well-known av.Codec* id (opus, vorbis, flac, aac,
//     vp8, vp9, h264, av1, pcm, text_utf8) — the passthrough fake, so
//     .Encode(codec.Opus()) round-trips bytes instead of running a real
//     encoder;
//   - Format(id) for every well-known container av.Format* id (flv, ogg, ivf,
//     annexb, matroska, webm, mp4) — the byte-faithful fake container, so
//     goav.File destinations and goav.FileInput sources work without real
//     containers;
//   - goav.WithClock(NewClock()), so realtime pacing advances a fake clock
//     instead of sleeping.
//
// opts are applied last and registration is last-wins, so extra options can
// both add adapters and override any of the defaults (including the clock).
func Runtime(opts ...goav.Option) goav.Runtime {
	options := make([]goav.Option, 0, 2+len(runtimeCodecs)+len(runtimeFormats)+len(opts))
	options = append(options, goav.WithStdFilters())
	for _, id := range runtimeCodecs {
		options = append(options, Codec(id))
	}
	for _, id := range runtimeFormats {
		options = append(options, Format(id))
	}
	options = append(options, goav.WithClock(NewClock()))
	options = append(options, opts...)
	return goav.New(options...)
}

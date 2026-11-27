<div align="center">

# goav

**Media pipelines in pure Go.** Decode, transform, encode, and mux inside your service:<br>the core needs no cgo, no FFmpeg, and no system libraries.

[![CI](https://github.com/thesyncim/goav/actions/workflows/ci.yml/badge.svg)](https://github.com/thesyncim/goav/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/thesyncim/goav.svg)](https://pkg.go.dev/github.com/thesyncim/goav) [![Go 1.27+](https://img.shields.io/badge/go-1.27%2B-00ADD8)](go.mod) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE) [![Status: pre-v1](https://img.shields.io/badge/status-pre--v1-orange)](docs/ARCHITECTURE.md#pre-v1-status--codec-decisions)

</div>

You describe media work as plain Go values, and goav plans it, builds the graph, and runs it.
The codecs and containers are pure Go (VP8, VP9, AV1, Opus; WebM, Matroska, MP4), so
your service still ships as one static binary.

```sh
go get github.com/thesyncim/goav@latest   # Go 1.27+
GOEXPERIMENT=simd go build ./...         # enables Go 1.27 portable SIMD kernels
```

Build, test, and run with `GOEXPERIMENT=simd` to enable Go 1.27 portable SIMD
paths for codecs, audio mix, and I420 resize (without it, everything falls back
to bit-exact scalar code). On an Apple M4 Max, SIMD cuts codec benchmark time by
~45% geomean and pushes 720p half-scale I420 resize past 2 GB/s at 0 allocs/op
([measurements and scope](docs/PERFORMANCE.md)).

## Quick start

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
)

// Scale a WebM to 720p and re-encode it as VP9.
func main() {
	src, err := os.Open("in.webm")
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()
	dst, err := os.Create("out.webm")
	if err != nil {
		log.Fatal(err)
	}
	video := goav.Read("in.webm", src).Video().
		Decode().
		Resize(1280, 720).
		Encode(codec.VP9(codec.Bitrate(2_000_000)))
	if err := bundle.Run(context.Background(), video.To(goav.Write("out.webm", dst))); err != nil {
		log.Fatal(err)
	}
}
```

`goav.Read` takes any `io.Reader` and `goav.Write` any `io.Writer`; `bundle.Run` closes
the writer when the run ends.

<details>
<summary>No WebM handy? Make a test clip with the CLI.</summary>

```sh
GOEXPERIMENT=simd go run github.com/thesyncim/goav/cmd/goav@latest run \
  'testsrc video size=1920x1080 duration=2s ! encode vp9 ! file in.webm
   testsrc audio duration=2s ! encode opus ! file in.webm'
```

</details>

## Fan out by reuse

Every operation returns a new `goav.Stream`, so a stream you use twice forks, and the work
before the fork runs once. Here one decode feeds two renditions, and one Opus encode of the
input's audio track is muxed into both files:

```go
in := goav.Read("in.webm", src)
video := in.Video().Decode()
audio := in.Audio().Decode().Encode(codec.Opus())
hd, sd := goav.Write("hd.webm", hdFile), goav.Write("sd.webm", sdFile)

err := bundle.Run(ctx,
    video.Resize(1280, 720).Encode(codec.VP9()).As("hd").To(hd),
    video.Resize(640, 360).Encode(codec.VP9()).As("sd").To(sd),
    audio.To(hd, sd),
)
```

`stream.To(...)` makes an `Output`. `bundle.Run` runs Outputs together with the bundled
codecs and containers; a runtime of your own runs them the same way: `rt.Run(ctx, outputs...)`.

The same recipe as text, for a command line, a config file, or a control socket. It builds the
identical graph ([`launch`](launch), [CLI](docs/CLI.md)):

```sh
GOEXPERIMENT=simd go install github.com/thesyncim/goav/cmd/goav@latest
goav run 'v := file in.webm ! video ! decode
          v ! resize 1280x720 ! encode vp9 ! as hd ! file hd.webm
          v ! resize 640x360 ! encode vp9 ! as sd ! file sd.webm
          file in.webm ! audio ! decode ! encode opus ! file hd.webm, file sd.webm'
```

## See the plan first

A recipe is data until it runs. `bundle.Describe(outputs...)` returns the exact graph a run
would build, without opening anything, and `graphrender.RenderURI(spec, "goav:graph")` prints
it. The quick start's nodes:

```text
pipeline goav
  source in.webm [demux, protocol=file]
  stage select-video [select, type=video]
  stage decode-video [packets -> frames, type=video]
  stage resize-video [resize, 1280x720, exact]
  stage encode-video [frames -> packets, codec=vp9, backend=govpx (go)]
  stage out.webm [mux, protocol=file]
```

## Errors name the fix

A mistake comes back before anything opens, as a `*goav.BuildError` with a stable code and
the fix. Leave out the encoder and you get:

```text
goav: cannot build job for video: decoded frames cannot be written to a muxed output without an encoder
Suggestions:
  - call .Encode(codec.VP8(...)), .Encode(codec.VP9(...)), or .Encode(codec.AV1(...)) before .To(goav.Write(...))
  - send decoded frames to goav.Sink(...)
  - leave out .Decode() to copy the packets: input.Video().To(output)
```

See [Errors & Error Catalog](docs/GUIDE.md#errors--error-catalog) for matching on codes; [`graphrender`](graphrender) also renders
DOT and Mermaid.

## What's in the box

| | Decode / demux | Encode / mux |
| --- | --- | --- |
| Video | VP8, VP9, AV1, H.264 (`-tags goav_goh264`) | VP8, VP9, AV1 |
| Audio | Opus | Opus (in WebM, Matroska, or MP4) |
| Containers | Matroska, WebM, IVF, MP4 | Matroska, WebM, MP4 (progressive or fragmented), IVF, Annex B (H.264) |

Plus `Resize` (area-averaged SIMD downscale, linear upscale) and `Resample`.
`bundle` registers all of these; `goav.New(...)` builds a runtime with only the
adapters you choose, with no global registry.

**Need H.264 or AAC?**

| Codec | Default (`bundle`, pure Go) | Opt-in / native |
| --- | --- | --- |
| H.264 decode | Opt-in via `-tags goav_goh264` (LGPL) | [`libcodec`](libcodec): VideoToolbox (macOS), OpenH264 (macOS/Linux/Windows) |
| H.264 encode | Descriptor-only (`codec.ErrUnavailable`) | [`libcodec`](libcodec): VideoToolbox (macOS), OpenH264 (macOS/Linux/Windows) |
| AAC | Not in `bundle` | [`libcodec`](libcodec) AudioToolbox (macOS) or [`gplcodec`](gplcodec) (arm64 decode, **GPL**) |

Everything `bundle` links by default is permissive; see [Adapters & Licenses](docs/EXTENDING.md#licenses).

## Recipes

| Need | Shape |
| --- | --- |
| Remux without decoding | `in.To(goav.Write("out.mkv", w))` |
| Audio and video in one file | pass `in.Video()…To(out)` and `in.Audio()…To(out)` to one `Run` |
| Frames into your code | `in.Video().Decode().To(goav.Sink(component.SinkFunc("frames", fn)))` |
| Mix, composite, switch | `goav.Mix(a, b)`, `goav.Composite(cam, goav.At(screen.Resize(480, 270), 1420, 790))`, `goav.Select(a, b)` |
| Package for HLS | `goav.Custom(hls.New("live.m3u8", hls.Dir(dir), hls.Window(6)))`; `hls.NewLadder` for renditions, `hls.Parts` with an `hls.Server` for LL-HLS, or `goav run --serve :8080 '... ! hls live.m3u8 parts=250ms'` |
| Change a running task | `task.Attach(ctx, task.Tap("live").Encode(codec.VP8()).To(preview))` |
| Test a pipeline | [`goavtest`](goavtest): deterministic sources, a recording sink, a fake clock |

RTP and WebRTC, in and out, live in their own modules ([Realtime](docs/REALTIME.md)), and
your own sources, codecs, and sinks plug in through small interfaces
([Adapters & Extending](docs/EXTENDING.md)). More in the [Developer Guide](docs/GUIDE.md)
and [`examples/`](examples), including a browser
[WebRTC ladder](examples/webrtc-runtime-ladder) and a `CGO_ENABLED=0` [native macOS Studio](examples/gio-webrtc-showcase).

## Built for live services

- **One clock per task.** Pacing, playout, pause/resume, seek, and rate share one timeline. Each fork backpressures or drops independently, with every drop counted ([Flow control](docs/GUIDE.md#flow-control--backpressure)).
- **Mutate while running.** Attach and detach outputs, switch inputs, and drive tasks over a JSON-RPC control socket ([Control plane](docs/REALTIME.md#control-plane-ctlserver--goav-ctl)).
- **Zero `unsafe` and zero assembly in core.** Hot-path graph, codec, and filter stages allocate 0 B/op per message, pinned by CI tests, with Go 1.27 SIMD acceleration ([Performance](docs/PERFORMANCE.md)).

## Modules

| Module | Adds |
| --- | --- |
| `github.com/thesyncim/goav` | recipes, planner, runtime, scheduled playout ([`playoutav`](playoutav)); bundled adapters in [`bundle`](bundle) |
| [`goav/rtpav`](rtpav) | RTP in and out: jitter buffer, (de)packetizers, RTCP feedback (Pion) |
| [`goav/webrtcav`](webrtcav) | WebRTC tracks in and out, WHEP and WHIP (Pion) |
| [`goav/player`](player) | a playback window, synced to the speaker: `play` in programs, `goav play FILE` |
| [`goav/cameraav`](cameraav) | cgo-free camera capture with bounded zero-allocation frame delivery |
| [`goav/libcodec`](libcodec) | codecs in shared libraries and OS frameworks, no cgo: VideoToolbox, AudioToolbox, OpenH264 |
| [`goav/gplcodec`](gplcodec) | opt-in copyleft codecs: `gplcodec.AAC()`, a pure-Go AAC-LC decoder (GPL; your binary becomes GPL) |
| [`goav/cmd/goav`](cmd/goav) | the `goav` CLI: run, describe, explain, play, cameras, ctl ([CLI](docs/CLI.md)) |
| [`goav/goavtest/expect`](goavtest/expect) | go-cmp assertions for pipeline tests |

The core module never pulls in Pion.

## Status

Pre-v1: breaking changes can land until the first tag (see [Architecture & Roadmap](docs/ARCHITECTURE.md#pre-v1-status--codec-decisions)).
Not related to `github.com/giorgisio/goav`, a cgo binding to FFmpeg.

---

[Docs](docs/README.md) · [Guide](docs/GUIDE.md) · [Extending](docs/EXTENDING.md) ·
[Realtime](docs/REALTIME.md) · [Architecture](docs/ARCHITECTURE.md) · [Performance](docs/PERFORMANCE.md) ·
[Changelog](CHANGELOG.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)

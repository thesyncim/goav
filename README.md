<div align="center">

# goav

**Media pipelines in pure Go.** Decode, transform, encode, and mux inside your service:<br>no cgo, no FFmpeg, no system libraries.

[![CI](https://github.com/thesyncim/goav/actions/workflows/ci.yml/badge.svg)](https://github.com/thesyncim/goav/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/thesyncim/goav.svg)](https://pkg.go.dev/github.com/thesyncim/goav) [![Go 1.27+](https://img.shields.io/badge/go-1.27%2B-00ADD8)](go.mod) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE) [![Status: pre-v1](https://img.shields.io/badge/status-pre--v1-orange)](docs/COMPATIBILITY.md)

</div>

You describe media work as plain Go values, and goav plans it, builds the graph, and runs it.
The codecs and containers are pure Go (VP8, VP9, AV1, Opus, AAC; WebM, Matroska, MP4), so
your service still ships as one static binary.

```sh
go get github.com/thesyncim/goav@latest   # Go 1.27+
GOEXPERIMENT=simd go build ./...         # always: SIMD codec paths, about 2x faster
```

Build, test, and run with `GOEXPERIMENT=simd`. It switches the codecs onto Go 1.27's
portable SIMD paths; without it they fall back to scalar code.

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
	dst, err := os.Create("out.webm")
	if err != nil {
		log.Fatal(err)
	}
	video := goav.FileInput("in.webm", src).Video().
		Decode().
		Resize(1280, 720).
		Encode(codec.VP9(codec.Bitrate(2_000_000)))
	if err := bundle.Run(context.Background(), video.To(goav.Write("out.webm", dst))); err != nil {
		log.Fatal(err)
	}
}
```

`goav.FileInput` takes any `io.Reader` and `goav.Write` any `io.Writer`; `bundle.Run` closes
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
in := goav.FileInput("in.webm", src)
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
  stage encode-video [frames -> packets, codec=vp9]
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

See [Errors](docs/ERRORS.md) for matching on codes; [`graphrender`](graphrender) also renders
DOT and Mermaid.

## What's in the box

| | Decode / demux | Encode / mux |
| --- | --- | --- |
| Video | VP8, VP9, AV1, H.264 (`goav_goh264` build tag) | VP8, VP9, AV1 |
| Audio | Opus, AAC-LC | Opus (in WebM, Matroska, or MP4) |
| Containers | Matroska, WebM, IVF, MP4 | Matroska, WebM, MP4 (progressive or fragmented), IVF, Annex B (H.264) |

Plus `Resize` and `Resample`. `bundle` registers all of these; `goav.New(...)` builds a
runtime with only the adapters you choose, and there is no global registry.
See [Adapters](docs/ADAPTERS.md).

## Recipes

| Need | Shape |
| --- | --- |
| Remux without decoding | `in.To(goav.Write("out.mkv", w))` |
| Audio and video in one file | pass `in.Video()…To(out)` and `in.Audio()…To(out)` to one `Run` |
| Frames into your code | `in.Video().Decode().To(goav.Sink(component.SinkFunc("frames", fn)))` |
| Mix, composite, switch | `goav.Mix(a, b)`, `goav.Composite(cam, goav.At(screen, 960, 540))`, `goav.Select(a, b)` |
| Package for HLS | `goav.Custom("live", hls.New("live.m3u8", hls.Dir(dir), hls.Window(6)))`, or `! hls out/live.m3u8` in a program |
| Change a running task | `task.Attach(ctx, task.Tap("live").Encode(codec.VP8()).To(preview))` |
| Test a pipeline | [`goavtest`](goavtest): deterministic sources, a recording sink, a fake clock |

RTP and WebRTC inputs live in their own modules ([RTP & WebRTC](docs/RTP_WEBRTC.md)), and
your own sources, codecs, and sinks plug in through small interfaces
([Extension cookbook](docs/EXTENSION_COOKBOOK.md)). More in [Use cases](docs/USE_CASES.md),
[Operations](docs/OPERATIONS.md), and [`examples/`](examples), including a browser
[WebRTC ladder](examples/webrtc-runtime-ladder) and a native [Gio control room](examples/gio-webrtc-showcase).

## Built for live services

- **One clock per task.** Pacing, playout, pause and resume, seek, and rate share one
  timeline. Each fork can backpressure or drop on its own, and every drop is counted.
  See [Flow control](docs/FLOW_CONTROL.md).
- **Change it while it runs.** Attach and detach outputs, switch inputs, and drive it all
  over a control socket ([Control plane](docs/CONTROL_PLANE.md)).
- **Fast without assembly.** Graph, codec, and filter stages allocate nothing per message,
  and with `GOEXPERIMENT=simd` Go 1.27's SIMD paths cut real-codec time by about 45%.
  See [Performance](docs/PERFORMANCE.md).

## Modules

| Module | Adds |
| --- | --- |
| `github.com/thesyncim/goav` | recipes, planner, runtime; bundled adapters in [`bundle`](bundle) |
| [`goav/rtpav`](rtpav) | RTP ingest: jitter buffer, depacketizers, RTCP feedback (Pion) |
| [`goav/webrtcav`](webrtcav) | WebRTC remote tracks as inputs (Pion) |
| [`goav/playoutav`](playoutav) | scheduled packet, frame, and event playout |

The core module never pulls in Pion.

## Status

Pre-v1: breaking changes can land until the first tag. See [V1 scope](docs/COMPATIBILITY.md#v1-scope) and
the [roadmap](docs/ROADMAP.md).

---

[All docs](docs/README.md) · [Architecture](docs/ARCHITECTURE.md) · [Adapter authoring](docs/ADAPTER_AUTHORING.md) ·
[RTP & WebRTC](docs/RTP_WEBRTC.md) · [vs GStreamer](docs/GSTREAMER_ALTERNATIVE.md) ·
[Changelog](CHANGELOG.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)

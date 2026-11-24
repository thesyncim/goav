# Roadmap

goav is **pre-v1**: breaking API changes may still land until the first v1 tag.

## Design model

Every recipe uses one value grammar — for initial builds, runtime attach,
inspection, and the CLI:

```text
input.Video/Audio/Stream() -> operations -> taps -> forks -> .To(destinations) -> rt.Run / rt.Build
```

- **Pure Go core (`CGO_ENABLED=0`)**: no cgo, no FFmpeg, no `unsafe`, no
  assembly in the root module. `GOEXPERIMENT=simd` enables Go 1.27 portable
  SIMD codec paths.
- **Explicit errors before I/O**: recipes validate shapes, adapters, and mux
  compatibility before opening files or network resources.
- **Zero-alloc hot paths**: graph delivery, codec/filter stages, and fragmented
  MP4 muxing reuse buffers and are pinned by allocation tests.

## Codec decisions

- **Bundled pure-Go codecs**: VP8, VP9, AV1, and Opus decode and encode.
- **H264 recipe encode** is deliberately descriptor-only until a
  vetted pure-Go H.264 encoder backend exists. Without an external encoder
  registered via `runconfig.WithEncoder`, H.264 encode returns
  `codec.ErrUnavailable` (use `libcodec` for VideoToolbox / OpenH264).
- **H264 decode**: available via `-tags goav_goh264` (LGPL) or `libcodec`.
- **AAC decode**: available via `libcodec.AudioToolbox()` (macOS) or opt-in
  `gplcodec.AAC()` (GPL, arm64).

## Non-goals for core

- Dynamic binary plugin loading (extensions are ordinary Go packages).
- Hardware codec backends in the root module (they live in `libcodec`).
- Pull-based graph scheduling (consumer backpressure bounds producers).

## Before v1

- Finalize and freeze the root recipe grammar, `codec`, `flow`, `component`,
  `provider`, and `runconfig` APIs.
- Cut signed tags for the root and nested modules (see [`RELEASING.md`](RELEASING.md)).

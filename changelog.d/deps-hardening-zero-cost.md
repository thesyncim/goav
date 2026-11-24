- Bump `github.com/thesyncim/{goav1,gopus,govpx}` across the root and nested modules
  and migrate `adapters/goav1` to `goav1`'s simplified public `Decoder` API with
  zero-allocation steady-state packet decoding and full AV1 post-filtering.
- Accept `yuv420p` alongside `i420` in `goav.Composite` and `adapters/govpx`
  descriptors so camera, H.264, and resize outputs compose without runtime pixel
  format errors.
- Harden VideoToolbox pixel-buffer locking and padded height bounds, `player`
  `Engine.Close` shutdown ordering and speaker cleanup, WHIP/WHEP answer body error
  propagation, `rtpav.Send.OpenStreams` atomicity, `gopus`/`goh264` multi-frame
  metadata pointer aliasing, `ctlserver` auth timeout/batch/watch caps and HTTP
  413 handling, MP4 demuxer 64-bit box/fragment overflow checks and pooled buffer
  reset ordering, and HLS atomic publish abort/cleanup, invalid box rejection, and
  LL-HLS end-of-stream announcements.
- Eliminate per-fragment allocations and full-fragment media double-copy in fragmented
  MP4 muxing (pinned at 0 allocs/op), avoid per-packet allocations in AV1/H.264/HEVC
  MP4 sample framing and WHEP receive pumping, carve I420 output planes from a single
  pooled block in `govpx` and `resize`, optimize `cameraav` NV12/YUYV pixel copies
  and `player` 720p+ YUV→RGBA conversion (0 allocs/frame with pooled row workers and
  2×2 I420 blocks), skip per-request timer/query allocations in `hls.Server`, and
  remove per-message reflection and redundant fanout/depth work in `pipeline` and
  join stages.
- Remove all remaining `unsafe` usage from the root module (`adapters/gopus`) and
  all avoidable non-FFI `unsafe` from `cameraav/pool.go`, `gplcodec`, and
  `libcodec/kit` using reusable scratch buffers and `encoding/binary` (zero
  allocations in steady state).
- Add `codec.AAC(...)` constructor matching `codec.Opus(...)`, `Window.RuntimeOptions()`
  in `player`, fix the README quick-start file leak, and polish `README.md`, `docs/`,
  and stale code comments to reflect current modules, codecs, and Go 1.27.4.

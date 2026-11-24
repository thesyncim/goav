- Bump `github.com/thesyncim/{goav1,gopus,govpx}` across the root and nested modules
  and migrate `adapters/goav1` to `goav1`'s simplified public `Decoder` API with
  zero-allocation steady-state packet decoding and full AV1 post-filtering.
- Accept `yuv420p` alongside `i420` in `goav.Composite` and `adapters/govpx`
  descriptors so camera, H.264, and resize outputs compose without runtime pixel
  format errors.
- Harden VideoToolbox pixel-buffer locking and padded height bounds, `player`
  `Engine.Close` shutdown ordering and speaker cleanup, WHIP/WHEP answer body error
  propagation, and `rtpav.Send.OpenStreams` atomicity.
- Eliminate per-fragment allocations and full-fragment media double-copy in fragmented
  MP4 muxing (pinned at 0 allocs/op), avoid per-packet allocations in AV1/H.264/HEVC
  MP4 sample framing, carve I420 output planes from a single pooled block in `govpx`
  and `resize`, and remove per-message reflection and redundant fanout/depth work in
  `pipeline` and join stages.
- Add `codec.AAC(...)` constructor matching `codec.Opus(...)` and document `goav/cameraav`
  in the module inventory.

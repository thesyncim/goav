# Encoder configuration — gather + unified design

Goal: one clean, expressive, state-of-the-art way to configure encoders that
covers the *common* knobs as typed surface and falls back to one codec-specific
escape hatch — unified with caps (Shape), without polluting the main grammar.

## 1. The common-encoder-settings catalog (grounded in govpx, gopus, std codecs)

Legend: ✓ has a typed home today · ~ partial/flag · ✗ no typed home (only the
unwired Config/Opaque/Control blobs).

### Rate control
- `Bitrate` target bps — ✓
- `RateControl` mode: CBR · VBR · ConstantQuality(CRF/CQ) · ConstantQP — ✗ (govpx RateControlMode)
- `MaxBitrate` / `MinBitrate` bps — ✗ (govpx Min/MaxBitrateKbps)
- `Quality` CRF/CQ level (codec-normalized) — ✗ (govpx CQLevel)
- `QPMin` / `QPMax` quantizer bounds — ✗ (govpx Min/MaxQuantizer)
- `BufferSize` VBV/HRD (size/initial/optimal ms) — ✗ (govpx BufferSizeMs…)
- Undershoot/Overshoot %, MaxIntraBitrate% — ✗ (vpx; likely codec-specific)

### GOP / frame structure
- `KeyframeInterval` (GOP size) — ✓
- `MaxBFrames` — ✗ (h264/av1)
- `Lookahead` frames — ✗ (govpx LookaheadFrames)
- `ClosedGOP`, `RefFrames`, `IntraRefresh`/`ErrorResilient` — ✗
- `DropFrame` (allowed + watermark) realtime shedding — ✗ (govpx)

### Profile / level
- `Profile` — ✓ · `Level` — ✓ · `Tier` (av1/hevc) — ✗

### Latency / speed / tune
- `Realtime` — ~ (EncodeConfig.Realtime flag) · `LowLatency` — ~ (flag)
- `Speed` normalized 0..n (cpu-used / x264 preset / deadline) — ✗ (govpx Deadline+CpuUsed)
- `Tune`: ZeroLatency · ScreenContent · Film — ✗ (govpx Tuning/ScreenContentMode)

### Threading / tiling
- `Threads` — ✗ (govpx Threads) · `Tiles`/`RowMT`/`Slices` — ✗ (govpx Log2TileRows/RowMT)

### Color (caps-adjacent)
- `PixelFormat` — ✓ (Shape) · `ColorRange` full/limited, `Primaries`/`Transfer`/`Matrix` — ✗
- Sharpness / NoiseSensitivity — ✗ (vpx-specific)

### Audio (Opus / AAC)
- `Bitrate` — ✓
- `AudioMode`: CBR · VBR · ConstrainedVBR — ✗ (gopus BitrateMode/ConstrainedVBR)
- `Complexity` 0..10 — ✗ (gopus) · `Application` voip/audio/lowdelay — ✗ (gopus)
- `FEC` — ✗ · `DTX` — ✗ · `PacketLoss` % — ✗ (gopus)
- `FrameDuration` 2.5..60ms — ✗ (gopus FrameSize) · `Bandwidth`/cutoff — ✗ (gopus)
- `Channels` / `SampleRate` — ✓ (Parameters/Shape; **caps overlap**)

**Coverage: ~5 of ~35 typed. The rest are missing or only reachable via the
three unwired custom blobs.**

## 2. Caps (Shape) overlap

goav's caps equivalent is `MediaShape` (Domain, MediaKind, Codec, Format, Width,
Height, PixelFormat, SampleRate, Channels, SampleFormat, Realtime) + `ShapeSet`
with `Accepts`/`CompatibleWith`/`Input/OutputShapes` negotiation. **It works, but
it overlaps three places:** Width/Height/FPS/Channels/SampleRate/PixelFormat live
in `MediaShape` *and* `av.CodecParameters` *and* are set via codec options
(`Channels`/`SampleRate`/`FPS`). Unification rule to adopt: **Shape = structural
identity (what the media *is*); EncoderSettings = encoder *behavior* (how to
produce it).** Structural fields stop being encoder settings; FPS is the one
genuine dual (caps cadence + encode target) — resolve by deriving the encode
target framerate from the negotiated Shape, not a separate setting.

## 3. Unified model — the three-tier escape ladder (decided)

Escalating power; each tier is used only when the one above cannot express it.
Surface style: **grouped struct options** + flat shortcuts for the 90% case.

**Tier 1 — common typed settings (portable across codecs).** Grouped struct
options by concern + shortcuts:
- shortcuts: `Bitrate(bps)`, `Profile(s)`, `Level(s)`, `KeyframeInterval(n)`, `FPS(num,den)`, `Speed(n)`, `Tune(Tuning)`
- `RateControl(RateControl{Mode, Target, Max, Min, Quality, QPMin, QPMax, Buffer})` — Mode ∈ {CBR,VBR,ConstantQuality,ConstantQP}
- `GOP(GOP{Keyframe, MaxBFrames, Lookahead, Closed, RefFrames})`
- `Audio(Audio{Mode, Complexity, Application, FEC, DTX, PacketLoss, FrameDuration, Bandwidth})` — Mode ∈ {CBR,VBR,ConstrainedVBR}
- `Color(Color{Range, Primaries, Transfer, Matrix})`, `Threading(Threading{Threads, RowMT, Tiles})`

**Tier 2 — raw `Control(func(enc any) error)`.** Invoked by the adapter at
encoder/decoder open with the *concrete* native encoder; the caller type-asserts
and applies anything. Nothing is ever unreachable. This single callback
**subsumes a typed `Config` blob** (decided): instead of passing data the adapter
must define + document + type-assert, you get the encoder and apply directly —
strictly more capable, one fewer concept. (A separate `Config any` tier was
considered and dropped for this reason.) For the rare construction-only knob, an
adapter may hand the callback its options builder instead — its documented choice.

**Caps = Shape.** Structural identity only (codec/resolution/format/channels);
settings reference it, never duplicate it. Removes the Width/Height/FPS/Channels
overlap between Shape, CodecParameters, and options.

This collapses today's three unwired blobs (`Config`/`Opaque`/`Controls`) into a
clean ladder where `Config` (tier 2) and `Control` (tier 3) have distinct,
non-overlapping jobs. `Param`/`Opaque` are removed.

## 4. Package boundary — keep goav root clean

Encoder settings do NOT live in the goav root (no `goav.Bitrate`). They live in
the **`codec`** package, which already owns the contract (`CodecSettings`,
`Encoder`, `Decoder`, `Descriptor`):
- `codec` owns: the grouped types (`RateControl`/`GOP`/`Audio`/`Color`/`Threading`
  + enums), the option type `Option = func(*CodecSettings)`, and every option
  func (`Bitrate`, `RateControl`, `GOP`, `Tune`, `Speed`, `Profile`, `Level`,
  `Config`, `Control`, …).
- `goav` keeps: the grammar + codec constructors `Opus/VP8/VP9/H264/AV1/Codec`,
  now `func(...codec.Option) CodecSpec`. goav already imports codec → no cycle.
- This removes ~12 codec-settings symbols from the goav root.

**Caps vs settings (forced by the boundary):** a `codec.Option` mutates
`CodecSettings` only, so `Channels`/`SampleRate`/`Format` (which are
`av.CodecParameters` / structural caps) cannot be settings options. They move to
**Shape** — the structural-identity-vs-encoder-behavior split. Codec constructors
set sensible caps defaults (Opus → 48k stereo); overrides come through Shape.

Adapters read the flat-but-grouped `CodecSettings`; tier 2 `Config any` and tier
3 `Control func(any) error` ride along. Wire govpx VP9 first (rate control + a
tier-3 Control) with a test proving behavior changes.

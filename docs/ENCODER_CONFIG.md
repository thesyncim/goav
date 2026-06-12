# Encoder configuration: two-tier model

One way to configure encoders: common knobs as typed surface, one raw escape
hatch, unified with shape, without polluting the main grammar.

**Tier 1: common typed settings (portable across codecs).** Options in the
`codec` package mutate `codec.CodecSettings` (carried by `CodecSpec.Settings`
and the decode/encode configs): `Bitrate`, `FPS`, `KeyframeInterval`,
`Profile`, `Level`, ... plus grouped structs by concern
(rate control, GOP, audio, color, threading) as they land. The grouped tier-1
catalog (Speed/Tune/Threads/Opus Complexity/FEC/DTX, ...) is still being wired;
the catalog of ~35 settings grounded in govpx/gopus lives in git history.

**Tier 2: `codec.Control(func(native any) error)`.** Invoked by the adapter at
open with the concrete native encoder/decoder or construction config; the caller
type-asserts and applies anything the library exposes. Nothing is ever
unreachable, and this single callback replaces any typed config blob
(`Config`/`Param`/`Opaque` were dropped). Each adapter documents the concrete
type it hands the callback.

**Caps = Shape.** `shape.Spec` owns structural identity (codec, resolution,
pixel/sample format, rate, channels); settings own encoder behavior and never
duplicate structural fields. Codec constructors set sensible caps defaults
(Opus -> 48k stereo); structural overrides come through Shape.

**Package boundary.** Encoder settings do not live in the goav root (no
`goav.Bitrate`): `codec` owns the option type and every option func; `goav`
keeps the grammar plus the codec constructors `Opus/VP8/VP9/H264/AV1/Codec`,
each `func(...codec.Option) codec.CodecSpec`.

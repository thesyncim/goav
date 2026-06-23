# Encoder configuration: two-tier model

Encoder configuration has one job: expose useful codec knobs without turning
the main grammar into an options drawer. The model is two-tiered: common typed
settings for portable behavior, plus one raw escape hatch for native adapter
details.

**Tier 1: common typed settings (portable across codecs).** Options in the
`codec` package mutate `codec.CodecSettings` (carried by `CodecSpec.Settings`
and the decode/encode configs): `Bitrate`, `FPS`, `KeyframeInterval`,
`Profile`, `Level`, audio shape overrides, and future tagged fields as they
land. The control plane and string launcher reflect over `CodecSettings`, so a
new exported setting with `goavctl`/`usage`/`help` tags becomes bindable and
appears in generated `goav ctl help attach` and `goav ctl capabilities` output.
Docs should point at that generated manifest instead of copying an option list.

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

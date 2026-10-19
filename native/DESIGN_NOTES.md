# Native codecs: design notes (review draft; deleted before the PR)

## Root changes (module github.com/thesyncim/goav)

Registry: many backends per codec.
- `codec.SimpleRegistry` holds every backend registered for a codec, keyed by
  (codec, backend name, mode); registering the same identity replaces it.
- `Select(codec.Need) (codec.Choice, error)`: the named backend
  (`Need.Backend`), else the most preferred backend whose declared capabilities
  fit. Default order: kindless (application factories) > hardware > native > go,
  later registration first within a kind. `Prefer(order...)` reorders by name or
  kind. A named backend is never substituted; a refusal (`*codec.SelectError`)
  lists each rejection and the backends that would serve.
- `EncoderFactory(id)`/`DecoderFactory(id)` stay, as `Select` with no needs.
- New exports: `codec.Need`, `Choice`, `SelectError`, `Rejection`,
  `BackendList`, `Descriptor.Check`, `BackendInfo.Label`, `SimpleRegistry.Select`,
  `Prefer`; `BackendKind` + `KindHardware/KindNative/KindGo`.
- Breaking: struct `codec.Backend` -> `codec.BackendInfo` (gains `Kind`), so
  `codec.Backend(name)` can be the selection option. `codec.BitDepth(n)` + `CodecSettings.Backend/BitDepth`.
- Capabilities gain `BitDepths`, `MaxWidth`, `MaxHeight`; `Descriptor.Realtime`
  is low-latency support (a tiebreak for realtime work, not a filter).
- `runconfig.WithCodecPreference(order...)` (one function, appended).
- launch: `encode CODEC backend= bit_depth=`, `decode backend=`.
- Describe: encode nodes gain `backend=videotoolbox (hardware)`. Explain:
  codec requirements gain `backend`, `backendReason`, `alternatives`, `knobs`.
- goav1's encoder identity `goav1-encoder` -> `goav1`.

Knob schema (in `codec`, so planning, Explain, and the kit share one implementation):
- `codec.Knob{Name, Type, Min, Max, Values, Default, Help, Live}`,
  `KnobType` (bool/int/float/enum/string), `Descriptor.Knobs` and `RawKnobs`.
- `Descriptor.ResolveKnobs(settings, lowLatency) (KnobValues, error)`: typed
  common settings map by name (bitrate, gop, profile, level; realtime defaults
  from low latency; rc/threads are conventional names), then `codec.Setting`
  keys; `<backend>.<knob>` scopes a knob, `<backend>-params="k=v:k=v"` is the
  raw tier, checked against `RawKnobs` when declared. Unknown names get a
  nearest-match suggestion (`codec.ErrInvalidSetting`).
- Planning refuses bad encode knobs (`encode_parameter_invalid`) once the
  backend is chosen; the kit re-checks at open.

## native module (github.com/thesyncim/goav/native)

- `native.OpenH264(opts...)`, `native.VideoToolbox(opts...)`,
  `native.AudioToolbox(opts...)` -> `runconfig.Option`; `native.Library(path)`,
  `native.Option`. Nothing loads in init; first open or `kit.Probe` loads.
- `native/kit` (for backend authors): `Codec` (declarative backend), `Register`,
  `Probe`, `Library.Bind` (struct of func/uintptr fields tagged `sym:"Name[,optional|,value]"`),
  `BindVTable`, `Handles[T]` (refcon table; one purego callback per C signature),
  `Error` + `ErrUnavailable` (load/symbol/version/platform carry the fix),
  `Fail`; sessions `VideoEncoder/VideoDecoder/AudioEncoder/AudioDecoder` over
  lent `Picture`/`Packet`/`Samples`; optional `Configured`, `Updater`;
  `Bitstream` (AnnexB/AVCC), `PixelFormat` (I420/NV12), `CodecHEVC`.
- The kit owns: I420<->NV12, Annex B<->AVCC, parameter sets in/out of band,
  avcC from SPS/PPS, timestamps and durations through sessions, one copy into
  pooled `av.NewBuffer` at the boundary, live knobs (Live+Updater, else restart
  on a keyframe; drained packets carried to the next call), Close idempotence.
- `codec.Control` is refused by native backends (raw params are the escape hatch).
- `native/codectest`: `Run(t, opts...)` conformance for every backend the
  options register, plus a cross-backend encode x decode matrix.

## Open questions for review

1. Knob schema in `codec` (root) vs only in the kit: chosen root so planning
   and Explain read it. Cost: ~12 new codec exports.
2. Kindless factories rank first (keeps `runconfig.WithEncoder` overriding the
   bundle as before). Alternative: treat kindless as `go`.
3. native pins root at a commit of this branch until merge (consumer-install).

# Native codecs: design notes (review draft; deleted before the PR)

## Root (github.com/thesyncim/goav)

- Registry holds many backends per codec, keyed (codec, backend, mode);
  same identity replaces. `Select(codec.Need)`: named backend, else most
  preferred fitting one. Default order: kindless > hardware > native > go,
  later registration first. `Prefer(id, mode, order...)` per codec and mode
  (empty = wildcard, most specific wins); `runconfig.WithCodecPreference(id, mode, order...)`.
- Fit = declared capabilities (pixel/sample formats, rates, profiles, bit
  depth, max size) **and settings**: `Need.Settings` (from
  `CodecSettings.KnobSettings()`) must parse against a knobbed backend's
  knobs, so rc=cqp skips a backend without cqp. Realtime is a tiebreak.
- Named backend never substituted. Named descriptor-only backend ->
  ErrUnavailable with its status (`not-installed`, ...). `*SelectError` lists
  rejections and satisfying backends.
- Knob schema in `codec`: `Knob`, `KnobType`, `Descriptor.Knobs/RawKnobs`,
  `ResolveKnobs`, `KnobValues`, `ErrInvalidSetting`; common vocabulary
  `KnobBitrate, KnobRateControl (cbr|vbr|cq|cqp), KnobQuality, KnobQP,
  KnobSpeed (0..10), KnobThreads, KnobTune, KnobGOP, KnobRealtime`. Scopes:
  `<backend>.<knob>`, raw `<backend>-params="k=v:k=v"`; `CheckScopes` refuses
  unknown scopes with a suggestion. Planning checks encode and decode knobs.
- `codec.Backend` struct -> `codec.BackendInfo{+Kind}`; `codec.Backend(name)`,
  `codec.BitDepth(n)`; Describe `backend=...`; Explain backend/reason/alternatives/knobs.
- Conformance found and fixed: goav1 encoder goroutine leak on Close/reset;
  gopus decode-after-Close sentinel.

## native module

- `native.OpenH264/VideoToolbox/AudioToolbox(opts...)`, `native.Library(abs path)`.
  native resolves the path (explicit > system absolute paths; lock/cache slot
  for later); kit takes a path only. Windows: LoadLibraryExW, DLL dir + System32.
- kit `Codec`: identity, Platforms, capabilities, knobs, Bitstream
  (Raw default; AnnexB/AVCC only for h264/hevc), Pixels (format string),
  `Libraries []Library{Name, Path, Fix, Symbols}` + `Version()` bound once at
  `Register` (goav.New). Failure -> descriptor-only with status
  not-installed / unsupported-version / unsupported-platform / missing-symbol;
  selection passes it over; `kit.Probe` returns the fix.
- Picture: format strings (i420/i422/i444/yuv4xxp10le/nv12), optional
  `Owner av.BufferOwner` for zero-copy lending (one Retain per plane).
- `kit.Params[T]` (c tags, `,inline`, `,first`) = raw schema + Apply.
- Live: `Updater` for Live knobs, else restart on keyframe; `kit.EventSetting`
  event carries knob=value; bitrate and keyframe events as before.
- `codectest.Run(t, opts...)`: per-backend suite + encode x decode matrix;
  passes for OpenH264 and for `bundle.Codecs()`. Skips not-installed backends
  with the fix unless GOAV_NATIVE_REQUIRE names them.

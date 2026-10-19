- Added HEVC: `av.CodecHEVC`, `codec.HEVC()`, and hvc1 in MP4 (the hvcC
  comes from the stream or the first keyframe's parameter sets).
- libcodec: a damaged unit no longer ends a decode (the kit drops it and asks
  for one keyframe); VideoToolbox no longer double-releases pictures after a
  dropped one. Runtimes share library bindings and hardware probes, so
  building one no longer leaks or re-probes. Backends declare an ABI window,
  a license (`kit.License`: class, notice, terms), and releases as plain
  values (`libcodec.Declarations`); statuses are active, not-installed,
  disabled, outside-window. `GOAV_LIBCODEC_DISABLE` turns backends off.
  Explain reports a backend's notice (`AdapterRequirement.Notice`).
  VideoToolbox HEVC no longer claims main10. Break: `kit.Codec` drops
  `Module` and `Package` (read from the constructors), `kit.Library` takes
  `System` paths, `kit.Find`, `kit.Releases`, and `kit.CodecHEVC` are gone,
  and a backend declaring no knobs takes no settings.
- A setting no backend takes is refused with each backend's reason and the
  nearest name, not as an unsupported codec.

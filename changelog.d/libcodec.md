- Added the `libcodec` nested module: codecs in shared libraries and OS
  frameworks, bound at run time through purego with `CGO_ENABLED=0`.
  `libcodec.VideoToolbox()` (H.264 and HEVC, hardware encode and decode,
  macOS), `libcodec.AudioToolbox()` (AAC-LC, macOS), and `libcodec.OpenH264()`
  (H.264, every OS; Cisco's binary carries the H.264 patent license). A
  library binds when the runtime is built; one that is missing registers its
  backend as `not-installed` and `kit.Probe` returns the fix. `libcodec/kit`
  declares a backend in one struct from any module (`libcodec/proof/extbackend`
  is a complete one in 104 lines), and `libcodec/codectest.Run` is its
  conformance suite.
- The codec registry holds many backends per codec. Selection takes the backend
  a request names (`codec.Backend("openh264")`, `encode h264 backend=openh264`,
  `decode backend=...`), else the most preferred one whose capabilities and
  settings fit: hardware, then shared libraries, then pure Go, reordered per
  codec and mode with `runconfig.WithCodecPreference`. Describe names the
  backend on each encode node; Explain reports it with the reason,
  alternatives, and knobs. Backends declare their settings as `codec.Knob`s,
  checked when a recipe is planned, with a common vocabulary (`rc`, `quality`,
  `qp`, `speed`, `threads`, `tune`, ...). Break: the descriptor struct
  `codec.Backend` is now `codec.BackendInfo` (with a `Kind`: hardware, lib,
  go), and goav1's encoder backend is named `goav1`. Migration:
  `Backend: codec.Backend{...}` becomes `Backend: codec.BackendInfo{...}`.
- Fixed: goav1's encoder leaked its worker goroutines on Close and on every
  reset; gopus decodes after Close return `codec.ErrClosed`.

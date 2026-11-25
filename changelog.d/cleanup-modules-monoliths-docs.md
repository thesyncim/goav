### Changed

- Fixed `player` CI `staticcheck` SA1019 failure on deprecated `(*oto.Player).Close()` by pausing the player and clearing the engine device callback via `SetDevice(nil, 0)`.
- Removed `unsafe` completely from `player` (`player/latency_darwin.go` now uses typed `purego` bindings) and `libcodec/proof/extbackend`.
- Added Go 1.27 portable `simd` kernels to `adapters/resize` (bit-exact single-pass 2x2 box downscale + in-register SIMD vertical pass, plus unrolled 3-tap/4-tap horizontal fast paths), cutting half-scale I420 resize time by >6x vs the previous 2-pass scalar path (~1.7x vs single-pass scalar) at 0 allocs/op.
- Skipped redundant full-canvas black/chroma fill in `video_composite.go` when a participating arm already covers the entire canvas (e.g. PiP backgrounds), and fixed `BenchmarkCompositeI420BlitKernel` to release its pooled canvas each iteration.
- Folded the zero-dependency `playoutav` package into the root `github.com/thesyncim/goav` module (import path unchanged) and consolidated the 9 core-only examples under a single `examples/go.mod`, reducing repository `go.mod` count from 22 to 13.
- Repinned all nested and example modules to `github.com/thesyncim/goav@v0.1.0-alpha.8`, added the missing local `replace` directive to `cameraav/go.mod`, and added `/cameraav` to Dependabot.
- Added `scripts/repin.sh` and `scripts/test-all-modules.sh` to automate cross-module root repinning and workspace-wide build/vet/test checks.
- Split the `container/matroska` `demuxer.go` (6,670 lines) and `muxer.go` (4,809 lines) monoliths and root `runtime_attach.go` (1,730 lines) into focused, domain-scoped files, and consolidated 8 micro root contract test files into `helper_contracts_test.go`.
- Added an upfront platform/license codec availability matrix to `README.md` and updated `docs/PERFORMANCE.md` with the new resize SIMD measurements.

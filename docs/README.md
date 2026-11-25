# Documentation

Everything you need to build, extend, operate, and tune `goav` pipelines:

| Doc | What's in it |
| --- | --- |
| [`GUIDE.md`](GUIDE.md) | Developer handbook: recipes, multi-input joins, live attach/detach, flow control, `*goav.BuildError` handling & generated error catalog, operations |
| [`EXTENDING.md`](EXTENDING.md) | Bundled codecs/containers/licenses, `libcodec`/`gplcodec`/`player`, and interfaces for custom sources, codecs, filters, joins, and sinks |
| [`REALTIME.md`](REALTIME.md) | RTP (`rtpav`), WebRTC/WHIP/WHEP (`webrtcav`), and the live control-plane socket (`ctlserver` / `goav ctl`) |
| [`CLI.md`](CLI.md) | The `goav` CLI and `launch` pipeline string grammar |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Pipeline model, package boundaries, pure-Go & pre-v1 roadmap decisions |
| [`PERFORMANCE.md`](PERFORMANCE.md) | Proven allocation pins, SIMD kernels, measured benchmarks, and perf-lab harness |
| [`RELEASING.md`](RELEASING.md) | Module pinning (`scripts/repin.sh`), multi-module checks (`scripts/test-all-modules.sh`), and signed tags |

Runnable examples live in [`examples/`](../examples). For security reporting and
contribution checks, see [`SECURITY.md`](../SECURITY.md) and [`CONTRIBUTING.md`](../CONTRIBUTING.md).

# Multi-input

A join turns several streams into one. It takes Streams and returns a Stream,
so its output encodes, taps, and forks like any other.

```go
// Mix two microphones into one Opus track.
mixed := goav.Mix(micA.Audio().Decode(), micB.Audio().Decode())
err := rt.Run(ctx, mixed.Encode(codec.Opus()).To(goav.Write("room.webm", w)))

// Picture in picture: the screen over the camera, at (960, 540).
pip := goav.Composite(cam.Video().Decode(), goav.At(screen.Video().Decode(), 960, 540))

// Switch between two live cameras; the first is active until switched.
live := goav.Select(camA.Video(), camB.Video())
err = task.Control(ctx, control.SelectActive("cam-b"))

// Several inputs into one file: Outputs that share a Destination share its mux.
rec := goav.Write("call.webm", w)
err = rt.Run(ctx, mic.Audio().Decode().Encode(codec.Opus()).To(rec), cam.Video().To(rec))

// Several files in one run: each stream reads its own file.
err = rt.Run(ctx, a.Video().To(rec), b.Audio().To(rec))
```

| Join | Inputs | Output |
| --- | --- | --- |
| `goav.Mix(streams...)` | audio frames (`.Decode()` packet inputs) | one mixed S16 audio stream; later inputs resample to the first's format, samples clamp |
| `goav.Composite(layers...)` | video frames, placed with `goav.At(s, x, y)` | one canvas the size of all layers' bounding box |
| `goav.Select(streams...)` | frames or packets with compatible facts | the active input as-is; `control.SelectActive(id)` switches it live |
| `goav.Join(name, stage, streams...)` | what `stage` declares | what `stage` declares: your own convergence stage |

## Behavior

- **Several files.** Files, providers, and `goav.Source` inputs share one run;
  each stream reads only its own input. With several inputs a file's stream
  ids carry its name (`1@a.webm`), so track 1 of two files stays two tracks in
  one mux (`TestCopyFromTwoFilesIntoOneMux`).
- **Timing.** Joins combine in arrival order. `.SyncByPTS()` on a Mix or
  Composite aligns inputs by presentation time instead: the earliest input
  leads, inputs within half a frame join it, inputs ahead sit the step out,
  and stale frames drop to catch up (counted under the `"sync"` reason in
  `task.Stats().Nodes["mix"].Dropped`). A seek or segment on one input
  re-syncs it; an ended input stops gating the rest.
- **Backpressure.** Offline inputs may lead a Mix or Composite by 16 buffered
  frames before their upstream waits, so a fast file cannot fill memory while
  a slow one catches up (`TestJoinBackpressuresFastArm`). Live joins keep
  buffering, so a sparse input never costs a live camera frames
  (`TestLiveJoinKeepsBufferingFastArm`).
- **Taps on inputs.** An input keeps its `.Decode()` and `.Tap(name)`: one
  decode feeds the join and anything attached from `task.Tap(name)`.
- **Hot paths.** Select passthrough and two-input Mix and Composite steps
  allocate nothing per message (`TestSelectorPassthroughAllocs`,
  `TestAudioMixStepAllocs`, `TestVideoCompositeStepAllocs`).
- **Your own join.** `goav.Join` lowers a caller-supplied stage through the
  same machinery as the built-ins: a third party can ship
  `Crossfade(streams...)` without core changes, proven from outside the core
  in `adapterproof/join_proof_test.go`. The stage contract is documented on
  `goav.Join`.
- **Joins nest.** A join's input can be another join: `goav.Mix(goav.Mix(a, b), c)`
  sub-mixes two inputs, `goav.Select(goav.Mix(a, b), goav.Mix(c, d))` switches
  between two live mixes (their ids are the sub-joins' names: `mix`, `mix-2`),
  and a sub-canvas is placed with `goav.At(goav.Composite(...), x, y)`. A nested
  join converts like any input (a 24 kHz sub-mix resamples into a 48 kHz outer
  mix), and its `.SyncByPTS()` scopes to its own inputs. It takes only taps and
  shape policies; other operations go on its inputs or after the outer join.

## Limits today

- Joins plan their inputs' streams up front, so join inputs must declare them:
  live and provider inputs (`goav.Input`) or `goav.Source`. Joining files is not
  supported yet (`mix_arm`).
- A join's output runs alone in one run: other Outputs beside it are refused
  with `stream_beside_join`.
- Forks across several inputs in one run are refused.
- A tap after a join's other operations is refused; tap right after the join.

## Dynamic Membership Boundary

`Mix(streams...)`, `Composite(layers...)`, and `Select(streams...)` are
recipe-time convergence: their upstream arms are part of the planned graph.
Runtime `task.Attach` and `input.OnStream` add downstream work from taps or
discovered streams; they do not mutate the arm set of an existing join.

For live audio rooms where tracks join and leave continuously, the supported
pattern today is source-owned track lifecycle: an application source owns the
participant registry, attaches ordinary Outputs from
`task.Track(input, participantStream)` before accepting media for that
participant, and emits `EventStreamAdded` / `EventStreamRemoved` for
observability. One Output can process the track independently while another
feeds a shared output mixer sink. `OnStream` is still available for sources that truly
discover streams automatically, but app-owned membership should use the
explicit input-stream anchor so the first frame is deterministic. The copyable
reference is `examples/dynamic-audio-room`.

A future first-class dynamic upstream mix API should be judged against a higher
bar than a convenience helper: it should preserve per-track routes, produce a
normal mixed stream that can itself be encoded or forked, and handle dynamic
source routing, per-track format validation/conversion, stream lifecycle
events, backpressure behavior, snapshots/stats, and detach/close semantics in
one coherent design. Until those pieces exist together, keeping the root
grammar small is the better API choice.

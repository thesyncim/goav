# Multi-input (convergence) — design + plan

The biggest expressiveness gap vs GStreamer: GoAV is structurally a **tree**
(one input fans out via `Branches`), with no **convergence** (N streams → one).
Fix convergence and a whole family falls out: audio mix, video composite,
sidechain, one-of-N selector, multi-port components, and the shape solver (a
join is exactly where two shapes must be negotiated).

## The 12 gaps are really 3 themes
- **A — Graph shape** (this doc): multi-input joins, shape-solving/auto-convert,
  selector, multi-port components. *All blocked by the same missing primitive.*
- **B — Control plane:** typed `Control` (keyframe/flush/drain), live switch,
  dynamic stream discovery, lifecycle. *Mostly surfacing machinery that already
  exists (`codec.ControlRequest`, `EventKeyframeRequired`, Attach/Pause/Rebranch).*
- **C — Time & scheduling:** seek/segment/rate, clock/sync, pull source. *A new
  axis; can come last.*

## The primitive: the dual of `Branches`
`Branches(Branch(...), Branch(...))` diverges 1→N. The mirror converges N→1:

```go
out := goav.Mix(
    goav.From(mic1).Audio(),
    goav.From(mic2).Audio(),
).Encode(codec.Opus()).To(dest)

goav.Composite(
    goav.From(cam).Video().Decode(),
    goav.From(slides).Video().Decode().Region(960, 540, 320, 180),
).Encode(codec.VP9()).To(dest)
```

Design properties (the bar: simplest, powerful, composable, **no noise for simple
ops**):
- **No noise:** `Mix`/`Composite` are new *entry points* parallel to `From`. A
  single-input chain never touches them — `From(x).Decode().Encode().To()` is
  byte-identical to today.
- **Composable:** the join output *is* a normal stream — it branches, taps,
  encodes, and **nests** (a Mix can feed a Composite; a join input can later be a
  tap → a true DAG).
- **One mechanism:** `Mix`/`Composite`/`Join(stage,…)` (audio, video, custom
  N-input escape) are thin faces over one `OpJoin` operation with N input arms.

## Validated architecture (why this is not a hack)
- The pipeline already supports convergence: a node accepts **multiple** input
  `EdgeSpec{From,To}` (the dual of the fan-out at media_plan_build.go:187). No new
  runtime graph capability is required.
- Each buffered node has a **single serial worker**, so a join stage needs **no
  internal locking** — lock-free by design holds.
- `av.Frame.StreamID` identifies which arm a frame came from → per-arm buffering
  needs no edge-origin plumbing.

## Status
**Slice 1 — DONE (runtime heart):** `audioMixStage` (`audio_mix.go`) — a real
`pipeline.Stage` that buffers per-arm by StreamID, advances one mix step when
every arm has a frame, sums S16-interleaved samples with clamping, and emits one
output EOS once all arms end. Tested (sum, clamp, alignment, EOS); `-race` clean.

**Slice 2 — DONE (expressible + runnable):** `Mix(arms...).To(sink)` — ONE new
public symbol that reuses the existing `Job`, so `.To`/`Build`/`Run` are
unchanged (thinnest possible API; single-input chains untouched). It lowers each
arm to a source node and converges them (N edges) into the mixer stage, then to a
Sink — a real running `Task`. `Describe` shows the convergence; tested
end-to-end (two audio sources → mixed `[150,150]` at a sink), `-race` clean.
First-slice scope: frame-source arms → Sink.

**Slice 3 — DONE (decode arms, zero new API):** packet-domain arms auto-insert a
decode stage before the mix — `Mix(From(opusSrc).Audio(), …).To(sink)` decodes
each arm to frames, then mixes. Reuses `builder.newDecodeStageNamed`; per-arm
unique decode node names. Tested (PCM packet arms → decode → mix → sink frame).
No public symbol added — the mixer just sees decoded frames.

**Slice 4 — DONE (encode the mixed output):** `Mix(...).Encode(codec.Opus()).To(dest)`
— `.Encode()` (method on the mix builder, no new top-level symbol) routes the
mixer through `compileEncodeDestinationPath` (build mixed input stream from the
arm shape → `prepareEncodeConfig` → encode stage → destination). Tested
(frame arms → mix → encode → packet sink). Together: `Mix(packetArms…).Encode(…).To(…)`
now decodes each arm, mixes, and re-encodes — a real audio mixer.

**Slice 5 — DONE (record to file):** `Mix(...).Encode(codec.Opus()).To(File(...))`
— the mixer routes through the reused encode→destination path; the file mux is
built and `openMuxDestinationStage` records the destination transaction onto the
shared `*builder`, carried to `newTask` so the file commits/aborts. Tested (mix →
encode → Ogg mux file). The real use case — mix audio and record — now works.

**Next slices:**
1. `Composite` (video) + `Select` (one-of-N) on the same `buildMix` mechanism.
2. Join shape-solving: auto-resample mismatched arms (today same-format S16).
2. Join shape-solving (theme A / gap #2): negotiate arm formats, insert
   resample/convert so the mixer's same-format precondition is guaranteed.
3. `Composite` (video) + `Join(stage,…)` (custom N-input) on the same mechanism.
4. `Select` (one-of-N) = a join + a runtime "active arm" control (theme B).
5. Theme B quick win: public `task.Control(ctx, RequestKeyframe(tap))` / `Flush()`
   over the existing `ControlRequest` machinery.

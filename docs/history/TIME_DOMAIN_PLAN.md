# Time-domain plan: GStreamer-class power, goav-sane surface

Status: complete (started and landed 2026-07-13, all six slices). Historical
record; `docs/ROADMAP.md` absorbed the survivors and `docs/FLOW_CONTROL.md`
owns the contract prose. Every slice names its executable evidence.

## Goal

Close the remaining structural gaps against GStreamer — the ones that make
GStreamer a *framework* rather than a library — without importing its
complexity: pipeline clock, synchronized playout, task state, accurate seek,
QoS feedback, and a latency model. Sanity rules: the grammar stays small,
capabilities are discovered by type assertion, the data plane stays lock-free
(atomics on the hot path, mutexes cold-side only), and every claim lands with
a pin. The exported surface is minimal by mandate: each slice adds the fewest
public identifiers that can carry the capability — internal types first,
exported only when a caller must name them; one constructor per concept; new
knobs ride existing vocabulary (flow, control, runconfig, lifecycle) instead
of new packages.

## Baseline (verified 2026-07-13)

- `av.Clock` (Now/Sleep) is injected per-runtime and consumed only by the
  realtime demux pacer (`format/pace.go`); `playoutav` uses a hardcoded
  timer. Rate is a per-source atomic; there is no shared timeline.
- `flow.SyncPolicy` gates align branches on a shared per-policy scheduler
  keyed by max-PTS per stream; sink-side synchronized playout does not exist.
- `control.Seek/Segment/Rate` reach `ControllableSource.Control`; the source
  re-anchors and emits `EventDiscontinuity`, but nothing flushes in-flight
  buffered messages — a seek drains stale media downstream first.
- Pause exists per-node (`pipeline.NodePauser`, buffered runner only), used
  by runtime attachments; there is no task-level pause and no preroll or
  readiness signal.
- Drop stats and `DropReporter` are observable post-hoc; nothing feeds
  lateness back upstream, though the event vocabulary already exists
  (`EventKeyframeRequired`, `EventBackpressure`, `EventBitrateChanged`).
- Latency pieces exist in isolation (BufferPolicy.MaxLatency, sync
  tolerance, the RTP jitter buffer); no model composes them.

## Slices

### T1. Task timeline (shared clock service) — LANDED

One timeline per task (internal `timeline` in `timeline.go`, consumed as
`av.Clock` — zero new exports): wraps the runtime `av.Clock` with an
atomically readable rate and pause epoch, mapping media time to run time. The
realtime demux pacer paces in timeline units instead of a private rate atom;
clock-aware sources receive the timeline through a structural
`UseClock(av.Clock)` assertion at provider open; `control.Rate` is task-wide —
it moves the shared timeline so every paced source re-anchors coherently, and
it no longer targets sources (breaking); `playoutav` gained clock injection
and dropped its hardcoded timer. Pause/Resume mechanics exist on the timeline
for T3 (no public wiring yet). Acceptance:
`TestTaskTimelineRateReanchorsAllSources`, `TestPlayoutUsesRuntimeClock`,
`timeline_test.go` (rate change mid-sleep wakes early, pause freezes Now,
concurrent readers under race); existing pacer/sync tests stay green.

### T2. Synchronized sink playout — LANDED

Opt-in deliver-when-due at the sink boundary: `flow.Playout(name)` (vocabulary
sibling of `flow.Sync`, options as self-methods `WithOffset`/`WithDropLate`)
declares that a destination's messages are held until `pts + latency offset`
is due on the task timeline, with hold-late (default) and drop-late modes and
drop stats via the existing `DropReporter` shape (folded under
`pipeline.DropSync`). `.Playout(policy)` attaches to stream chains and
branches exactly like `.Sync(policy)`; the internal gate lowers like a sync
gate, receives the task timeline through the runtime-clone clock seam, and
marks the timeline paced so `control.Rate` retimes playout. Branches reusing
one policy value share the first-message anchor (the A/V alignment), and
`EventDiscontinuity` resets it. `playoutav` remains the external proof that
scheduled *sources* need no core support; this slice is the *sink* half
GStreamer calls sink sync. Acceptance: `TestPlayoutSinkDeliversWhenDue`,
`TestPlayoutSinkFreezesOnPause`, `TestPlayoutAlignsBranchesSharingPolicy`,
`TestPlayoutDropLateShedsOverdueMedia`, `TestPlayoutRateChangeRetimesDelivery`,
`TestPlayoutDiscontinuityResetsAnchor`, refusal pins
(`TestPlayoutGateRequiresValidMediaTimebase`,
`TestPlayoutAttachRefusesStreamWithoutTimebase`).

### T3. Task state: Pause/Resume and readiness — LANDED

`Pause(ctx)`/`Resume(ctx)` on `LiveTask` freeze/continue the shared timeline
(paced sources and playout sinks stall coherently); Pause pauses TIME, not
the data plane — free-running tasks refuse like `control.Rate`;
`snapshot.Task.Paused` reports the fact; `av.EventTaskReady` through `Watch`
is the readiness signal. Contract prose: `docs/FLOW_CONTROL.md`. Acceptance:
`TestTaskPauseFreezesPacedFlow`, `TestTaskReadinessEventFires`,
`TestGraphReadinessReportsOnceAfterEverySinkFed`,
`TestTaskPauseRefusesFreeRunning`, `TestTaskPauseSnapshotAndCloseContract`,
`TestTaskRateChangeWhilePausedAppliesOnResume`, and
`TestTaskSeekWhilePausedAppliesAtNextReadBoundary` (seek while paused applies
at the pump's next read boundary; re-anchored media delivers even paused).

### T4. Seek with flush — LANDED

`task.Control(seek/segment)` is an accurate seek: the control is delivered to
the source first (a refused seek costs no media), then every buffered queue
downstream of it is drained — packets/frames shed under the new
`pipeline.DropFlush` reason, queued events preserved, and from a queued
`EventDiscontinuity` on everything kept (post-seek media is never flushed).
The capability stayed internal — no `NodeFlusher` export: the root asserts
`FlushDownstream(NodeRef)` structurally on the buffered runner, like
`UseClock`, over the live routing table. The direct runner buffers nothing
and seeks unchanged; segment keeps its window semantics on top. Acceptance:
`TestSeekFlushesInFlightMedia` (backlog counted, no pre-seek PTS after the
discontinuity), `TestSegmentFlushesLikeSeek`,
`TestSeekWithoutFlushableNodesStillSeeks`, the
`pipeline/flush_test.go` queue-semantics pins, and
`TestSeekFlushUnderLiveTraffic` (race).

### T5. QoS feedback — LANDED

Overdue playout admits (delivered late or shed), sync-gate sheds, and
buffered MaxLatency sheds publish `av.EventQoS` on the task's Watch stream
(payload via `av.QoSMetadata`/`av.EventQoSReport`), rate-limited per producer
to one report per second; on-time media pays nothing. Opt-in
`runconfig.WithQoSPolicy` maps reports to `control.Control` values the task
issues to itself — `Watch` + `Control`, packaged; no built-in policy is
exported (the func signature is the whole contract); refused controls surface
as `EventQoS` with `Cause`, payload-free so the policy never loops. Contract
prose: `docs/FLOW_CONTROL.md`. Acceptance: `TestQoSReportsLateness` (fields
and rate limit), `TestQoSPolicyDrivesBitrateControl`,
`TestGraphBufferedMaxLatencyShedReportsQoS`, and
`TestQoSPolicyRefusalSurfacesThroughWatch`.

### T6. Latency model + claims flip — LANDED

`Explain` composes the declared latency contributions along each synced or
playout path into one `path_latency_budget` decision: sync tolerance +
playout offset + the runtime's declared buffered `MaxLatency`, with queue
capacity reported as a message count, not a fake duration — declared policy
only, no runtime measurement, zero new exports (the code is a plan-decision
string). Two honesty deviations from the plan text above: playout offsets do
NOT default from the latency figure (independent declared budgets; inheriting
one would silently move delivery timing — `docs/FLOW_CONTROL.md` records
why), and jitter-buffer delay is absent because no provider seam declares
latency to the root (rtpav is a nested module). Claims flipped:
`docs/GSTREAMER_ALTERNATIVE.md` cites landed tests for clock service, sink
sync, task state, seek accuracy, and QoS; pull scheduling stays deferred
(backpressure covers it); `docs/NORTH_STAR.md` folded into
`docs/ROADMAP.md`. Acceptance: `TestExplainReportsPathLatency`; doc pins
green.

## Sequencing

T1 → T2 → T3 → T4 can land independently after T1; T5 needs T2's lateness
source; T6 last. Each slice: full gate (build/vet/test/race subset/
staticcheck/doc pins), commit, push — one safe point per slice.

## Non-goals (unchanged)

No cgo, no JIT, no global registries, no GStreamer element parity, no plugin
loading. Container/codec breadth (MP4 mux, MPEG-TS, H.264 encode) stays
demand-driven roadmap — power here means the time domain, not the format
matrix.

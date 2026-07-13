# Time-domain plan: GStreamer-class power, goav-sane surface

Status: in progress (started 2026-07-13). Owner: maintainer. This plan moves
to `docs/history/` when its slices close; `docs/ROADMAP.md` absorbs the
survivors. Follows repo culture: every slice names its executable evidence.

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

### T4. Seek with flush

`task.Control(seek)` gains flush semantics: pause emission at the source,
flush every buffered node downstream of it (new `pipeline.NodeFlusher`
capability on the buffered runner; drops recorded under a new `DropFlush`
reason), reset sync-gate and join pending state (they already reset on
`EventDiscontinuity`), inject the seek, resume. Segment keeps its existing
window semantics on top. Acceptance: TestSeekFlushesInFlightMedia (planned) (no
pre-seek PTS reaches a sink after the discontinuity),
TestSeekWithoutFlushableNodesStillSeeks (planned) (direct runner path).

### T5. QoS feedback

Playout/sync gates and MaxLatency shedding produce typed QoS reports
(lateness, drop pressure) as watchable events; an opt-in
`runconfig.WithQoSPolicy` maps reports to the existing control vocabulary
(SetBitrate, keyframe request, Rate) per task — the automatic path is exactly
what an application could do by hand with `Watch` + `Control`, packaged.
Acceptance: TestQoSReportsLateness (planned), TestQoSPolicyDrivesBitrateControl (planned)
(fake encoder observes the control).

### T6. Latency model + claims flip

Compose declared latencies (jitter buffer delay, sync tolerance, playout
offset, queue depth × observed pacing) into a per-path latency figure exposed
through `Explain` diagnostics and snapshots; playout sinks default their
offset from it. Then flip the deferred rows in
`docs/GSTREAMER_ALTERNATIVE.md` (clock service, sink sync, pull-scheduling
note) to cited tests, update `docs/ROADMAP.md` deferred list, tier rows in
`docs/API_SURFACE.md`, and `CHANGELOG.md`. Acceptance: doc pins green;
TestExplainReportsPathLatency (planned).

## Sequencing

T1 → T2 → T3 → T4 can land independently after T1; T5 needs T2's lateness
source; T6 last. Each slice: full gate (build/vet/test/race subset/
staticcheck/doc pins), commit, push — one safe point per slice.

## Non-goals (unchanged)

No cgo, no JIT, no global registries, no GStreamer element parity, no plugin
loading. Container/codec breadth (MP4 mux, MPEG-TS, H.264 encode) stays
demand-driven roadmap — power here means the time domain, not the format
matrix.

# Flow control

Flow control answers one question: when a live branch cannot keep up, who pays
the cost? In goav the answer is branch-local by default. A slow preview branch
should not stall a record branch, and an intentional drop should show up in
stats instead of disappearing.

Branch buffers are branch-local policy (`flow` package): `Blocking`,
`DropOldest`, `DropNewest`, `Latest`, `Unbounded`, with options for copy
bounds/mode, `MaxBytes`, and `MaxDelay` latency shedding.

What holds today (all `-race` clean, with tests):

- `flow.Blocking(capacity)` truly backpressures: a full queue blocks the
  producer (ctx-cancel escapes a stuck consumer) instead of erroring or tearing
  the pipeline down. The bare buffered default stays error-on-full
  (`ErrBackpressure`).
- The buffered producer path is lock-free: emit reads an atomically-swapped
  routing snapshot and per-node state, so a blocking send holds no graph lock
  and cannot deadlock topology changes or re-emitting stages.
- Fanout backpressure is independent: a full or slow branch sheds or paces
  itself without stalling siblings or the source; per-branch drop counters and
  reasons (`DropOldest`, `DropOverflow`, latency shedding) are reported through
  stats and snapshots.
- Branch-local `flow.SyncPolicy` gates can align live audio/video branches without
  changing unsynced delivery. When `flow.SyncDropLate()` sheds a message, stats use
  the existing drop accounting with `pipeline.DropSync`.
- Sink playout is opt-in deliver-when-due: `.Playout(flow.Playout(name))` holds
  a chain's or branch's packets/frames until their media time (plus a
  `WithOffset` latency budget) is due on the task timeline, so real-time
  destinations render on time, branches sharing one policy value align by PTS
  on the shared anchor, and pausing the timeline freezes delivery coherently
  (`TestPlayoutSinkDeliversWhenDue`, `TestPlayoutAlignsBranchesSharingPolicy`,
  `TestPlayoutSinkFreezesOnPause`). Hold-late is the default;
  `WithDropLate(tolerance)` sheds overdue media instead, reported through the
  same `pipeline.DropSync` accounting
  (`TestPlayoutDropLateShedsOverdueMedia`). Events pass through ungated, and
  `EventDiscontinuity` resets the shared anchor.
- Task pause is timeline control, not a data-plane gate: `LiveTask.Pause(ctx)`
  freezes the shared task timeline so paced sources and playout gates stall
  coherently; `Resume(ctx)` continues from the frozen reading with no pause
  gap in media time (`TestTaskPauseFreezesPacedFlow`). Free-running tasks
  refuse Pause with `format.ErrRateUnsupported` exactly like `control.Rate`
  (`TestTaskPauseRefusesFreeRunning`); already-due or seek-re-anchored media
  still delivers while paused (pause holds waits, it does not gate delivery);
  rate while paused lands on resume, seeks apply at the source's next read
  boundary, and `snapshot.Task.Paused` reports the frozen fact.
  Readiness is the preroll analog: `av.EventTaskReady` arrives through
  `Watch` exactly once, when every sink present at run start received its
  first media message (`TestTaskReadinessEventFires`; scope details live on
  the `av` constant); one atomic load on the sink delivery path,
  `TestGraphBufferedSteadyEmitAllocs` stays at zero.
- Lateness is watchable QoS data: an overdue playout admit (delivered late or
  shed), a sync-gate shed, and a `MaxLatency` queue shed each publish
  `av.EventQoS` through `Watch` — stream, gate/node, lateness, delivered-or-dropped
  (`av.EventQoSReport`), one report per producer per second
  (`TestQoSReportsLateness`, `TestGraphBufferedMaxLatencyShedReportsQoS`) —
  the task's delivery view, not network conditions. Opt-in
  `runconfig.WithQoSPolicy` turns reports into ordinary `task.Control` calls
  (`TestQoSPolicyDrivesBitrateControl`); refusals come back as `av.EventQoS`
  with `Cause` (`TestQoSPolicyRefusalSurfacesThroughWatch`).
- The latency budget is plan-time and declared, never measured: `Explain`
  emits one `path_latency_budget` decision per synced or playout path, summing
  the path's declared sync tolerance, playout offset, and the runtime's
  declared buffered queue `MaxLatency`; queue capacity rides along as a
  message count, never converted into a fake duration
  (`TestExplainReportsPathLatency`). A playout policy without `WithOffset`
  keeps offset zero rather than defaulting from a sync tolerance: the two
  declare different budgets (inter-branch skew allowance vs render-latency
  headroom), and inheriting one from the other would silently move delivery
  timing whenever an unrelated declaration changes. Transport-internal delays
  (the rtpav jitter buffer lives in a nested module) are not composed — no
  provider seam declares latency to the root today.
- Seek is flush-accurate on buffered tasks: once a source records a
  `control.Seek`/`control.Segment`, `task.Control` drains the stale pre-seek
  media queued downstream of it instead of letting it play out first — shed
  packets/frames are honest stats under `pipeline.DropFlush`, queued events
  survive, and nothing from a queued `EventDiscontinuity` on is touched
  (`TestSeekFlushesInFlightMedia`). Direct tasks buffer nothing and seek
  unchanged (`TestSeekWithoutFlushableNodesStillSeeks`).
- Custom sources see flow control per push: `push.X(...)` returns
  `(source.Result, error)` where deliberate sheds are `Dropped` with a nil error
  and `ErrBackpressure` keeps its flow-control meaning.

The producer-side cost of both paths is measured by `BenchmarkSourcePush`
(dropping vs blocking), and the steady buffered path is allocation-pinned by
`pipeline.TestGraphBufferedSteadyEmitAllocs`; see `docs/PERFORMANCE.md`.

## Runtime branch detach outcomes

Runtime branches are flow-control boundaries too: they may own buffers, taps,
and destinations that should close differently depending on why the branch is
leaving.

- `Mutable.Detach(ctx, attachment)` removes the branch and reports its
  destinations as closed.
- `Mutable.Detach(ctx, attachment, lifecycle.DrainBranch())` drains/finalizes the branch as
  committed. Use this for ordinary recording or participant-leave flows where
  the output should be kept.
- `Mutable.Detach(ctx, attachment, lifecycle.AbortBranch())` marks the branch output as
  abandoned. Use this after failed admission or diagnostic captures that should
  not commit.
- `OnStream(match, Branch(...))` drains rule-created branches when a
  dynamically discovered stream disappears.

```go
task.Watch(inspect.WatchTypes(
    av.EventBranchAttached,
    av.EventBranchDetached,
    av.EventDestinationCommitted,
    av.EventDestinationAborted,
    av.EventDestinationCommitError,
))
```

That watcher reports runtime branch lifecycle changes with the attachment
id/name and detach disposition, plus destination finalization events with the
destination name and runtime attachment metadata where applicable.

Remaining design choice: whether the bare buffered default should also block
rather than error on full (currently: only explicit `Blocking` blocks).

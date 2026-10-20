# Flow control

Flow control answers one question: when a live branch cannot keep up, who pays
the cost? Each branch chooses its queue policy. A blocking branch can pace its
shared producer and siblings; a dropping preview can instead bound its latency.
Intentional drops show up in stats.

Branch buffers are branch-local policy (`flow` package): `Blocking`,
`DropOldest`, `DropNewest`, `Latest`, `Unbounded`, with `BufferMaxBytes` and
`BufferMaxDelay` latency shedding.

What holds today (all `-race` clean, with tests):

- `flow.Blocking(capacity)` truly backpressures: a full queue blocks the
  producer (ctx-cancel escapes a stuck consumer) instead of erroring or tearing
  the pipeline down. The bare buffered default stays error-on-full
  (`ErrBackpressure`).
- A buffered emit takes no lock: it reads an atomically swapped routing
  snapshot and claims a slot and a queue position by CAS on the consumer's
  lock-free ring, so a blocking send cannot deadlock topology changes or
  re-emitting stages. A blocked producer parks on its own wake channel and
  is woken once the queue has both a free position and a free slot.
- Fanout visits targets on one producer. A full `Blocking` queue waits there,
  delaying later siblings and the next source message. Direct handlers run
  inline and also pace that producer. Dropping queues shed independently;
  per-branch drop counters and reasons (`DropOldest`, `DropOverflow`, latency
  shedding) are reported through stats and snapshots.
- A `.Buffer(...)` holds wherever the fork declares it: a recipe fork as well
  as an Output added with runtime `Attach`
  (`TestBuildTimeBranchBufferIsHonored`). On an offline run a declared buffer
  promotes the graph to buffered, so the fork runs on its own worker instead
  of inside its upstream's call (`TestBranchBufferPromotesOfflineGraph`).
  Where several files meet at one node (a shared mux), the buffered graph
  keeps their time order by letting each packet's work finish before the
  next enters, trading pipelining across packets for the same bytes.
- Encode ladders run their rungs in parallel by default: when one source fans
  out to two or more encoding branches, with only built-in operations and
  goav-owned outputs (Write/URI) each fed by one branch, each branch runs on
  its own worker. No caller code runs inside such a branch and each file still
  receives one branch's packets in order, so only wall time changes; a Sink,
  a `.Do` stage, or a shared output keeps the fanout serial unless a branch
  declares `.Buffer`
  (`TestEncodeLadderRunsRungsConcurrently`, `TestParallelLadderMatchesSerialRungs`).
- `Explain().Graph.Execution` reports the selected runner and admission mode;
  Explain Decisions report why they were chosen.
  Each node's `Execution` identifies a source, inline handler, or serial worker,
  with its actual queue policy and whether it can block its producer. The same
  fields appear in `Describe`, `task.Spec`, and `task.Snapshot().Spec`. A custom
  stage or sink can change an offline ladder from buffered to direct execution;
  the reason is inspectable. An explicit `.Buffer(...)` opts into worker
  concurrency. Every node's handler remains serialized.
- Stream-local `flow.SyncPolicy` gates can align live audio/video streams without
  changing unsynced delivery. When `flow.SyncDropLate()` sheds a message, stats use
  the existing drop accounting with `pipeline.DropSync`.
- Sink playout is opt-in deliver-when-due: `.Playout(flow.Playout(name))` holds
  a stream's packets/frames until their media time (plus a
  `WithOffset` latency budget) is due on the task timeline, so real-time
  destinations render on time, streams sharing one policy value align by PTS
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
  boundary, and `goav.TaskSnapshot.Paused` reports the frozen fact.
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
- Before delivering `control.Seek`/`control.Segment`, a buffered task sheds
  stale media from queues exclusively downstream of the targeted source.
  Same-source forks and rejoins remain exclusive. A queue shared with another
  upstream, and every queue after it, is preserved so seeking one arm cannot
  erase another arm's media. Those queues may deliver a bounded admitted
  pre-seek tail before the discontinuity. Shed packets/frames are recorded
  under `pipeline.DropFlush`; queued events and everything following a queued
  `EventDiscontinuity` survive (`TestSeekFlushesInFlightMedia`,
  `TestFlushDownstreamPreservesSharedQueues`). Direct tasks buffer nothing
  (`TestSeekWithoutFlushableNodesStillSeeks`).
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

- `task.Detach(ctx, attachment)` removes the branch and reports its
  destinations as closed.
- `task.Detach(ctx, attachment, goav.DrainBranch())` commits only after branch
  removal finishes delivering every queued message. A timeout or removal error
  fails remaining transactions before downstream retirement; whole-task
  `Close` abandons unfinished drains. Successful recording or participant-leave
  flows retain the completed output
  (`TestRuntimeBranchDrainDetachDeliversQueuedFrames`).
- `task.Detach(ctx, attachment, goav.AbortBranch())` marks the branch output as
  abandoned. Use this after failed admission or diagnostic captures that should
  not commit.
- Outputs from `input.OnStream(match)` drain their rule-created branches when a
  dynamically discovered stream disappears.

```go
task.Watch(goav.WatchTypes(
    av.EventBranchAttached,
    av.EventBranchDetached,
    av.EventDestinationCommitted,
    av.EventDestinationAborted,
    av.EventDestinationClosed,
    av.EventDestinationCommitError,
))
```

That watcher reports runtime branch lifecycle changes with the attachment
id/name and detach disposition, plus destination finalization events with the
destination name and runtime attachment metadata where applicable.

Remaining design choice: whether the bare buffered default should also block
rather than error on full (currently: only explicit `Blocking` blocks).

## Failure isolation

An Output may declare `.OnFailure(goav.IsolateBranch)`; the default is
`goav.FailTask`. Returned handler errors retire only its validated exclusive
suffix. This does not isolate stalls: a full blocking queue still paces its
shared producer. Snapshots retain failed branch and destination outcomes;
Detach joins pending retirement. See [Runtime contracts](RUNTIME_CONTRACTS.md)
for topology refusals, finalization states, and all-outputs-isolated semantics.

# Flow control: current state

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
- Borrowed packet fanout copies the producer-borrowed payload once into
  graph-owned backing, then binds refcounted read-only views into subscriber
  slots. Owned packets/frames and `CopyAlways` still copy per branch so a
  mutating subscriber cannot corrupt siblings.
- Custom sources see flow control per push: `push.X(...)` returns
  `(PushResult, error)` where deliberate sheds are `Dropped` with a nil error
  and `ErrBackpressure` keeps its flow-control meaning.

The producer-side cost of both paths is measured by `BenchmarkSourcePush`
(dropping vs blocking), and the steady buffered path is allocation-pinned by
`pipeline.TestGraphBufferedSteadyEmitAllocs`; see `docs/PERFORMANCE.md`.

## Runtime branch detach outcomes

Runtime branches are flow-control boundaries too: they may own buffers, taps,
and destinations that should close differently depending on why the branch is
leaving.

- `Task.Detach(ctx, attachment)` removes the branch and reports its
  destinations as closed.
- `Task.Detach(ctx, attachment, DrainBranch())` drains/finalizes the branch as
  committed. Use this for ordinary recording or participant-leave flows where
  the output should be kept.
- `Task.Detach(ctx, attachment, AbortBranch())` marks the branch output as
  abandoned. Use this after failed admission or diagnostic captures that should
  not commit.

```go
task.Watch(goav.WatchTypes(
    av.EventBranchAttached,
    av.EventBranchDetached,
))
```

That watcher reports runtime branch lifecycle changes with the attachment
id/name and detach disposition.

Remaining design choice: whether the bare buffered default should also block
rather than error on full (currently: only explicit `Blocking` blocks).

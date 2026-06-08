# Flow control (design)

Status: **decided — implementing.** Maintainer: (A) `Blocking` should truly
block with backpressure; (B) leave the bare default (`DropNever`) as
error-on-full (unchanged).

Plan: slice **FC-1** — buffered producer-side `atomic.Pointer` routing snapshot
(mirror of P2b/direct) so `emit` reads routes→target `*bufferedNode` and enqueues
**without `g.mu`** and without a per-message target slice (wide-fanout stays
0-alloc); queue-close-vs-send safety moves to per-node `queueMutex` + a
`queueClosed` flag set by Close/Remove. Then slice **FC-2** — add
`pipeline.DropBlock`, map `goav.Blocking`→it, blocking send
`select { case q<-slot: ; case <-ctx.Done(): }`, slow-consumer test.

## The bug

`goav.Blocking(capacity)` is documented as "intentional backpressure … when slow
branches should apply backpressure" (`branch_buffer.go`). But it maps to
`pipeline.DropNever`, and a full `DropNever` queue returns `ErrBackpressure`
(`drop.go` → `enqueue` `default`), which propagates up through `emit` →
`source.Start` → `Run` returns an error → **the whole pipeline tears down**. So a
single slow consumer kills the source and every sibling. `Blocking` does the
opposite of what it promises.

## Why the naive fix is unsafe (deadlock hazard)

The obvious fix — make the full-queue send block,
`select { case q<-m: ; case <-ctx.Done(): }` — is **not safe as-is**, because the
buffered producer `emit` enqueues to all targets sequentially **while holding
`g.mu.RLock`** (it is released only after the route loop). Blocking the send
extends that hold, and:

- A concurrent topology write (`AddSink`/`Connect`/`Close`, `g.mu.Lock`) then
  waits behind the blocked `RLock`.
- Go's `RWMutex` blocks *new* `RLock` acquisitions once a writer is pending. A
  downstream stage's `Handle` (run by a lock-free worker) calls `emit`, which
  needs a fresh `RLock` → blocks → the worker stops draining → the queue the
  source is blocked on never drains → **deadlock**.

It also head-of-line-blocks siblings: one full target stalls delivery to the
others (acceptable for pure backpressure, wrong when siblings use drop policies).

## Design

1. **Distinct `Block` policy.** Add `pipeline.DropBlock` (or a `Block bool` on
   `BufferPolicy`); map `goav.Blocking` to it. Leave the bare default
   (`""`→`DropNever`) and its tested `ErrBackpressure`-on-full behavior untouched
   (`TestGraphBufferedBackpressure`). This isolates the change to users who
   explicitly asked for backpressure.
2. **Producer-side lock-free first (prereq).** Extend the P2c routing snapshot to
   the buffered producer: `emit`/`enqueue` read the target (queue, policy,
   counters) from an `atomic.Pointer` snapshot and take **no `g.mu`**, so a
   blocking send holds no lock and cannot deadlock a topology write or a
   re-emitting stage. Queue-close-vs-send safety moves to the per-node
   `queueMutex` + a `closed` flag (same shape as P2c's `removed`/`closing`).
3. **Then** the blocking send for `DropBlock`: `select { case q<-slot:
   case <-ctx.Done(): return ctx.Err() }`. Source paces to the slowest blocking
   consumer; no teardown.
4. **Independent fanout backpressure (follow-up).** A full/slow target must not
   abort delivery to siblings or the source. With per-target snapshots this is a
   policy choice per edge; full design later.

Proof: a slow-consumer test — source + two sinks, one slow `Blocking` sink; assert
the pipeline does not tear down, the source is paced (not errored), and the fast
sibling still receives. `-race` clean.

## Decision needed (maintainer)

When a `Blocking` branch's queue is full and the consumer is **permanently
stuck**, the source blocks until `ctx` is cancelled (correct backpressure, but a
dead consumer stalls the source). Two sub-questions:

- **A.** Confirm `Blocking` should block-with-backpressure (recommended) vs. be
  renamed to advertise the current drop/teardown behavior.
- **B.** Should the **bare default** buffered branch (no explicit policy) also
  change from error-teardown to block, or stay `ErrBackpressure`? (Recommend:
  keep default as-is; only `Blocking` blocks. Changing the default is a broader
  semantic shift.)

# Performance

`goav` treats construction and steady-state media flow differently.

Cold paths may allocate: runtime/graph construction, registry setup, recipe
planning and validation, codec open/configuration, format probing, and
caller-side output buffer allocation.

Hot paths must avoid hidden allocation:

- per RTP packet
- per depacketized packet
- per decoded/encoded frame
- per mux/demux packet
- per direct pipeline message

The API can stay expressive only if those expressions collapse into direct,
reusable runtime objects. Declarative recipes may build plans, diagnostics, and
graphs up front; once running, stages reuse caller-owned messages, result
structs, frame planes, packet buffers, and scratch storage.
The intended shape is one cold-path executable `WorkPlan` and runtime `WorkPatch`;
packet, frame, event, and mux/demux loops must not route
through fluent recipe objects or workflow-specific compiler dispatch.

## Rules

- Prefer `Into` methods that fill caller-owned result structs; reset before reuse.
- Preallocate result slices and frame plane buffers; return capacity errors
  instead of appending beyond capacity.
- Avoid `fmt`, map writes, closure allocation, and error wrapping in hot paths.
- Keep recipe, flow, branch, tap, destination, and codec abstractions cold-path
  only; do not dispatch through them for each packet or frame.
- Prefer one planned graph over workflow-specific graph modes; a new operation
  must not add a per-packet abstraction layer.
- Fanout shares payload references unless an explicit policy requires a copy.

## Branch concurrency and fanout isolation

Two executors sit behind the one `Graph` interface: **direct** (synchronous,
inline, zero-copy, no isolation — a slow branch blocks siblings) and
**buffered** (each node on its own goroutine with a bounded queue, so a slow
branch drains independently). Per-branch concurrency is selected by giving a
branch a buffered policy — a planner/recipe choice, not identity magic.
Isolation goals for fanout-heavy work: no global serialization per message
(per-node atomic stats, atomically-swapped routing snapshots); independent
backpressure (a full branch paces itself, not siblings or the source); shared
payload bytes across the fanout instead of per-target copies.

## Current Guards

Run `go test ./...` plus the tagged adapter suites
(`go test -tags goav_goh264 ./adapters/goh264`, `go test ./adapters/govpx`).

Allocation guards cover: packet/frame/event and result resets, timestamp
helpers, drop-policy decisions, direct and buffered graph pass-through with
copied borrowed-media slots, codec decoder/encoder stages, format demux/mux,
IVF and Annex B read/write, resize/resample filters, RTP sequence/jitter/
depacketization (Opus, VP8, VP9, AV1, H264), the RTP source loop with timestamp
discontinuity tracking, RTCP feedback scratch, and the concrete gopus, govpx,
goav1, and goh264 adapter hot paths.

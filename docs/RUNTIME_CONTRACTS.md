# Runtime contracts and migration

Execution, resource finalization, branch failure, and RTP configuration have
separate observable outcomes. These contracts apply to recipes built with
`Runtime.Build` and to outputs added through `LiveTask.Attach`.

## Completion and finalization

`task.Run(ctx)` reports execution. A nil result permits successful finalization;
it does not mean a file was committed. The task enters `TaskFinalizing` until
`Close` finishes. `Runtime.Run(ctx, outputs...)` performs both phases and joins
their errors.

Destination snapshots have stable IDs and record actual finalization:

| State | Meaning |
| --- | --- |
| `DestinationOpen` | Execution may still deliver media. |
| `DestinationFinalizing` | Finalization has not completed. |
| `DestinationCommitted` | Successful finalization completed, including any transactional Commit. |
| `DestinationAborted` | A transactional Abort completed. |
| `DestinationClosed` | The resource closed without a commit or abort outcome. |
| `DestinationFailed` | Its actual finalizer returned an error. |

Each output has its own result. One failed commit does not relabel a sibling's
successful commit. Plain writers and sinks cannot undo delivered bytes or
frames: abandoning them records Closed rather than claiming an Abort occurred.
`av.EventDestinationClosed` accompanies that outcome; commit, abort, and
finalization-error events retain their existing meanings.

Starting Close before Run completes irrevocably abandons active outputs. A
later nil Run result cannot upgrade that decision to commit. Close after a
successful Run allows commit. Explicit `DrainBranch()` requests successful
finalization of the detached branch after its queued work drains.

A caller's deadline or `pipeline.ErrCloseWait` describes that call's wait. It
does not become a durable resource failure. Cleanup retains ownership and may
finish later. Repeated Close waits within its own context and reports retained
actual failures; `Stats.PendingCleanup` reports work still retiring. A second
`CloseContext` caller honors its deadline while another caller is closing.
This does not make an arbitrary user finalizer's synchronous Close interruptible.

`Snapshot().Err` supports `errors.Is`; `Error` is its JSON text. A node's
`pipeline.FinalizationError` carries both its name and stable identity, so two
nodes returning the same sentinel remain distinct. Snapshots retain actual
outcomes after nodes retire and after observer channels close. Events are best
effort, so use snapshots for authoritative state. A failed detached output's
cleanup can populate Err while the task remains Running and healthy outputs
continue.

## Optional output failures

The default policy is `FailTask`. Opt into isolation on an Output:

```go
required := input.Audio().Decode().To(goav.Sink(recording))
preview := input.Audio().Decode().To(goav.Sink(monitor)).OnFailure(goav.IsolateBranch)
task, err := rt.Build(ctx, required, preview)
```

The planner finds the exclusive downstream suffix feeding that Output. Sources
and shared ancestors remain task-wide. Shared destinations, joins, and unsafe
cross-boundary connections are refused with `branch_isolation_unsafe` before
opening planned resources. A custom graph must support `pipeline.FaultIsolator`.
Build and Attach use the same policy and checks.

A returned handler error atomically stops delivery to that suffix, cancels its
context, and starts automatic retirement through the normal drain/close path.
Healthy siblings continue. The error is retained as `BranchFailureError`;
`BranchFailed`, `PendingCleanup`, and `Retired` describe the failed branch even
after its nodes disappear. Detach joins pending retirement and returns cleanup
errors within its context and configured close-wait bound. Names remain reserved
until explicit Detach, so an old handle cannot remove a later attachment.
Healthy detached records are pruned; failed records last for the task's lifetime.

Isolation covers returned handler errors. It does not isolate panics, stalled
handlers, or a shared producer blocked on an optional branch's full queue.
Shutdown itself is not a branch failure. If every output opts into isolation,
Run can succeed even when every output failed; Explain emits
`all_outputs_isolated`, and callers inspect branch outcomes. Cleanup failures
still surface from Close.

## Inspectable execution

`Spec.Execution` reports `Mode`, shared-producer `Fanout`, and `Admission`.
Each `NodeSpec.Execution` reports source/inline/worker delivery, its normalized
queue, and whether it can block the producer. Selection reasons live in Explain
Decisions, including `execution_mode`, `serial_execution`, `branch_coupling`,
`time_ordered`, and `time_order_runtime`.

The execution planner supplies the same queue policies to lowering and
inspection, including Select's pinned lossless control queue. An explicit
branch buffer can promote the entire graph to buffered execution. A custom
stage or sink can keep an otherwise automatic encode ladder direct until the
recipe explicitly requests buffering. Existing defaults remain unchanged.

Timestamp admission uses the runtime's selection rules: eligible offline
sources take turns by packet time; buffered merges also settle each packet's
work before admitting the next. Direct graphs may order eligible file sources
while unrelated custom sources run freely. A plain blocking Admitter disables
this scheduling. Providers and decorators whose capabilities are unknown before
opening report `Admission: "runtime"`; the built task resolves it to
`"timestamp"` or `"arrival"`. One source alone uses arrival admission.

Full blocking queues and inline handlers pace their shared producer and later
siblings. Worker concurrency does not imply independent progress. See
[Flow control](FLOW_CONTROL.md) for queue and latency policies.

## RTP reader migration

`rtpav.PacketReader` now requires one atomic read method:

```go
ReadRTP(context.Context) (*rtp.Packet, *av.Event, rtpav.PayloadMap, error)
```

Migrate old two-result readers and `ReadRTPWithConfig` implementations to this
method. A stale decorator overriding the old method no longer satisfies the
interface. `rtpav.DecorateReader` and `ReadRTPFunc` wrap the authoritative method
and preserve optional RTCP feedback. There is no dynamic fallback through
`Events()` plus a later `PayloadMap()` read. Events remains an observer stream;
return nil when no observer is needed.

A read returns a packet, an optional preceding event, and the immutable map for
that exact record. Event-only reads are valid. Apply the event before the packet;
process valid records returned with EOF before draining. The next read may reuse
packet/event memory, so an asynchronous queue must copy it. Payload maps remain
immutable. `NewStaticPayloadMap` owns copies of configuration data.

`rtpav.Static(legacy, payloads)` adapts `StaticReader` only for fixed
configuration. A nil payload argument snapshots the legacy reader's initial map.
Codec changes and stream addition/removal are explicit refusals; dynamic readers
must implement atomic records. Malformed or regressing records fail with
`ErrInvalidConfiguration` or `ErrEpochRegression`; a legacy dynamic change fails
with `ErrOrderedConfigurationRequired`.

A map tagged through `PayloadCodec.StreamID` belongs to one track and has that
track's monotonic epoch. Separate tracks can use the same payload type and codec
with independent epochs. Untagged maps may cover distinct codecs and use a
session-wide map epoch announced by a configuration event; that map epoch may
differ from an event's stream epoch. An empty map on StreamRemoved uses the
event stream's epoch, so removing one track cannot advance another track's
generation. Same-codec tracks require stream tags.

Stream removal drains buffered complete media under its original map and
clears that stream incarnation. StreamAdded may reuse the ID with a fresh epoch;
packets arriving after removal are refused with `ErrUnknownStream`. Configuration
boundaries discard incomplete assemblies, while EOF drains valid complete
media. Jitter release retains each packet's configuration provenance. A single
configured JitterBuffer has one sequence/SSRC window; concurrent stream scopes
are refused with `ErrJitterStreamConflict`. Use one Source and jitter buffer per
track. WHEP supports multiple tracks without a shared jitter buffer.
`JitterBuffer` now requires `Reset()`: after draining an old stream incarnation,
Source resets its sequence and SSRC window before accepting a replacement or
re-added stream, and also resets it at an explicit discontinuity. Custom jitter implementations and wrappers must forward Reset;
`JitterRing` already implements it.

WebRTC UpdateCodec keeps an already-started read on its original generation
when its payload type still exists there. Updates may coalesce; a payload type
found only in a skipped intermediate generation is refused with
`ErrPayloadNotFound`, never guessed. UpdateTrack is a replacement boundary.
WHEP uses one bounded ordered queue for packets, configuration, and terminal
errors, preserving each track's identity and map. Configuration is never sent
through a lossy side channel, and cancellation releases blocked queue writers.

Custom `pipeline.Graph` implementations must also implement `CleanupError()`
and retain actual finalization errors independently of their event stream.
Consumers should update root and transport module pins together; the published
consumer gate tests the versions selected by player, CLI, and WebRTC dependencies.

These changes strengthen correctness and inspection. The allocation tests and
benchmark smoke do not establish comparative throughput or latency leadership;
that requires the release performance workloads in [Performance](PERFORMANCE.md).

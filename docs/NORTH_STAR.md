# North Star

This is the design contract that keeps the project from growing a separate API
for every media workflow. goav has one user-facing model:

```text
From(inputs...) -> stream selection -> operations -> taps -> branches -> destinations -> task
```

That model must hold for initial builds, runtime attach, diagnostics, control,
and examples. Normal users should not need graph handles, string routing, or a
separate workflow API for recording, transcoding, preview, diagnostics, or late
attachment.

## Contract

- A direct stream is an implicit `Branch("main")`.
- A branch is an ordered operation list plus destinations, source/tap anchor,
  media kind, buffer policy, and detach policy.
- A flow is reusable operations; it owns no source, destination, runtime state,
  or lifecycle.
- A destination value is the routing handle. Reusing one destination value or
  matching `Mux(name, destination)` groups branches into one mux or sink group.
- Shape validation is central. Inputs, operations, taps, flows, branches, and
  destinations all participate in the same compatibility check.
- Build and Attach share the same lowering model: `WorkPlan` for a full task,
  `WorkPatch` for a runtime branch update.
- Runtime observation is composition: `Branch + Do + Sink`, plus `Events`,
  `Watch`, `Snapshot`, `Stats`, `Explain`, and graph rendering.
- Runtime controls lower into `Controllable.Control`, `Mutable.Attach`,
  `Attachment.Rebranch`, `Mutable.Detach`, `Watch`, `Snapshot`, `Stats`, or
  `Close`; the control-plane binder never calls arbitrary methods.

The internal target shape is:

```text
BranchSpec -> operationSpec list -> WorkPlan / WorkPatch -> executable task
```

Every fluent operation appends one internal operation record:

```text
Decode -> OpDecode
Copy -> OpCopy
Shape/Auto/Require/Prefer -> shape operation
Resize/Resample -> transform operation
Do -> stage operation
Encode -> encode operation
Tap -> tap operation
```

## Evidence Map

The acceptance tests keep the design honest. The numbers below are the stable
labels used in test failure messages; use them as a map when reviewing whether
a new feature strengthens the grammar or bypasses it.

| Area | Current evidence |
| --- | --- |
| Grammar | #1 README and docs guards keep the public vocabulary on Input, Stream, Tap, Branch, Destination, Flow, Task. #2 direct chains lower like `Branch("main")`. #3 flows expose no destinations. #4 destination grouping is explicit by handle reuse or `Mux(name, destination)`. |
| Planner | #5 Build and Attach share canonical operation lowering. #6 Attach emits `WorkPatch` downstream of taps. #7 Explain reads from `WorkPlan`. #8 Snapshot reflects plan plus patches. #9 legacy workflow packages are gone. #10 workflow-kind dispatch is gone from normal recipes. |
| Shape | #11 Resize requires video frames. #12 Resample requires audio frames. #13 frames cannot go to byte destinations without Encode. #14 packet Copy to File succeeds. #15 decoded frames can end in Sink. #16 errors include operation, actual/expected shape, and fix. #17 conversions are inserted only under an explicit policy. |
| Branches | #18 branches after Decode share one decoder. #19 dropping preview branches do not stall archive branches. #20 Blocking backpressures. #21 branch drop counters are visible. #22 mutable branch output cannot corrupt siblings. |
| Runtime mutation | #23 Attach opens destinations before mutation. #24 attach failure rolls back. #25/#26 drain and abort are pinned for Rebranch and standalone `Mutable.Detach`. #27 Rebranch starts replacement before old detach. #28 failed Rebranch keeps the old branch. #29 Pause/Resume affects one branch. |
| Events and control | #30 Watch filters and stream/attach/backpressure events are pinned; `EventBranchAttached`/`EventBranchDetached` report runtime branch lifecycle, and destination commit/abort/error events report finalization. #31 Snapshot reports typed task, branch, destination, tap, and drop state. #32 Keyframe reaches adapters or fails clearly. #33 SetBitrate reaches encoders or fails clearly. |
| Sources | #34 custom packet source Copy to File. #35 custom frame source Encode to File. #36 source.Push reports Accepted/Dropped. #37 source EOS commits destinations. |
| Dynamic streams | #38 late streams attach branches. #39 ambiguous stream selection lists candidates and fixes. #40 removal detaches with rule-selected drain, abort, or plain detach through `OnRemove(...)`. |
| Multi-input and joins | #41 multiple inputs can share one destination. #42 codec/format/timebase mux compatibility is checked. #43 Mix joins audio branches. #44 join shape mismatch is solved or refused before mutation. |

## Current State

Done:

- `mediaPlan` and `runtimeBranch` parallel execution records were collapsed
  into `WorkPlan` and `WorkPatch`.
- Destination routing is by stable destination handle.
- Runtime branches use the same operation list as build-time branches.
- Mix, Composite, Select, custom joins, nested joins, and tap-backed join arms
  lower through the same recipe compile.
- Shape solving inserts real planned conversions under `.Auto(...)`; hard
  assertions use `.Require(...)`; preferences use `.Prefer(...)`.
- Dynamic streams attach through the normal branch planner.
- Watch, Snapshot, Stats, Attach, Rebranch, Detach, and task controls are
  public task capabilities.
- `Mutable.Detach` has explicit drain/abort outcomes, and branch attach/detach
  events are watchable without graph handles.
- Destination commit, abort, and commit-error events are watchable for task and
  runtime-branch destinations.
- `OnStream` rules can select their stream-removal behavior with
  `OnRemove(...)`.
- `SwitchAt` supports frame, keyframe, and media-time boundaries.
- Mux compatibility preflight rejects malformed declared timebase facts while
  still deferring unknown facts.
- Generated-source CLI runs and control sockets expose the same task model.

Still planned:

- Continue folding residual `streamIntent` validation/planning readers into the
  operation/work-plan model. Explain stream rows and adapter requirements, plus
  mux compatibility, already consume codec facts from `WorkPlan` operations.
- Expand `SwitchAt` boundaries beyond frame/keyframe/media-time if future
  runtime replacement modes need them.
- Finish the time-shape work: pipeline-wide clock service, A/V sink sync, and
  pull scheduling beyond branch-local `flow.SyncPolicy` gates.
- Decide the release minimum Go version before v1.

## Working Rule

New functionality belongs in the grammar only when it can answer these
questions without special cases:

1. What operation record does it append?
2. What shape facts does it consume and produce?
3. How does it appear in `Explain`, `Describe`, and `Snapshot`?
4. How does the same branch attach at runtime?
5. How does it fail before resources open?
6. How can a test source or external adapter exercise it end to end?

If those answers require a separate workflow path, keep the feature out of the
front-door API until the shared planner can express it.

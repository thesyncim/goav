# North Star — one grammar, one planner, one task

goav is a Go-native media **work planner** — *not GStreamer in Go*. Borrow the
power, not the vocabulary. **No public** Element/Pad/Bin/Bus/Probe/Caps/Pipeline-State.

Users describe `inputs + operations + taps + branches + destinations`; goav
provides shape solving, branch isolation, safe runtime attach/rebranch, custom
sources/destinations, multi-input composition, lifecycle-aware destinations,
events/snapshots, and clear errors.

## The one grammar

```
From(inputs...) -> select stream -> operations -> Tap -> Branches -> Destinations -> Task
```
Public nouns: **Input, Chain, Shape, Tap, Branch, Destination, Flow, Source, Task**
(`Operation` is a method on a Chain, not a headline noun). Never promote
`Record/Transcode/Path/Target/Output(s)/To("label")/Runtime.Graph/graph handles`.

Core identities:
- direct chain = implicit `Branch("main")`
- planned branch = `BranchSpec -> WorkPlan`
- runtime branch = `BranchSpec -> WorkPatch`
- Flow = reusable operation list · Destination = routing handle · Shape = compatibility contract

One planner for Build and Attach; one operation list; one branch/destination/shape
model; no workflow dispatch; no string routing; no graph handles for normal work.

## Target shapes

```
type BranchSpec struct { Name string; Source BranchSource; Media av.MediaType;
    Operations []OperationSpec; Destinations []Destination; Buffer BranchBuffer;
    Detach DetachPolicy; Err error }
type FlowSpec struct { Name string; Media av.MediaType; Operations []OperationSpec; Err error }
```
Every fluent method appends exactly one `OperationSpec`: Decode→OpDecode, Copy→OpCopy,
Shape→OpShape, Resize/Resample→OpTransform, Do→OpStage, Encode→OpEncode, Tap→OpTap.

`WorkPlan{Inputs,Operations,Taps,Branches,Destinations,Edges,Diagnostics}` /
`WorkPatch{Operations,Taps,Branches,Destinations,Edges,Rollback}` are the only
executable truth; Explain/Describe/Build/Attach/Snapshot all read from them.

## Feature areas (status)

- **IR collapse** (§1,2,15): one operation list; delete BranchSpec's parallel fields,
  branchComposePlan, mediaPlan-vs-workPlan, string routing. **TODO (biggest).**
- **Shape solving** (§3,4): validation upgraded to SOLVING — the one compile walk
  propagates shapes, selects converting adapters from the filter registry by
  capability delta, and inserts them as real planned operations under
  `.Auto(shape.AllowResample()/AllowResize()/AllowConvert())` (default OFF;
  refusals carry the exact policy to add; joins use an implicit arm policy);
  shape.Contract honored on Do stages. **DONE.** TODO: `.Require/.Prefer`,
  contracts on Source/Sink/Destination/Flow.
- **Tap** (§5): carry name/domain/kind/shape/producing-op/branch-owner/attach-policy/timebase. **partial.**
- **Branch control plane** (§6,7,10): pause/resume/stop/rebranch **DONE** (lock-free,
  both runners, gapless); typed `Task.Control` (Keyframe/Deliver/SelectActive via
  `pipeline.NodeInjector`, untargeted rides the data path, `.AtTap` by tap name) **DONE**;
  typed TaskState/BranchState/DestinationState in Snapshot **DONE**. TODO: SwitchAt*
  policies; `.At(node)` stays expert-only.
- **Branch isolation/ownership** (§8): lock-free isolation, independent fanout
  backpressure, per-branch atomic stats, MaxLatency/MaxBytes shedding, Blocking —
  **DONE (data plane)**. TODO: public CopyMode contract surfacing.
- **Events/Snapshot/Watch** (§9): typed EventFilter Watch; richer Snapshot. **partial.**
- **Dynamic streams** (§11): `OnStream(match, branches...)` rules on the job;
  sources announce via `av.EventStreamAdded` (typed `Event.Stream` payload);
  late branches attach through the one planner anchored source+stream
  (RouteByStream), solver included; removal detaches with drain; failures
  surface as `av.EventAttachError`. **DONE.** TODO: `When` conditions beyond
  stream identity; ambiguity candidate listing.
- **Multi-input/Join** (§12): Mix/Composite/Select grammars **DONE** over lock-free
  stages + `task.Control` live switch; variadic `From(a, b...)` with `InputName`
  narrowing and one shared Destination **DONE**; JoinSpec lowering through ONE join
  builder (profile tables, `Job.join` single route) **DONE**; join outputs compose
  (`.Tap`/`.Branches` via the shared chain lowering) **DONE**; planned `OpJoin`
  node in the IR — joins compile through the one recipe compile, `Describe()` ≡
  `Build()` guarded, the `buildJoin` graph-assembly route deleted **DONE**.
- **Time/sync** (§13): minimal TimeShape (TimeBase/Clock/Live/Latency) + Sync/Attach-at policies. **TODO.**
- **Source backpressure** (§14): result-aware `push.X(...) (PushResult, error)` —
  Accepted/Dropped per push, sheds stay nil-error. **DONE.**

## Acceptance tests (definition of done) — `[x]` holds · `[~]` partial · `[ ]` todo

Grammar: 1[~] README clean · 2[x] direct chain ≡ Branch("main") (both naming paths,
hard guards) · 3[ ] Flow no destinations/To · 4[~] Destination reuse groups by handle.
Planner: 5[x] Build+Attach share the canonical operation lowering · 6[x] Attach emits
WorkPatch only downstream of taps · 7[x] Explain from WorkPlan · 8[x] Snapshot =
WorkPlan+patches · 9[x] no
transcode import in core (package deleted) · 10[ ] no workflow-kind dispatch.
Shape: 11[x] Resize requires video frame · 12[x] Resample requires audio frame ·
13[x] frame→File w/o Encode fails · 14[x] packet→File w/ Copy ok · 15[x] Decode→frame
Sink ok · 16[x] errors include branch/op/actual/expected/fix · 17[x] auto-insert only
when enabled.
Branches: 18[~] two branches share one decoder · 19[x] slow Latest/DropOldest doesn't
stall archive · 20[x] Blocking backpressures · 21[x] per-branch drop counts · 22[~]
mutable frame branch can't corrupt sibling.
Runtime: 23[~] Attach opens destinations before mutation · 24[~] Attach failure rolls
back+aborts · 25[~] Detach+drain commits · 26[~] Detach+abort aborts · 27[x] Rebranch
starts replacement before detach · 28[x] Rebranch failure leaves old intact · 29[x]
Pause/Resume only that branch.
Events/Control: 30[~] events: attach/detach/shape-change/backpressure/commit · 31[~]
Snapshot full · 32[~] RequestKeyframe reaches adapter or fails clearly · 33[ ]
SetBitrate reaches encoder or fails clearly.
Source: 34[~] custom packet source Copy→File · 35[~] custom frame source encode→File ·
36[x] push reports drops/backpressure · 37[~] Source EOS commits destinations.
Dynamic: 38[ ] late stream attach · 39[~] ambiguous selection lists candidates ·
40[ ] stream removal detaches by policy.
Multi/Join: 41[x] From(a,v) one shared Destination · 42[~] multi-input mux validates
timebase · 43[x] Join mixes two audio branches · 44[x] Join shape mismatch
auto-resamples or fails before mutation.

## Attack plan (2026-06-09, decided): internal path unity

Themes A (graph shape) and B (control plane: `task.Control` + `pipeline.NodeInjector`,
keyframe/event injection, live Select switch) are closed at the surface. The next big
thing is NOT more API — it is making all power flow through one internal path.
Measured residue: `runtimeBranch` 251 refs (runtime_attach.go 2,298 lines),
`mediaPlan` 113 refs (2,567 lines of parallel plan), three join builders 700
near-mirror lines, `branchComposePlan` 23 refs, `destinationNames` 25 refs;
`work_plan.go`+`work_patch.go` exist (875 lines) but are not yet the truth.

Stages (each green, in dependency order):
1. **JoinSpec** — Mix/Composite/Select lower through ONE join builder. **DONE**
   (+ join outputs compose: `.Tap`/`.Branches` through the shared chain lowering).
2. **Typed lifecycle states** — TaskState/BranchState/DestinationState in
   Snapshot. **DONE.**
3. **runtimeBranch → WorkPatch** — **DONE.** The attach-side parallel IR is
   deleted; Attach walks the same canonical OperationSpec chain as Build
   (shared shape algebra + stage/destination constructors) and emits the
   workPatch as THE plan; snapshots render from it.
4. **mediaPlan → WorkPlan-primary** — **DONE.** The `mediaPlan` aggregate is
   deleted; `buildWorkPlan` builds the plan once inside the compile; Explain /
   lowering / mux-compat / taps all read workPlan; destinations route by stable
   handle IDs (`destination/<label>`), not name strings; the opaque
   `Job.Plan()` is off the public surface.
5. **Planned JOIN node** — **DONE.** Joins compile through the one recipe
   compile: `joinPlan` plans N arm sub-chains converging into an `OpJoin` node,
   `Describe()` ≡ `Build()` is guard-tested per kind, and the hand-assembled
   `buildJoin` route is deleted (`Job.join` survives only as captured intent).
6. **Shape solver** — **DONE.** Validation became solving in the one compile
   walk; the mix arm-resample hook and the branch-compose inline resize/
   resample synthesis are absorbed (deleted) into it.
7. Remaining: SwitchAt* policies; time/clock/seek (theme C — pull scheduling
   is the keystone).

## Execution order (condensed)

Work residue-by-residue, one deletion per slice, each slice green: operations as
the single source of truth (parallel BranchSpec/streamIntent fields die as their
readers re-point to `OperationSpec`), then `branchComposePlan` + the lowerer
unification, then `mediaPlan` → `WorkPlan`, then destination-by-handle routing
(`destinationNames` dies). Naming unification for single-branch compositions
(direct chain ≡ named branch encode nodes) is a maintainer design call recorded
in git history. After the collapse: shape solving, SwitchAt* policies,
dynamic streams, TimeShape.

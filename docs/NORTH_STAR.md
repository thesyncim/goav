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

- **IR collapse** (§1,2,15): one operation list; delete BranchSpec's parallel fields
  (decode/decodeCodec/encode/postEncodeTaps/destinationNames/from/tap/label/policy/buffer),
  branchComposePlan + transcode-in-core, mediaPlan-vs-workPlan, string routing. **TODO (biggest).**
- **Shape solving** (§3,4): upgrade validation → solving (validate order, infer facts,
  select adapters, auto-insert safe conversions when enabled): `.Require/.Prefer/.Auto(AllowConvert/Resample/Resize/FormatConvert)`; ShapeContract on Source/Do/Sink/Destination/Flow. **TODO.**
- **Tap** (§5): carry name/domain/kind/shape/producing-op/branch-owner/attach-policy/timebase. **partial.**
- **Branch control plane** (§6,7,10): pause/resume/stop/rebranch + lifecycle states + Control
  (RequestKeyframe/SetBitrate/Flush/Drain/Discontinuity). **DONE this session:** pause/resume/stop/rebranch
  (lock-free, both runners, gapless, integration-tested). **TODO:** SwitchAt* policies, formal state enums, Control(...) typed controls.
- **Branch isolation/ownership** (§8): isolated downstream buffers, per-branch drops/stats,
  copy-if-mutable. **DONE (data plane):** lock-free isolation, independent fanout backpressure, per-branch atomic stats, MaxLatency shedding, Blocking. **TODO:** public CopyMode contract surfacing.
- **Events/Snapshot/Watch** (§9): typed EventFilter Watch; richer Snapshot. **partial.**
- **Dynamic streams** (§11): OnStream/When; late-stream attach; ambiguity lists candidates. **TODO.**
- **Multi-input/Join** (§12): From(a,b...) one Destination; explicit Join (MixAudio etc.). **partial multi-input; Join TODO.**
- **Time/sync** (§13): minimal TimeShape (TimeBase/Clock/Live/Latency) + Sync/Attach-at policies. **TODO.**
- **Source backpressure** (§14): result-aware `Push.X(ctx,...) (PushResult, error)`. **TODO.**

## Acceptance tests (definition of done) — `[x]` holds · `[~]` partial · `[ ]` todo

Grammar: 1[~] README clean · 2[~] direct chain ≡ Branch("main") — **structurally identical** `Describe()` graphs (same source/select/decode/mux + edges + shapes); ONLY the encode node name differs (`encode-audio` direct vs `encode-main` branch). **Investigated:** the difference is the implicit branch's name (direct → named by stream `audio`; explicit → `main`) feeding `"encode-"+branch.Name`. Tried unifying `planOperationNodeName` (`planBranchPrivateOwner`: map `"main"`→stream) — **reverted:** it broke `Describe()≡Build()` because branch-composition jobs resolve node names through *different plan representations* (`branchComposePlan` → `mediaPlan` → `graphPlan`) for Describe vs Build, so naming can't be unified in one spot. **→ #2 is GATED on collapsing those plans into one (steps 3–5). Do them first, then #2 falls out for free.** · 3[ ] Flow no destinations/To · 4[~] Destination reuse groups by handle.
Planner: 5[ ] Build+Attach same planner · 6[~] Attach emits WorkPatch only downstream of taps · 7[ ] Explain from WorkPlan · 8[~] Snapshot = WorkPlan+patches · 9[ ] no transcode import in core · 10[ ] no workflow-kind dispatch.
Shape: 11[x] Resize requires video frame · 12[x] Resample requires audio frame · 13[x] frame→File w/o Encode fails · 14[x] packet→File w/ Copy ok · 15[x] Decode→frame Sink ok · 16[~] errors include branch/op/actual/expected/fix · 17[ ] auto-insert only when enabled.
Branches: 18[~] two branches share one decoder · 19[x] slow Latest/DropOldest doesn't stall archive · 20[x] Blocking backpressures · 21[x] per-branch drop counts · 22[~] mutable frame branch can't corrupt sibling.
Runtime: 23[~] Attach opens destinations before mutation · 24[~] Attach failure rolls back+aborts · 25[~] Detach+drain commits · 26[~] Detach+abort aborts · 27[x] Rebranch starts replacement before detach · 28[x] Rebranch failure leaves old intact · 29[x] Pause/Resume only that branch.
Events/Control: 30[~] events: attach/detach/shape-change/backpressure/commit · 31[~] Snapshot full · 32[ ] RequestKeyframe reaches adapter or fails clearly · 33[ ] SetBitrate reaches encoder or fails clearly.
Source: 34[~] custom packet source Copy→File · 35[~] custom frame source encode→File · 36[ ] push reports drops/backpressure · 37[~] Source EOS commits destinations.
Dynamic: 38[ ] late stream attach · 39[~] ambiguous selection lists candidates · 40[ ] stream removal detaches by policy.
Multi/Join: 41[~] From(a,v) one shared Destination · 42[~] multi-input mux validates timebase · 43[ ] Join mixes two audio branches · 44[ ] Join shape mismatch fails before mutation.

(Flow-control acceptance — 19,20,21,27,28,29 — landed this session; lock them in as guards.)

## Execution order (safe slices, one residue/feature per iteration)

Revised after investigating #2 (see Grammar #2): the plan collapse must come
**before** node-naming/#2, because naming diverges across the 3 plan reps.

1. **DONE** Lock in `[x]` guards (#13/14/15 pass; #11/12 next).
2. **Operations = single source of truth** (§1, #3): re-point readers of
   postEncodeTaps/encode/decode/decodeCodec to the OpTap/OpEncode/OpDecode
   operations; delete each field once unread. One field/slice.
   **postEncodeTaps DONE** — `operationSpecTaps` reads taps from `OpTap`; deleted
   `chainStepTapIntents`/`postPacketTapIntents` + the build/runtime fallback loops;
   deleted the spec-level fields on `jobStreamBuild`/`streamBuild`/`BranchSpec`/`chainSpec`.
   Only `runtimeBranch.postEncodeTaps` remains, *derived* from operations for the
   deferred-tap-after-lazy-encode insert. Next field: encode/decode/decodeCodec.
3. **#9/#5**: quarantine→delete `branchComposePlan` + transcode import from core
   (compat helpers live outside core, emit From/Branch specs). This removes
   `planBranchesFromBranchComposePlan` so there is one branch-plan source.
   **Mapped:** `branchComposePlan` has TWO sources — `composePlan()` (recipe
   `Branches()`) and `branchComposePlanFromTranscode` (legacy transcode API). The
   recipe path round-trips `BranchSpec → branchComposeBranch → streamIntent`
   (`streamIntentFromBranchComposeBranch`); `branchComposeBranchOperationSpecs`
   already derives ops from `.Operations`, so the scalar mirrors
   (Resize/Resample/DecodeConfig/Encode/Decode/Copy) only re-materialize
   `streamIntent.Decode/Encode` + feed the transcode compile path (`runtime_transcode.go`).
   Collapse order: (3a) make recipe `Branches()` populate `state.intent` branch
   `streamIntent`s so `buildMediaPlan` takes `planBranches` (one branch-plan source,
   no round-trip); (3b) `branchComposePlan` becomes transcode-only → move the bridge
   to a compat pkg, drop the core `transcode` import; (3c) then encode/decode
   `streamIntent` collapse (the woven 51/102-read fields) + #2 fall out together.
   **3a blockers (tested+reverted, then diagnosed):** `buildMediaPlan` can't just
   use `intent.Streams` instead of `streamIntentsFromBranchComposePlan(state.plan)`.
   The two streamIntent converters diverge on ≥4 fields — `branchStreamIntent`
   (intent source) sets `From` (the branch tap-anchor), `Taps`, and a **lossless**
   `Encode`; `streamIntentFromBranchComposeBranch` (plan source) sets `CodecChange`
   but leaves `From`/`Taps` empty and uses the **lossy** `codecSpecFromEncodeConfig`.
   The reverted probe broke `TestPlannedBranchSplitOperationsRespectEarlierTapAnchors`
   via `From` (the anchor drives split tap-anchor planning), not the split itself.
   So 3a = reconcile the two converters into one (carry From/Taps + a faithful
   Encode on the compose path, or recompute split via `splitOperationSpecsByShared`),
   a dedicated multi-field reconciliation — not a loop-sized slice.
   **#2 layer (corrected):** `graphPlan.Describe()` returns `p.spec()` (the lowerer
   graph spec), NOT the mediaPlan — so `Describe()≡Build()` is automatic (both share
   `p.spec()`). **#2 lives purely in the lowerer spec generation:** direct chains use
   the stream lowerer (`mediaPlanStreamLowererForState`); `Branch("main")` uses
   `mediaPlanBranchComposerLowerer`→`newMediaPlanBranchComposeGraph`→`planBranchComposeRoutes`
   (a *separate* graph-building subsystem from `branchComposePlan`), which names the
   encode node differently. `buildMediaPlan`/`planBranches`/`planOperationNodeName` are
   the **Explain/operationPlan/workPlan side only** (step 4, slice 26 prep) and do NOT
   feed Describe. So: #2 = unify the two lowerer spec subsystems (stream vs
   branch-compose), so a 1-branch composition lowers identically to a direct chain.
   The `branchComposePlan` retirement = re-point `newMediaPlanBranchComposeGraph` off
   `branchComposePlan` onto the stream-lowerer path. (Earlier "Describe's mediaPlan"
   claim was wrong — corrected here.)
4. **#7/#26**: fold `mediaPlan` into a `WorkPlan` view; one plan for
   Explain/Describe/Build/Attach/Snapshot → naming resolves once.
5. **#2 falls out**: with one plan path, direct chain and `Branch("main")` name
   identically; the `planBranchPrivateOwner` (`"main"`→stream) tweak then closes #2
   cleanly. Remove the Skip in `TestNorthStarDirectChainEqualsExplicitMainBranch`.
6. **#4/Destination-by-ID**: drop destinationNames/string routing; same handle =
   one group; same name+different config = planning error.
7. Then features: shape solving (§3), Control plane (§10), PushResult (§14),
   Join/multi-input (§12), dynamic streams (§11), TimeShape (§13).
7. **Shape solving** (§3): `.Auto(AllowConvert/...)` + ShapeContract on custom components; insert safe conversions when enabled.
8. **Control plane** (§10): typed Control(RequestKeyframe/SetBitrate) routed to capable source/encoder via existing keyframe-event machinery; SwitchAt* rebranch policies.
9. **PushResult** (§14), **Join/multi-input** (§12), **dynamic streams** (§11), **TimeShape** (§13).

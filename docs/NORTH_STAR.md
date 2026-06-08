# North Star — one grammar, one plan

goav is a Go-native media **work planner**. Users describe
`input + operations + taps + branches + destinations`; goav produces one
inspectable `WorkPlan`, one runnable `Task`, late branches via `Attach`, and clear
shape/destination errors. **One way to do one thing.**

## The one grammar

```
From(inputs...) -> select stream -> operations -> Tap -> Branches -> Destinations -> Task
```

Public nouns: **Input, Chain, Tap, Branch, Destination, Flow, Task**. (`Operation`
is a *method on a Chain*, not a headline noun.)

Never promote: `Record, Transcode, Decode helper, Path, Target, Output, Outputs,
To("label"), Runtime.Graph, graph handles`.

Core rule: **a direct chain is just an implicit `Branch("main")`** — it lowers to
the same internal model as the explicit `Branches(Branch("main")...)` form. No
separate build paths for copy-to-file / decode-to-sink / encode-to-file / branch
composition / runtime attach / transcode — all are branches with operations and
destinations.

## One internal model

```
Composition -> WorkPlan -> pipeline.Graph -> Task          (build)
BranchSpec + TapInfo -> WorkPatch -> apply atomically       (attach)
```
Same planner, validation, shape checks, destination grouping, lifecycle for both.

Every fluent call appends exactly one `OperationSpec`: `Decode→OpDecode`,
`Copy→OpCopy`, `Shape→OpShape`, `Resize/Resample→OpTransform`, `Do→OpStage`,
`Encode→OpEncode`, `Tap→OpTap`. Branch, direct chain, runtime branch, and Flow all
share this one operation representation.

Target `BranchSpec`: `{Name, Source, Media, Operations, Destinations, Buffer,
Detach, Err}`. Target `FlowSpec`: `{Name, Media, Operations, Err}` (no To, no
destinations, no labels, no source, no runtime state).

## Residues to delete (current debt)

- `BranchSpec` parallel fields: `decode, decodeCodec, encode, postEncodeTaps,
  destinationNames, from, tap, tapDomain, policy, label, buffer (pipeline.BufferPolicy)`.
  → derive from `Operations` / `Source` / `BranchBuffer`; delete once no readers.
- `branchComposePlan` / `branchComposeBranch` / `branchComposeTarget` and the
  `goav/transcode` import from core (`branch_compose_plan.go`, `runtime.go`,
  `runtime_transcode.go`). Core must not depend on transcode.
- `mediaPlan` as a separate semantic plan → make it a view of `WorkPlan`.
- string destination refs (`destinationNames`, `Destinations []string`) →
  Destination identity by stable internal ID; same handle = one group; same name +
  different config = planning error.

## Acceptance checklist (definition of done)

Status: `[ ]` todo · `[~]` partial · `[x]` holds today (verify with a guard test).

- [ ] 1. README uses no Record/Transcode/Path/Target/Outputs/Output(label)/To("label")/Runtime.Graph. (front-door tests partly enforce)
- [ ] 2. Direct chain and explicit `Branch("main")` produce equivalent `WorkPlan`.
- [ ] 3. `BranchSpec` normalizes to only operations + destinations.
- [ ] 4. `Flow` has no destination fields and no `To`.
- [~] 5. Flow applies to direct chain, planned branch, and runtime branch.
- [~] 6. Same `Destination` handle (audio+video branches) creates one group.
- [~] 7. Same destination name + different config fails during planning.
- [ ] 8. Shape mismatch fails before graph mutation with an actionable error.
- [x] 9. Resize requires a video frame shape. (guard TODO)
- [x] 10. Resample requires an audio frame shape. (guard TODO)
- [x] 11. Frame branch to File without Encode fails. (guard TODO)
- [x] 12. Packet branch to Sink(packet) succeeds. (guard TODO)
- [x] 13. Decode to Sink(frame) succeeds. (guard TODO)
- [x] 14. Copy to File succeeds. (guard TODO)
- [ ] 15. Planned branch and runtime attach use the same planner.
- [~] 16. Runtime attach emits `WorkPatch` only downstream of an existing tap.
- [~] 17. Two branches after one decoder share the decoder.
- [~] 18. Detach does not close shared upstream nodes.
- [x] 19. Branch buffers isolate slow branches unless `Blocking` is selected. (pause/backpressure done)
- [~] 20. Transactional Object commits on clean completion.
- [~] 21. Transactional Object aborts on failed attach.
- [~] 22. Custom Source feeds frame sink and encode-to-file paths.
- [~] 23. Multi-input audio/video can write one shared Destination.
- [~] 24. Ambiguous stream selection lists candidates.
- [ ] 25. No core composition code imports `goav/transcode`.
- [ ] 26. Explain is generated from `WorkPlan`.
- [~] 27. Snapshot reports branches, taps, destinations, and lifecycle state.
- [ ] 28. A new operation needs only a constructor + shape contract + component builder — no workflow compiler.

## Execution order (safe slices, one residue at a time)

1. Lock in `[x]` items as guard tests (#9–14, #19) — establishes the executable spec.
2. Make `Operations` the single source of truth; re-point readers of
   `postEncodeTaps`/`encode`/`decode`/`decodeCodec` to the operation list; delete each
   field once unread. (#3)
3. Prove #2 (`Describe()` of direct chain == explicit `Branch("main")`).
4. Quarantine then delete `branchComposePlan` + the transcode import from core. (#25, #5)
5. Fold `mediaPlan` into a `WorkPlan` view; Explain/Describe/Build/Attach/Snapshot read one plan. (#26)
6. Destination-by-ID; drop `destinationNames` string refs. (#7)

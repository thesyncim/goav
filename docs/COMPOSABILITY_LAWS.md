# Composability laws

These are the design laws that keep goav from turning into several workflow
APIs. Read this as the short reviewer checklist: if a change breaks one of
these laws, it is probably adding a new workflow surface instead of extending
the grammar. The tests below are executable evidence; this document is the map.

| Law | Evidence |
|---|---|
| A direct stream chain is syntax sugar for `Branch("main")`. | `TestNorthStarDirectChainEqualsExplicitMainBranch`, `TestNorthStarDirectChainEqualsExplicitMainBranchWithTransform` |
| `Flow` owns ordered operations only; sources, destinations, runtime, lifecycle, and branch ownership stay outside flows. | `TestStoredOperationListsMirrorFlowBranchAndDirectStreamWork`, `TestFlowBranchesStayOnJobAndBuildIntent`, `TestFlowReportsShapeContractAndTaps` |
| Build-time branches and runtime `Mutable.Attach` use the same branch grammar and operation lowering. | `TestTaskAttachRuntimeFlowCopyBranchFromPacketTap`, `TestTaskAttachRuntimeDecodeResampleEncodeMuxBranchFromPacketTap`, `TestTaskAttachRuntimeFlowCustomEncodeMuxBranch` |
| `Describe()` is the graph shape that `Build()` installs. | `TestStreamRecipeDescribeMatchesBuiltGraph`, `TestBranchCompositionRecipeDescribeMatchesBuiltGraph`, `TestJoinDescribeEqualsBuildMix`, `TestJoinDescribeEqualsBuildNestedMix` |
| Generated recipe cases preserve structural laws under a deterministic corpus: `Describe()` equals the built graph, a direct chain equals `Branch("main")`, runtime `Attach` installs the same branch subgraph as build-time branches, and nested mixes match their flat equivalent where no per-stage clamp occurs. | `TestCompositionLawsHoldForGeneratedRecipes` |
| `Explain()` reports solver insertions and soft preferences without opening resources. | `TestAutoInsertsFormatConvertThroughRegisteredAdapter`, `TestRequireSatisfiedByAutoConversion`, `TestPreferUnsatisfiableIgnoredWithDiagnostic` |
| Destination grouping is explicit: `Mux(name, destination)` groups matching destinations by caller intent, while repeated ungrouped handles refuse. | `TestMuxGroupsBranches`, `TestMuxSurvivesWithAndCopy`, `TestFromMultiInputPlanDedupesMuxDestination`, `TestSameHandleGroupingRequiresMux`, `TestTaskAttachRuntimeBranchGroupSharesMuxSinkDestination`, `TestTaskAttachRuntimeBranchGroupSharesMuxDestination` |
| Branch-local policy and mutation cannot corrupt siblings. | `TestCopyContractMutableFanoutBranchCannotCorruptSibling`, `TestFromMultiInputChainsKeepIndependentAutoPolicies`, `TestBranchBufferIsNormalBranchAPI` |
| Runtime attach failures roll back atomically and close prepared components. | `TestTaskAttachRuntimeBranchGroupRollsBackOnLaterFailure`, `TestTaskAttachRollsBackRuntimeFilterWhenGraphConnectFails`, `TestTaskAttachRollsBackRuntimeTerminalStageWhenGraphConnectFails` |
| External adapters and joins have no private power unavailable to built-ins. | `TestExternalAdaptersComposeThroughPublicGrammar`, `TestExternalJoinComposesThroughPublicGrammar`; every `examples/*` module builds and tests on each commit (ci.yml "Examples modules") |
| Join arm taps compose as ordinary stream points: an arm may declare `.Tap(...)`, and a later tap-reference arm can converge that same point. | `TestJoinArmTapComposes`, `TestMixChainArmTapDecodesOnceAndMixes`, `TestJoinDescribeEqualsBuildTapArm` |
| Branch frame transforms from packet-domain parents request the same decode+transform lowerer shape as an equivalent direct decoded chain. | `TestBranchAutoInsertMatchesDirectChain` |
| Nested select routing is explicit: target an inner decision point with `control.SelectActive(...).AtTap(name)` on a tap declared at that select output; untargeted nested select controls refuse as ambiguous. | `TestNestedSelectRoutingPinned` |
| Runtime taps owned by a detached branch disappear with that branch; future attaches must use a surviving `Inspectable.Taps()` entry or reattach the publishing branch. | `TestAttachAfterSiblingDetachRefusalNamesFix` |
| Join outputs are ordinary stream points and may fan out through `.Branches(...)` with independent branch chains. | `TestJoinOutputBranches`, `TestMixBranchesFanOutMixedStream`, `TestMixBranchesEncodeAndMonitorIndependently` |

When a new front-door feature is proposed, add its law here before adding a
new public symbol. If it cannot be described as input shape, ordered operation,
tap, branch, destination, task, or runtime attachment behavior, it probably
belongs outside the public grammar.

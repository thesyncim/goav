# Composability laws

These are the design laws that keep goav from turning into several workflow
APIs. Read this as the short reviewer checklist: if a change breaks one of
these laws, it is probably adding a new workflow surface instead of extending
the grammar. The tests below are executable evidence; this document is the map.

| Law | Evidence |
|---|---|
| A stream that never forks lowers exactly like a single `main` branch. | `TestNorthStarDirectChainEqualsExplicitMainBranch`, `TestNorthStarDirectChainEqualsExplicitMainBranchWithTransform` |
| A reusable operation sequence owns ordered operations only; sources, destinations, runtime, lifecycle, and fork ownership stay outside it. | `TestStoredOperationListsMirrorFlowBranchAndDirectStreamWork`, `TestFlowBranchesStayOnJobAndBuildIntent`, `TestFlowReportsShapeContractAndTaps` |
| Build-time forks and Outputs added with runtime `task.Attach` use the same stream operations and operation lowering. | `TestValueAttachFromTap`, `TestTaskAttachRuntimeFlowCopyBranchFromPacketTap`, `TestTaskAttachRuntimeDecodeResampleEncodeMuxBranchFromPacketTap`, `TestTaskAttachRuntimeFlowCustomEncodeMuxBranch` |
| `rt.Describe` is the graph shape that `rt.Build` installs. | `TestStreamRecipeDescribeMatchesBuiltGraph`, `TestBranchCompositionRecipeDescribeMatchesBuiltGraph`, `TestJoinDescribeEqualsBuildMix`, `TestJoinDescribeEqualsBuildNestedMix` |
| Generated recipe cases preserve structural laws under a deterministic corpus: `Describe()` equals the built graph, an unforked stream equals a single `main` branch, runtime `Attach` installs the same branch subgraph as build-time forks, and nested mixes match their flat equivalent where no per-stage clamp occurs. | `TestCompositionLawsHoldForGeneratedRecipes` |
| `Explain()` reports solver insertions and soft preferences without opening resources. | `TestAutoInsertsFormatConvertThroughRegisteredAdapter`, `TestRequireSatisfiedByAutoConversion`, `TestPreferUnsatisfiableIgnoredWithDiagnostic` |
| Destination identity is the handle: one destination value reused from several Outputs feeds one mux or sink, while separately constructed same-name handles refuse. | `TestValueGrammarMuxesStreamsIntoOneFile`, `TestMuxGroupsBranches`, `TestSameHandleSharesOneMux`, `TestWithKeepsHandleIdentity`, `TestDistinctHandlesWithOneNameAreRefused`, `TestFromMultiInputPlanDedupesMuxDestination`, `TestTaskAttachRuntimeBranchGroupSharesOneSinkHandle`, `TestTaskAttachRuntimeBranchGroupSharesMuxDestination` |
| Fork-local policy and mutation cannot corrupt siblings. | `TestCopyContractMutableFanoutBranchCannotCorruptSibling`, `TestFromMultiInputChainsKeepIndependentAutoPolicies`, `TestBranchBufferIsNormalBranchAPI` |
| Runtime attach failures roll back atomically and close prepared components. | `TestTaskAttachRuntimeBranchGroupRollsBackOnLaterFailure`, `TestTaskAttachRollsBackRuntimeFilterWhenGraphConnectFails`, `TestTaskAttachRollsBackRuntimeTerminalStageWhenGraphConnectFails` |
| External adapters and joins have no private power unavailable to built-ins. | `TestExternalAdaptersComposeThroughPublicGrammar`, `TestExternalJoinComposesThroughPublicGrammar`; every `examples/*` module builds and tests on each commit (ci.yml "Examples modules") |
| Join arm taps compose as ordinary stream points: an arm may declare `.Tap(name)`, and one decode feeds both the join and the tap. | `TestJoinArmTapComposes`, `TestMixChainArmTapDecodesOnceAndMixes`, `TestJoinDescribeEqualsBuildTapArm` |
| Fork frame transforms from packet-domain parents request the same decode+transform lowerer shape as an equivalent unforked decoded stream. | `TestBranchAutoInsertMatchesDirectChain` |
| Nested select routing is explicit: target an inner decision point with `control.SelectActive(...).AtTap(name)` on a tap declared at that select output; untargeted nested select controls refuse as ambiguous. | `TestNestedSelectRoutingPinned` |
| Runtime taps owned by a detached branch disappear with that branch; future attaches must use a surviving `task.Taps()` entry or reattach the publishing branch. | `TestAttachAfterSiblingDetachRefusalNamesFix` |
| Join outputs are ordinary Streams and may fan out by reuse into independent forks. | `TestJoinOutputBranches`, `TestMixBranchesFanOutMixedStream`, `TestMixBranchesEncodeAndMonitorIndependently` |

When a new front-door feature is proposed, add its law here before adding a
new public symbol. If it cannot be described as input shape, ordered operation,
tap, fork, destination, task, or runtime attachment behavior, it probably
belongs outside the public grammar.

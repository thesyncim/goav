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
| `Explain()` reports solver insertions and soft preferences without opening resources. | `TestAutoInsertsFormatConvertThroughRegisteredAdapter`, `TestRequireWithAutoInsertsConversion`, `TestPreferUnsatisfiableIgnoredWithDiagnostic` |
| Destination grouping is explicit: `Mux(name, destination)` groups matching destinations by caller intent, while repeated ungrouped handles refuse. | `TestMuxGroupsBranches`, `TestMuxSurvivesWithAndCopy`, `TestFromMultiInputPlanDedupesMuxDestination`, `TestSameHandleGroupingRequiresMux`, `TestTaskAttachRuntimeBranchGroupSharesMuxSinkDestination`, `TestTaskAttachRuntimeBranchGroupSharesMuxDestination` |
| Branch-local policy and mutation cannot corrupt siblings. | `TestCopyContractMutableFanoutBranchCannotCorruptSibling`, `TestFromMultiInputChainsKeepIndependentAutoPolicies`, `TestBranchBufferIsNormalBranchAPI` |
| Runtime attach failures roll back atomically and close prepared components. | `TestTaskAttachRuntimeBranchGroupRollsBackOnLaterFailure`, `TestTaskAttachRollsBackRuntimeFilterWhenGraphConnectFails`, `TestTaskAttachRollsBackRuntimeTerminalStageWhenGraphConnectFails` |
| External adapters and joins have no private power unavailable to built-ins. | `TestExternalAdaptersComposeThroughPublicGrammar`, `TestExternalJoinComposesThroughPublicGrammar`, `TestExternalExampleModulesAreCopyable` |

When a new front-door feature is proposed, add its law here before adding a
new public symbol. If it cannot be described as input shape, ordered operation,
tap, branch, destination, task, or runtime attachment behavior, it probably
belongs outside the public grammar.

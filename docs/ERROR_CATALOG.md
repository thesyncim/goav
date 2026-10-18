# Error Catalog

<!-- Kept in lockstep with errcode/errcode.go by TestErrorCatalogDocMatchesErrcodeCatalog; update both together. -->

This catalog is the checked index of goav's public BuildError refusal codes. `TestErrorCatalogDocMatchesErrcodeCatalog` verifies, both directions, that every `Code` constant in `errcode/errcode.go` has a Full Code Index row here (with its constant name and an existing coverage test) and that every row resolves to a declared constant or derived join code, so a new code must update the source catalog and this checked document together.

Every current catalog row names coverage. If a future row is marked `catalog-only`, it still needs a dedicated bad recipe, rendered golden error, fixed recipe, public contract check, and test name before the v1 error catalog is complete. Rows naming tests already have public grammar snippets, rendered-error assertions, fix coverage, or BuildError field checks in the named test.

## Acceptance Snippet Coverage

| Code | Test | Bad recipe | Rendered error | Fix coverage | Public check |
|---|---|---|---|---|---|
| `tap_domain_mismatch` | `TestJoinArmTapBeforeDecodeIsRefused` | a Mix arm taps its packets before .Decode() | the refusal and its move-the-tap fix are asserted by the test | tap the arm after .Decode(), where join arms carry frames | `BuildError.Code` |
| `tap_invalid` | `TestStreamChainRejectsInvalidPostEncodeAndTapContracts` | .Tap("") | empty tap name refusal is asserted by the test | use a non-empty tap name such as .Tap("audio.frames") | `BuildError.Code` |
| `join_name_invalid` | `TestJoinRejectsInvalidNames` | goav.Join("Cross Fade", stage, streams...) | invalid custom join name variants are asserted by the test | use a snake-safe non-reserved custom join name | `BuildError.Code` |
| `join_stage_invalid` | `TestJoinRejectsStageMismatch` | goav.Join("funnel", nil, streams...) or stage.Name() mismatch | nil/mismatched join stage refusal is asserted by the test | pass a non-nil stage whose Name() equals the join name | `BuildError.Code` |
| `mix_inputs` | `TestMixRequiresTwoArms` | goav.Mix(a.Audio()).To(...) | Mix minimum arm count is asserted by the test | pass at least two Mix arms | `BuildError.Code` |
| `mix_arm` | `TestMixRequiresExplicitDecodeForPacketArms` | a Mix arm has invalid media, packets without .Decode(), duplicate stream ids, or unsupported nested operations | Mix arm validation details are asserted by the test | make every Mix arm an audio stream with a distinct stream id and supported operations | `BuildError.Code` |
| `composite_inputs` | `TestCompositeRequiresTwoArms` | goav.Composite(cam.Video()).To(...) | Composite minimum arm count is asserted by the test | pass at least two Composite arms | `BuildError.Code` |
| `composite_arm` | `TestCompositeRejectsDuplicateStreamIDs` | Composite arms declare duplicate stream ids or invalid video arms | Composite arm validation details are asserted by the test | make every Composite arm a valid video stream with a distinct stream id | `BuildError.Code` |
| `select_inputs` | `TestSelectRequiresTwoArms` | goav.Select(a.Video()).To(...) | Select minimum arm count is asserted by the test | pass at least two Select arms | `BuildError.Code` |
| `select_arm` | `TestSelectRequiresDistinctArmIDs` | Select arms declare duplicate stream ids or invalid arms | Select arm validation details are asserted by the test | make every Select arm valid and give each a distinct stream id | `BuildError.Code` |
| `stream_rule_invalid` | `TestOnStreamValidation` | input.OnStream(source.StreamMatch{}) or a malformed discovered stream | invalid OnStream rule variants are asserted by the test | pass source.MatchMedia(...), source.MatchCodec(...), source.MatchStreamID(...), or source.MatchStream(fn) | `BuildError.Code` |
| `runtime_missing` | `TestRecipeReportsOmittedRuntime` | a recipe built with no runtime configured | adapter-backed omitted runtime guidance is asserted by the test | build on a runtime: rt.Run(ctx, outputs...), or bundle.Build/bundle.Run for the bundled runtime | `BuildError.Code` |
| `recipe_graph_unsupported` | `TestRecipeResolvedUnsupportedContracts` | describe/build a recipe shape that has no graph plan | unsupported recipe graph helper is asserted by the test | use a supported front-door recipe shape | `BuildError.Code` |

## Full Code Index

| Section | Constant | Code | Kind | When it fires | Example coverage |
|---|---|---|---|---|---|
| Tap errcode. | `errcode.TapInvalid` | `tap_invalid` | refusal | TapInvalid fires when a tap name is empty. | TestStreamChainRejectsInvalidPostEncodeAndTapContracts |
| Tap errcode. | `errcode.TapDomainMismatch` | `tap_domain_mismatch` | refusal | TapDomainMismatch fires when a tap sits where its stream does not carry the domain it needs, such as a join arm's tap before the arm's .Decode(). | TestJoinArmTapBeforeDecodeIsRefused |
| Join codes. | `errcode.JoinNameInvalid` | `join_name_invalid` | refusal | JoinNameInvalid fires when a custom join (goav.Join) declares an unusable name: empty, not snake-safe ([a-z][a-z0-9_]*), reserved by a built-in kind (mix, composite, select), or colliding with another join node in the same join tree. | TestJoinRejectsInvalidNames |
| Join codes. | `errcode.JoinStageInvalid` | `join_stage_invalid` | refusal | JoinStageInvalid fires when a custom join's convergence stage is nil or reports a Name() different from the join's name. | TestJoinRejectsStageMismatch |
| Derived join codes (built-in Mix, Composite, Select profiles). | `joinErrorCode("mix", "inputs")` | `mix_inputs` | refusal | Mix is given fewer than two arms. | TestMixRequiresTwoArms |
| Derived join codes (built-in Mix, Composite, Select profiles). | `joinErrorCode("mix", "arm")` | `mix_arm` | refusal | A Mix arm is invalid: wrong media, duplicate stream ids, an unconvertible format, or a nested arm carrying .Encode. | TestMixRequiresExplicitDecodeForPacketArms |
| Derived join codes (built-in Mix, Composite, Select profiles). | `joinErrorCode("composite", "inputs")` | `composite_inputs` | refusal | Composite is given fewer than two arms. | TestCompositeRequiresTwoArms |
| Derived join codes (built-in Mix, Composite, Select profiles). | `joinErrorCode("composite", "arm")` | `composite_arm` | refusal | A Composite arm is invalid: wrong media, duplicate stream ids, an unconvertible format, or unsupported nested operations. | TestCompositeRejectsDuplicateStreamIDs |
| Derived join codes (built-in Mix, Composite, Select profiles). | `joinErrorCode("select", "inputs")` | `select_inputs` | refusal | Select is given fewer than two arms. | TestSelectRequiresTwoArms |
| Derived join codes (built-in Mix, Composite, Select profiles). | `joinErrorCode("select", "arm")` | `select_arm` | refusal | A Select arm is invalid: duplicate stream ids, incompatible media facts, or unsupported nested operations. | TestSelectRequiresDistinctArmIDs |
| Stream rule codes (OnStream grammar). | `errcode.StreamRuleInvalid` | `stream_rule_invalid` | refusal | StreamRuleInvalid fires when an OnStream rule is malformed (no matcher, no branch, or an unusable discovered stream). | TestOnStreamValidation |
| Job and compiler errcode. | `errcode.RuntimeMissing` | `runtime_missing` | refusal | RuntimeMissing fires when a job reaches Build with no runtime configured. | TestRecipeReportsOmittedRuntime |
| Job and compiler errcode. | `errcode.RecipeGraphUnsupported` | `recipe_graph_unsupported` | refusal | RecipeGraphUnsupported fires when the recipe intent matches no supported graph plan. | TestRecipeResolvedUnsupportedContracts |

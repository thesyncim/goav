# Error Catalog

This is the checked index of every code a goav `BuildError` can carry. Switch on
`Family` first, then on `Code` (an `errcode.Code` string; the most common have
constants in `errcode`). The table below is generated from the source and pinned by
`TestErrorCatalogDocMatchesErrcodeCatalog`: a new code fails the test until it is
listed here (`GOAV_UPDATE_GOLDEN=1` regenerates it). A custom `goav.Join(name, ...)`
derives `<name>_inputs` and `<name>_arm` (family `join`); vendor codes are `external`.

<!-- BEGIN GENERATED CODES -->
| Family | Codes |
| --- | --- |
| `branch` | `branch_decode_domain_mismatch`, `branch_decode_duplicate`, `branch_operation_chain_unsupported`, `branch_transform_media_mismatch`, `decode_config_conflict` |
| `buffer` | `branch_buffer_invalid`, `branch_buffer_unsupported` |
| `codec` | `decode_adapter_incompatible`, `decode_adapter_missing`, `decode_adapter_unavailable`, `encode_adapter_incompatible`, `encode_adapter_missing`, `encode_adapter_unavailable` |
| `compiler` | `compiler_pass_failed`, `graph_plan_invalid`, `recipe_graph_unsupported`, `runtime_missing` |
| `destination` | `destination_duplicate`, `destination_format_unknown`, `destination_invalid`, `destination_mux_incompatible`, `destination_muxer_missing`, `destination_shape_mismatch`, `output_destination_missing`, `output_duplicate`, `output_format_missing`, `output_format_unknown`, `output_invalid`, `output_kind_mixed`, `output_muxer_missing`, `output_scheme_unsupported`, `output_writer_missing` |
| `encode` | `encode_auto_unresolved`, `encode_destination_missing`, `encode_duplicate`, `encode_missing`, `encode_parameter_invalid`, `encode_stream_mismatch` |
| `input` | `explicit_graph_source_missing`, `input_demuxer_missing`, `input_duplicate`, `input_format_unknown`, `input_invalid`, `input_scheme_unsupported`, `input_unknown`, `source_callback_missing`, `source_shape_invalid`, `source_shape_mismatch`, `source_shape_unsupported` |
| `join` | `composite_arm`, `composite_inputs`, `join_name_invalid`, `join_stage_invalid`, `mix_arm`, `mix_inputs`, `select_arm`, `select_inputs` |
| `runtime_branch` | `runtime_branch_anchor_missing`, `runtime_branch_copy_domain_mismatch`, `runtime_branch_decode_codec_missing`, `runtime_branch_decode_domain_mismatch`, `runtime_branch_encode_domain_mismatch`, `runtime_branch_encode_missing`, `runtime_branch_graph_error`, `runtime_branch_invalid`, `runtime_branch_mux_codec_missing`, `runtime_branch_node_duplicate`, `runtime_branch_tap_duplicate`, `runtime_branch_tap_missing`, `runtime_branch_transform_error` |
| `shape` | `shape_adapter_ambiguous`, `shape_adapter_missing`, `shape_conversion_refused`, `shape_requirement_unmet` |
| `stream` | `stage_missing`, `stream_ambiguous`, `stream_codec_missing`, `stream_invalid`, `stream_missing`, `stream_selector_invalid`, `stream_step_after_encode` |
| `stream_rule` | `stream_rule_invalid` |
| `tap` | `tap_domain_mismatch`, `tap_invalid` |
| `transform` | `operation_shape_mismatch`, `transcode_resize_invalid`, `transform_adapter_incompatible`, `transform_adapter_missing`, `transform_invalid`, `transform_media_mismatch` |
<!-- END GENERATED CODES -->

The snippets below pin the public constants' refusals: the bad recipe, the
rendered error, and the fix.

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


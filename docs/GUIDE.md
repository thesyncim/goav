# Developer Guide

This is the practical handbook for composing, running, controlling, and operating
`goav` pipelines.

## Core Grammar

Recipes are plain immutable Go values. Nothing opens until a `Runtime` runs them:

```text
Input -> Stream -> operations (.Decode, .Resize, .Resample, .Encode, .Tap, .Buffer, .Sync, .Playout) -> .To(Destinations...) -> Output
```

- **Fan-out by reuse:** using a `Stream` twice forks it; work before the fork runs once.
- **Shared destinations:** passing the same `goav.Destination` to two outputs muxes both streams into one container.
- **Inspect before I/O:** `rt.Describe(outs...)` returns the exact `pipeline.Spec` (`graphrender` prints ASCII/DOT/Mermaid); `rt.Explain(ctx, outs...)` returns a structured `plan.Report` with shapes, adapters, and warnings.

### Common Recipe Shapes

| Task | Pattern |
| --- | --- |
| Remux without re-encoding | `in.To(goav.Write("out.mkv", w))` |
| Multi-rendition ladder | Fork `v := in.Video().Decode()` into `v.Resize(...).Encode(...).To(hd)` and `v.Resize(...).Encode(...).To(sd)` |
| Inspect frames in Go | `in.Video().Decode().To(goav.Sink(component.SinkFunc("frames", fn)))` |
| Audio mix | `goav.Mix(a.Audio().Decode(), b.Audio().Decode())` (add `goav.ByPTS()` for PTS-aligned mixing) |
| Video composite / PiP | `goav.Composite(bg.Video().Decode(), goav.At(cam.Video().Decode().Resize(480, 270), 1420, 790))` |
| Live input switch | `goav.Select(a.Video(), b.Video())` + `task.Control(ctx, control.Select(...))` |
| Dynamic output attach | Anchor with `.Tap("live")`, then `att, err := task.Attach(ctx, task.Tap("live").Encode(...).To(dst))` |
| HLS / LL-HLS packaging | `goav.Custom(hls.New("live.m3u8", hls.Dir(dir), hls.Window(6)))` or `hls.NewLadder` |

## Multi-Input & Synchronization

- **Converging joins** (`Mix`, `Composite`, `Select`, `Join`) require distinct stream IDs across arms and explicit `.Decode()` for frame joins (`Mix`, `Composite`).
- **One clock per task:** pacing, playout, `task.Pause(ctx)`, `task.Resume(ctx)`, `control.Seek(...)`, and `control.Rate(...)` share one task timeline.
- **Dynamic streams:** for live inputs whose tracks arrive after startup, attach `input.OnStream(source.MatchMedia(av.MediaVideo), ...)` or anchor later with `task.Track(input, stream)`.

## Flow Control & Backpressure

By default, edges execute synchronously on the producer's goroutine (zero-alloc direct handoff). Add `.Buffer(flow.BranchBuffer{...})` on any fork to decouple a slow branch with an explicit bounded ring:

| Policy | Behavior when full | Use for |
| --- | --- | --- |
| `flow.DropBlock` | Producer blocks until space frees | Offline transcoding, lossless recording |
| `flow.DropOldest` | Evicts oldest queued item, counts drop | Live previews, monitoring taps |
| `flow.DropNewest` | Drops incoming item, counts drop | Rate-limited side outputs |

Every drop is counted atomically and exposed via `task.Stats()` (readable lock-free while running). Branch failures follow `BranchFailurePolicy` (`FailTask` vs `IsolateBranch`).

## Production Operations

- **Build vs Run:** use `rt.Build(ctx, outs...)` when you need `task.Stats()`, `task.Snapshot()`, `task.Watch(...)`, `task.Attach`/`Detach`, or `task.Control`.
- **Finalization:** `task.Run(ctx)` stops when all sources reach EOS or `ctx` cancels; always call `task.Close()` and check its error to verify container finalization (e.g., MP4 `moov` / Matroska cues).
- **Events:** `sub := task.Watch(goav.WatchTypes(...))` gives a per-watcher buffered channel that never blocks the data plane.
- **Live mutations:** `task.Attach(ctx, outs...)` applies a group atomically and rolls back on failure; `task.Detach(ctx, att, goav.DrainBranch())` or `goav.AbortBranch()` controls finalization disposition.

## Errors & Error Catalog

Every build, explain, or attach refusal returns a `*goav.BuildError` before any file or socket opens. Match on `Family` (`errcode.Family`) first, then `Code` (`errcode.Code`):

- `err.Family` — coarse category (`codec`, `destination`, `join`, `shape`, …)
- `err.Code` — stable machine-readable refusal code
- `err.Suggestions` — concrete user-facing fixes
- `errors.Is(err, target)` — unwraps underlying sentinel causes (e.g., `codec.ErrUnavailable`)

The table below is generated from the source and pinned by
`TestErrorCatalogDocMatchesErrcodeCatalog` (`GOAV_UPDATE_GOLDEN=1` regenerates it).
Custom `goav.Join(name, ...)` derives `<name>_inputs` and `<name>_arm` (family `join`).

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

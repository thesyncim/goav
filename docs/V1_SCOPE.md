# V1 Scope

This document freezes the v1 surface: the workflows a v1 release promises to
keep stable, and the features that ship today but stay **governed pre-v1** until
a release decision explicitly retains them. It is the contract the beginner docs
teach to, and the line reviewers hold new work against.

It complements two neighbours: [`SIMPLIFICATION_TARGET.md`](history/SIMPLIFICATION_TARGET.md)
records *why* the surface is being trimmed and tracks the reduction work;
[`ROADMAP.md`](ROADMAP.md) records *when* governed pre-v1 features might graduate.
This page records *what the v1 promise covers*.

## Product shape

The v1 product is a small, composable, explicit, pure-Go media recipe layer:

```text
From(inputs...) -> stream selection -> operations -> taps -> branches -> destinations -> Task
```

The normal user model is `Input -> Stream -> Operations -> Output -> Task`. A
recipe is inert Go data until `Describe`, `Explain`, `Build`, or `Run` compiles
it; failures surface as structured `*goav.BuildError` values before resources
open.

## v1-supported workflows

These workflows are the v1 front door. They are frozen: a v1 release keeps them
working, and the beginner docs teach only these.

| Workflow | Spelling | Frozen because |
| --- | --- | --- |
| Packet copy (record/remux) | `From(input).Copy().To(goav.Write(...))` | The simplest correct path; no decode, no codec adapter. |
| Decode to sink | `From(input).Audio().Decode().To(goav.Sink(...))` | Hands decoded frames to application code with one operation. |
| Decode -> transform -> encode -> mux | `.Decode().Resize(...).Encode(codec).To(...)` | The canonical transcode; the shape solver inserts conversions explicitly. |
| Explicit fanout | `.Copy().Branches(...)` / `.Decode().Branches(...)` | One stream point fans out with each branch owning its destinations. |
| Custom source and custom sink | `goav.Source(...)`, `goav.Sink(...)`, `goav.Custom(...)` | Application-owned media boundaries plug in with no global process state. |
| Describe / Explain before Build | `job.Describe()`, `job.Explain(ctx)` | The plan is inspectable before any resource opens. |
| Structured build errors | `*goav.BuildError` (Family, Code, Phase, fixes) | Every user-actionable refusal is machine-readable and carries a fix. |

A new contributor can implement a custom source, a custom sink, and a simple
decode pipeline using only these workflows and the
[extension cookbook](EXTENSION_COOKBOOK.md) — without reading the expert graph,
runtime mutation, or control-plane docs.

## Governed pre-v1 (experimental)

These features are implemented, tested, and documented, but they are **not part
of the v1 promise** unless a release decision explicitly retains and freezes
them. "Governed" means each one is pinned by tests and described in a focused
doc; it does **not** mean unreliable. It means the beginner surface does not
depend on them and v1 does not commit to their shape.

| Feature | Lives in | Why it is not yet frozen |
| --- | --- | --- |
| Expert graph | [`expert`](API_SURFACE.md) (tier C), `graphrender` | Handle-based graph escape hatch off the grammar; not needed for the normal path. |
| Runtime Attach / Detach / Rebranch | [`OPERATIONS.md`](OPERATIONS.md), `LiveTask` | Live graph mutation; depends on `BuildLive`, outside the `Build` front door. |
| Control-plane socket host | [`CONTROL_PLANE.md`](CONTROL_PLANE.md), `ctlserver` | Out-of-band live control over a socket; an application-hosting concern. |
| Broad `OnStream` dynamic rules | [`USE_CASES.md`](USE_CASES.md) | Only identity stream-rule cases are v1; broader dynamic breadth stays governed. |
| `Mix` / `Composite` / `Select` / custom `Join` | [`MULTI_INPUT.md`](MULTI_INPUT.md), [`OPERATIONS.md`](OPERATIONS.md) | N-to-1 convergence; powerful but not a beginner workflow. |
| Live provider transcode | [`USE_CASES.md`](USE_CASES.md) | Held until compiler support for live transcode is real, not just wired. |

Reaching for one of these is fine — they are first-class, governed features.
The point of the freeze is that the *beginner* surface (`Build` -> `Task` with
`Run`/`Close`) never forces a user through them.

## v1 guarantees

These invariants hold across the v1 surface and are enforced by tests:

1. **Front-door model is stable.** `From(inputs...) -> streams -> operations ->
   taps -> branches -> destinations -> task` does not change shape.
2. **No required graph handles.** Normal users never need graph handles, string
   routing, runtime internals, or workflow-specific APIs.
3. **One lowering path.** `Describe`, `Explain`, and `Build` lower through the
   same plan; there is no hidden second path. Runtime `Attach` shares the same
   operation validation and lowering.
4. **`Build` returns the narrow `Task`** (`Run`/`Close`). Inspection, events,
   live control, and runtime mutation are opt-in through `BuildLive` /
   `LiveTask` — advanced surface only.
5. **Structured errors.** Every user-actionable failure is a `*goav.BuildError`
   with stable `Family`/`Code`, an explicit `Phase`, operation/node context,
   machine-readable details, and concrete fixes.
6. **No global adapter registry.** Adapter registration is explicit and
   per-runtime; there is no process-global state.
7. **Root import stays pure.** Importing `github.com/thesyncim/goav` does not
   pull bundled adapter packages into the root package dependency graph.
8. **No hot-path allocation regressions.** Existing allocation pins are
   preserved; performance claims are backed by reproducible benchmark artifacts.

## Module and dependency decision

`goav/bundle` is a **package in the root module**, not a nested module. This is
a deliberate decision, not a deferral:

- Package-level purity is sufficient. `TestRootImportDoesNotPullBundledAdapters`
  and `TestRootModuleDependencyPurity` already guarantee that importing the root
  package pulls no bundled adapter, and that root-module requirements stay inside
  `github.com/thesyncim/*` with no third-party requires.
- A nested `bundle` module would add a separate `go.mod`, per-module tags, and
  release coordination for no isolation the package boundary does not already
  provide. The transport/provider modules (`rtpav`, `webrtcav`, `playoutav`)
  are nested because they own source-provider vocabularies outside the root
  grammar; `bundle` carries only `thesyncim/*` backends, so it stays in the
  root module.

If a future requirement demands module-level isolation of the bundled backends
beyond the current policy, this decision is revisited; today the package
boundary is the v1 answer. See [`API_INVENTORY.md`](history/API_INVENTORY.md) for the
dependency baseline.

## Release gate

Do not cut v1 just because docs and pins are green. Cut v1 only after the
release candidate meets this scope or records an explicit maintainer decision
for every retained governed-pre-v1 exception. The maintainer's `v0.1.0` tag is
the final freeze action.

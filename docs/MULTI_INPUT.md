# Multi-input convergence — final design

GoAV was structurally a tree (one input fanning out via `Branches`); convergence
(N streams → one) is the dual, and it landed as:

- `Mix(arms...)` — sums S16 audio arms (per-arm buffering by StreamID, clamping,
  one output EOS after all arms end). Packet arms auto-decode; mismatched rates
  auto-resample to the first arm's format.
- `Composite(arms...)` — paints video arms onto a canvas at `.Region(x, y)` offsets.
- `Select(arms...)` — passthrough one-of-N switch (no decode/encode); the first
  arm is active by default and `task.Control(ctx, goav.SelectActive(id))`
  switches live through the control plane.
- Variadic `From(inputs...)` — N inputs, per-chain `InputName(...)` narrowing,
  one shared `Destination` value muxing the encoded chains.

Design properties: joins are entry points parallel to `From` (single-input
chains are untouched); the join output is a normal stream point (`.Tap`,
`.Branches`, `.Encode().To(...)`, runtime attach from join taps); all three are
thin faces over one join operation with N input arms. The pipeline already
supported N input edges per node, and each buffered node has a single serial
worker, so join stages need no internal locking — lock-free by design holds.

All three plan through the ONE recipe compile: the joinSpec normalizes into the
compile state, `joinPlan` plans N arm sub-chains converging into an `OpJoin`
node (workPlan edges carry the N-to-1), and `Describe()` ≡ `Build()` is
guard-tested per kind. Per-kind behavior lives only in the joinProfiles table.
Remaining: move join arm shape-solving into the central solver; joins as arms
of joins (nesting) needs an arm-shaped join handle.

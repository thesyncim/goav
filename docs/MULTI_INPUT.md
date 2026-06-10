# Multi-input convergence — final design

GoAV was structurally a tree (one input fanning out via `Branches`); convergence
(N streams → one) is the dual, and it landed as:

- `Mix(arms...)` — sums S16 audio arms (per-arm buffering by StreamID, clamping,
  one output EOS after all arms end). Packet arms auto-decode; mismatched rates
  auto-resample to the first arm's format.
- `Composite(arms...)` — paints video arms onto a canvas at `.Region(x, y)` offsets.
- `.SyncByPTS()` on Mix/Composite — opt-in timestamp alignment (default stays
  arrival order): per step the minimum head PTS on a common ns clock leads,
  arms within half a frame join it, ahead arms sit the step out (silence /
  unpainted, canvas keeps the absent arm's extent), stale frames drop to catch
  up, an arm discontinuity (Seek/Segment) flushes that arm and re-syncs, and an
  ended arm stops gating so the rest keep joining (both modes). Catch-up drops
  surface on the join node's counters via the optional `pipeline.DropReporter`
  capability (polled at snapshot time, atomic counter — lock-free hot path):
  `task.Stats().Nodes["mix"].Dropped` under the `"sync"` reason, and in
  `task.Snapshot()`.
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
guard-tested per kind (nested case included). Per-kind behavior lives only in
the joinProfiles table. Arm shape-solving goes through the central solver
(`armExpected`/`armPolicy`).

Joins nest: an arm is a `JoinArm` — a source chain or another join — so
`Mix(Mix(a, b), c)` sub-mixes two arms and mixes the result with a third,
`Select(Mix(a, b), Mix(c, d))` switches between two live mixes (arm ids are
the sub-joins' output ids: mix, mix-2), and composites nest as sub-canvases
(placed with the nested composite's `.Region(x, y)`). A nested join
contributes its JOINED output stream; `joinPlan` recurses through the one
compile, the solver converts a sub-join's output like any arm (an outer mix
resamples a 24kHz sub-mix to its 48kHz target), `.SyncByPTS()` on a nested
join scopes to ITS arms, mix clamping applies independently at each stage,
and a nested join may not carry `.Encode/.To/.Branches` (it is an arm, not a
terminal — `mix_arm`-family errors / not expressible).

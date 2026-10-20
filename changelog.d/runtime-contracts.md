- Separate execution success from actual output finalization. Retain per-node and
  per-destination outcomes, honor concurrent CloseContext deadlines, and distinguish
  caller wait timeouts from durable cleanup errors. Early Close deterministically
  abandons active outputs; nontransactional abandonment reports Closed. Unfinished
  branch drains abort on task Close or incomplete retirement. Explicit drains
  require the optional RunErrorReporter graph capability; unsupported custom
  graphs are refused before mutation.
- Add opt-in Output.OnFailure(IsolateBranch), with FailTask as the default. Validate
  exclusive suffixes before opening resources, retire failed branches automatically,
  and retain failure/cleanup snapshots while healthy siblings continue.
- Report actual execution queues and admission mode from shared planning rules,
  including Select's pinned lossless queue and timestamp ordering constraints.
- Require atomic four-result RTP reads, preserve per-track configuration through
  decorators, jitter, and bounded WHEP aggregation, and add a fixed-configuration
  legacy adapter. PacketReader, JitterBuffer, and custom pipeline.Graph implementations
  require migration; see docs/RUNTIME_CONTRACTS.md.
- Extend published-consumer validation to WebRTC ordered records and refresh
  nested module pins so installed player and CLI consumers receive these contracts.
- Preserve unrelated input media during seek: flush only source-exclusive queues,
  including same-source forks/rejoins, and retain the bounded tail after a shared join.

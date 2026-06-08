# Performance

`goav` treats construction and steady-state media flow differently.

Cold paths may allocate:

- runtime and graph construction
- registry setup
- declarative recipe planning and validation
- codec open/configuration
- format probing
- output buffer allocation by the caller

Hot paths must avoid hidden allocation:

- per RTP packet
- per depacketized packet
- per decoded/encoded frame
- per mux/demux packet
- per direct pipeline message

The API can stay expressive only if those expressions collapse into direct,
reusable runtime objects. Declarative recipes are allowed to build plans,
diagnostics, and graphs up front; once running, stages should reuse caller-owned
messages, result structs, frame planes, packet buffers, and scratch storage.
The intended shape is one cold-path executable `WorkPlan` and runtime `WorkPatch`;
packet, frame, event, and mux/demux loops must not route
through fluent recipe objects or workflow-specific compiler dispatch.

## Rules

- Prefer `Into` methods that fill caller-owned result structs.
- Reset result structs before reuse.
- Preallocate result slices and frame plane buffers.
- Return capacity errors instead of appending beyond capacity.
- Avoid `fmt`, map writes, closure allocation, and error wrapping in hot paths.
- Keep recipe, flow, branch, tap, destination, and codec abstractions cold-path
  only; do not dispatch through them for each packet or frame.
- Prefer one planned graph over multiple workflow-specific graph modes; adding a
  new operation should not add a new per-packet abstraction layer.
- Fanout should share payload references unless an explicit policy requires a
  copy.

## Branch concurrency and fanout isolation

There are two executors behind the one `Graph` interface:

- **Direct** (`pipeline/direct.go`): synchronous. The source emits and each
  branch is handled inline on the *same goroutine*, in order. Zero-copy and
  lowest latency, but no isolation — a slow branch (e.g. decoding one simulcast
  layer) blocks its siblings and the source.
- **Buffered** (`pipeline/buffered.go`): each node/branch runs on its *own
  goroutine* with a bounded queue, so a slow branch drains independently while
  siblings keep flowing. This is the model an SFU wants for per-layer decode.

Per-branch concurrency therefore already exists; it is selected by giving a
branch a buffered policy (a planner/recipe choice, not identity-magic). For
fanout-heavy work (simulcast decode, many subscribers) the isolation goals are:

1. **No global serialization per message.** Both executors must not take a
   process-wide lock on every emit/deliver, or N concurrent branches collapse
   onto one mutex and stop scaling (Track P2: per-node `atomic.Uint64` stats and
   an atomically-swapped routing snapshot).
2. **Independent backpressure.** A full or slow branch applies backpressure to
   *itself* only; it must not abort delivery to siblings or tear down the source
   (Track P3: true blocking send + per-target backpressure).
3. **Share payload bytes across the fanout** with an atomic refcount instead of
   copying once per target (Track P3), so CPU is spent on decode, not memcpy.

## Current Guards

Run:

```sh
go test ./...
go test -tags goav_goh264 ./adapters/goh264
go test ./adapters/govpx
```

Current allocation guards cover:

- `av.Packet`, `av.Frame`, and `av.Event` reset
- `av.TimeBase`, `av.Timestamp`, and `av.Duration` conversion helpers
- codec result reset
- pipeline drop-policy decisions
- direct pipeline pass-through
- bounded buffered graph immutable-message pass-through, copied borrowed media
  slots, and drop decisions
- codec decoder stage packet-to-frame flow
- codec encoder stage frame-to-packet flow
- format demux source packet/event flow
- format mux stage packet write flow
- IVF demux and mux packet read/write paths
- Annex B mux packet write path
- I420/YUV420P resize filter path
- RTP sequence detection
- RTP jitter ring
- Opus depacketization
- VP8, VP9, AV1, and H264 video depacketization
- RTP source packet-to-pipeline loop, including timestamp discontinuity
  tracking
- RTCP feedback scratch for NACK/PLI/FIR
- `gopus` packet-loss decode into a preallocated frame
- build-tagged `goh264` adapter borrowed-frame mapping and loss request
  emission
- build-tagged `govpx` adapter VP8/VP9 I420 output preparation and loss request
  emission
- build-tagged `govpx` adapter VP8/VP9 encode frame mapping

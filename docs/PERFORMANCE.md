# Performance

`goav` treats construction and steady-state media flow differently.

Cold paths may allocate:

- runtime and graph construction
- registry setup
- codec open/configuration
- format probing
- output buffer allocation by the caller

Hot paths must avoid hidden allocation:

- per RTP packet
- per depacketized packet
- per decoded/encoded frame
- per mux/demux packet
- per direct pipeline message

## Rules

- Prefer `Into` methods that fill caller-owned result structs.
- Reset result structs before reuse.
- Preallocate result slices and frame plane buffers.
- Return capacity errors instead of appending beyond capacity.
- Avoid `fmt`, map writes, closure allocation, and error wrapping in hot paths.
- Fanout should share payload references unless an explicit policy requires a
  copy.

## Current Guards

Run:

```sh
go test ./...
```

Current allocation guards cover:

- `av.Packet`, `av.Frame`, and `av.Event` reset
- codec result reset
- direct pipeline pass-through
- RTP sequence detection
- RTP jitter ring
- Opus depacketization
- `gopus` packet-loss decode into a preallocated frame


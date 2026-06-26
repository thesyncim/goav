# Provider source

This module shows the transport-provider seam: an external package implements
`provider.Source`, returns a running `pipeline.Source`, and passes it to
`goav.Input(provider)`.

Copy this when a source needs an open phase, stream discovery, transport-owned
timing, or provider-level shape facts.

Run it from the repo root:

```sh
cd examples/provider-source
go run .
```

Expected output:

```text
provider: demo-transport
frames: [[101 102] [103 104]]
opened: 1 started: 1 closed: 1
```

The checked output lives in `testdata/expected.txt`.

Failure example:

```go
goav.Input(nil)
```

A nil provider is refused before the task starts with `FamilyInput` and
`Code("input_invalid")`.
This is the copyable pattern for transport packages that need an open phase,
stream discovery, and shape facts without using goav internals.

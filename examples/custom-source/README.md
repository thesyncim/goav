# Custom source example

This standalone module shows an application-owned source that pushes decoded
S16 audio frames through the public `goav.Source` and `source.Push` APIs.

Copy this when your application already owns media buffers and does not need a
transport/provider open phase.

Run it from the repo root:

```sh
cd examples/custom-source
go run .
```

Expected output:

```text
frames: [[10 20] [30 40]]
accepted: 2 dropped: 0
```

The checked output lives in `testdata/expected.txt`.

Failure example:

```go
goav.Source("broken", shape.Frame(av.MediaAudio, shape.Audio(48000, 1, av.SampleFormatS16)), nil)
```

A nil callback is refused before the task starts with
`FamilyInput` and `Code("source_callback_missing")`. This is the copyable
pattern for packages that already own media buffers and need to feed goav
without implementing a transport provider.

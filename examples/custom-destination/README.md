# Custom Destination Example

This module shows the plain byte-destination seam: `goav.Writer` opens after
goav has selected the output format and streams, so the callback can validate
`provider.Info` before accepting bytes.

Run it:

```sh
go run .
```

Expected output:

```text
name: mem://voice.ogg
format: ogg mime: audio/ogg streams: 1
kind: demo-destination closed: 1 bytes: true
```

The checked output lives in `testdata/expected.txt`.

Failure example: the test passes a nil writer opener. The recipe refuses when
the destination opens and does not need any internal goav package to express
that failure path.

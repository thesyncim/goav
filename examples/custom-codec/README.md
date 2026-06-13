# Custom Codec Example

This module shows an external codec adapter registered with
`goav.WithEncoder` and `goav.WithDecoder`. The toy codec copies S16 audio frame
bytes into packets and decodes those packets back into frames.

Run it:

```sh
go run .
```

Expected output:

```text
decoded: [[5 6]]
```

The checked output lives in `testdata/expected.txt`.

Failure example: registering only the descriptor, without the encoder/decoder
factory, makes `.Encode(codec.Codec("example/pcm", av.MediaAudio))` fail with
`encode_adapter_unavailable` before the task opens resources.

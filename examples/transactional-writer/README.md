# Transactional Writer Example

This module shows the object-store upload pattern: `goav.Writer` opens after
the output format and streams are known, and a returned
`provider.TransactionalWriter` receives `Commit` on success or `Abort` on
failure.

Run it:

```sh
go run .
```

Expected output:

```text
format: ogg
commits: 1 aborts: 0 bytes: true
```

The checked output lives in `testdata/expected.txt`.

Failure example: the test installs a custom stage that returns an error before
the encoder. The writer is opened, receives `Abort`, and is not committed.

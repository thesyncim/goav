# WebRTC Runtime Ladder Demo

This standalone module receives camera/microphone WebRTC tracks in the browser,
decodes VP8, VP9, AV1, and Opus through `goav`, and attaches runtime branches
that encode Opus, VP8, and VP9 back to browser tracks. Runtime changes are
streamed back to the page with graph stats, task state, and an event feed.

Run:

```sh
go run .
```

Open http://localhost:8080, choose a camera upload codec, start the session, and
add or retune output renditions while the graph is running.

The example is split by responsibility:

- `server.go` and `signaling.go` handle HTTP routes and WebRTC signaling.
- `session.go` owns live task state, event history, and state streaming.
- `renditions.go` owns runtime branch add/update/remove behavior.
- `graph_debug.go` converts `goav` pipeline specs into browser-friendly graph
  and debug views.
- `static/` contains the browser UI, styles, and client runtime code.

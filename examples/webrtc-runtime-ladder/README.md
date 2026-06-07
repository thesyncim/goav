# WebRTC Runtime Ladder Demo

This standalone module receives camera/microphone WebRTC tracks in the browser,
decodes VP8, VP9, AV1, and Opus through `goav`, and attaches runtime branches
that encode Opus, VP8, and VP9 back to browser tracks.

Run:

```sh
go run .
```

Open http://localhost:8080, choose a camera upload codec, start the session, and
add or retune output renditions while the graph is running.

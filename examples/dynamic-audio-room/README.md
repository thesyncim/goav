# Dynamic audio room

This example models a real application room where participants can join,
leave, or reconnect while downstream goav work sees one stable mixed audio
stream.

Use this pattern when runtime membership is owned by your product:

- voice rooms, watch parties, and collaborative editors
- one mixed stream feeding recording, transcription, monitoring, or playback
- participant lifecycle events needed for observability

The room stays outside the root `goav` API on purpose. `goav.Mix(arms...)`
is the right tool when all mix arms are known when the recipe is built. A live
room has a different ownership model: the application owns membership and
publishes a stable `goav.Source` stream into the media graph.

Run it:

```sh
go run .
```

The tests use `goavtest/expect` and prove:

- participant add/remove events
- S16 summing and clamping
- rejection of frames for inactive participants
- golden output for the runnable script

A first-class dynamic upstream mix API should only be added if it solves the
general problem: source routing, stream lifecycle, shape conversion,
backpressure, snapshots, and detach/close semantics in one coherent design.

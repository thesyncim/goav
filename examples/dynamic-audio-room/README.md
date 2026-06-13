# Dynamic audio room

This example models a real application room where participants can join,
leave, or reconnect while downstream goav work sees each participant as its
own audio stream.

Use this pattern when runtime membership is owned by your product:

- voice rooms, watch parties, and collaborative editors
- independent per-track processing for levels, moderation, or transcription
- one optional mixed output feeding playback, recording, or monitoring
- participant lifecycle events needed for observability

The room stays outside the root `goav` API on purpose. `goav.Mix(arms...)`
is the right tool when all mix arms are known when the recipe is built. A live
room has a different ownership model: the application owns membership,
attaches ordinary runtime branches from `input.Stream(participantStream)`
before accepting that participant's media, publishes lifecycle events through
`EventStreamAdded` / `EventStreamRemoved`, and lets one branch do per-track
work while another branch feeds the optional shared mix output.

Run it:

```sh
go run .
```

The tests use `goavtest/expect` and prove:

- participant add/remove events
- per-track processing and recording
- S16 summing and clamping
- rejection of frames for inactive participants
- golden output for the runnable script

A first-class dynamic upstream mix API should only be added if it solves the
general problem: preserve per-track routes, produce a normal mixed stream for
downstream encode/branch work, and handle source routing, stream lifecycle,
shape conversion, backpressure, snapshots, and detach/close semantics in one
coherent design.

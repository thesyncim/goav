# Realtime: RTP, WebRTC & Control Plane

## RTP & WebRTC (`rtpav`, `webrtcav`)

RTP and WebRTC live in nested modules so core `goav` never pulls in Pion:

- **`rtpav`**: RTP receiver (`rtpav.Receive`), depacketizers & packetizers (Opus, VP8, VP9, H.264, AV1), `JitterBuffer`, `SequenceDetector`, and RTCP feedback (`FeedbackWriter` for NACK/PLI/REMB).
- **`webrtcav`**: Pion `TrackRemote` / `TrackLocal` adapters, `TrackSet` (handles same-stream codec replacement without rebuilding the graph), and WHIP/WHEP HTTP signaling helpers.

Both plug into the standard recipe grammar via `goav.From(...)` and `goav.Custom(...)`:

- Receive RTP/WebRTC streams with `goav.From(rtpav.Receive(reader, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8())))`.
- Combine live tracks with `.Sync(...)`, `.Buffer(...)`, `goav.Mix`, `goav.Composite`, or `task.Attach` for dynamic recording/monitoring branches.
- See [`examples/webrtc-runtime-ladder`](../examples/webrtc-runtime-ladder) and [`examples/gio-webrtc-showcase`](../examples/gio-webrtc-showcase) for complete end-to-end applications.

## Control Plane (`ctlserver` & `goav ctl`)

Expose any running `goav.Task` over a Unix domain socket or TCP listener using `ctlserver`:

- **Built-in methods:** `status`, `snapshot`, `stats`, `taps`, `spec`, `explain`, `attach`, `detach`, `rebranch`, `pause`, `resume`, `seek`, `rate`, `select`, `keyframe`, `watch`, and `shutdown`.
- **Custom methods & elements:** register typed application handlers with `ctlserver.Method(...)` and custom `launch.Stage` elements with `ctlserver.Elements(...)` so remote clients can attach custom pipeline fragments by name.
- **Protocol:** newline-delimited JSON-RPC frames over `unix://` or `tcp://` with optional bearer-token authentication (`ctlserver.WithToken(...)`).
- **CLI client:** drive any host with `goav ctl --addr unix:///tmp/goav.sock <command>` (see [`CLI.md`](CLI.md)).
- See [`examples/control-plane-host`](../examples/control-plane-host) for a runnable server + client example.

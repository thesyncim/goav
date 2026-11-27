# goav Studio — Native macOS Pure-Go Media & WebRTC App

A **zero-cgo (`CGO_ENABLED=0`)**, **zero-FFmpeg**, **100% pure-Go** native macOS broadcast & WebRTC studio built on `goav`.

Capture your **macOS camera** (`cameraav`) and **macOS display** (`screenav`) natively via `purego`, compose live **Picture-in-Picture** or **Side-by-Side** scenes (`goav.Composite`), fan out simultaneous **VP8 / VP9 / AV1** WebRTC simulcast tracks, record locally to **WebM / MKV / MP4**, serve a live **LL-HLS** origin, and push **WHIP** — all from one running `goav.Task`, with a **Live `goav` Code & Pipeline Graph Inspector** showing how ~15 lines of Go power the entire studio.

## Run on macOS (Zero cgo!)

```sh
chmod +x run-macos.sh
CGO_ENABLED=0 GOEXPERIMENT=simd go run -exec ./run-macos.sh .
```

The `run-macos.sh` wrapper bundles the `CGO_ENABLED=0` binary into a cached, ad-hoc signed `goav Studio.app` with `NSCameraUsageDescription` and `NSScreenCaptureUsageDescription` so macOS can prompt for Camera and Screen Recording access cleanly.

### Headless / LAN Mode

Run without opening the native macOS window (or expose on your LAN so other devices can join `/watch`):

```sh
CGO_ENABLED=0 GOEXPERIMENT=simd go run . -headless -addr :8080
```

Then open `http://localhost:8080` for the Studio Control Room, or `http://<your-mac-ip>:8080/watch` on any browser to watch the live WebRTC simulcast tracks.

## What's Inside

- **Native macOS Window (`macosapp/`, `CGO_ENABLED=0`)**: Opens a dark-mode `NSWindow` + `WKWebView` via `purego` + Objective-C runtime directly — no cgo, no Electron, no external UI runtime.
- **Native macOS Camera (`cameraav`, `CGO_ENABLED=0`)**: AVFoundation capture via `purego`, zero-allocation planar `yuv420p` delivery.
- **Native macOS Screen Capture (`screenav/`, `CGO_ENABLED=0`)**: CoreGraphics + `AVCaptureScreenInput` via `purego` with hardware NV12 scaling and cursor capture.
- **Live Compositing (`goav.Composite`)**: Switch between Test Pattern, Camera, Screen, Picture-in-Picture (4 corners), and Side-by-Side.
- **Pure-Go Multi-Codec WebRTC Simulcast (No H.264)**: Hot-attach, pause, resume, live-retarget bitrate (`control.SetBitrate`), and live-rebranch (`attachment.Rebranch`) **VP8**, **VP9**, and **AV1** outputs from one shared `Tap("program")`.
- **Simultaneous Outputs**:
  - **Local Recording**: Hot-attach WebM, Matroska, or MP4 file muxers without stopping the stream.
  - **Live LL-HLS Origin**: Hot-attach `container/hls` (250ms parts, VP9 fMP4) served at `/hls/live.m3u8`.
  - **WHIP Live Push**: Hot-attach `webrtcav.WHIP` (`whips://...` / `whip+http://...`).
  - **Multi-Viewer `/watch` Room**: Share a link to broadcast all active WebRTC tracks to multiple browsers.
- **Live "Show the `goav` Code" & Graph Inspector**: Real-time generated Go code, CLI `goav run` equivalent, interactive SVG DAG, and `graphrender` Mermaid output reflecting every click.

## Tests

```sh
CGO_ENABLED=0 GOEXPERIMENT=simd go test -v ./...
```

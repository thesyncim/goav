# cameraav

Live camera frames for goav, built with `CGO_ENABLED=0` on macOS, Linux and
Windows (amd64 and arm64). macOS uses AVFoundation through `purego`; Linux uses
V4L2 ioctls and mmap; Windows uses Media Foundation COM. No FFmpeg executable or
C compiler is required. OS drivers, frameworks, and camera access still apply.
The macOS adapter includes two small Go-assembler C-ABI trampolines (amd64/arm64)
for retain/exchange/signal callback handoff. They never enter Go or process pixels.
This narrowly documented exception to the repository assembly guard avoids
purego reflective callback allocations; all pixel conversion remains Go.

```go
camera := goav.Input(cameraav.New(cameraav.Config{
    Width: 1280, Height: 720, FPS: 30,
}))
output := camera.Video().Encode(codec.VP8()).To(goav.URI("camera.webm"))
```

Use `cameraav.Devices(ctx)` for opaque device IDs and supply one as `Config.Device`.
An empty ID selects the first/default camera. The default mode is 640x480 at
30 fps. Dimensions and raw formats are exact. Integer frame rates allow native
interval quantization smaller than one 100 ns UVC tick; the original native
interval and timestamps are preserved. Distinct rates such as 29.97 fps never
match a 30 fps request. Unsupported modes fail explicitly. The provider opens at
Build time and begins capture when its source starts. Describe and Explain do
not enumerate devices, prompt for access, or activate the camera.

Register `runconfig.Scheme("camera", cameraav.Scheme{})` for URL inputs. The CLI
registers it automatically:

```sh
goav cameras --json
goav describe 'uri "camera://default?width=1280&height=720&fps=30" ! video ! encode vp8 ! file camera.webm'
goav run 'uri "camera://default" ! video ! encode vp8 ! uri "whips://media.example/live"'
```

Select an ID with a percent-encoded `device` query parameter. Other parameters
are `width`, `height`, `fps`, and `queue`. Unknown or repeated options fail.
The queue defaults to two frames and permits 1–16; the total Go pixel storage
is bounded to 128 MiB, including two working slots.

## Ownership and allocation contract

The source produces planar `yuv420p`, copying active pixels out of
native buffers before lending the frame to the pipeline. Native row padding is
not exposed. Each capture owns a fixed set of frame headers and pixel buffers,
allocated during OpenSource. Frame headers are borrowed during Emit; use the
normal goav Ref/Retain rules to hold media longer.

A full delivery queue drops its oldest frame. If downstream references keep all
preallocated slots occupied, the oldest queued frame is evicted first. If no
queued frame can be reclaimed, the incoming frame is dropped. Retaining more
than two frames reduces effective queue capacity. Buffers are never
expanded or overwritten while retained. DropReporter exposes queue/pool sheds;
OS/device-internal drops are not necessarily included. The macOS native bridge
retains at most one pending sample plus the sample being copied. V4L2 has a
bounded driver buffer ring. Media Foundation has one requested read in flight.

Allocation tests require **zero steady-state Go allocations** for raw pixel
conversion and queue handoff, macOS native callback handoff, and macOS native
sample-to-Go copying. The fixed Go pixel storage does not depend on sync.Pool or
GC. OS allocation and driver buffering are outside that Go-heap contract. Setup,
permission prompts, explicit consumer Ref calls, and errors may allocate.
The capture goroutine uses fixed native argument storage; concurrent cold calls
may use pools, and purego itself has internal pooling. Allocation assertions run
without the race detector, whose sync.Pool behavior intentionally differs.

Valid native capture timestamps are rebased at the first delivered frame; gaps
remain visible when frames drop. Linux uses monotonic arrival time when a driver
marks timestamps unknown/COPY or supplies zero. Native frames older than a
previously delivered arrival timestamp are dropped until capture catches up;
a regression in the native monotonic clock fails explicitly. Cancellation stops native reads/callbacks;
Close waits for completion before releasing native state. Windows bounds the
asynchronous flush-notification wait to two seconds and retains timeout errors. A device fault
or unexpected format change fails the source. Native startup/shutdown calls are
subject to the operating system's own completion behavior.

## Platform access and initial format support

- **macOS:** permission is explicit. An app needs `NSCameraUsageDescription` in
  its Info.plist. `RequestAccess(ctx)` refuses an unprepared executable instead
  of risking the OS terminating it. The CLI packaging helper below supplies
  this metadata. Capture requests AVFoundation video-range NV12.
- **Linux:** the user needs read/write access to the selected `/dev/video*` node.
  Device paths are validated as V4L2 character devices before opening them for
  capture. Native mmap storage has its own 128 MiB total cap. This backend
  supports single-planar streaming capture in NV12 or YUYV,
  progressive frames, and an integer frame rate with the interval quantization
  described above. Multiplanar-only, MJPEG-only,
  and libcamera/PipeWire-only devices are refused.
- **Windows:** enable camera access for desktop applications in privacy settings.
  Media Foundation must be installed (including the Media Feature Pack on Windows
  N editions). This initial backend selects native NV12 or YUY2 modes, avoiding a
  hidden codec/conversion graph. Explicit interlaced or non-video-range modes
  are refused. When a Windows driver omits range metadata, samples retain the
  native YUV levels; this adapter does not invent color metadata. Unsupported
  modes are explicit errors.

Capture is video-only. Fractional frame-rate selection and native compressed
camera formats are not yet part of this API.

Build the CLI with cgo disabled, then package it on macOS:

```sh
scripts/camera/package-macos.sh /path/to/goav /new/path/GoAV.app
/new/path/GoAV.app/Contents/MacOS/goav cameras --request-access
```

The helper never overwrites an existing app. It creates a local ad-hoc-signed
bundle; distribution signing/notarization is the application's responsibility.

## Validation

Normal tests never activate a physical camera. They cover ownership after Ref,
bounded retention, cleanup, URL planning, native ABI handoff, real synthetic
CoreVideo/CoreMedia and Media Foundation samples (including padded native
pitch), V4L2 dequeue/copy/requeue behavior, and allocation counts. The CI matrix runs the
module on all three operating systems. Set `GOAV_CAMERA_TEST_DEVICE` explicitly
for an opt-in capture smoke test; it checks frames and timestamps without saving
or transmitting images. On Linux CI, a Vivid virtual device is used when present.

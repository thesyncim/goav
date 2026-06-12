# Control-plane host playground

This is a self-contained `goav ctl` playground. The host builds a live
`goavtest.TestSource` named `fixture` that behaves like a VP8 camera, decodes it
to a frame tap named `frames`, and exposes a Unix control socket.

It demonstrates:

- built-in source controls against the test source: `rate`, `seek`, `segment`;
- a custom `fixture.controls` command that reports what the test source
  recorded;
- server-aware `help attach` output for app-owned branch components;
- stock CLI transcode branches (`resize`, `vp8enc`, `filesink`);
- thumbnail/sample branches from the same running tap;
- an in-process app sink (`memorysink`);
- a custom video encoder spelling (`acmeenc`) with native settings;
- graph rendering, rebranching, and detach.

Start the host:

```sh
go run ./examples/control-plane-host --control unix:///tmp/goav-control-plane-host.sock
```

In another shell, set a shorter variable:

```sh
CTL='goav ctl --control unix:///tmp/goav-control-plane-host.sock'
```

Inspect the host-owned grammar:

```sh
$CTL help attach
$CTL help control vendor.rate
$CTL help control fixture.controls
$CTL taps
```

Control the fake source. These are normal built-in source controls targeting
the `fixture` source node:

```sh
$CTL control rate value=0.5 source=fixture
$CTL control seek position=2s source=fixture
$CTL control segment start=1s end=3s source=fixture
```

Prove the controls reached the test source:

```sh
$CTL control fixture.controls
$CTL control fixture.controls type=rate
```

Attach a stock VP8/WebM transcode from the decoded `frames` tap:

```sh
$CTL attach frames as archive \
  'meter label="left ! right" ! resize 640x360 ! vp8enc bitrate=900k fps=30 keyframe_interval=30 ! filesink location="/tmp/goav archive.webm" format=webm'
```

Attach a low-rate thumbnail recording:

```sh
$CTL attach frames as thumbnails \
  'thumbnail every=5 label=sample ! resize 160x90 ! vp8enc bitrate=160k fps=1 keyframe_interval=1 ! filesink location="/tmp/goav thumbnails.ivf" format=ivf'
```

Attach an app-owned in-process preview sink:

```sh
$CTL attach frames as memory \
  'thumbnail every=3 label=preview ! memorysink name=preview'
```

Attach a custom encoder path. The CLI spelling is short, but the host maps
`bitrate`, `quality`, and `lookahead` into a real `codec.CodecSpec` and native
adapter settings:

```sh
$CTL attach frames as acme-preview \
  'thumbnail every=2 label=acme ! acmeenc bitrate=250k quality=preview lookahead=shallow ! memorysink name=acme-preview'
```

Render the running graph:

```sh
$CTL graph
$CTL graph format=text
```

Retarget the live in-process thumbnail branch without restarting the host:

```sh
$CTL rebranch memory \
  'thumbnail every=10 label=slow ! memorysink name=slow-preview'
```

Detach the branches:

```sh
$CTL detach archive
$CTL detach thumbnails
$CTL detach memory
$CTL detach acme-preview
```

The emitted command list printed by the host is generated from the same strings
this guide uses, and the example tests drive the socket with the same request
shape the CLI sends.

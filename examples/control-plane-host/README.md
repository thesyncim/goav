# Control-plane host

> Demonstrates the control plane, a governed pre-v1 feature rather than the
> v1 front door. See [V1 scope](../../docs/COMPATIBILITY.md#v1-scope).

An application that serves its running task with `ctlserver`, adding methods
and pipeline elements of its own. The task plays a stand-in deck, `player` (a
fade from black to white at 30 fps that applies seek and records every
control), through a tap named `live`. The host adds:

- methods: `cue name= [offset=]` seeks the player to a named cue,
  `controls [type=]` reports the controls the player received, and
  `collected` reports what the host's elements measured and kept;
- elements: a `meter` stage (mean luma per frame), a `thumbnail every=N` sink
  that keeps one frame in N, a `memory` sink, and an `acme` encoder that maps
  its typed settings onto the host's own codec, reaching native settings
  through `codec.Control`.

Attached programs may write files only under `--files` (default: the temp
directory), through `launch.Dir`. See [CONTROL_PLANE.md](../../docs/CONTROL_PLANE.md)
for the API and the protocol.

```sh
cd examples/control-plane-host
GOEXPERIMENT=simd go run . --control unix:///tmp/goav-host.sock
```

From another shell, with `goav` built from this checkout
(`GOEXPERIMENT=simd go install ./cmd/goav` at the repository root):

```sh
ctl() { goav ctl --control unix:///tmp/goav-host.sock "$@"; }
ctl capabilities                      # every method and element, with params
ctl seek position=2s source=player    # a built-in control
ctl cue name=chorus offset=500ms      # the host's: {"cue": "chorus", "position": "42.5s"}
ctl controls type=seek                # what reached the player
ctl attach 'tap live ! meter name=cam ! thumbnail every=30 name=thumbs'
ctl attach 'tap live ! resize 160x90 ! encode vp8 ! as preview ! file preview.webm'
ctl attach 'tap live ! acme bitrate=250k lookahead=deep ! as acme ! memory name=acme'
ctl collected
ctl describe format=text
ctl rebranch thumbs 'tap live ! thumbnail every=90 name=slow' switch=next-keyframe
ctl detach preview                    # finalizes preview.webm
ctl stop
```

`attach` answers `{"attachment": "thumbs"}`, the name `rebranch` and `detach`
take. `collected` shows the host's state:

```json
{
  "levels": {"cam": 136},
  "kept": {"acme": 60, "thumbs": 3},
  "opened": ["acme bitrate=250000 lookahead=deep"]
}
```

A bad setting is refused at attach, before the task changes, and the error
points at the element; over the wire its `data` carries the line and column:

```text
$ ctl attach 'tap live ! thumbnail every=0'
launch: 1:12: thumbnail: every=0 keeps no frames; write every=1 or more
    tap live ! thumbnail every=0
               ^^^^^^^^^^^^^^^^^
```

`main_test.go` drives the same sequence through a `ctlserver.Client`.

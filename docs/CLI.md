# CLI

Use the Go library for application code and reusable pipelines. Use `goav run`
for a self-contained generated-source smoke test or control-plane playground,
and use `goav ctl` to inspect or steer a task that an application has exposed
through `ctlserver`.

`goav run` is intentionally narrow. It does not parse arbitrary goav recipes:
it builds one generated video source, zero or more video operations, and one
file sink.

## `goav run`

```text
goav run [--runtime demo|default|test] [--control unix://PATH] '<pipeline>'
```

Pipeline steps are separated by `!`. Values may be quoted with single or
double quotes; backslash escapes the next byte inside a quoted value.

```text
testsrc video [name=<id>] width=<px> height=<px> fps=<n[/d]|decimal> frames=<n>|duration=<d> realtime=<bool> [format=i420|yuv420p] [pattern=bars|gradient|solid]
tap name=<tap-name>
resize width=<px> height=<px>
encode codec=<id> media=<video|audio> [bitrate=<rate>] [fps=<n[/d]|decimal>] [keyframe_interval=<n>] [profile=<name>] [level=<name>] [sample_rate=<hz>] [channels=<n>] [native_key=value...]
filesink location=<path> [format=<id>]
```

Rules:

- The first step must be `testsrc video`; aliases such as `videosrc`, `w`,
  `h`, `size`, `framerate`, `live`, and `pix_fmt` are refused with a canonical
  spelling.
- The last step must be `filesink`.
- A generated frame source must pass through `encode` before `filesink`.
- `tap` publishes a frame tap for later `goav ctl attach`.
- `resize` is video-only and requires explicit `width` and `height`.
- `encode` requires `codec` and `media`; duplicate aliases such as `id`,
  `type`, `rate`, `framerate`, `keyint`, `gop`, `samplerate`, `ch`,
  `clockrate`, and `bitrate_bps` are refused.
- Known file extensions infer common formats; use `format=<id>` to override or
  target a custom muxer.

Runtime choices:

- `demo`: bundled adapters with a deterministic clock.
- `default` or `bundle`: bundled adapters with normal runtime timing.
- `test`: deterministic `goavtest` runtime for fake/private codecs.

With `--control unix://PATH`, `goav run` serves the live task through
`ctlserver` while the task runs. The command returns JSON after the task stops.

## `goav ctl`

```text
goav ctl [--control unix://PATH] <command> [args...]
```

`goav ctl help` prints the built-in command set. Against a live socket,
`help attach`, `help rebranch`, and `capabilities` include the host's
`ctlserver.CapabilitySet`, custom branch steps, custom encoder spellings, and
runtime-discovered encoders/muxers.

Use `goav ctl` for cold-path operations: graph inspection, stats/events,
controls, attach, rebranch, detach, and stop. Hot-path media logic belongs in
the Go library.

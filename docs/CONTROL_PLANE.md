# Control plane

`goav ctl` is the cold-path control surface for a running task. The
application owns the task and exposes a Unix socket with package `ctl`; operators
or automation talk to that socket with structured requests.

```sh
goav ctl --control unix:///tmp/goav-live.sock control bitrate stream=video value=1200k
goav ctl --control unix:///tmp/goav-live.sock watch type=stats --follow
goav ctl --control unix:///tmp/goav-live.sock attach frames as archive \
  'encode codec=opus media=audio bitrate=128k ! filesink location=archive.ogg'
```

The control layer is allowlisted and lowers into the same task APIs normal Go
code uses: `Task.Control`, `Task.Attach`, `Attachment.Rebranch`, `Task.Detach`,
`Snapshot`, `Stats`, `Watch`, and `Close`. Reflection is used only on this cold
path to bind known command structs, validate fields, parse JSON, and generate
help from tags. There is no global registry and no user-provided method name
dispatch.

## No-Code Generated Source

For a self-contained control-plane playground, `goav run --control` can host a
generated source and a real encoder directly from one string. Start a live AV1
pipeline:

```sh
goav run --control unix:///tmp/goav-live.sock \
  'testsrc video name=fixture width=1280 height=720 fps=30 duration=30s realtime=true pattern=bars ! tap name=frames ! encode codec=av1 media=video bitrate=1200k fps=30 keyframe_interval=60 min_qindex=20 max_qindex=180 tune=zerolatency ! filesink location=/tmp/goav-av1.mkv format=matroska'
```

Then drive the running graph from another shell:

```sh
goav ctl --control unix:///tmp/goav-live.sock taps
goav ctl --control unix:///tmp/goav-live.sock graph
goav ctl --control unix:///tmp/goav-live.sock control rate value=0.5 source=fixture
goav ctl --control unix:///tmp/goav-live.sock control seek position=2s source=fixture
goav ctl --control unix:///tmp/goav-live.sock attach frames as preview \
  'resize width=320 height=180 ! encode codec=av1 media=video bitrate=300k fps=2 keyframe_interval=1 ! filesink location=/tmp/goav-preview.ivf'
goav ctl --control unix:///tmp/goav-live.sock graph format=text
goav ctl --control unix:///tmp/goav-live.sock detach preview
goav ctl --control unix:///tmp/goav-live.sock stop
```

That flow needs no application code. The same string launcher can create a short
decoder-readable AV1 IVF file:

```sh
goav run \
  'testsrc video width=1280 height=720 fps=30 duration=3s realtime=true pattern=bars ! encode codec=av1 media=video bitrate=1200k fps=30 keyframe_interval=60 min_qindex=20 max_qindex=180 tune=zerolatency ! filesink location=/tmp/goav-av1.ivf'
```

Known file extensions infer the destination format for `.ivf`, `.mkv`,
`.webm`, and Annex B elementary-stream paths; keep `format=<id>` when you need
to override the extension or target a custom container id.

## Bootstrap Host

This is the smallest production shape:

1. Build a normal task and name stable taps with `FrameTap` or `PacketTap`.
2. Declare host-owned capabilities with typed settings structs.
3. Group commands, branch steps, sinks, and native encoder spellings in one
   `ctl.CapabilitySet`.
4. Call `ctl.ValidateCapabilities` in startup/tests to catch empty names,
   alias collisions, and non-struct settings before opening a socket.
5. Start `ctl.ServeUnixWithOptions`.
6. Use `goav ctl --control unix://...` to inspect, control, attach, rebranch,
   detach, and render diagnostics from the same running graph.

The default branch grammar is intentionally useful before a host adds any
custom CLI grammar. If a task runtime has an encoder registered with
`goav.WithEncoder`, it is callable from attach/rebranch as:

```sh
goav ctl --control unix:///tmp/goav-live.sock attach frames as preview \
  'encode codec=x_acme_video media=video bitrate=900k fps=30 lookahead=deep ! filesink location=preview.webm'
```

`help attach` and `help rebranch` list those runtime-discovered encoders and
muxers from the running task. Common codec options become typed settings, and
extra encoder `key=value` pairs are carried as `CodecSettings.Custom` so the
adapter can validate and apply its own vocabulary. Add a typed encoder spelling
when you want a friendly name, richer validation, or `codec.Control` host code
for native handles.

The executable local harness is
`Example_bootstrapControlPlaneHost` in `ctl/example_test.go`. It builds a live
fixture task, starts `Run` in the background, calls a custom control, attaches a
custom branch step plus custom encoder settings into a temp filesink, and
renders a flowchart from the live task. Run it with:

```sh
go test ./ctl -run Example_bootstrapControlPlaneHost -count=1
```

For a long-running host process that you can keep open in one terminal while
driving it from another, run:

```sh
go run ./examples/control-plane-host --control unix:///tmp/goav-control-plane-host.sock
```

That example is a self-contained playground: it starts a live
`goavtest.TestSource` named `fixture`, accepts normal source controls such as
`goav ctl control rate value=0.5 source=fixture`, reports the controls captured
by that test source with `goav ctl control fixture.controls`, and demonstrates
stock transcode branches, thumbnail branches, custom branch steps, custom
encoder settings, graph rendering, rebranching, and detach. The complete
copyable command sequence lives in `examples/control-plane-host/README.md`.

```go
ctx := context.Background()
const customCodec = av.CodecID("x_acme_audio")

encoderFactory := &acmeEncoderFactory{
    Descriptor: codec.Descriptor{
        ID:   customCodec,
        Name: "ACME audio",
        Type: av.MediaAudio,
    },
}

task, err := goav.From(liveInput).
    Audio().Decode().Tap(goav.FrameTap("frames")).
    To(primaryDestination).
    UseRuntime(goav.Default(
        goav.WithEncoder(encoderFactory.Descriptor, encoderFactory),
    )).
    Build(ctx)
if err != nil {
    return err
}
defer task.Close()
```

Add a custom command by declaring the argument struct and handler. The struct
tags drive CLI parsing, JSON binding, validation, and generated help.

```go
type SetRate struct {
    Value  float64 `goavctl:"value,required" usage:"value=<float>" help:"playback rate"`
    Source string  `goavctl:"source,required" usage:"source=<source-name>" help:"source node to retime"`
}

rateCommand := ctl.NewCommand[SetRate](
    "vendor.rate",
    "vendor playback-rate control",
    func(ctx context.Context, task goav.Task, cmd SetRate) (ctl.ControlResponse, error) {
        ctrl := goav.Rate(cmd.Value).At(pipeline.NodeRef(cmd.Source))
        if err := task.Control(ctx, ctrl); err != nil {
            return ctl.ControlResponse{}, err
        }
        return ctl.ControlResponse{
            Operation: "control vendor.rate",
            Result:    map[string]any{"value": cmd.Value, "source": cmd.Source},
        }, nil
    },
)
```

Expose custom branch components and custom encoder settings through typed
helpers. Every `key=value` token is bound into your struct, the same tags drive
help output, and the host returns normal Go values or the real `codec.CodecSpec`
the runtime already understands.

```go
type MeterSettings struct {
    Window time.Duration `goavctl:"window,duration" usage:"[window=<duration>]" help:"observation window"`
}

meter := ctl.NewBranchStep[MeterSettings](
    "meter",
    "observe frames before encoding",
    func(branch *ctl.BranchPipeline, _ MeterSettings) error {
        branch.Do(goav.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit goav.Emit) error {
            recordLevel(frame)
            return emit.Frame(frame)
        }))
        return nil
    },
)

type ACMESettings struct {
    Bitrate   int    `goavctl:"bitrate,required,rate" usage:"bitrate=<rate>" help:"target bitrate"`
    Quality   string `goavctl:"quality" usage:"[quality=<profile>]" help:"native quality profile"`
    Lookahead string `goavctl:"lookahead" usage:"[lookahead=<mode>]" help:"native lookahead mode"`
}

acme := ctl.NewEncoderSpec[ACMESettings](
    "acmeenc",
    "ACME audio encoder with native settings",
    func(args ACMESettings) (codec.CodecSpec, error) {
        return codec.Codec(customCodec, av.MediaAudio,
            codec.Bitrate(args.Bitrate),
            codec.Profile(args.Quality),
            codec.Control(func(native any) error {
                options := native.(*acme.Options)
                options.Lookahead = args.Lookahead
                return nil
            }),
        ), nil
    },
)

capabilities := ctl.CapabilitySet{
    Commands: []ctl.CommandSpec{rateCommand},
    Pipeline: ctl.PipelineRegistry{
        Steps:    []ctl.BranchPipelineStepSpec{meter},
        Encoders: []ctl.EncoderSpec{acme},
    },
}

if err := ctl.ValidateCapabilities(capabilities); err != nil {
    return err
}
```

Use `ctl.NewError` from custom command, step, or encoder callbacks when a
custom value is missing or invalid. The structured code, node,
details, suggestions, and cause are preserved in the CLI/socket response.

Start the socket after the task is built. The same options apply whether the
socket is used by humans, scripts, supervisors, or tests.

```go
err = ctl.ServeUnixWithOptions(ctx, task, "unix:///tmp/goav-live.sock",
    ctl.WithCapabilities(capabilities),
)
```

Operate it from the CLI:

```sh
goav ctl --control unix:///tmp/goav-live.sock help
goav ctl --control unix:///tmp/goav-live.sock help control vendor.rate
goav ctl --control unix:///tmp/goav-live.sock help attach
goav ctl --control unix:///tmp/goav-live.sock capabilities
goav ctl --control unix:///tmp/goav-live.sock taps
goav ctl --control unix:///tmp/goav-live.sock control vendor.rate value=0.5 source=fixture
goav ctl --control unix:///tmp/goav-live.sock control --json '{"type":"rate","rate":0.75,"node":"fixture"}'
goav ctl --control unix:///tmp/goav-live.sock control deliver --json '{"type":"vendor.force_idr","stream_id":"video","metadata":{"source":"cli"}}' at=frames
goav ctl --control unix:///tmp/goav-live.sock attach frames as archive \
  'meter ! acmeenc bitrate=128k quality=voice lookahead=deep ! filesink location=/tmp/archive.ogg'
goav ctl --control unix:///tmp/goav-live.sock graph
goav ctl --control unix:///tmp/goav-live.sock graph format=dot
goav ctl --control unix:///tmp/goav-live.sock rebranch archive \
  'meter ! acmeenc bitrate=96k quality=voice lookahead=shallow ! filesink location=/tmp/archive-low.ogg'
goav ctl --control unix:///tmp/goav-live.sock detach archive
```

`help attach`, `help rebranch`, and `capabilities` are server-aware: responses
include the built-in branch-pipeline grammar, runtime-discovered encoders and
muxers, plus every command, branch step, sink, and encoder spelling registered
on that server, including aliases, summaries, usage strings, and typed fields.
That makes app-owned branch components discoverable from the same CLI surface
that invokes them.

Custom names and aliases are validated as one namespace per server. A custom
control cannot reuse a built-in control verb or another custom alias, and a
custom branch step or encoder cannot shadow built-in branch-pipeline spellings
such as `copy`, `encode`, `resize`, or `filesink`. Collisions fail with
`invalid_registry` before a socket starts or a branch pipeline mutates the
running graph.

Branch-pipeline values can be quoted with single or double quotes. Use quotes
for paths or custom settings that contain spaces, `!`, or `=`:

```sh
goav ctl --control unix:///tmp/goav-live.sock attach frames as archive \
  'meter label="left ! right" ! filesink location="/tmp/archive copy.ogg" format=ogg'
```

Raw JSON is for automation that already has the protocol object. `control
--json` decodes into the real `goav.Control` representation; `control deliver
--json` decodes into `av.Event` and then lowers to `goav.Deliver(event)`. Nested
event metadata is rejected instead of being stringified silently, so a caller
must choose the exact conversion before sending the request. Raw JSON uses the
documented canonical field names and rejects unknown or duplicate fields before
anything is applied; use `stream_id`, `bitrate`, `rate`, `start`, `end`, and
`active` rather than CLI-only field names such as `stream` or `value`.

Render a live flowchart from the same running task:

```sh
goav ctl --control unix:///tmp/goav-live.sock graph
goav ctl --control unix:///tmp/goav-live.sock graph format=dot
goav ctl --control unix:///tmp/goav-live.sock graph format=text
```

Or from host code:

```go
flowchart, err := graphrender.RenderTaskFlowchart(task)
if err != nil {
    return err
}
fmt.Println(flowchart)

snap := task.Snapshot()
flowchart, err = graphrender.RenderSnapshotFlowchart(snap)
if err != nil {
    return err
}

attachmentFlowchart, err := graphrender.RenderBranchFlowchart(attachment.Snapshot())
if err != nil {
    return err
}
fmt.Println(attachmentFlowchart)
```

The render helpers read only public snapshot data. Runtime branch-owned nodes
are annotated with the branch name and lifecycle state, for example
`branch=archive (attached)`. Use `graphrender.RenderTaskURI(task,
"goav://graph/dot")`, `RenderSnapshotURI(snap, "goav:graph")`, or
`RenderBranchURI(attachment.Snapshot(), "goav://graph/dot")` when DOT or text
is easier to feed into other tooling.

## Built-In Requests

The socket protocol is JSON. The CLI is the normal entry point, but hosts and
tests can use the same request shape directly:

```go
request, err := ctl.RequestFromCLI([]string{
    "control", "bitrate", "stream=video", "value=1200k", "at=encoded",
})
if err != nil {
    return err
}
response := server.Handle(ctx, request)
```

Supported built-ins include:

- `control keyframe stream=<stream-id> [at=<tap>]`
- `control bitrate stream=<stream-id> value=<rate> [at=<tap>]`
- `control seek position=<duration> [source=<source>|node=<node>]`
- `control rate value=<float> [source=<source>|node=<node>]`
- `control segment start=<duration> end=<duration> [source=<source>|node=<node>]`
- `control select active=<arm-or-stream-id> [selector=<name>|at=<tap>]`
- `control deliver ...` and `control deliver --json '<av.Event JSON>'`
- `control --json '<goav.Control JSON>'`
- `inspect`, `snapshot`, `stats`, `taps`, `streams`, `branches`,
  `destinations`, `capabilities`
- `graph [format=mermaid|dot|text]` and `flowchart [format=mermaid|dot|text]`
- `events --follow`, `watch [type=<event-type>] [stream=<stream-id>] --follow`
- `attach`, `rebranch`, `detach`, `stop`

## Custom Codecs

Register the codec implementation on the runtime, then call it in an attach or
rebranch pipeline with the generic `encode` step. This is the default path for
custom encoders and does not require a custom encoder spelling. Use
`goav.Default(...)` when you want the stock codecs, formats, and filters plus
your adapter; use
`goav.New(...)` only when you are intentionally registering every required
codec, filter, prober, demuxer, and muxer yourself.

```go
rt := goav.Default(
    goav.WithEncoder(codec.Descriptor{
        ID:   av.CodecID("x_pcm_s16"),
        Name: "ACME PCM S16",
        Type: av.MediaAudio,
    }, acmeEncoderFactory),
    goav.WithFormatAdapter(acmecontainer.Register), // if no stock container accepts the codec
)
```

```sh
goav ctl --control unix:///tmp/goav-live.sock attach frames as record \
  'encode codec=x_pcm_s16 media=audio bitrate=128k sample_rate=16000 channels=1 profile=voice dither=triangular ! filesink location=record.ogg format=ogg'
```

The generic encoder step supports the common codec options: `bitrate`,
`profile`, `level`, `sample_rate`, `channels`, `clock_rate`,
`keyframe_interval`, and `fps`. Any other `key=value` pair is passed through as
`CodecSettings.Custom`, for example `dither=triangular`, `lookahead=deep`, or
AV1 settings such as `min_qindex=20 max_qindex=180 tune=zerolatency`.
Ambiguous or duplicate encoder spellings such as `rate`, `framerate`, `keyint`,
`gop`, `samplerate`, `ch`, `clockrate`, and `bitrate_bps` are rejected with
suggestions; use the canonical option names above.
File sinks follow the same rule: use `filesink location=<path> [format=<id>]`.
Transform steps use one spelling as well: `resize width=<px> height=<px>` and
`resample sample_rate=<hz> channels=<n>`.
Generated `goav run` sources use `testsrc video` with
`width=<px> height=<px> fps=<n>` and either `frames=<n>` or `duration=<d>`;
duplicate aliases such as `w`, `h`, `size`, `framerate`, `live`, `pix_fmt`,
and `pixel_format` are rejected with suggestions.

The destination container must accept the selected codec. Standard codecs can
often use the standard containers registered by `goav.Default`; a private codec
usually needs a matching `WithFormatAdapter`, `WithMuxer`, or an app-owned
custom destination step. Runtime muxers registered with `WithMuxer` are callable
by `filesink location=<path> [format=<id>]` and appear in `help attach`.

When an encoder needs host-side validation or direct native handle setup,
expose a named encoder step with `ctl.NewEncoderSpec`. The CLI spelling stays
short and the host keeps full control over validation and native adapter calls:

```sh
goav ctl --control unix:///tmp/goav-live.sock attach frames as archive \
  'acmeenc bitrate=128k quality=cinema lookahead=deep ! filesink location=archive.ogg'
```

## Custom Branch Components

Custom branch steps can add external stages, sinks, or compound branch grammar:

```go
type MeterSettings struct {
    Window time.Duration `goavctl:"window,duration" usage:"[window=<duration>]" help:"observation window"`
}

meter := ctl.NewBranchStep[MeterSettings](
    "meter",
    "observe frames before encoding",
    func(branch *ctl.BranchPipeline, _ MeterSettings) error {
        branch.Do(goav.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit goav.Emit) error {
            recordLevel(frame)
            return emit.Frame(frame)
        }))
        return nil
    },
)
```

Custom destination steps call `branch.Destination(...)`. This is the pattern for
object stores, upload services, analytics queues, or any app-owned sink that is
not a local file:

```go
type ObjectSinkSettings struct {
    Bucket string `goavctl:"bucket,required" usage:"bucket=<name>" help:"object bucket"`
    Key    string `goavctl:"key,required" usage:"key=<path>" help:"object key"`
}

objectSink := ctl.NewBranchStep[ObjectSinkSettings](
    "objectsink",
    "upload branch messages to object storage",
    func(branch *ctl.BranchPipeline, args ObjectSinkSettings) error {
        writer := objectStoreWriter(args.Bucket, args.Key)
        branch.Destination(goav.Writer(writer,
            goav.Name("object:"+args.Key),
            goav.Format(av.FormatID("ogg")),
        ))
        return nil
    },
)
```

```sh
goav ctl --control unix:///tmp/goav-live.sock attach frames as monitored \
  'meter ! encode codec=x_pcm_s16 media=audio ! filesink location=monitored.ogg format=ogg'

goav ctl --control unix:///tmp/goav-live.sock attach frames as upload \
  'meter ! acmeenc bitrate=128k quality=voice lookahead=deep ! objectsink bucket=archive key=session-001.ogg'
```

Unknown commands, fields, taps, branches, nodes, and pipeline steps return
structured errors with available names and suggestions.

## Safety Model

- Reflection is confined to argument binding, validation, and help generation.
- No arbitrary method names, unexported internals, or global registries are
  exposed.
- Commands lower into existing task/control APIs.
- Raw JSON fallback decodes into the real `goav.Control` or `av.Event` shapes
  instead of introducing a second control model.
- Custom controls, custom branch steps, custom codec names, custom sinks, and
  custom encoder names are per-server allowlists.
- Custom command, branch-step, encoder, and alias names must be unique in that
  server's allowlists; built-in branch-pipeline spellings are reserved.
- Branch-pipeline strings are parsed by the allowlisted cold-path parser; custom
  behavior is callable only through explicit `CapabilitySet` entries chosen by
  the host application.

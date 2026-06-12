# Control plane

`goav ctl` is the cold-path control surface for a running task. The
application owns the task and exposes a Unix socket with package `ctl`; operators
or automation talk to that socket with structured requests.

```sh
goav ctl --control unix:///tmp/goav-live.sock control bitrate stream=video value=1200k
goav ctl --control unix:///tmp/goav-live.sock watch type=stats --follow
goav ctl --control unix:///tmp/goav-live.sock attach frames as archive \
  'encode codec=opus media=audio bitrate=128k ! filesink location=archive.ogg format=ogg'
```

The control layer is allowlisted and lowers into the same task APIs normal Go
code uses: `Task.Control`, `Task.Attach`, `Attachment.Rebranch`, `Task.Detach`,
`Snapshot`, `Stats`, `Watch`, and `Close`. There is no global registry and no
user-provided method name dispatch.

## Bootstrap Host

This is the smallest production shape:

1. Build a normal task and name stable taps with `FrameTap` or `PacketTap`.
2. Add explicit control verbs with `ctl.CommandSpec`.
3. Add custom branch-pipeline steps and encoder names with
   `ctl.PipelineRegistry`.
4. Start `ctl.ServeUnixWithOptions`.
5. Use `goav ctl --control unix://...` to inspect, control, attach, rebranch,
   detach, and render diagnostics from the same running graph.

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

That example uses the same extension points as the snippets below and includes
copyable commands in `examples/control-plane-host/README.md`.

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
    UseRuntime(goav.New(
        goav.WithStdFilters(),
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

rateCommand := ctl.CommandSpec{
    Name:     "vendor.rate",
    Summary:  "vendor playback-rate control",
    ArgsType: reflect.TypeOf(SetRate{}),
    Apply: func(ctx context.Context, task goav.Task, args any) (ctl.ControlResponse, error) {
        cmd := args.(SetRate)
        ctrl := goav.Rate(cmd.Value).At(pipeline.NodeRef(cmd.Source))
        if err := task.Control(ctx, ctrl); err != nil {
            return ctl.ControlResponse{}, err
        }
        return ctl.ControlResponse{
            Operation: "control vendor.rate",
            Result:    map[string]any{"value": cmd.Value, "source": cmd.Source},
        }, nil
    },
}
```

Expose custom branch components and custom encoder settings through an explicit
pipeline registry. Every `key=value` token on the CLI arrives in `StepArgs`; the
host maps those strings to normal Go values and returns the real
`codec.CodecSpec` the runtime already understands.

```go
registry := ctl.PipelineRegistry{
    Steps: []ctl.BranchPipelineStepSpec{{
        Name:    "meter",
        Summary: "observe frames before encoding",
        Usage:   "[window=<duration>]",
        Apply: func(branch *ctl.BranchPipeline, _ ctl.StepArgs) error {
            branch.Do(goav.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit goav.Emit) error {
                recordLevel(frame)
                return emit.Frame(frame)
            }))
            return nil
        },
    }},
    Encoders: []ctl.EncoderSpec{{
        Name:    "acmeenc",
        Summary: "ACME audio encoder with native settings",
        Usage:   "bitrate=<bps> quality=<profile> lookahead=<mode>",
        Apply: func(args ctl.StepArgs) (codec.CodecSpec, error) {
            bitrate, err := strconv.Atoi(args["bitrate"])
            if err != nil {
                return codec.CodecSpec{}, err
            }
            return codec.Codec(customCodec, av.MediaAudio,
                codec.Bitrate(bitrate),
                codec.Profile(args["quality"]),
                codec.Control(func(native any) error {
                    options := native.(*acme.Options)
                    options.Lookahead = args["lookahead"]
                    return nil
                }),
            ), nil
        },
    }},
}
```

Start the socket after the task is built. The same options apply whether the
socket is used by humans, scripts, supervisors, or tests.

```go
err = ctl.ServeUnixWithOptions(ctx, task, "unix:///tmp/goav-live.sock",
    ctl.WithCommands(rateCommand),
    ctl.WithPipelineRegistry(registry),
)
```

Operate it from the CLI:

```sh
goav ctl --control unix:///tmp/goav-live.sock help
goav ctl --control unix:///tmp/goav-live.sock help control vendor.rate
goav ctl --control unix:///tmp/goav-live.sock help attach
goav ctl --control unix:///tmp/goav-live.sock taps
goav ctl --control unix:///tmp/goav-live.sock control vendor.rate value=0.5 source=fixture
goav ctl --control unix:///tmp/goav-live.sock attach frames as archive \
  'meter ! acmeenc bitrate=128000 quality=voice lookahead=deep ! filesink location=/tmp/archive.ogg format=ogg'
goav ctl --control unix:///tmp/goav-live.sock graph
goav ctl --control unix:///tmp/goav-live.sock graph format=dot
goav ctl --control unix:///tmp/goav-live.sock rebranch archive \
  'meter ! acmeenc bitrate=96000 quality=voice lookahead=shallow ! filesink location=/tmp/archive-low.ogg format=ogg'
goav ctl --control unix:///tmp/goav-live.sock detach archive
```

`help attach` and `help rebranch` are server-aware: the response includes the
built-in branch-pipeline grammar plus every `BranchPipelineStepSpec` and
`EncoderSpec` registered on that server, including aliases, summaries, and
`Usage` strings. That makes app-owned branch components discoverable from the
same CLI surface that invokes them.

Custom names and aliases are validated as one namespace per server. A custom
control cannot reuse a built-in control verb or another custom alias, and a
custom branch step or encoder cannot shadow built-in branch-pipeline spellings
such as `copy`, `encode`, `opus`, or `file`. Collisions fail with
`invalid_registry` before a socket starts or a branch pipeline mutates the
running graph.

Branch-pipeline values can be quoted with single or double quotes. Use quotes
for paths or custom settings that contain spaces, `!`, or `=`:

```sh
goav ctl --control unix:///tmp/goav-live.sock attach frames as archive \
  'meter label="left ! right" ! filesink location="/tmp/archive copy.ogg" format=ogg'
```

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
```

`RenderTaskFlowchart` reads only public `Snapshot()` data. Runtime branch-owned
nodes are annotated with the branch name and lifecycle state, for example
`branch=archive (attached)`. Use `graphrender.RenderTaskURI(task,
"goav://graph/dot")` or `RenderTaskURI(task, "goav:graph")` when DOT or text
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
  `destinations`
- `graph [format=mermaid|dot|text]` and `flowchart [format=mermaid|dot|text]`
- `events --follow`, `watch [type=<event-type>] [stream=<stream-id>] --follow`
- `attach`, `rebranch`, `detach`, `stop`

## Custom Codecs

Register the codec implementation on the runtime, then call it in an attach or
rebranch pipeline with the generic `encode` step:

```go
rt := goav.New(
    goav.WithEncoder(codec.Descriptor{
        ID:   av.CodecID("x_pcm_s16"),
        Name: "ACME PCM S16",
        Type: av.MediaAudio,
    }, acmeEncoderFactory),
)
```

```sh
goav ctl --control unix:///tmp/goav-live.sock attach frames as record \
  'encode codec=x_pcm_s16 media=audio bitrate=128k sample_rate=16000 channels=1 profile=voice ! filesink location=record.ogg format=ogg'
```

The generic encoder step supports the common codec options: `bitrate`,
`profile`, `level`, `sample_rate`, `channels`, `clock_rate`,
`keyframe_interval`, and `fps`.

When an encoder exposes native settings beyond those common options, expose a
named encoder step with `ctl.EncoderSpec`. The CLI spelling stays short and the
host keeps full control over validation and native adapter calls:

```sh
goav ctl --control unix:///tmp/goav-live.sock attach frames as archive \
  'acmeenc bitrate=128000 quality=cinema lookahead=deep ! filesink location=archive.ogg format=ogg'
```

## Custom Branch Components

Custom branch steps can add external stages, sinks, or compound branch grammar:

```go
ctl.BranchPipelineStepSpec{
    Name:    "meter",
    Summary: "observe frames before encoding",
    Usage:   "[window=<duration>]",
    Apply: func(branch *ctl.BranchPipeline, args ctl.StepArgs) error {
        branch.Do(goav.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit goav.Emit) error {
            recordLevel(frame)
            return emit.Frame(frame)
        }))
        return nil
    },
}
```

Custom destination steps call `branch.Destination(...)`. This is the pattern for
object stores, upload services, analytics queues, or any app-owned sink that is
not a local file:

```go
ctl.BranchPipelineStepSpec{
    Name:    "objectsink",
    Summary: "upload branch messages to object storage",
    Usage:   "bucket=<name> key=<path>",
    Apply: func(branch *ctl.BranchPipeline, args ctl.StepArgs) error {
        writer := objectStoreWriter(args["bucket"], args["key"])
        branch.Destination(goav.Writer(writer,
            goav.Name("object:"+args["key"]),
            goav.Format(av.FormatID("ogg")),
        ))
        return nil
    },
}
```

```sh
goav ctl --control unix:///tmp/goav-live.sock attach frames as monitored \
  'meter ! encode codec=x_pcm_s16 media=audio ! filesink location=monitored.ogg format=ogg'

goav ctl --control unix:///tmp/goav-live.sock attach frames as upload \
  'meter ! acmeenc bitrate=128000 quality=voice lookahead=deep ! objectsink bucket=archive key=session-001.ogg'
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
- Custom controls, custom branch steps, custom codec names, and custom encoder
  names are per-server allowlists.
- Custom command, branch-step, encoder, and alias names must be unique in that
  server's allowlists; built-in branch-pipeline spellings are reserved.
- Branch-pipeline strings are parsed by the allowlisted cold-path parser; custom
  behavior is callable only through explicit `PipelineRegistry` entries chosen
  by the host application.

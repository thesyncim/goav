# Control plane

`goav ctl` speaks to a running task over a Unix socket:

```sh
goav ctl --control unix:///tmp/goav-live.sock control bitrate stream=video value=1200k
goav ctl --control unix:///tmp/goav-live.sock watch type=stats --follow
goav ctl --control unix:///tmp/goav-live.sock attach frames as archive \
  'encode codec=x_pcm_s16 media=audio bitrate=128k ! filesink location=archive.ogg format=ogg'
```

The host application owns the task and the socket. Use package `ctl` to expose
the safe command surface:

```go
err := ctl.ServeUnixWithOptions(ctx, task, "unix:///tmp/goav-live.sock")
```

The control layer is cold-path only. It is allowlisted, structured, and lowers
into the same task APIs normal Go code uses: `Task.Control`, `Task.Attach`,
`Attachment.Rebranch`, `Task.Detach`, `Snapshot`, `Stats`, `Watch`, and `Close`.
There is no global registry and no user-provided method name dispatch.

## Custom control verbs

Add one command struct, one handler, and one explicit server option:

```go
type ForceKeyframe struct {
    Stream av.StreamID `goavctl:"stream,required" usage:"stream=<stream-id>" help:"stream to refresh"`
}

command := ctl.CommandSpec{
    Name:     "vendor.force_keyframe",
    Summary:  "vendor keyframe request",
    ArgsType: reflect.TypeOf(ForceKeyframe{}),
    Apply: func(ctx context.Context, task goav.Task, args any) (ctl.ControlResponse, error) {
        cmd := args.(ForceKeyframe)
        if err := task.Control(ctx, goav.Keyframe(cmd.Stream)); err != nil {
            return ctl.ControlResponse{}, err
        }
        return ctl.ControlResponse{
            Operation: "control vendor.force_keyframe",
            Result:    map[string]any{"stream": cmd.Stream},
        }, nil
    },
}

err := ctl.ServeUnixWithOptions(ctx, task, socket, ctl.WithCommands(command))
```

Then operators can call it with the same protocol as built-ins:

```sh
goav ctl --control unix:///tmp/goav-live.sock control vendor.force_keyframe stream=video
goav ctl --control unix:///tmp/goav-live.sock help control vendor.force_keyframe
```

## Custom codecs

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

## Custom encoder options

If an encoder exposes native settings beyond the common codec options, expose a
named encoder step. The step receives every `key=value` token and returns the
real `codec.CodecSpec` the runtime already understands:

```go
registry := ctl.PipelineRegistry{
    Encoders: []ctl.EncoderSpec{{
        Name: "acmeenc",
        Apply: func(args ctl.StepArgs) (codec.CodecSpec, error) {
            return codec.Codec(av.CodecID("x_acme_audio"), av.MediaAudio,
                codec.Profile(args["profile"]),
                codec.Control(func(native any) error {
                    opts := native.(*acme.Options)
                    opts.Lookahead = args["lookahead"]
                    return nil
                }),
            ), nil
        },
    }},
}

err := ctl.ServeUnixWithOptions(ctx, task, socket, ctl.WithPipelineRegistry(registry))
```

```sh
goav ctl --control unix:///tmp/goav-live.sock attach frames as archive \
  'acmeenc profile=cinema lookahead=deep ! filesink location=archive.ogg format=ogg'
```

## Custom branch components

Custom branch steps can add external stages:

```go
registry := ctl.PipelineRegistry{
    Steps: []ctl.BranchPipelineStepSpec{{
        Name: "meter",
        Apply: func(branch *ctl.BranchPipeline, args ctl.StepArgs) error {
            branch.Do(goav.FrameFunc("meter", func(ctx context.Context, frame *av.Frame, emit goav.Emit) error {
                recordLevel(frame)
                return emit.Frame(frame)
            }))
            return nil
        },
    }},
}
```

```sh
goav ctl --control unix:///tmp/goav-live.sock attach frames as monitored \
  'meter ! encode codec=x_pcm_s16 media=audio ! filesink location=monitored.ogg format=ogg'
```

Unknown commands, fields, taps, branches, nodes, and pipeline steps return
structured errors with available names and suggestions.

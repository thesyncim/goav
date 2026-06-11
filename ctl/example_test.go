package ctl_test

import (
	"context"
	"fmt"
	"reflect"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/ctl"
)

func ExampleServeUnixWithOptions_customCommand() {
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

	var task goav.Task
	_ = ctl.ServeUnixWithOptions(context.Background(), task, "unix:///tmp/goav-live.sock", ctl.WithCommands(command))
}

func ExampleWithPipelineRegistry_customEncoder() {
	const customCodec = av.CodecID("x_acme_audio")
	registry := ctl.PipelineRegistry{
		Encoders: []ctl.EncoderSpec{{
			Name: "acmeenc",
			Apply: func(args ctl.StepArgs) (codec.CodecSpec, error) {
				return codec.Codec(customCodec, av.MediaAudio,
					codec.Profile(args["profile"]),
					codec.Control(func(native any) error {
						// Type-assert native to the concrete encoder/options type
						// documented by the adapter package and apply every knob
						// that is not part of the common codec settings.
						return nil
					}),
				), nil
			},
		}},
	}
	_ = ctl.WithPipelineRegistry(registry)

	request, _ := ctl.RequestFromCLI([]string{
		"attach", "frames", "as", "archive",
		"acmeenc profile=cinema ! filesink location=archive.ogg format=ogg",
	})
	fmt.Println(request.Pipeline)

	// Output:
	// acmeenc profile=cinema ! filesink location=archive.ogg format=ogg
}

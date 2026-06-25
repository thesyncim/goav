package goav_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/provider"
	goavruntime "github.com/thesyncim/goav/runtime"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

func exampleFrame(sample int16) av.Frame {
	payload := make([]byte, 2)
	binary.LittleEndian.PutUint16(payload, uint16(sample))
	return av.Frame{
		Type:  av.MediaAudio,
		Audio: &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 1},
		Planes: []av.Plane{{
			Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
		}},
	}
}

func ExampleSource_pushAccounting() {
	ctx := context.Background()
	accepted, dropped := 0, 0
	input := goav.Source("mic",
		shape.Frame(av.MediaAudio, shape.Audio(48000, 1, av.SampleFormatS16)),
		func(_ context.Context, push source.Push) error {
			for _, sample := range []int16{1, 2} {
				frame := exampleFrame(sample)
				result, err := push.Frame(&frame)
				if err != nil {
					return err
				}
				if result.Accepted {
					accepted++
				}
				if result.Dropped {
					dropped++
				}
			}
			return push.EOS()
		})

	out := goavtest.NewCollector()
	err := goav.From(input).
		Audio().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)

	fmt.Println("err:", err)
	fmt.Println("pushes:", accepted, "accepted,", dropped, "dropped")
	fmt.Println("frames:", out.S16())

	// Output:
	// err: <nil>
	// pushes: 2 accepted, 0 dropped
	// frames: [[1] [2]]
}

type exampleUpload struct {
	bytes.Buffer
	commits int
	aborts  int
	closes  int
}

func (u *exampleUpload) Close() error {
	u.closes++
	return nil
}

func (u *exampleUpload) Commit(context.Context) error {
	u.commits++
	return nil
}

func (u *exampleUpload) Abort(context.Context) error {
	u.aborts++
	return nil
}

func ExampleWriter_transactionalUpload() {
	ctx := context.Background()
	upload := &exampleUpload{}
	var opened provider.Info

	target := goav.Writer("mem://voice.ogg",
		func(_ context.Context, info provider.Info) (io.WriteCloser, error) {
			opened = info
			return upload, nil
		},
		goav.Format(av.FormatOgg),
		goav.MIME("audio/ogg"),
		goav.Metadata(av.Metadata{"kind": "call-recording"}),
	)

	err := goav.From(goavtest.Audio(48000, 1, []int16{7, 8})).
		Audio().
		Encode(codec.Opus()).
		To(target).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)

	fmt.Println("err:", err)
	fmt.Println("format:", opened.Format, "streams:", len(opened.Streams), "metadata:", opened.Metadata["kind"])
	fmt.Println("commit:", upload.commits, "abort:", upload.aborts, "close:", upload.closes, "bytes:", upload.Len() > 0)

	// Output:
	// err: <nil>
	// format: ogg streams: 1 metadata: call-recording
	// commit: 1 abort: 0 close: 1 bytes: true
}

const exampleCodecID = av.CodecID("x_example_audio")

type exampleNativeEncoder struct {
	Lookahead string
}

type exampleEncoderFactory struct {
	desc   codec.Descriptor
	config codec.EncodeConfig
	native exampleNativeEncoder
}

func (f *exampleEncoderFactory) NewEncoder(_ context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	f.config = config
	if config.Settings.Control != nil {
		if err := config.Settings.Control(&f.native); err != nil {
			return nil, err
		}
	}
	return exampleEncoder{desc: f.desc}, nil
}

type exampleEncoder struct {
	desc codec.Descriptor
}

func (e exampleEncoder) Descriptor() codec.Descriptor { return e.desc }

func (e exampleEncoder) Open(context.Context, codec.EncodeConfig) error { return nil }

func (e exampleEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if frame == nil {
		return nil
	}
	if len(out.Packets) == cap(out.Packets) {
		return codec.ErrResultFull
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	out.Packets[index] = av.Packet{
		StreamID: frame.StreamID,
		Type:     frame.Type,
		Keyframe: true,
		Payload:  av.Buffer{Bytes: []byte{0x42}, Ownership: av.BufferImmutable},
	}
	return nil
}

func (e exampleEncoder) FlushInto(context.Context, *codec.EncodeResult) error { return nil }

func (e exampleEncoder) HandleEvent(context.Context, *av.Event) error { return nil }

func (e exampleEncoder) Close() error { return nil }

func exampleEncoderDescriptor() codec.Descriptor {
	return codec.Descriptor{
		ID:       exampleCodecID,
		Name:     "example audio encoder",
		Type:     av.MediaAudio,
		Modes:    []codec.Mode{codec.ModeEncode},
		Realtime: true,
		Capabilities: codec.Capabilities{
			SampleFormats: []string{av.SampleFormatS16},
		},
		Backend: codec.Backend{
			Name:    "example",
			Module:  "example.test/media",
			Package: "exampleenc",
			Status:  "test fixture",
		},
	}
}

func ExampleWithEncoder_customSettings() {
	ctx := context.Background()
	factory := &exampleEncoderFactory{desc: exampleEncoderDescriptor()}
	out := goavtest.NewCollector()

	err := goav.From(goavtest.Audio(48000, 1, []int16{1, 2})).
		Audio().
		Encode(codec.Codec(exampleCodecID, av.MediaAudio,
			codec.Bitrate(64_000),
			codec.Profile("speech"),
			codec.Control(func(native any) error {
				native.(*exampleNativeEncoder).Lookahead = "deep"
				return nil
			}),
		)).
		To(out.Sink()).
		UseRuntime(goavtest.Runtime(goavruntime.WithEncoder(factory.desc, factory))).
		Run(ctx)

	fmt.Println("err:", err)
	fmt.Println("bitrate:", factory.config.Settings.Bitrate)
	fmt.Println("profile:", factory.config.Settings.Profile)
	fmt.Println("native lookahead:", factory.native.Lookahead)
	fmt.Println("packets:", len(out.Packets()))

	// Output:
	// err: <nil>
	// bitrate: 64000
	// profile: speech
	// native lookahead: deep
	// packets: 1
}

func ExampleTask_flowchart() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	main := goavtest.NewCollector()
	task, err := goav.From(goavtest.LiveAudio("mic", 48000, 1, []int16{1})).
		Audio().
		Tap(goav.FrameTap("frames")).
		To(main.Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	if err := main.Wait(ctx, func(c *goavtest.Collector) bool { return len(c.Frames()) > 0 }); err != nil {
		fmt.Println(err)
		_ = task.Close()
		return
	}
	preview := goavtest.NewCollector()
	if _, err := task.Attach(ctx, goav.Branch("preview").From(goav.FrameTap("frames")).To(preview.Sink())); err != nil {
		fmt.Println(err)
		_ = task.Close()
		return
	}

	flowchart, err := graphrender.RenderTaskFlowchart(task)
	if err != nil {
		fmt.Println(err)
		_ = task.Close()
		return
	}
	fmt.Println(strings.HasPrefix(flowchart, "flowchart LR"))
	fmt.Println(strings.Contains(flowchart, "branch=preview (attached)"))

	_ = task.Close()
	select {
	case <-runErr:
	case <-time.After(time.Second):
		fmt.Println("run stopped:", false)
	}

	// Output:
	// true
	// true
}

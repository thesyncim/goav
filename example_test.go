package goav_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/inspect"
	"github.com/thesyncim/goav/shape"
)

// pcmSource returns a custom frame-domain input that pushes one S16 PCM frame
// per sample slice, then ends the stream.
func pcmSource(name string, sampleRate int, channels int, frames ...[]int16) goav.InputSpec {
	return pcmSourceAt(name, sampleRate, channels, nil, frames...)
}

// pcmSourceAt is pcmSource with an optional PTS stamp (in milliseconds, 20ms
// frames) per pushed frame, for the PTS-aligned join examples.
func pcmSourceAt(name string, sampleRate int, channels int, ptsMS []int64, frames ...[]int16) goav.InputSpec {
	return goav.Source(name,
		shape.Frame(av.MediaAudio, shape.Audio(sampleRate, channels, av.SampleFormatS16), shape.Stream(av.StreamID(name))),
		func(_ context.Context, push goav.SourcePush) error {
			for i, samples := range frames {
				pcm := make([]byte, len(samples)*2)
				for j, sample := range samples {
					binary.LittleEndian.PutUint16(pcm[j*2:], uint16(sample))
				}
				frame := av.Frame{
					StreamID: av.StreamID(name),
					Type:     av.MediaAudio,
					Audio:    &av.AudioFrame{SampleRate: sampleRate, Channels: channels, SampleFormat: av.SampleFormatS16, Samples: len(samples) / channels},
					Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: pcm, Ownership: av.BufferImmutable}}},
				}
				if i < len(ptsMS) {
					frame.PTS = av.Timestamp{Value: ptsMS[i], Base: av.TimeBase{Num: 1, Den: 1000}}
					frame.Duration = av.Duration{Value: 20, Base: av.TimeBase{Num: 1, Den: 1000}}
				}
				if _, err := push.Frame(&frame); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// i420Source returns a custom frame-domain input that pushes one solid-color
// I420 video frame (every luma sample = luma), then ends the stream.
func i420Source(name string, width int, height int, luma byte) goav.InputSpec {
	return goav.Source(name,
		shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420), shape.Stream(av.StreamID(name))),
		func(_ context.Context, push goav.SourcePush) error {
			chromaW, chromaH := (width+1)/2, (height+1)/2
			yPlane := bytes.Repeat([]byte{luma}, width*height)
			uPlane := make([]byte, chromaW*chromaH)
			vPlane := make([]byte, chromaW*chromaH)
			frame := av.Frame{
				StreamID: av.StreamID(name),
				Type:     av.MediaVideo,
				Video:    &av.VideoFrame{Width: width, Height: height, PixelFormat: av.PixelFormatI420},
				Planes: []av.Plane{
					{Buffer: av.Buffer{Bytes: yPlane, Ownership: av.BufferImmutable}, Stride: width},
					{Buffer: av.Buffer{Bytes: uPlane, Ownership: av.BufferImmutable}, Stride: chromaW},
					{Buffer: av.Buffer{Bytes: vPlane, Ownership: av.BufferImmutable}, Stride: chromaW},
				},
			}
			if _, err := push.Frame(&frame); err != nil {
				return err
			}
			return push.EOS()
		})
}

// pcmSamples reads the S16 samples back out of a delivered audio frame.
func pcmSamples(frame *av.Frame) []int16 {
	data := frame.Planes[0].Buffer.Bytes
	samples := make([]int16, len(data)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(data[i*2:]))
	}
	return samples
}

// opusTestPacket returns a self-delimiting CELT packet the Opus decoder
// accepts, standing in for media a capture pipeline would produce.
func opusTestPacket() []byte {
	data := make([]byte, 50)
	data[0] = 0xf8
	for i := 1; i < len(data); i++ {
		data[i] = byte(i * 7)
	}
	return data
}

// ExampleFrom is the direct chain: a custom source feeding decode, encode, and
// a typed destination — the front door of the recipe grammar.
func ExampleFrom() {
	ctx := context.Background()

	// A custom source pushes encoded packets the application already owns.
	mic := goav.Source("mic",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, 1, av.SampleFormatS16)),
		func(_ context.Context, push goav.SourcePush) error {
			packet := av.Packet{StreamID: "mic", Payload: av.Buffer{Bytes: opusTestPacket(), Ownership: av.BufferImmutable}}
			if _, err := push.Packet(&packet); err != nil {
				return err
			}
			return push.EOS()
		})

	// Decode to PCM, re-encode at 96 kbps, and count the packets at a sink.
	encoded := 0
	err := goav.From(mic).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(goav.Sink(goav.SinkFunc("count", func(_ context.Context, msg goav.Message) error {
			if msg.Packet != nil {
				encoded++
			}
			return nil
		}))).
		UseRuntime(bundle.MustNew()).
		Run(ctx)

	fmt.Println(err, encoded)
	// Output: <nil> 1
}

// ExampleFrom_multipleInputs feeds two live inputs into one job: each chain
// narrows to its input with InputName, and reusing one destination value muxes
// both encoded streams into one shared container.
func ExampleFrom_multipleInputs() {
	camera := i420Source("camera", 16, 16, 100)
	mic := pcmSource("mic", 48_000, 1, make([]int16, 960))
	out := goav.File("call.webm", io.Discard)

	job := goav.From(camera, mic).
		Video(goav.InputName("camera")).Encode(codec.VP8(codec.Bitrate(1_000_000))).To(out).
		Audio(goav.InputName("mic")).Encode(codec.Opus(codec.Bitrate(96_000))).To(out)

	spec, err := job.Describe()
	if err != nil {
		fmt.Println(err)
		return
	}
	streams := 0
	for _, edge := range spec.Edges {
		if edge.To == "call.webm" {
			streams++
		}
	}
	fmt.Printf("%d encoded streams mux into call.webm\n", streams)
	// Output: 2 encoded streams mux into call.webm
}

// ExampleFrom_autoResample opts the chain into the shape solver: the mic is
// 44.1kHz but Opus pins 48kHz, so under the explicit AllowResample policy the
// planner inserts the conversion as a real planned operation and Explain
// reports it.
func ExampleFrom_autoResample() {
	mic := pcmSource("mic", 44_100, 2, make([]int16, 882*2))

	report, err := goav.From(mic).
		Audio().
		Auto(shape.AllowResample()).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(goav.File("voice.webm", io.Discard)).
		UseRuntime(bundle.MustNew()).
		Explain(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, warning := range report.Warnings {
		fmt.Println(warning.Code, warning.Message)
	}
	// Output: shape_conversion_inserted inserted resample 44.1kHz→48kHz before encode-opus (AllowResample)
}

// ExampleMix converges two audio sources into one stream: the dual of
// Branches. The summed output is a normal stream point that takes Encode, To,
// Tap, and Branches like any chain.
func ExampleMix() {
	var mixed [][]int16
	speakers := goav.Sink(goav.SinkFunc("speakers", func(_ context.Context, msg goav.Message) error {
		if msg.Frame != nil {
			mixed = append(mixed, pcmSamples(msg.Frame))
		}
		return nil
	}))

	err := goav.Mix(
		goav.From(pcmSource("mic-1", 48_000, 1, []int16{100, 200})).Audio(),
		goav.From(pcmSource("mic-2", 48_000, 1, []int16{50, -50})).Audio(),
	).To(speakers).Run(context.Background())

	fmt.Println(err, mixed)
	// Output: <nil> [[150 150]]
}

// ExampleMix_syncByPTS aligns join arms by timestamp instead of arrival order:
// arm two starts one 20ms frame later, so the leading step plays arm one
// alone, the PTS-matched frames sum, and the trailing step plays arm two
// alone.
func ExampleMix_syncByPTS() {
	var mixed [][]int16
	var pts []int64
	out := goav.Sink(goav.SinkFunc("out", func(_ context.Context, msg goav.Message) error {
		if msg.Frame != nil {
			mixed = append(mixed, pcmSamples(msg.Frame))
			pts = append(pts, msg.Frame.PTS.Value)
		}
		return nil
	}))

	err := goav.Mix(
		goav.From(pcmSourceAt("song-a", 48_000, 1, []int64{0, 20}, []int16{10, 10}, []int16{20, 20})).Audio(),
		goav.From(pcmSourceAt("song-b", 48_000, 1, []int64{20, 40}, []int16{1, 1}, []int16{2, 2})).Audio(),
	).SyncByPTS().To(out).Run(context.Background())

	fmt.Println(err, mixed, pts)
	// Output: <nil> [[10 10] [21 21] [2 2]] [0 20 40]
}

// ExampleComposite paints two video arms onto one canvas at their Region
// offsets: a 4x4 camera at the origin and a second 4x4 camera 4 pixels right
// produce one 8x4 frame.
func ExampleComposite() {
	var canvasW, canvasH int
	var leftLuma, rightLuma byte
	preview := goav.Sink(goav.SinkFunc("preview", func(_ context.Context, msg goav.Message) error {
		if msg.Frame != nil && msg.Frame.Video != nil && len(msg.Frame.Planes) != 0 {
			canvasW = msg.Frame.Video.Width
			canvasH = msg.Frame.Video.Height
			luma := msg.Frame.Planes[0].Buffer.Bytes
			leftLuma = luma[0]
			rightLuma = luma[4]
		}
		return nil
	}))

	err := goav.Composite(
		goav.From(i420Source("cam", 4, 4, 100)).Video().Region(0, 0),
		goav.From(i420Source("screen", 4, 4, 200)).Video().Region(4, 0),
	).To(preview).Run(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("canvas %dx%d\n", canvasW, canvasH)
	fmt.Println("left luma:", leftLuma, "right luma:", rightLuma)
	// Output:
	// canvas 8x4
	// left luma: 100 right luma: 200
}

// ExampleSelect forwards exactly one live arm and switches arms through the
// control plane — no node names, no rebuild.
func ExampleSelect() {
	ctx := context.Background()
	preview := goav.Sink(goav.SinkFunc("preview", func(context.Context, goav.Message) error {
		return nil
	}))

	task, err := goav.Select(
		goav.From(i420Source("cam-1", 16, 16, 100)).Video(),
		goav.From(i420Source("cam-2", 16, 16, 200)).Video(),
	).To(preview).Build(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer task.Close()

	go func() { _ = task.Run(ctx) }()

	// The first arm is live by default; switch to the second mid-run.
	if err := task.Control(ctx, control.SelectActive("cam-2")); err != nil {
		fmt.Println(err)
	}
}

// ExampleTask_attach adds a runtime branch to a built task: the branch anchors
// on a typed tap and receives the same frames as the planned chain, without
// touching the source or its siblings.
func ExampleTask_attach() {
	ctx := context.Background()
	mainFrames, monitorFrames := 0, 0
	countInto := func(counter *int) goav.Destination {
		return goav.Sink(goav.SinkFunc("count", func(_ context.Context, msg goav.Message) error {
			if msg.Frame != nil {
				*counter++
			}
			return nil
		}))
	}

	task, err := goav.From(pcmSource("mic", 48_000, 1, []int16{1, 2})).
		Audio().
		Tap(goav.FrameTap("mic.frames")).
		To(countInto(&mainFrames)).
		Build(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer task.Close()

	if _, err := task.Attach(ctx, goav.Branch("monitor").
		From(goav.FrameTap("mic.frames")).
		To(countInto(&monitorFrames))); err != nil {
		fmt.Println(err)
		return
	}

	if err := task.Run(ctx); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("main:", mainFrames, "monitor:", monitorFrames)
	// Output: main: 1 monitor: 1
}

// ExampleAttachment_rebranch rotates a live recording without a delivery gap:
// the replacement attaches and starts receiving before the old branch
// detaches, gated to the next keyframe so the new file starts at a decodable
// sync point.
func ExampleAttachment_rebranch() {
	ctx := context.Background()
	var part1, part2 bytes.Buffer

	task, err := goav.From(goav.FileInput("input.ivf", bytes.NewReader(tinyIVF()))).
		Video().
		Copy().
		Tap(goav.PacketTap("video.encoded")).
		To(goav.File("live.ivf", io.Discard)).
		Build(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer task.Close()

	rec, err := task.Attach(ctx, goav.Branch("rec").
		From(goav.PacketTap("video.encoded")).
		Copy().
		To(goav.File("part-001.ivf", &part1)))
	if err != nil {
		fmt.Println(err)
		return
	}

	go func() { _ = task.Run(ctx) }()

	// Rotate to the next file at a clean decode point; the replaced branch
	// drains so part-001 commits complete.
	if _, err := rec.Rebranch(ctx,
		goav.Branch("rec").From(goav.PacketTap("video.encoded")).Copy().To(goav.File("part-002.ivf", &part2)),
		goav.SwitchAt(goav.NextKeyframe()),
		goav.DrainOldBranch(),
	); err != nil {
		fmt.Println(err)
	}
}

// ExampleTask_control drives a running task without naming graph nodes:
// untargeted controls enter at the source boundary and ride the data path to
// every relevant node.
func ExampleTask_control() {
	ctx := context.Background()

	task, err := goav.From(goav.FileInput("input.ivf", bytes.NewReader(tinyIVF()))).
		Video().
		Decode().
		Encode(codec.VP8(codec.Bitrate(1_000_000))).
		To(goav.File("out.ivf", io.Discard)).
		Build(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer task.Close()

	go func() { _ = task.Run(ctx) }()

	for _, control := range []control.Control{
		control.Keyframe("video"),            // every live encoder for the stream produces a keyframe
		control.SetBitrate("video", 900_000), // live encoders retarget mid-stream
		control.Seek(30 * time.Second),       // sources reposition to a media position
	} {
		if err := task.Control(ctx, control); err != nil {
			fmt.Println(err)
		}
	}
}

// ExampleTask_watch subscribes one consumer to a filtered slice of the task's
// event stream: each watcher owns a buffered channel that closes when the task
// closes, and a slow watcher sheds events for itself only.
func ExampleTask_watch() {
	ctx := context.Background()
	task, err := goav.From(pcmSource("mic", 48_000, 1, []int16{1, 2})).
		Audio().
		To(goav.Sink(goav.SinkFunc("out", func(context.Context, goav.Message) error { return nil }))).
		Build(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	eos := task.Watch(inspect.WatchTypes(av.EventEndOfStream)).Events()

	if err := task.Run(ctx); err != nil {
		fmt.Println(err)
		return
	}
	if err := task.Close(); err != nil {
		fmt.Println(err)
		return
	}

	ended := false
	for event := range eos {
		ended = ended || event.Type == av.EventEndOfStream
	}
	fmt.Println("end of stream:", ended)
	// Output: end of stream: true
}

// ExampleBuildError matches a structured refusal: every failed build carries a
// typed code from the codes catalog, the failing operation, and concrete
// fixes, and wraps a sentinel for errors.Is.
func ExampleBuildError() {
	// Decoded frames cannot feed a muxed file without an encoder: the build
	// refuses before any resource opens.
	_, err := goav.From(goav.FileInput("input.ivf", bytes.NewReader(tinyIVF()))).
		Video().
		Decode().
		To(goav.File("frames.ivf", io.Discard)).
		Build(context.Background())

	var buildErr *goav.BuildError
	if errors.As(err, &buildErr) {
		fmt.Println("code:", buildErr.Code)
		fmt.Println("catalog match:", buildErr.Code == errcode.EncodeMissing)
		fmt.Println("has fixes:", len(buildErr.Suggestions) > 0)
	}
	fmt.Println("build refusal:", errors.Is(err, goav.ErrUnsupportedBuild))
	// Output:
	// code: encode_missing
	// catalog match: true
	// has fixes: true
	// build refusal: true
}

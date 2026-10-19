package goav_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/inspect"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

var errTenth = errors.New("destination failed on its 10th message")

// failOnTenth counts calls and fails the 10th and every later one.
type failOnTenth struct{ n atomic.Int64 }

func (f *failOnTenth) next() error {
	if f.n.Add(1) >= 10 {
		return errTenth
	}
	return nil
}

// tenthSink is a caller Sink that fails on its 10th message.
func tenthSink(name string) goav.Destination {
	var fail failOnTenth
	return goav.Sink(component.SinkFunc(name, func(context.Context, component.Message) error {
		return fail.next()
	}))
}

// tenthWriter is a muxed output's io.Writer that fails on its 10th write, the
// shape of a disk-full or broken-pipe mux write.
type tenthWriter struct{ fail failOnTenth }

func (w *tenthWriter) Write(p []byte) (int, error) {
	if err := w.fail.next(); err != nil {
		return 0, err
	}
	return len(p), nil
}

// TestLiveTaskFailsFastOnDestinationError pins the runners' shared error
// contract on live sources, which never end on their own: the first
// destination failure stops the task within a second, Run returns that error
// (not the context.Canceled it causes), and every destination aborts instead
// of committing. It covers a direct graph, a buffered graph, and a ladder the
// compiler promotes to buffered because two rungs encode from one source.
func TestLiveTaskFailsFastOnDestinationError(t *testing.T) {
	for _, tt := range []struct {
		name    string
		runtime func() *goav.Runtime
		outputs func() []goav.Output
	}{
		{"direct", func() *goav.Runtime { return goavtest.Runtime() }, func() []goav.Output {
			return []goav.Output{goavtest.LiveAudio("live", 48000, 1).Audio().To(tenthSink("sink"))}
		}},
		{"buffered", func() *goav.Runtime {
			return goavtest.Runtime(runconfig.WithBufferPolicy(pipeline.BufferPolicy{Capacity: 4, Drop: pipeline.DropBlock}))
		}, func() []goav.Output {
			return []goav.Output{goavtest.LiveAudio("live", 48000, 1).Audio().To(tenthSink("sink"))}
		}},
		{"ladder promoted to buffered", func() *goav.Runtime { return goavtest.Runtime() }, func() []goav.Output {
			video := endlessVideo("cam", 32, 24).Video()
			return []goav.Output{
				video.Encode(codec.VP8()).As("high").To(goav.Write("high.webm", &tenthWriter{})),
				video.Resize(16, 12).Encode(codec.VP8()).As("low").To(goav.Write("low.webm", &tenthWriter{})),
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			outs := tt.outputs()
			task, err := tt.runtime().Build(ctx, outs...)
			if err != nil {
				t.Fatal(err)
			}
			defer task.Close()
			events := task.Watch(inspect.WatchTypes(av.EventDestinationCommitted, av.EventDestinationAborted, av.EventDestinationCommitError)).Events()

			start := time.Now()
			runDone := make(chan error, 1)
			go func() { runDone <- task.Run(ctx) }()
			select {
			case err = <-runDone:
			case <-time.After(time.Second):
				t.Fatal("Run did not return within 1s of the destination failing")
			}
			if !errors.Is(err, errTenth) {
				t.Fatalf("Run = %v after %v, want the destination's error", err, time.Since(start))
			}
			if errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want the destination's error, not the cancellation it caused", err)
			}

			ended := 0
			for ended < len(outs) {
				select {
				case event := <-events:
					if event.Type == av.EventDestinationCommitted {
						t.Fatalf("destination %q committed after a failed run", event.Metadata[av.MetadataDestinationName])
					}
					ended++
				case <-time.After(time.Second):
					t.Fatalf("%d of %d destinations reported an abort", ended, len(outs))
				}
			}
		})
	}
}

// endlessVideo is an offline (not realtime) I420 source that never ends: it
// pushes frames until the task stops it, so only a fail-fast runner can end
// the run, and offline keeps the ladder's buffering down to the compiler's
// rung promotion alone.
func endlessVideo(name string, width, height int) goav.InputSpec {
	return goav.Source(name,
		shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420)),
		func(ctx context.Context, push source.Push) error {
			luma := make([]byte, width*height)
			chroma := make([]byte, (width+1)/2*((height+1)/2))
			base := av.TimeBase{Num: 1, Den: 30}
			for n := int64(0); ctx.Err() == nil; n++ {
				frame := &av.Frame{
					StreamID: av.StreamID(name),
					Type:     av.MediaVideo,
					PTS:      av.Timestamp{Value: n, Base: base},
					Duration: av.Duration{Value: 1, Base: base},
					Video:    &av.VideoFrame{Width: width, Height: height, PixelFormat: av.PixelFormatI420},
					Planes: []av.Plane{
						{Buffer: av.Buffer{Bytes: luma}, Stride: width},
						{Buffer: av.Buffer{Bytes: chroma}, Stride: (width + 1) / 2},
						{Buffer: av.Buffer{Bytes: chroma}, Stride: (width + 1) / 2},
					},
				}
				if _, err := push.Frame(frame); err != nil && !errors.Is(err, goav.ErrBackpressure) {
					return nil // swallow it, like a careless source: the runner must not need it
				}
			}
			return nil
		})
}

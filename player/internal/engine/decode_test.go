package engine

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/runconfig"
)

// testClip encodes a short interleaved testsrc clip (VP8 + Opus in WebM).
// The sources run live so the muxer interleaves them by time, as a real
// recording is.
func testClip(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	fs, err := launch.Dir(dir)
	if err != nil {
		t.Fatal(err)
	}
	outs, err := launch.Compile(`testsrc video size=160x120 duration=1500ms live ! encode vp8 keyframe_interval=15 ! file clip.webm
		testsrc audio duration=1500ms live ! encode opus ! file clip.webm`, launch.Files(fs))
	if err != nil {
		t.Fatal(err)
	}
	if err := bundle.Run(context.Background(), outs...); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "clip.webm")
}

// The sink receives a real decode on a task paced by the engine's device
// clock: video is presented against the heard audio, and the task's own
// Pause, Seek, and Rate controls drive playback.
func TestRealDecodePlaysInSync(t *testing.T) {
	if testing.Short() {
		t.Skip("plays a clip in real time")
	}
	path := testClip(t)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	e := New(100*time.Millisecond, nil)
	go RunNullDevice(ctx, e)

	rt := bundle.MustNew(runconfig.WithRealtime(true), runconfig.WithClock(e.Clock()))
	in := goav.FileInput(path, f)
	dest := goav.Sink(Sink{Engine: e})
	task, err := rt.Build(ctx,
		in.Video().Decode().To(dest),
		in.Audio().Decode().Resample(OutputRate, OutputChannels).To(dest))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	// The presenter: a 125 Hz display converting every frame it shows.
	var (
		mu       sync.Mutex
		lit      int
		lastPTS  time.Duration
		backward bool
	)
	presenterDone := make(chan struct{})
	go func() {
		defer close(presenterDone)
		rgba := make([]byte, 4*160*120)
		for ctx.Err() == nil {
			time.Sleep(8 * time.Millisecond)
			frame := e.NextVideo(e.Clock().Now())
			if frame == nil {
				continue
			}
			pts, _ := frame.PTS.ToDuration()
			err := ConvertRGBA(rgba, frame, MatrixFor(frame.Video.Height))
			frame.Release()
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			for i := 0; i < len(rgba); i += 4 {
				if rgba[i]|rgba[i+1]|rgba[i+2] != 0 {
					lit++
					break
				}
			}
			if pts < lastPTS {
				backward = true
			}
			lastPTS = pts
			mu.Unlock()
		}
	}()

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	time.Sleep(500 * time.Millisecond)
	if err := task.Pause(ctx); err != nil {
		t.Fatalf("task refused Pause: %v", err)
	}
	e.SetPaused(true)
	paused := e.Stats().Position
	time.Sleep(300 * time.Millisecond)
	if moved := e.Stats().Position - paused; moved > 5*time.Millisecond || moved < -5*time.Millisecond {
		t.Fatalf("position moved %v while paused", moved)
	}
	if err := task.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	e.SetPaused(false)
	time.Sleep(200 * time.Millisecond)

	before := e.Stats().Position
	e.Flush()
	if err := task.Control(ctx, control.Seek(100*time.Millisecond)); err != nil {
		t.Fatalf("task refused Seek: %v", err)
	}
	mu.Lock()
	lastPTS = 0
	mu.Unlock()
	time.Sleep(400 * time.Millisecond)
	if after := e.Stats().Position; after >= before {
		t.Fatalf("position %v after seeking back from %v", after, before)
	}

	rate, err := control.Rate(1.5)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Control(ctx, rate); err != nil {
		t.Fatalf("task refused Rate: %v", err)
	}
	e.SetRate(1.5)

	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	e.End()
	for !e.Drained() {
		if ctx.Err() != nil {
			t.Fatal("engine did not drain")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-presenterDone

	st := e.Stats()
	t.Logf("presented=%d dropped=%d underruns=%d avoffset=%v max=%v position=%v", st.Presented, st.Dropped, st.Underruns, st.AVOffset, st.MaxAVOffset, st.Position)
	mu.Lock()
	defer mu.Unlock()
	if st.Presented < 30 || lit != int(st.Presented) {
		t.Fatalf("presented %d frames, %d with a lit picture", st.Presented, lit)
	}
	if backward {
		t.Fatal("presentation went backwards outside a seek")
	}
	if st.MaxAVOffset > 50*time.Millisecond {
		t.Fatalf("max A/V offset %v, want <= 50ms", st.MaxAVOffset)
	}
	if st.Underruns > 1 {
		t.Fatalf("underruns = %d", st.Underruns)
	}
}

package player

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
)

type fakeTask struct {
	Task

	mu          sync.Mutex
	paused      bool
	pauseCalls  int
	resumeCalls int
	rate        float64
	seeks       []time.Duration
	onSeek      func(target time.Duration)
	onResume    func()
}

func (f *fakeTask) Pause(context.Context) error {
	f.mu.Lock()
	f.paused = true
	f.pauseCalls++
	f.mu.Unlock()
	return nil
}

func (f *fakeTask) Resume(context.Context) error {
	f.mu.Lock()
	f.paused = false
	f.resumeCalls++
	cb := f.onResume
	f.mu.Unlock()
	if cb != nil {
		cb()
	}
	return nil
}

func (f *fakeTask) Control(_ context.Context, ctrl control.Control) error {
	f.mu.Lock()
	switch ctrl.Type() {
	case control.SeekType:
		pos := ctrl.Position()
		f.seeks = append(f.seeks, pos)
		cb := f.onSeek
		f.mu.Unlock()
		if cb != nil {
			cb(pos)
		}
		return nil
	case control.RateType:
		f.rate = ctrl.Rate()
	}
	f.mu.Unlock()
	return nil
}

func TestGameWindowStateResult(t *testing.T) {
	w := New()
	g := &game{w: w, ctrlWake: make(chan struct{}, 1), rate: 3}
	if finished, err := g.result(); finished || err != nil {
		t.Fatalf("initial result = (%v, %v), want (false, nil)", finished, err)
	}
	wantErr := errors.New("boom")
	g.mu.Lock()
	g.state, g.taskErr = windowEnded, wantErr
	g.mu.Unlock()
	if finished, err := g.result(); !finished || !errors.Is(err, wantErr) {
		t.Fatalf("ended result = (%v, %v), want (true, %v)", finished, err, wantErr)
	}
	// stopControls runs in defer when runWindow returns; it must preserve the
	// ended result so Window.Run does not hang waiting on an already-drained done.
	g.stopControls()
	if finished, err := g.result(); !finished || !errors.Is(err, wantErr) {
		t.Fatalf("ended+closed result = (%v, %v), want (true, %v)", finished, err, wantErr)
	}
}

// Rapid bursts of pause, rate, and seek controls must coalesce cleanly without
// dropping the latest desired state.
func TestGameControlsCoalesceWithoutDropping(t *testing.T) {
	w := New()
	task := &fakeTask{}
	seekStarted := make(chan struct{})
	releaseSeek := make(chan struct{})
	var once sync.Once
	task.onSeek = func(target time.Duration) {
		once.Do(func() {
			close(seekStarted)
			<-releaseSeek
		})
		if target == 10*time.Second {
			// Deliver a picture asynchronously so paused Seek's WaitPicture returns immediately.
			go func() {
				_ = w.engine.PushVideo(context.Background(), &av.Frame{
					Type:  av.MediaVideo,
					PTS:   av.Timestamp{Value: target.Microseconds(), Base: av.TimeBase{Num: 1, Den: 1_000_000}},
					Video: &av.VideoFrame{Width: 2, Height: 2, PixelFormat: av.PixelFormatI420},
					Planes: []av.Plane{
						{Buffer: av.Buffer{Bytes: make([]byte, 4)}, Stride: 2},
						{Buffer: av.Buffer{Bytes: make([]byte, 1)}, Stride: 1},
						{Buffer: av.Buffer{Bytes: make([]byte, 1)}, Stride: 1},
					},
				})
			}()
		}
	}

	g := &game{
		ctx:      context.Background(),
		w:        w,
		task:     task,
		ctrlWake: make(chan struct{}, 1),
		rate:     3,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.runControls()
	}()
	defer func() {
		g.stopControls()
		<-done
	}()

	// Block the worker on an initial seek, then fire >16 controls (more than
	// the old channel capacity) while it is busy.
	g.seek(5 * time.Second)
	<-seekStarted

	for range 25 {
		g.togglePause() // odd count -> final paused == true
	}
	g.rate = len(rates) - 1 // 4x
	g.setRate()
	g.seek(5 * time.Second)
	g.seek(5 * time.Second)

	close(releaseSeek)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task.mu.Lock()
		ok := task.paused && task.rate == 4 && len(task.seeks) >= 2 && task.seeks[len(task.seeks)-1] == 10*time.Second
		task.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	t.Fatalf("controls did not converge: paused=%v rate=%v seeks=%v", task.paused, task.rate, task.seeks)
}

// Unpausing while a paused seek is in flight must leave both engine and task
// unpaused once the seek completes.
func TestGameConcurrentUnpauseDuringPausedSeek(t *testing.T) {
	w := New()
	task := &fakeTask{}
	resumedForSeek := make(chan struct{})
	var once sync.Once
	task.onResume = func() {
		once.Do(func() { close(resumedForSeek) })
	}

	g := &game{
		ctx:      context.Background(),
		w:        w,
		task:     task,
		ctrlWake: make(chan struct{}, 1),
		rate:     3,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.runControls()
	}()
	defer func() {
		g.stopControls()
		<-done
	}()

	// Pause, wait for task to pause, then start a paused seek.
	g.togglePause()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task.mu.Lock()
		p := task.paused
		task.mu.Unlock()
		if p {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	g.seek(5 * time.Second)
	// Wait until Engine.Seek calls task.Resume() and enters WaitPicture().
	<-resumedForSeek

	// User unpauses while the paused seek is still waiting for its preview picture.
	g.togglePause()
	// Deliver a picture so WaitPicture returns immediately, Seek re-pauses
	// task, and the worker then reapplies the unpause.
	frame := &av.Frame{
		Type:  av.MediaVideo,
		PTS:   av.Timestamp{Value: 5_000_000, Base: av.TimeBase{Num: 1, Den: 1_000_000}},
		Video: &av.VideoFrame{Width: 2, Height: 2, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: make([]byte, 4)}, Stride: 2},
			{Buffer: av.Buffer{Bytes: make([]byte, 1)}, Stride: 1},
			{Buffer: av.Buffer{Bytes: make([]byte, 1)}, Stride: 1},
		},
	}
	if err := w.engine.PushVideo(context.Background(), frame); err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task.mu.Lock()
		taskPaused := task.paused
		resumes := task.resumeCalls
		task.mu.Unlock()
		g.mu.Lock()
		pending := g.pauseDirty || g.seekPending
		gPaused := g.paused
		g.mu.Unlock()
		if !pending && !gPaused && !taskPaused && resumes >= 2 && !w.engine.Stats().Paused {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	t.Fatalf("paused state desynced after unpause during paused seek: task.paused=%v resumes=%d engine.paused=%v", task.paused, task.resumeCalls, w.engine.Stats().Paused)
}

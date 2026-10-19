package player

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/player/internal/engine"
)

// Window is a playback window and the speaker it plays through. Its
// Destination takes decoded video and audio; its Clock is the speaker's
// hardware clock, which the task should run on; Run shows the window.
type Window struct {
	engine  *engine.Engine
	latency time.Duration

	mu     sync.Mutex
	title  string
	width  int
	height int
	used   atomic.Bool
	ran    atomic.Bool
	dest   goav.Destination
}

// Option configures a Window.
type Option func(*Window)

// Title sets the window title prefix (default "goav").
func Title(title string) Option { return func(w *Window) { w.title = title } }

// Size sets the initial window size; without it the window takes the size of
// the first video frame.
func Size(width, height int) Option {
	return func(w *Window) { w.width, w.height = width, height }
}

// Latency sets how much audio is buffered before playback starts and after a
// seek (default 200ms): more absorbs decode hiccups, less starts sooner.
func Latency(latency time.Duration) Option {
	return func(w *Window) {
		w.latency = latency
	}
}

// New prepares a window. Nothing opens until Run: building the task first is
// free, and a program that never routes to the window costs nothing.
func New(opts ...Option) *Window {
	w := &Window{title: "goav"}
	for _, opt := range opts {
		if opt != nil {
			opt(w)
		}
	}
	w.engine = engine.New(w.latency, nil)
	w.dest = goav.Sink(engine.Sink{Engine: w.engine})
	return w
}

// Destination is the window as a goav destination: route decoded video and
// decoded audio to the same value and they play together. Audio of any rate
// and channel count is accepted (s16 or f32, interleaved); .Resample(48000, 2)
// before it gives the best quality.
func (w *Window) Destination() goav.Destination {
	w.used.Store(true)
	return w.dest
}

// Clock is the speaker's clock. Build the runtime with
// runconfig.WithClock(w.Clock()) and runconfig.WithRealtime(true) so the task
// timeline runs in the speaker's time: media is then delivered exactly as fast
// as it is heard, and the task's Pause, Rate, and Seek drive playback.
func (w *Window) Clock() av.Clock { return w.engine.Clock() }

// Used reports whether anything took the window's Destination (directly or
// through the play element), so a host knows whether to Run it.
func (w *Window) Used() bool { return w.used.Load() }

// playSettings are the play element's settings.
type playSettings struct {
	Title string      `launch:"title"`
	Size  launch.Size `launch:"size"`
}

// Element is the launch element "play [title=T] [size=WxH]": a destination
// that plays what reaches it in this window. Every use in a program is the
// same window, so video and audio statements ending in play play together.
func (w *Window) Element() launch.Element {
	return launch.Sink("play", "play decoded video and audio in a window", func(s playSettings) (goav.Destination, error) {
		w.mu.Lock()
		defer w.mu.Unlock()
		if s.Title != "" {
			w.title = s.Title
		}
		if s.Size.Width > 0 && s.Size.Height > 0 {
			w.width, w.height = s.Size.Width, s.Size.Height
		}
		return w.Destination(), nil
	})
}

// Stats is a point-in-time view of playback.
type Stats struct {
	// Position is the media time being heard (without audio: shown).
	Position time.Duration
	// Presented and Dropped count video frames shown and discarded late.
	Presented, Dropped uint64
	// Underruns counts times the audio queue ran dry mid-playback.
	Underruns uint64
	// AVOffset is the heard position minus the last shown frame's time
	// (positive: the picture was late); MaxAVOffset is the largest magnitude.
	AVOffset, MaxAVOffset time.Duration
}

// Stats reports playback counters.
func (w *Window) Stats() Stats {
	s := w.engine.Stats()
	return Stats{
		Position:    s.Position,
		Presented:   s.Presented,
		Dropped:     s.Dropped,
		Underruns:   s.Underruns,
		AVOffset:    s.AVOffset,
		MaxAVOffset: s.MaxAVOffset,
	}
}

// Run opens the speaker and the window, runs task, and returns when the media
// has played out, the window is closed, or ctx ends; closing the window closes
// the task. Space pauses, the arrows seek (←/→ 5s, ↓/↑ 30s), [ and ] change
// the rate, f toggles full screen, i the overlay, and q or Esc quits — all
// through the task's own Pause, Resume, and Control.
//
// Run must be called from the main goroutine (the windowing systems require
// it) and at most once per process (one audio device context per process).
func (w *Window) Run(ctx context.Context, task goav.LiveTask) error {
	if task == nil {
		return errors.New("player: Run needs a task")
	}
	if w.ran.Swap(true) {
		return errors.New("player: a window runs once")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var notice string
	if stop, err := openSpeaker(w.engine); err == nil {
		defer stop()
	} else {
		go engine.RunNullDevice(ctx, w.engine)
		notice = fmt.Sprintf("no audio device (%v): playing silently", err)
	}

	done := make(chan error, 1)
	go func() {
		err := task.Run(ctx)
		w.engine.End()
		done <- err
	}()
	w.mu.Lock()
	title, width, height := w.title, w.width, w.height
	w.mu.Unlock()
	g := newGame(w, task, title, width, height, notice, done)
	runErr := runWindow(g)
	cancel()
	closeErr := task.Close()
	finished, taskErr := g.result()
	if !finished {
		select {
		case taskErr = <-done:
		case <-time.After(5 * time.Second):
			taskErr = errors.New("player: task did not stop after the window closed")
		}
	}
	w.engine.Close()
	if errors.Is(taskErr, context.Canceled) {
		taskErr = nil
	}
	return errors.Join(runErr, taskErr, closeErr)
}

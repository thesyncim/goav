package player

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/hajimehoshi/ebiten/v2/ebitenutil"
	"github.com/hajimehoshi/ebiten/v2/inpututil"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/player/internal/engine"
)

var rates = []float64{0.25, 0.5, 0.75, 1, 1.25, 1.5, 2, 4}

// windowState is the lifecycle state of a playback window.
type windowState uint8

const (
	windowRunning windowState = iota
	windowEnded
	windowClosed
	windowEndedClosed
)

// game is the ebiten side of a Window: it presents the engine's video on
// the display refresh, draws the overlay, and turns keys into task controls.
type game struct {
	ctx   context.Context
	w     *Window
	task  Task
	title string
	sized bool
	done  <-chan error

	// ctrlWake wakes the control worker when pending control state changes.
	ctrlWake chan struct{}

	image  *ebiten.Image
	rgba   []byte
	vw, vh int

	rate    int
	overlay bool
	titleAt time.Time

	mu          sync.Mutex
	state       windowState
	taskErr     error
	status      string
	statusAt    time.Time
	paused      bool
	pauseDirty  bool
	wantRate    float64
	rateDirty   bool
	wantSeek    time.Duration
	seekPending bool
}

func newGame(ctx context.Context, w *Window, task Task, title string, width, height int, notice string, done <-chan error) *game {
	g := &game{ctx: ctx, w: w, task: task, title: title, done: done, ctrlWake: make(chan struct{}, 1), rate: 3}
	if width > 0 && height > 0 {
		ebiten.SetWindowSize(width, height)
		g.sized = true
	} else {
		ebiten.SetWindowSize(960, 540)
	}
	ebiten.SetWindowTitle(title)
	ebiten.SetWindowResizingMode(ebiten.WindowResizingModeEnabled)
	ebiten.SetTPS(ebiten.SyncWithFPS)
	if notice != "" {
		g.setStatus(notice)
	}
	go g.runControls()
	return g
}

func runWindow(g *game) error {
	defer g.stopControls()
	return ebiten.RunGameWithOptions(g, nil)
}

func (g *game) result() (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state == windowEnded || g.state == windowEndedClosed, g.taskErr
}

func (g *game) setStatus(s string) {
	g.mu.Lock()
	g.status, g.statusAt = s, time.Now()
	g.mu.Unlock()
}

func (g *game) wakeControls() {
	select {
	case g.ctrlWake <- struct{}{}:
	default:
	}
}

func (g *game) stopControls() {
	g.mu.Lock()
	if g.state == windowEnded || g.state == windowEndedClosed {
		g.state = windowEndedClosed
	} else {
		g.state = windowClosed
	}
	g.mu.Unlock()
	g.wakeControls()
}

// runControls applies pending pause, rate, and seek intents off the render
// loop, coalescing bursts so rapid input never drops or desyncs paused state.
func (g *game) runControls() {
	for range g.ctrlWake {
		for {
			g.mu.Lock()
			if g.state >= windowClosed {
				g.mu.Unlock()
				return
			}
			switch {
			case g.pauseDirty:
				paused := g.paused
				g.pauseDirty = false
				g.mu.Unlock()
				g.applyPause(paused)
			case g.rateDirty:
				rate := g.wantRate
				g.rateDirty = false
				g.mu.Unlock()
				g.applyRate(rate)
			case g.seekPending:
				target := g.wantSeek
				paused := g.paused
				g.seekPending = false
				g.mu.Unlock()
				g.applySeek(target, paused)
			default:
				g.mu.Unlock()
				goto wait
			}
		}
	wait:
	}
}

func (g *game) applyPause(paused bool) {
	op, verb := g.task.Resume, "resume"
	if paused {
		op, verb = g.task.Pause, "pause"
	}
	if err := op(g.ctx); err != nil {
		g.setStatus(fmt.Sprintf("%s held by backpressure: %v", verb, err))
		return
	}
	g.setStatus(verb)
}

func (g *game) applyRate(r float64) {
	ctrl, err := control.Rate(r)
	if err == nil {
		err = g.task.Control(g.ctx, ctrl)
	}
	// The engine consumes at the new rate either way: on a free-running
	// task that alone sets the pace through backpressure.
	g.w.engine.SetRate(r)
	if err != nil {
		g.setStatus(fmt.Sprintf("rate %gx (task: %v)", r, err))
		return
	}
	g.setStatus(fmt.Sprintf("rate %gx", r))
}

func (g *game) applySeek(target time.Duration, paused bool) {
	if err := g.w.engine.Seek(g.ctx, g.task, target, paused); err != nil {
		g.setStatus(fmt.Sprintf("seek: %v", err))
		return
	}
	g.setStatus("seek " + clock(target))
}

func (g *game) Update() error {
	var ended bool
	select {
	case err := <-g.done:
		g.mu.Lock()
		g.state, g.taskErr = windowEnded, err
		g.mu.Unlock()
		if err != nil {
			return ebiten.Termination
		}
		ended = true
	default:
		g.mu.Lock()
		ended = g.state == windowEnded
		g.mu.Unlock()
	}
	if ended && g.w.engine.Drained() {
		return ebiten.Termination
	}
	if ebiten.IsWindowBeingClosed() || inpututil.IsKeyJustPressed(ebiten.KeyQ) || inpututil.IsKeyJustPressed(ebiten.KeyEscape) {
		return ebiten.Termination
	}
	g.keys()
	if time.Since(g.titleAt) >= 250*time.Millisecond {
		g.titleAt = time.Now()
		s := g.w.Stats()
		g.mu.Lock()
		paused := g.paused
		g.mu.Unlock()
		state := ""
		if paused {
			state = "  paused"
		}
		ebiten.SetWindowTitle(fmt.Sprintf("%s  %s  %gx  dropped %d%s", g.title, clock(s.Position), rates[g.rate], s.Dropped, state))
	}
	return nil
}

func (g *game) keys() {
	switch {
	case inpututil.IsKeyJustPressed(ebiten.KeySpace):
		g.togglePause()
	case inpututil.IsKeyJustPressed(ebiten.KeyArrowLeft):
		g.seek(-5 * time.Second)
	case inpututil.IsKeyJustPressed(ebiten.KeyArrowRight):
		g.seek(5 * time.Second)
	case inpututil.IsKeyJustPressed(ebiten.KeyArrowDown):
		g.seek(-30 * time.Second)
	case inpututil.IsKeyJustPressed(ebiten.KeyArrowUp):
		g.seek(30 * time.Second)
	case inpututil.IsKeyJustPressed(ebiten.KeyBracketLeft) && g.rate > 0:
		g.rate--
		g.setRate()
	case inpututil.IsKeyJustPressed(ebiten.KeyBracketRight) && g.rate < len(rates)-1:
		g.rate++
		g.setRate()
	case inpututil.IsKeyJustPressed(ebiten.KeyF):
		ebiten.SetFullscreen(!ebiten.IsFullscreen())
	case inpututil.IsKeyJustPressed(ebiten.KeyI):
		g.overlay = !g.overlay
	}
}

func (g *game) togglePause() {
	g.mu.Lock()
	g.paused = !g.paused
	paused := g.paused
	g.pauseDirty = true
	g.mu.Unlock()
	// The engine pauses at once (the speaker goes silent, the picture
	// holds); the task timeline follows so the pacer stops reading. A
	// free-running task refuses, and backpressure stalls it instead.
	g.w.engine.SetPaused(paused)
	g.wakeControls()
}

func (g *game) seek(delta time.Duration) {
	pos := g.w.engine.Stats().Position
	g.mu.Lock()
	if g.seekPending {
		pos = g.wantSeek
	}
	g.wantSeek = max(pos+delta, 0)
	g.seekPending = true
	g.mu.Unlock()
	g.wakeControls()
}

func (g *game) setRate() {
	r := rates[g.rate]
	g.mu.Lock()
	g.wantRate = r
	g.rateDirty = true
	g.mu.Unlock()
	g.wakeControls()
}

func (g *game) Draw(screen *ebiten.Image) {
	if f := g.w.engine.NextVideo(g.w.engine.Clock().Now()); f != nil {
		w, h := f.Video.Width, f.Video.Height
		if w <= 0 || h <= 0 {
			f.Release()
			return
		}
		if g.image == nil || w != g.vw || h != g.vh {
			if g.image != nil {
				g.image.Deallocate()
			}
			g.image, g.vw, g.vh = ebiten.NewImage(w, h), w, h
			g.rgba = make([]byte, 4*w*h)
			if !g.sized {
				g.sized = true
				ebiten.SetWindowSize(min(w, 1280), min(w, 1280)*h/w)
			}
		}
		err := engine.ConvertRGBA(g.rgba, f, engine.MatrixFor(h))
		f.Release()
		if err != nil {
			g.setStatus(err.Error())
		} else {
			g.image.WritePixels(g.rgba)
		}
	}
	if g.image != nil {
		sw, sh := screen.Bounds().Dx(), screen.Bounds().Dy()
		scale := min(float64(sw)/float64(g.vw), float64(sh)/float64(g.vh))
		op := &ebiten.DrawImageOptions{Filter: ebiten.FilterLinear}
		op.GeoM.Scale(scale, scale)
		op.GeoM.Translate((float64(sw)-scale*float64(g.vw))/2, (float64(sh)-scale*float64(g.vh))/2)
		screen.DrawImage(g.image, op)
	}
	g.mu.Lock()
	status, age, paused := g.status, time.Since(g.statusAt), g.paused
	g.mu.Unlock()
	if g.overlay || paused || age < 2*time.Second {
		s := g.w.Stats()
		text := fmt.Sprintf("%s  %gx\nshown %d  dropped %d  underruns %d\nA/V %+.1fms (max %.1fms)",
			clock(s.Position), rates[g.rate], s.Presented, s.Dropped, s.Underruns,
			float64(s.AVOffset)/float64(time.Millisecond), float64(s.MaxAVOffset)/float64(time.Millisecond))
		if paused {
			text += "\npaused"
		}
		if status != "" && age < 4*time.Second {
			text += "\n" + status
		}
		ebitenutil.DebugPrintAt(screen, text, 8, 8)
	}
}

func (g *game) Layout(width, height int) (int, int) {
	scale := ebiten.Monitor().DeviceScaleFactor()
	return int(float64(width) * scale), int(float64(height) * scale)
}

// clock formats a media position as m:ss.d.
func clock(d time.Duration) string {
	d = max(d, 0)
	return fmt.Sprintf("%d:%04.1f", int(d/time.Minute), (d % time.Minute).Seconds())
}

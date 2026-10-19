package player

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/hajimehoshi/ebiten/v2/ebitenutil"
	"github.com/hajimehoshi/ebiten/v2/inpututil"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/player/internal/engine"
)

var rates = []float64{0.25, 0.5, 0.75, 1, 1.25, 1.5, 2, 4}

// game is the ebiten side of a Window: it presents the engine's video on
// the display refresh, draws the overlay, and turns keys into task controls.
type game struct {
	w     *Window
	task  goav.LiveTask
	title string
	sized bool
	done  <-chan error

	// controls run off the render loop, in order.
	controls chan func()

	image  *ebiten.Image
	rgba   []byte
	vw, vh int

	paused  bool
	rate    int
	overlay bool
	titleAt time.Time

	mu       sync.Mutex
	status   string
	statusAt time.Time
	finished bool
	taskErr  error
	ended    bool
}

func newGame(w *Window, task goav.LiveTask, title string, width, height int, notice string, done <-chan error) *game {
	g := &game{w: w, task: task, title: title, done: done, controls: make(chan func(), 16), rate: 3}
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
	go func() {
		for fn := range g.controls {
			fn()
		}
	}()
	return g
}

func runWindow(g *game) error {
	defer close(g.controls)
	return ebiten.RunGameWithOptions(g, nil)
}

func (g *game) result() (error, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.taskErr, g.finished
}

func (g *game) setStatus(s string) {
	g.mu.Lock()
	g.status, g.statusAt = s, time.Now()
	g.mu.Unlock()
}

// control queues fn behind earlier controls without blocking the frame.
func (g *game) control(fn func()) {
	select {
	case g.controls <- fn:
	default:
		g.setStatus("busy: control dropped")
	}
}

func (g *game) Update() error {
	select {
	case err := <-g.done:
		g.mu.Lock()
		g.finished, g.taskErr = true, err
		g.mu.Unlock()
		g.ended = true
		if err != nil {
			return ebiten.Termination
		}
	default:
	}
	if g.ended && g.w.engine.Drained() {
		return ebiten.Termination
	}
	if ebiten.IsWindowBeingClosed() || inpututil.IsKeyJustPressed(ebiten.KeyQ) || inpututil.IsKeyJustPressed(ebiten.KeyEscape) {
		return ebiten.Termination
	}
	g.keys()
	if time.Since(g.titleAt) >= 250*time.Millisecond {
		g.titleAt = time.Now()
		s := g.w.Stats()
		state := ""
		if g.paused {
			state = "  paused"
		}
		ebiten.SetWindowTitle(fmt.Sprintf("%s  %s  %gx  dropped %d%s", g.title, clock(s.Position), rates[g.rate], s.Dropped, state))
	}
	return nil
}

func (g *game) keys() {
	e, task := g.w.engine, g.task
	switch {
	case inpututil.IsKeyJustPressed(ebiten.KeySpace):
		g.paused = !g.paused
		paused := g.paused
		// The engine pauses at once (the speaker goes silent, the picture
		// holds); the task timeline follows so the pacer stops reading. A
		// free-running task refuses, and backpressure stalls it instead.
		e.SetPaused(paused)
		g.control(func() {
			op, verb := task.Resume, "resume"
			if paused {
				op, verb = task.Pause, "pause"
			}
			if err := op(context.Background()); err != nil {
				g.setStatus(fmt.Sprintf("%s held by backpressure: %v", verb, err))
				return
			}
			g.setStatus(verb)
		})
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

func (g *game) seek(delta time.Duration) {
	target := max(g.w.engine.Stats().Position+delta, 0)
	e, task := g.w.engine, g.task
	g.control(func() {
		// Flush first: it releases a sink blocked on stale media, so the
		// seek lands promptly; the new position re-prerolls.
		e.Flush()
		if err := task.Control(context.Background(), control.Seek(target)); err != nil {
			g.setStatus(fmt.Sprintf("seek: %v", err))
			return
		}
		g.setStatus("seek " + clock(target))
	})
}

func (g *game) setRate() {
	r := rates[g.rate]
	e, task := g.w.engine, g.task
	g.control(func() {
		ctrl, err := control.Rate(r)
		if err == nil {
			err = task.Control(context.Background(), ctrl)
		}
		// The engine consumes at the new rate either way: on a free-running
		// task that alone sets the pace through backpressure.
		e.SetRate(r)
		if err != nil {
			g.setStatus(fmt.Sprintf("rate %gx (task: %v)", r, err))
			return
		}
		g.setStatus(fmt.Sprintf("rate %gx", r))
	})
}

func (g *game) Draw(screen *ebiten.Image) {
	if f := g.w.engine.NextVideo(g.w.engine.Clock().Now()); f != nil {
		w, h := f.Video.Width, f.Video.Height
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
	status, age := g.status, time.Since(g.statusAt)
	g.mu.Unlock()
	if g.overlay || g.paused || age < 2*time.Second {
		s := g.w.Stats()
		text := fmt.Sprintf("%s  %gx\nshown %d  dropped %d  underruns %d\nA/V %+.1fms (max %.1fms)",
			clock(s.Position), rates[g.rate], s.Presented, s.Dropped, s.Underruns,
			float64(s.AVOffset)/float64(time.Millisecond), float64(s.MaxAVOffset)/float64(time.Millisecond))
		if g.paused {
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

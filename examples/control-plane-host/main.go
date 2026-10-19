// Command control-plane-host is an application that serves its running goav
// task on a control socket, with methods and pipeline elements of its own,
// for goav ctl, a dashboard, or a test to drive.
package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/ctlserver"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
)

func main() {
	address := flag.String("control", "unix:///tmp/goav-host.sock", "where to serve: unix:///path or tcp://host:port")
	dir := flag.String("files", os.TempDir(), "the directory attached programs may read and write")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("serving %s; try: goav ctl --control %s capabilities\n", *address, *address)
	if err := newHost().serve(ctx, *address, *dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// host is the application around the task: its player, and what the
// elements its clients attach have measured, kept, and opened.
type host struct {
	player *player

	mu     sync.Mutex
	levels map[string]float64 // meter name: mean luma of its latest frame
	kept   map[string]int     // sink name: frames or packets it kept
	opened []string           // the ACME encoders opened, with their settings
}

func newHost() *host {
	return &host{player: &player{}, levels: map[string]float64{}, kept: map[string]int{}}
}

// serve plays the player through a tap named live and serves the task at
// address until ctx ends or a client calls stop. Attached programs touch
// files only under dir.
func (h *host) serve(ctx context.Context, address, dir string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rt, err := bundle.New(runconfig.WithEncoder(acmeDescriptor, acmeFactory{h}))
	if err != nil {
		return err
	}
	display := component.SinkFunc("display", func(context.Context, component.Message) error { return nil })
	task, err := rt.Build(ctx, goav.Input(h.player).Video().Tap("live").To(goav.Sink(display)))
	if err != nil {
		return err
	}
	defer task.Close()
	files, err := launch.Dir(dir)
	if err != nil {
		return err
	}
	options := append([]ctlserver.Option{ctlserver.Files(files), ctlserver.Elements(h.elements()...)}, h.methods()...)
	server, err := ctlserver.New(task, options...)
	if err != nil {
		return err
	}
	ran := make(chan error, 1)
	go func() {
		ran <- task.Run(ctx)
		cancel() // the task stopped: stop serving
	}()
	if err := server.ListenAndServe(ctx, address); err != nil {
		return err
	}
	if err := <-ran; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, pipeline.ErrClosed) {
		return err
	}
	return nil
}

// cues are the positions the cue method seeks the player to.
var cues = map[string]time.Duration{"intro": 0, "chorus": 42 * time.Second, "outro": 3 * time.Minute}

type cueParams struct {
	Name   string        `json:"name" ctl:"required" help:"intro, chorus, or outro"`
	Offset time.Duration `json:"offset" help:"how far past the cue: 500ms"`
}

type controlsParams struct {
	Type string `json:"type" help:"only controls of this type: seek or segment"`
}

// received is one control the player received, as controls reports it.
type received struct {
	Type     av.EventType `json:"type"`
	Position string       `json:"position,omitempty"`
	End      string       `json:"end,omitempty"`
}

// report is what collected returns.
type report struct {
	Levels map[string]float64 `json:"levels"`
	Kept   map[string]int     `json:"kept"`
	Opened []string           `json:"opened"`
}

// methods are the host's own methods, typed by their params structs.
func (h *host) methods() []ctlserver.Option {
	return []ctlserver.Option{
		ctlserver.Method("cue", "seek the player to a named cue", func(ctx context.Context, task goav.LiveTask, p cueParams) (any, error) {
			at, ok := cues[p.Name]
			if !ok {
				return nil, fmt.Errorf("no cue %q; the cues are intro, chorus, and outro", p.Name)
			}
			at += p.Offset
			if err := task.Control(ctx, control.Seek(at).At(pipeline.NodeRef("player"))); err != nil {
				return nil, err
			}
			return map[string]string{"cue": p.Name, "position": at.String()}, nil
		}),
		ctlserver.Method("controls", "the controls the player has received", func(_ context.Context, _ goav.LiveTask, p controlsParams) (any, error) {
			out := []received{}
			for _, e := range h.player.received() {
				if p.Type != "" && string(e.Type) != p.Type {
					continue
				}
				r := received{Type: e.Type}
				if at, ok := e.Timestamp.ToDuration(); ok {
					r.Position = at.String()
				}
				if end, ok := av.EventSegmentEnd(&e); ok {
					r.End = end.String()
				}
				out = append(out, r)
			}
			return out, nil
		}),
		ctlserver.Method("collected", "what the meters measured, the sinks kept, and the ACME encoders opened", func(context.Context, goav.LiveTask, struct{}) (any, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return report{maps.Clone(h.levels), maps.Clone(h.kept), slices.Clone(h.opened)}, nil
		}),
	}
}

type meterArgs struct {
	Name string `launch:"name" help:"what collected calls it; meter by default"`
}

type thumbnailArgs struct {
	Every int    `launch:"every,required" help:"keep one frame in every N"`
	Name  string `launch:"name" help:"thumbnail by default"`
}

type memoryArgs struct {
	Name string `launch:"name" help:"memory by default"`
}

type acmeArgs struct {
	Bitrate   int    `launch:"bitrate,required" help:"bits per second: 250k"`
	Lookahead string `launch:"lookahead" help:"shallow (default) or deep, a native ACME setting"`
}

// elements are the host's own pipeline elements, for programs clients attach.
// Each is typed by its settings struct; a constructor's error refuses the
// program, pointing at the element.
func (h *host) elements() []launch.Element {
	return []launch.Element{
		launch.Stage("meter", "measure each frame's mean luma", func(a meterArgs) (pipeline.Stage, error) {
			name := cmp.Or(a.Name, "meter")
			return component.FrameFunc(name, func(_ context.Context, f *av.Frame, emit component.Emit) error {
				h.measure(name, f)
				return emit.Frame(f)
			}), nil
		}),
		launch.Sink("thumbnail", "keep one frame in every N as a thumbnail", func(a thumbnailArgs) (goav.Destination, error) {
			if a.Every < 1 {
				return goav.Destination{}, fmt.Errorf("every=%d keeps no frames; write every=1 or more", a.Every)
			}
			name, seen := cmp.Or(a.Name, "thumbnail"), 0
			return goav.Sink(component.SinkFunc(name, func(_ context.Context, m component.Message) error {
				if m.Frame != nil {
					if seen%a.Every == 0 {
						h.keep(name)
					}
					seen++
				}
				return nil
			})), nil
		}),
		launch.Sink("memory", "keep what arrives in the host's memory", func(a memoryArgs) (goav.Destination, error) {
			name := cmp.Or(a.Name, "memory")
			return goav.Sink(component.SinkFunc(name, func(_ context.Context, m component.Message) error {
				if m.Frame != nil || m.Packet != nil {
					h.keep(name)
				}
				return nil
			})), nil
		}),
		launch.Encoder("acme", "encode with the host's ACME codec", func(a acmeArgs) (codec.Spec, error) {
			return codec.Codec(acmeCodec, av.MediaVideo,
				codec.Bitrate(a.Bitrate),
				codec.Control(func(native *acmeOptions) error {
					native.Lookahead = cmp.Or(a.Lookahead, "shallow")
					return nil
				}),
			), nil
		}),
	}
}

func (h *host) measure(name string, f *av.Frame) {
	if len(f.Planes) == 0 || len(f.Planes[0].Buffer.Bytes) == 0 {
		return
	}
	luma, sum := f.Planes[0].Buffer.Bytes, 0
	for _, y := range luma {
		sum += int(y)
	}
	h.mu.Lock()
	h.levels[name] = float64(sum) / float64(len(luma))
	h.mu.Unlock()
}

func (h *host) keep(name string) {
	h.mu.Lock()
	h.kept[name]++
	h.mu.Unlock()
}

// player is the host's stand-in for a media deck: a fade from black to
// white, played live at 30 frames a second. It applies seek, and records
// every control it receives for the controls method.
type player struct {
	mu       sync.Mutex
	next     int  // the next frame to play
	jumped   bool // a seek moved next; downstream hears of it first
	controls []av.Event
}

const width, height = 320, 180

var frameRate = av.TimeBase{Num: 1, Den: 30}

func (p *player) SourceShape() shape.Spec {
	return shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420), shape.Stream("player"), shape.Realtime(true))
}

func (p *player) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	return p, []av.Stream{{ID: "player", Type: av.MediaVideo, TimeBase: frameRate,
		Codec: av.CodecParameters{Type: av.MediaVideo, Width: width, Height: height, PixelFormat: av.PixelFormatI420}}}, nil
}

func (p *player) Name() string { return "player" }
func (p *player) Close() error { return nil }

// Start plays a frame every 30th of a second until the task stops. Like a
// real deck, it drops a frame the graph cannot take now rather than fall
// behind.
func (p *player) Start(ctx context.Context, emitter pipeline.Emitter) error {
	tick := time.NewTicker(time.Second / 30)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		p.mu.Lock()
		n, jumped := p.next, p.jumped
		p.next, p.jumped = n+1, false
		p.mu.Unlock()
		msgs := []pipeline.Message{{Kind: pipeline.MessageFrame, Frame: fadeFrame(n)}}
		if jumped {
			msgs = slices.Insert(msgs, 0, pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity, StreamID: "player"}})
		}
		for _, msg := range msgs {
			switch err := emitter.Emit(ctx, &msg); {
			case errors.Is(err, pipeline.ErrClosed):
				return nil
			case err != nil && !errors.Is(err, pipeline.ErrBackpressure):
				return err
			}
		}
	}
}

// Control records a control and applies a seek.
func (p *player) Control(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Event == nil {
		return errors.New("player: a control is an event")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.controls = append(p.controls, *msg.Event)
	if at, ok := msg.Event.Timestamp.ToDuration(); ok && msg.Event.Type == av.EventSeek {
		p.next, p.jumped = int(at*30/time.Second), true
	}
	return nil
}

func (p *player) received() []av.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.controls)
}

// fadeFrame is frame n of the fade, which steps from black to white every
// 16 frames.
func fadeFrame(n int) *av.Frame {
	chroma := av.Buffer{Bytes: bytes.Repeat([]byte{128}, width*height/4)}
	return &av.Frame{
		StreamID: "player",
		Type:     av.MediaVideo,
		PTS:      av.Timestamp{Value: int64(n), Base: frameRate},
		Duration: av.Duration{Value: 1, Base: frameRate},
		Video:    &av.VideoFrame{Width: width, Height: height, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: bytes.Repeat([]byte{byte(n % 16 * 17)}, width*height)}, Stride: width},
			{Buffer: chroma, Stride: width / 2},
			{Buffer: chroma, Stride: width / 2},
		},
	}
}

// acmeCodec is the host's own video codec, which the acme element encodes.
const acmeCodec = av.CodecID("x_acme_video")

var acmeDescriptor = codec.Descriptor{ID: acmeCodec, Name: "ACME video", Type: av.MediaVideo}

// acmeOptions are the ACME library's native settings: codec.Control reaches
// them when an encoder opens.
type acmeOptions struct{ Lookahead string }

// acmeFactory opens ACME encoders; a real one wraps the vendor's library.
type acmeFactory struct{ h *host }

func (f acmeFactory) NewEncoder(_ context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	var native acmeOptions
	if config.Settings.Control != nil {
		if err := config.Settings.Control(&native); err != nil {
			return nil, err
		}
	}
	f.h.mu.Lock()
	f.h.opened = append(f.h.opened, fmt.Sprintf("acme bitrate=%d lookahead=%s", config.Settings.Bitrate, native.Lookahead))
	f.h.mu.Unlock()
	return acmeEncoder{}, nil
}

// acmeEncoder encodes each frame as a one-byte packet.
type acmeEncoder struct{}

func (acmeEncoder) Descriptor() codec.Descriptor                   { return acmeDescriptor }
func (acmeEncoder) Open(context.Context, codec.EncodeConfig) error { return nil }
func (acmeEncoder) EncodeInto(_ context.Context, f *av.Frame, out *codec.EncodeResult) error {
	if f == nil {
		return nil
	}
	if len(out.Packets) == cap(out.Packets) {
		return codec.ErrResultFull
	}
	out.Packets = out.Packets[:len(out.Packets)+1]
	p := &out.Packets[len(out.Packets)-1]
	p.Reset()
	p.StreamID, p.Type, p.PTS, p.Keyframe = f.StreamID, f.Type, f.PTS, true
	p.Payload = av.Buffer{Bytes: []byte{1}}
	return nil
}
func (acmeEncoder) FlushInto(context.Context, *codec.EncodeResult) error { return nil }
func (acmeEncoder) HandleEvent(context.Context, *av.Event) error         { return nil }
func (acmeEncoder) Close() error                                         { return nil }

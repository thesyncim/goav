package main

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// testPatternInput is a live realtime synthetic YUV420P video source.
// It renders broadcast color bars with a smooth animated sweep bar and pulse
// so the studio works immediately with zero OS permissions (and in CI/headless).
type testPatternInput struct {
	name          string
	streamID      av.StreamID
	width, height int
	fps           int
	variant       int // 0 = studio bars, 1 = camera-placeholder gradient
}

func newTestPatternInput(name string, streamID av.StreamID, width, height, fps, variant int) *testPatternInput {
	return &testPatternInput{
		name:     name,
		streamID: streamID,
		width:    width,
		height:   height,
		fps:      fps,
		variant:  variant,
	}
}

func (p *testPatternInput) Name() string   { return p.name }
func (p *testPatternInput) Detail() string { return "live synthetic studio pattern" }

func (p *testPatternInput) SourceShape() shape.Spec {
	return shape.Spec{
		Domain:      shape.DomainFrame,
		MediaKind:   av.MediaVideo,
		PixelFormat: av.PixelFormatYUV420P,
		Width:       p.width,
		Height:      p.height,
		Realtime:    true,
	}
}

func (p *testPatternInput) OpenSource(_ context.Context) (pipeline.Source, []av.Stream, error) {
	src := &testPatternSource{
		name:     p.name,
		streamID: p.streamID,
		width:    p.width,
		height:   p.height,
		fps:      p.fps,
		variant:  p.variant,
		stopCh:   make(chan struct{}),
	}
	stream := av.Stream{
		ID:       p.streamID,
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: int64(time.Second)},
		Codec: av.CodecParameters{
			Width:       p.width,
			Height:      p.height,
			PixelFormat: av.PixelFormatYUV420P,
		},
	}
	return src, []av.Stream{stream}, nil
}

type testPatternSource struct {
	name          string
	streamID      av.StreamID
	width, height int
	fps           int
	variant       int

	once   sync.Once
	stopCh chan struct{}
}

func (s *testPatternSource) Name() string { return s.name }

func (s *testPatternSource) Close() error {
	s.once.Do(func() { close(s.stopCh) })
	return nil
}

// SMPTE-inspired BT.601 Y, U, V color bars.
var colorBars = [8][3]byte{
	{235, 128, 128}, // white
	{210, 16, 146},  // yellow
	{170, 166, 16},  // cyan
	{145, 54, 34},   // green
	{106, 202, 222}, // magenta
	{81, 90, 240},   // red
	{41, 240, 110},  // blue
	{22, 128, 128},  // near-black
}

func (s *testPatternSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	interval := time.Second / time.Duration(s.fps)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	w, h := s.width, s.height
	cw, ch := w/2, h/2
	ySize := w * h
	uvSize := cw * ch
	tb := av.TimeBase{Num: 1, Den: int64(time.Second)}
	dur := av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: int64(s.fps)}}

	// Precompute static base pattern; animate a moving scanner & pulse each frame.
	baseY := make([]byte, ySize)
	baseU := make([]byte, uvSize)
	baseV := make([]byte, uvSize)

	if s.variant == 0 {
		for row := range h {
			for col := range w {
				bar := (col * len(colorBars)) / w
				if row > h*3/4 {
					// Lower dark broadcast strip with gradient ramp
					baseY[row*w+col] = byte(24 + (col*180)/w)
				} else {
					baseY[row*w+col] = colorBars[bar][0]
				}
			}
		}
		for row := range ch {
			for col := range cw {
				bar := (col * len(colorBars)) / cw
				if row > ch*3/4 {
					baseU[row*cw+col] = 128
					baseV[row*cw+col] = 128
				} else {
					baseU[row*cw+col] = colorBars[bar][1]
					baseV[row*cw+col] = colorBars[bar][2]
				}
			}
		}
	} else {
		// Deep indigo-to-cyan gradient for secondary PiP placeholder.
		for row := range h {
			for col := range w {
				baseY[row*w+col] = byte(45 + ((row+col)*120)/(w+h))
			}
		}
		for row := range ch {
			for col := range cw {
				baseU[row*cw+col] = byte(155 + (col*55)/cw)
				baseV[row*cw+col] = byte(95 + (row*70)/ch)
			}
		}
	}

	var frameNum int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopCh:
			return pipeline.ErrClosed
		case <-ticker.C:
			y := make([]byte, ySize)
			u := make([]byte, uvSize)
			v := make([]byte, uvSize)
			copy(y, baseY)
			copy(u, baseU)
			copy(v, baseV)

			// Smooth animated moving vertical accent band + pulsing box.
			phase := float64(frameNum) * 0.08
			bandX := int((0.5 + 0.45*math.Sin(phase)) * float64(w-1))
			bandW := max(8, w/40)
			for row := 0; row < h; row++ {
				rowOff := row * w
				for dx := -bandW; dx <= bandW; dx++ {
					x := bandX + dx
					if x >= 0 && x < w {
						y[rowOff+x] = 240
					}
				}
			}
			// Moving indicator box in the lower strip.
			boxY0 := h*3/4 + h/24
			boxY1 := min(h-4, boxY0+h/8)
			boxX0 := int((frameNum * 9) % int64(max(1, w-w/10)))
			boxX1 := min(w-1, boxX0+w/10)
			for row := boxY0; row < boxY1; row++ {
				rowOff := row * w
				for col := boxX0; col < boxX1; col++ {
					y[rowOff+col] = 235
				}
			}

			pts := frameNum * int64(interval)
			frame := &av.Frame{
				StreamID: s.streamID,
				Type:     av.MediaVideo,
				PTS:      av.Timestamp{Value: pts, Base: tb},
				Duration: dur,
				Video: &av.VideoFrame{
					Width:       w,
					Height:      h,
					PixelFormat: av.PixelFormatYUV420P,
				},
				Planes: []av.Plane{
					{Buffer: av.Buffer{Bytes: y}, Stride: w},
					{Buffer: av.Buffer{Bytes: u}, Stride: cw},
					{Buffer: av.Buffer{Bytes: v}, Stride: cw},
				},
			}
			msg := pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}
			if err := emitter.Emit(ctx, &msg); err != nil {
				return err
			}
			frameNum++
		}
	}
}

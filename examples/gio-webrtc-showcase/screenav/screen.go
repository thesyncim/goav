// Package screenav captures a macOS display natively without cgo, using
// CoreGraphics and AVFoundation through purego, and exposes it as a standard
// goav provider.Source producing planar yuv420p frames.
package screenav

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

var (
	ErrUnsupported = errors.New("screenav: unsupported platform or capture format")
	ErrPermission  = errors.New("screenav: screen recording permission required")
	ErrNoDisplay   = errors.New("screenav: display not found")
)

// Display describes an active macOS display.
type Display struct {
	ID     uint32 `json:"id"`
	Name   string `json:"name"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	Main   bool   `json:"main"`
}

// Config configures native macOS screen capture.
// Zero DisplayID selects the main display.
type Config struct {
	DisplayID     uint32
	Width, Height int
	FPS           int
	Queue         int
	CaptureCursor bool
	buffers       *framePool
}

// Displays enumerates active displays without starting capture.
func Displays(ctx context.Context) ([]Display, error) { return displays(ctx) }

// HasAccess reports whether the process currently has macOS Screen Recording permission.
func HasAccess() bool { return hasAccess() }

// RequestAccess prompts macOS for Screen Recording permission if not yet granted.
func RequestAccess() bool { return requestAccess() }

// Input is a reusable goav provider.Source declaration.
type Input struct{ config Config }

// New creates a screen capture provider.Source. No native I/O occurs until OpenSource.
func New(config Config) *Input { return &Input{config: config} }

func (*Input) Name() string   { return "screen" }
func (*Input) Detail() string { return "native macOS screen capture" }

func (in *Input) SourceShape() shape.Spec {
	c, _ := normalize(in.config)
	return shape.Spec{
		Domain:      shape.DomainFrame,
		MediaKind:   av.MediaVideo,
		PixelFormat: av.PixelFormatYUV420P,
		Width:       c.Width,
		Height:      c.Height,
		Realtime:    true,
	}
}

func (in *Input) OpenSource(ctx context.Context) (pipeline.Source, []av.Stream, error) {
	c, err := normalize(in.config)
	if err != nil {
		return nil, nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, nil, err
	}
	c.buffers = newFramePool(c)
	dev, err := openDisplay(ctx, c)
	if err != nil {
		c.buffers.close()
		return nil, nil, err
	}
	if err = ctx.Err(); err != nil {
		c.buffers.close()
		return nil, nil, errors.Join(err, dev.Close())
	}
	s := &source{
		config:  c,
		device:  dev,
		buffers: c.buffers,
		queue:   make(chan *av.Frame, c.Queue),
		done:    make(chan struct{}),
	}
	c.buffers.evict = s.evictOldest
	return s, []av.Stream{videoStream(c)}, nil
}

func normalize(c Config) (Config, error) {
	if c.Width == 0 {
		c.Width = 1280
	}
	if c.Height == 0 {
		c.Height = 720
	}
	if c.FPS == 0 {
		c.FPS = 30
	}
	if c.Queue == 0 {
		c.Queue = 2
	}
	if c.Width < 2 || c.Height < 2 || c.Width > 7680 || c.Height > 4320 || c.Width%2 != 0 || c.Height%2 != 0 || c.FPS < 1 || c.FPS > 120 || c.Queue < 1 || c.Queue > 16 {
		return c, fmt.Errorf("screenav: invalid capture mode %dx%d@%dfps", c.Width, c.Height, c.FPS)
	}
	return c, nil
}

var nanoseconds = av.TimeBase{Num: 1, Den: int64(time.Second)}

func videoStream(c Config) av.Stream {
	return av.Stream{
		ID:       "screen",
		Type:     av.MediaVideo,
		TimeBase: nanoseconds,
		Codec: av.CodecParameters{
			Width:       c.Width,
			Height:      c.Height,
			PixelFormat: av.PixelFormatYUV420P,
		},
	}
}

type captureDevice interface {
	Run(context.Context, func(*av.Frame)) error
	Close() error
}

type screenSourceState uint8

const (
	screenIdle screenSourceState = iota
	screenRunning
	screenClosing
	screenClosed
)

type source struct {
	config   Config
	device   captureDevice
	buffers  *framePool
	queue    chan *av.Frame
	dropped  atomic.Uint64
	mu       sync.Mutex
	state    screenSourceState
	cancel   context.CancelFunc
	done     chan struct{}
	closeErr error
}

func (*source) Name() string { return "screen" }

func (s *source) DroppedMessages() uint64 {
	if s.buffers != nil {
		return s.dropped.Load() + s.buffers.dropped.Load()
	}
	return s.dropped.Load()
}

func (s *source) evictOldest() bool {
	select {
	case old := <-s.queue:
		releaseFrame(old)
		s.dropped.Add(1)
		return true
	default:
		return false
	}
}

func (s *source) offer(frame *av.Frame) {
	select {
	case s.queue <- frame:
		return
	default:
	}
	select {
	case old := <-s.queue:
		releaseFrame(old)
		s.dropped.Add(1)
	default:
	}
	select {
	case s.queue <- frame:
	default:
		releaseFrame(frame)
		s.dropped.Add(1)
	}
}

func (s *source) Start(ctx context.Context, emitter pipeline.Emitter) (result error) {
	parent := ctx
	s.mu.Lock()
	switch s.state {
	case screenClosing, screenClosed:
		s.mu.Unlock()
		return pipeline.ErrClosed
	case screenRunning:
		s.mu.Unlock()
		return errors.New("screenav: source already started")
	}
	s.state = screenRunning
	ctx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()

	type outcome struct{ run, close error }
	nativeDone := make(chan outcome, 1)
	go func() {
		err := s.device.Run(ctx, s.offer)
		nativeDone <- outcome{run: err, close: s.device.Close()}
	}()

	var native outcome
	var ended bool
	defer func() {
		s.cancel()
		if !ended {
			native = <-nativeDone
		}
		if native.run == ctx.Err() && ctx.Err() != nil {
			if result == nil {
				result = native.run
			}
		} else if native.run != nil {
			result = errors.Join(result, native.run)
		}
		s.mu.Lock()
		ownClose := s.state == screenClosing && parent.Err() == nil
		s.mu.Unlock()
		if ownClose && errors.Is(result, context.Canceled) {
			result = pipeline.ErrClosed
		}
		if native.close != nil {
			result = errors.Join(result, native.close)
		}
		for {
			select {
			case frame := <-s.queue:
				releaseFrame(frame)
			default:
				s.mu.Lock()
				s.state = screenClosed
				if s.buffers != nil {
					s.buffers.close()
				}
				s.closeErr = native.close
				close(s.done)
				s.mu.Unlock()
				return
			}
		}
	}()

	var firstPTS, lastPTS int64
	var msg pipeline.Message
	havePTS := false
	fps := s.config.FPS
	if fps <= 0 {
		fps = 30
	}
	interval := time.Second / time.Duration(fps)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// macOS AVCaptureScreenInput only delivers callbacks when screen pixels
	// change. Keep the latest frame's Y/U/V planes so we emit a steady FPS
	// stream when the desktop is idle — preventing goav.Composite or muxers
	// from stalling on a static screen.
	ySize := s.config.Width * s.config.Height
	uvSize := (s.config.Width / 2) * (s.config.Height / 2)
	var lastY, lastU, lastV []byte
	haveLast := false
	var lastEmit time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case native = <-nativeDone:
			ended = true
			return nil
		case frame := <-s.queue:
			if !havePTS {
				firstPTS = frame.PTS.Value
				lastPTS = firstPTS
				havePTS = true
			}
			pts := frame.PTS.Value
			if pts < firstPTS {
				pts = lastPTS
			}
			if pts <= lastPTS {
				pts = lastPTS + int64(interval)
			}
			lastPTS = pts
			frame.PTS.Value = pts - firstPTS

			if len(frame.Planes) >= 3 && len(frame.Planes[0].Buffer.Bytes) >= ySize {
				if lastY == nil {
					lastY = make([]byte, ySize)
					lastU = make([]byte, uvSize)
					lastV = make([]byte, uvSize)
				}
				copy(lastY, frame.Planes[0].Buffer.Bytes[:ySize])
				copy(lastU, frame.Planes[1].Buffer.Bytes[:uvSize])
				copy(lastV, frame.Planes[2].Buffer.Bytes[:uvSize])
				haveLast = true
			}

			msg = pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}
			err := emitter.Emit(ctx, &msg)
			releaseFrame(frame)
			lastEmit = time.Now()
			if err != nil {
				return err
			}
		case <-ticker.C:
			if !haveLast || time.Since(lastEmit) < interval {
				continue
			}
			lastPTS += int64(interval)
			dup := s.buffers.take(lastPTS - firstPTS)
			if dup == nil {
				continue
			}
			copy(dup.Planes[0].Buffer.Bytes[:ySize], lastY)
			copy(dup.Planes[1].Buffer.Bytes[:uvSize], lastU)
			copy(dup.Planes[2].Buffer.Bytes[:uvSize], lastV)
			msg = pipeline.Message{Kind: pipeline.MessageFrame, Frame: dup}
			err := emitter.Emit(ctx, &msg)
			releaseFrame(dup)
			lastEmit = time.Now()
			if err != nil {
				return err
			}
		}
	}
}

func (s *source) Close() error {
	s.mu.Lock()
	switch s.state {
	case screenClosing, screenClosed:
		done := s.done
		s.mu.Unlock()
		<-done
		s.mu.Lock()
		err := s.closeErr
		s.mu.Unlock()
		return err
	case screenRunning:
		s.state = screenClosing
		s.cancel()
		s.mu.Unlock()
		<-s.done
		s.mu.Lock()
		err := s.closeErr
		s.mu.Unlock()
		return err
	default: // screenIdle
		s.state = screenClosing
		s.mu.Unlock()
		err := s.device.Close()
		if s.buffers != nil {
			s.buffers.close()
		}
		s.mu.Lock()
		s.state = screenClosed
		s.closeErr = err
		close(s.done)
		s.mu.Unlock()
		return err
	}
}

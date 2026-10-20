// Package cameraav captures camera video without cgo. It uses AVFoundation on
// macOS, V4L2 on Linux, and Media Foundation on Windows. Operating-system device
// drivers and camera permission are still required. Import this optional module
// to use camera inputs without adding native dependencies to goav itself.
package cameraav

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
	ErrUnsupported   = errors.New("cameraav: unsupported platform or camera format")
	ErrPermission    = errors.New("cameraav: camera permission required")
	ErrNoDevice      = errors.New("cameraav: camera not found")
	ErrFormatChanged = errors.New("cameraav: camera changed the negotiated format")
)

// Config selects exact dimensions and an integer frame rate, allowing native
// interval quantization smaller than 100 ns. Zero values select the default device,
// 640x480 at 30 frames/second, and a two-frame queue. Device is the opaque ID
// returned by Devices, not a display name. Unsupported modes fail explicitly.
// The queue drops its oldest frame when full; device callbacks never wait for
// the pipeline. Capture owns Queue+2 fixed slots; downstream retention of more
// than two frames reduces queue capacity and can cause incoming drops.
// Timestamps retain real capture gaps when frames are dropped.
type Config struct {
	Device             string
	Width, Height, FPS int
	Queue              int
	buffers            *framePool
}

// Device identifies a native video capture device. IDs are platform-specific.
type Device struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Devices enumerates cameras without starting capture or requesting permission.
func Devices(ctx context.Context) ([]Device, error) { return devices(ctx) }

// RequestAccess requests camera permission on macOS, if needed. The application's
// Info.plist must contain NSCameraUsageDescription. On Linux and Windows access
// is managed by device permissions/privacy settings; this method cannot grant it.
func RequestAccess(ctx context.Context) error { return requestAccess(ctx) }

// Input is a reusable provider.Source declaration. New performs no native I/O.
type Input struct{ config Config }

func New(config Config) *Input { return &Input{config: config} }
func (*Input) Name() string    { return "camera" }
func (*Input) Detail() string  { return "live camera capture" }
func (in *Input) SourceShape() shape.Spec {
	c, _ := normalize(in.config)
	return shape.Spec{Domain: shape.DomainFrame, MediaKind: av.MediaVideo, PixelFormat: av.PixelFormatYUV420P, Width: c.Width, Height: c.Height, Realtime: true}
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
	device, err := openDevice(ctx, c)
	if err != nil {
		c.buffers.close()
		return nil, nil, err
	}
	if err = ctx.Err(); err != nil {
		c.buffers.close()
		return nil, nil, errors.Join(err, device.Close())
	}
	s := &source{device: device, buffers: c.buffers, queue: make(chan *av.Frame, c.Queue), done: make(chan struct{})}
	c.buffers.evict = s.evictOldest
	return s, []av.Stream{videoStream(c)}, nil
}
func normalize(c Config) (Config, error) {
	if c.Width == 0 {
		c.Width = 640
	}
	if c.Height == 0 {
		c.Height = 480
	}
	if c.FPS == 0 {
		c.FPS = 30
	}
	if c.Queue == 0 {
		c.Queue = 2
	}
	if c.Width < 2 || c.Height < 2 || c.Width > 8192 || c.Height > 8192 || c.Width%2 != 0 || c.Height%2 != 0 || c.FPS < 1 || c.FPS > 240 || c.Queue < 1 || c.Queue > 16 || int64(c.Width)*int64(c.Height)*3/2*int64(c.Queue+2) > 128<<20 {
		return c, fmt.Errorf("cameraav: invalid capture mode or queue memory budget")
	}
	return c, nil
}

var nanoseconds = av.TimeBase{Num: 1, Den: int64(time.Second)}

func videoStream(c Config) av.Stream {
	return av.Stream{ID: "camera", Type: av.MediaVideo, TimeBase: nanoseconds, Codec: av.CodecParameters{Width: c.Width, Height: c.Height, PixelFormat: av.PixelFormatYUV420P}}
}

// Run owns native callback/read activity until it returns. Close releases an
// opened device after Run has returned, or without Run ever being called.
type captureDevice interface {
	Run(context.Context, func(*av.Frame)) error
	Close() error
}
type source struct {
	device          captureDevice
	buffers         *framePool
	queue           chan *av.Frame
	dropped         atomic.Uint64
	mu              sync.Mutex
	started, closed bool
	cancel          context.CancelFunc
	done            chan struct{}
	closeErr        error
}

func (*source) Name() string { return "camera" }
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
	// One backend producer. Frames own their buffers; no native memory escapes.
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
	if s.closed {
		s.mu.Unlock()
		return pipeline.ErrClosed
	}
	if s.started {
		s.mu.Unlock()
		return errors.New("cameraav: source already started")
	}
	s.started = true
	ctx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()
	type outcome struct{ run, close error }
	nativeDone := make(chan outcome, 1)
	go func() { err := s.device.Run(ctx, s.offer); nativeDone <- outcome{run: err, close: s.device.Close()} }()
	var native outcome
	var ended bool
	defer func() {
		s.cancel()
		if !ended {
			native = <-nativeDone
		}
		// A device failure can arrive after Emit or cancellation ends the loop.
		// Keep it durable; only our exact cancellation sentinel is redundant.
		if native.run == ctx.Err() && ctx.Err() != nil {
			if result == nil {
				result = native.run
			}
		} else if native.run != nil {
			result = errors.Join(result, native.run)
		}
		s.mu.Lock()
		ownClose := s.closed && parent.Err() == nil
		s.mu.Unlock()
		if ownClose && result == context.Canceled {
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
				s.closed = true
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
			if pts < firstPTS || (havePTS && pts < lastPTS) || (firstPTS < 0 && pts > int64(^uint64(0)>>1)+firstPTS) {
				releaseFrame(frame)
				return errors.New("cameraav: invalid or regressing capture timestamp")
			}
			lastPTS = pts
			frame.PTS.Value -= firstPTS
			msg = pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}
			err := emitter.Emit(ctx, &msg)
			releaseFrame(frame)
			if err != nil {
				return err
			}
		}
	}
}
func (s *source) Close() error {
	s.mu.Lock()
	if s.closed {
		done := s.done
		s.mu.Unlock()
		<-done
		s.mu.Lock()
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	if s.started {
		s.cancel()
		s.mu.Unlock()
		<-s.done
		s.mu.Lock()
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	err := s.device.Close()
	if s.buffers != nil {
		s.buffers.close()
	}
	s.mu.Lock()
	s.closeErr = err
	close(s.done)
	s.mu.Unlock()
	return err
}

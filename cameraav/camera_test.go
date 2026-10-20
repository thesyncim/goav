package cameraav

import (
	"context"
	"errors"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig(w, h int) Config {
	c := Config{Width: w, Height: h, FPS: 30, Queue: 2}
	c.buffers = newFramePool(c)
	return c
}
func TestPixelsAndOwnedMemory(t *testing.T) {
	c := testConfig(4, 2)
	y := []byte{1, 2, 3, 4, 99, 99, 5, 6, 7, 8}
	uv := []byte{11, 21, 12, 22}
	f, err := copyNV12(c, 123, y, uv, 6, 4)
	if err != nil {
		t.Fatal(err)
	}
	held := f.Ref()
	releaseFrame(f)
	defer held.Release()
	for i := range y {
		y[i] = 0
	}
	for i := range uv {
		uv[i] = 0
	}
	want := [][]byte{{1, 2, 3, 4, 5, 6, 7, 8}, {11, 12}, {21, 22}}
	for p := range want {
		for i, v := range want[p] {
			if held.Planes[p].Buffer.Bytes[i] != v {
				t.Fatalf("plane %d=%v", p, held.Planes[p].Buffer.Bytes)
			}
		}
	}
	if held.PTS.Value != 123 || held.Video.PixelFormat != av.PixelFormatYUV420P {
		t.Fatalf("frame=%+v", held)
	}
	packed := []byte{1, 10, 2, 20, 3, 30, 4, 40, 5, 12, 6, 22, 7, 32, 8, 42}
	f, err = copyYUYV(c, 0, packed, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseFrame(f)
	if f.Planes[1].Buffer.Bytes[0] != 11 || f.Planes[2].Buffer.Bytes[1] != 41 {
		t.Fatal("chroma conversion")
	}
	if _, err = copyNV12(c, 0, y[:2], uv, 6, 4); err == nil {
		t.Fatal("accepted truncated frame")
	}
}
func TestCameraSteadyStateAllocs(t *testing.T) {
	c := testConfig(640, 480)
	y, uv := make([]byte, c.Width*c.Height), make([]byte, c.Width*c.Height/2)
	s := &source{queue: make(chan *av.Frame, 2)}
	allocs := testing.AllocsPerRun(100, func() {
		f, err := copyNV12(c, 0, y, uv, c.Width, c.Width)
		if err != nil {
			panic(err)
		}
		s.offer(f)
		releaseFrame(<-s.queue)
	})
	if allocs != 0 {
		t.Fatalf("copy + queue = %g allocations/frame", allocs)
	}
	for i := 0; i < 2; i++ {
		f, _ := copyNV12(c, 0, y, uv, c.Width, c.Width)
		s.offer(f)
	}
	allocs = testing.AllocsPerRun(100, func() { f, _ := copyNV12(c, 0, y, uv, c.Width, c.Width); s.offer(f) })
	for len(s.queue) > 0 {
		releaseFrame(<-s.queue)
	}
	if allocs != 0 {
		t.Fatalf("saturated queue = %g allocations/frame", allocs)
	}
	if s.DroppedMessages() == 0 {
		t.Fatal("drops not counted")
	}
}
func BenchmarkCameraNV12(b *testing.B) {
	c := testConfig(1280, 720)
	y, uv := make([]byte, c.Width*c.Height), make([]byte, c.Width*c.Height/2)
	b.ReportAllocs()
	b.SetBytes(int64(len(y) + len(uv)))
	b.ResetTimer()
	for b.Loop() {
		f, err := copyNV12(c, 0, y, uv, c.Width, c.Width)
		if err != nil {
			b.Fatal(err)
		}
		releaseFrame(f)
	}
}
func TestSchemeIsPureAndStrict(t *testing.T) {
	for _, raw := range []string{"camera://default", "camera://default?device=%2Fdev%2Fvideo0&width=1280&height=720&fps=30&queue=3"} {
		u, _ := url.Parse(raw)
		in, err := (Scheme{}).Input(u)
		if err != nil {
			t.Fatal(err)
		}
		if !in.SourceShape().Realtime {
			t.Fatal("not realtime")
		}
	}
	for _, raw := range []string{"camera://oops", "camera://default?width=-1", "camera://default?width=3", "camera://default?fps=30&fps=60", "camera://default?typo=1", "camera://default?width=8192&height=8192&queue=16"} {
		u, _ := url.Parse(raw)
		if _, err := (Scheme{}).Input(u); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

type fakeCamera struct {
	started  chan struct{}
	frames   chan *av.Frame
	closed   atomic.Int32
	closeErr error
}

func (f *fakeCamera) Run(ctx context.Context, emit func(*av.Frame)) error {
	close(f.started)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frame := <-f.frames:
			emit(frame)
		}
	}
}
func (f *fakeCamera) Close() error { f.closed.Add(1); return f.closeErr }

type cameraEmitter func(context.Context, *pipeline.Message) error

func (f cameraEmitter) Emit(ctx context.Context, m *pipeline.Message) error { return f(ctx, m) }
func testSource(f *fakeCamera) *source {
	return &source{device: f, queue: make(chan *av.Frame, 2), done: make(chan struct{})}
}
func TestCloseWakesSourceAndRetainsCleanupError(t *testing.T) {
	cause := errors.New("device close failed")
	f := &fakeCamera{started: make(chan struct{}), frames: make(chan *av.Frame), closeErr: cause}
	s := testSource(f)
	done := make(chan error, 1)
	go func() {
		done <- s.Start(context.Background(), cameraEmitter(func(context.Context, *pipeline.Message) error { return nil }))
	}()
	<-f.started
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			if err := s.Close(); !errors.Is(err, cause) {
				t.Errorf("Close=%v", err)
			}
		})
	}
	wait := make(chan struct{})
	go func() { wg.Wait(); close(wait) }()
	select {
	case <-wait:
	case <-time.After(time.Second):
		t.Fatal("Close blocked")
	}
	<-done
	if f.closed.Load() != 1 {
		t.Fatalf("native closes=%d", f.closed.Load())
	}
}
func TestCloseBeforeStart(t *testing.T) {
	f := &fakeCamera{}
	s := testSource(f)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background(), nil); !errors.Is(err, pipeline.ErrClosed) {
		t.Fatal(err)
	}
	if f.closed.Load() != 1 {
		t.Fatal(f.closed.Load())
	}
}
func TestCaptureTimestampsAndRetainedFrames(t *testing.T) {
	f := &fakeCamera{started: make(chan struct{}), frames: make(chan *av.Frame)}
	s := testSource(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan *av.Frame, 2)
	done := make(chan error, 1)
	go func() {
		done <- s.Start(ctx, cameraEmitter(func(_ context.Context, m *pipeline.Message) error { received <- m.Frame.Ref(); return nil }))
	}()
	<-f.started
	c := testConfig(2, 2)
	for _, pts := range []int64{1e9, 1e9 + 100e6} {
		frame := planarFrame(c, pts)
		frame.Planes[0].Buffer.Bytes[0] = 7
		f.frames <- frame
	}
	first, second := <-received, <-received
	defer first.Release()
	defer second.Release()
	cancel()
	<-done
	if first.PTS.Value != 0 || second.PTS.Value != 100e6 || first.Planes[0].Buffer.Bytes[0] != 7 {
		t.Fatal("timestamps or ownership changed")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("cancellation became cleanup failure: %v", err)
	}
}

func TestRetainedFramesExhaustFixedPoolWithoutOverwrite(t *testing.T) {
	c := testConfig(4, 2)
	defer c.buffers.close()
	held := make([]*av.Frame, 0, len(c.buffers.slots))
	for i := range c.buffers.slots {
		f := planarFrame(c, int64(i))
		f.Planes[0].Buffer.Bytes[0] = byte(i + 1)
		held = append(held, f.Ref())
		releaseFrame(f)
	}
	if f := planarFrame(c, 99); f != nil {
		releaseFrame(f)
		t.Fatal("allocated while every slot retained")
	}
	for i, f := range held {
		if f.Planes[0].Buffer.Bytes[0] != byte(i+1) {
			t.Fatal("overwrote retained frame")
		}
	}
	held[0].Release()
	f := planarFrame(c, 100)
	if f == nil {
		t.Fatal("released slot unavailable")
	}
	releaseFrame(f)
	for _, f := range held[1:] {
		f.Release()
	}
	if c.buffers.dropped.Load() != 1 {
		t.Fatal("pool exhaustion not counted")
	}
}

type lateFaultCamera struct {
	entered chan struct{}
	fault   error
}

func (d *lateFaultCamera) Run(ctx context.Context, _ func(*av.Frame)) error {
	close(d.entered)
	<-ctx.Done()
	return d.fault
}
func (*lateFaultCamera) Close() error { return nil }
func TestLateDeviceFailureSurvivesCancellation(t *testing.T) {
	fault := errors.New("camera unplugged during cancellation")
	d := &lateFaultCamera{entered: make(chan struct{}), fault: fault}
	s := &source{device: d, queue: make(chan *av.Frame, 2), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- s.Start(ctx, cameraEmitter(func(context.Context, *pipeline.Message) error { return nil }))
	}()
	<-d.entered
	cancel()
	if err := <-result; !errors.Is(err, fault) {
		t.Fatalf("lost device failure: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("execution error became cleanup error: %v", err)
	}
}
func TestOwnCloseReportsClosed(t *testing.T) {
	f := &fakeCamera{started: make(chan struct{}), frames: make(chan *av.Frame)}
	s := testSource(f)
	result := make(chan error, 1)
	go func() {
		result <- s.Start(context.Background(), cameraEmitter(func(context.Context, *pipeline.Message) error { return nil }))
	}()
	<-f.started
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, pipeline.ErrClosed) {
		t.Fatalf("Start=%v", err)
	}
}

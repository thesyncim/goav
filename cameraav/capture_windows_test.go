//go:build windows && (amd64 || arm64)

package cameraav

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"golang.org/x/sys/windows"
)

func TestMediaFoundationCallbackHandoff(t *testing.T) {
	obj, cb, err := newCallback()
	if err != nil {
		t.Fatal(err)
	}
	defer comRelease(obj)
	deliver := func() {
		if result := com(obj, 3, 0, firstVideo, 0, 123, 0); result != 0 {
			panic(result)
		}
		s := <-cb.samples
		if s.pts != 123 || s.sample != 0 {
			panic("callback ABI")
		}
	}
	deliver()
	if !raceEnabled {
		if n := testing.AllocsPerRun(1000, deliver); n != 0 {
			t.Fatalf("callback handoff=%g allocations", n)
		}
	}
}
func TestMediaFoundationLoadsWithoutCamera(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := startMF(); err != nil {
		t.Fatal(err)
	}
	stopMF()
}

// Real Media Foundation objects exercise COM calls and the copy path without
// camera permission, hardware, or synthetic Go implementations of COM methods.
func TestMediaFoundationSampleCopyZeroAllocs(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := startMF(); err != nil {
		t.Fatal(err)
	}
	defer stopMF()
	createBuffer, createSample := mfplat.NewProc("MFCreateMemoryBuffer"), mfplat.NewProc("MFCreateSample")
	if err := createBuffer.Find(); err != nil {
		t.Fatal(err)
	}
	if err := createSample.Find(); err != nil {
		t.Fatal(err)
	}
	c := testConfig(4, 2)
	defer c.buffers.close()
	d := &windowsCapture{config: c, nv12: true}
	var buffer, sample uintptr
	var pins runtime.Pinner
	pins.Pin(d)
	pins.Pin(&buffer)
	pins.Pin(&sample)
	defer pins.Unpin()
	result, _, _ := createBuffer.Call(12, ptr(&buffer))
	if err := hr(result); err != nil {
		t.Fatal(err)
	}
	defer comRelease(buffer)
	result, _, _ = createSample.Call(ptr(&sample))
	if err := hr(result); err != nil {
		t.Fatal(err)
	}
	defer comRelease(sample)
	if err := hr(com(buffer, 3, ptr(&d.data), ptr(&d.maximum), ptr(&d.length))); err != nil {
		t.Fatal(err)
	}
	copy(unsafe.Slice((*byte)(nativePointer(d.data)), 12), []byte{1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 11, 21})
	if err := hr(com(buffer, 4)); err != nil {
		t.Fatal(err)
	}
	if err := hr(com(buffer, 6, 12)); err != nil {
		t.Fatal(err)
	} // SetCurrentLength
	if err := hr(com(sample, 42, buffer)); err != nil {
		t.Fatal(err)
	} // AddBuffer
	copyFrame := func() {
		f, err := d.copySample(sample, 123)
		if err != nil {
			panic(err)
		}
		if f.PTS.Value != 12300 || f.Planes[0].Buffer.Bytes[7] != 8 || f.Planes[1].Buffer.Bytes[1] != 11 || f.Planes[2].Buffer.Bytes[1] != 21 {
			panic("native sample copy")
		}
		releaseFrame(f)
	}
	copyFrame()
	if !raceEnabled {
		if n := testing.AllocsPerRun(1000, copyFrame); n != 0 {
			t.Fatalf("native sample copy=%g allocations", n)
		}
	}
}

func startTestMF(t *testing.T) {
	t.Helper()
	runtime.LockOSThread()
	if err := startMF(); err != nil {
		runtime.UnlockOSThread()
		t.Fatal(err)
	}
	t.Cleanup(func() { stopMF(); runtime.UnlockOSThread() })
}

func newTestMFSample(t *testing.T) uintptr {
	t.Helper()
	var sample uintptr
	var pins runtime.Pinner
	pins.Pin(&sample)
	defer pins.Unpin()
	result, _, _ := mfplat.NewProc("MFCreateSample").Call(ptr(&sample))
	if err := hr(result); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { comRelease(sample) })
	return sample
}

// A real IMF2DBuffer2 supplies a padded native layout and a separate packed
// IMFMediaBuffer::Lock representation. Exercise both against the same pixels.
func TestMediaFoundationPadded2DBufferCopy(t *testing.T) {
	for _, nv12 := range []bool{true, false} {
		name, fourCC := "NV12", nv12Type.Data1
		if !nv12 {
			name, fourCC = "YUY2", yuy2Type.Data1
		}
		t.Run(name, func(t *testing.T) {
			startTestMF(t)
			c := testConfig(62, 4) // Width deliberately needs native row padding.
			defer c.buffers.close()
			d := &windowsCapture{config: c, nv12: nv12}
			var buffer, native2D uintptr
			var pins runtime.Pinner
			pins.Pin(d)
			pins.Pin(&buffer)
			pins.Pin(&native2D)
			defer pins.Unpin()
			result, _, _ := mfplat.NewProc("MFCreate2DMediaBuffer").Call(uintptr(c.Width), uintptr(c.Height), uintptr(fourCC), 0, ptr(&buffer))
			if err := hr(result); err != nil {
				t.Fatal(err)
			}
			defer d.release(buffer)
			if err := hr(d.call(buffer, 0, ptr(&buffer2DIID), ptr(&native2D))); err != nil {
				t.Fatal(err)
			}
			defer d.release(native2D)
			if err := hr(d.call(native2D, 10, 2, ptr(&d.data), ptr(&d.pitch), ptr(&d.bufferStart), ptr(&d.length))); err != nil {
				t.Fatal(err)
			}
			func() {
				defer d.call(native2D, 4)
				minimum := c.Width
				if !nv12 {
					minimum *= 2
				}
				if int(d.pitch) <= minimum || d.data != d.bufferStart {
					t.Fatalf("need padded top-down fixture: pitch=%d minimum=%d offset=%d", d.pitch, minimum, d.data-d.bufferStart)
				}
				stride := int(d.pitch)
				rows := c.Height
				if nv12 {
					rows += c.Height / 2
				}
				if int(d.length) < stride*rows {
					t.Fatalf("native allocation=%d, need %d", d.length, stride*rows)
				}
				data := unsafe.Slice((*byte)(nativePointer(d.data)), int(d.length))
				for i := range data {
					data[i] = 0xee
				}
				for row := 0; row < c.Height; row++ {
					for col := 0; col < c.Width; col++ {
						i := row*stride + col
						if !nv12 {
							i = row*stride + 2*col
						}
						data[i] = byte(row*c.Width + col + 1)
					}
				}
				for row := 0; row < rows-c.Height; row++ {
					for col := 0; col < c.Width/2; col++ {
						i := (c.Height+row)*stride + 2*col
						data[i], data[i+1] = byte(40+row+col), byte(80+row+col)
					}
				}
				if !nv12 {
					for row := 0; row < c.Height; row++ {
						for col := 0; col < c.Width/2; col++ {
							i := row*stride + 4*col
							data[i+1], data[i+3] = byte(40+row*2+col), byte(80+row*2+col)
						}
					}
				}
			}()
			sample := newTestMFSample(t)
			if err := hr(d.call(sample, 42, buffer)); err != nil {
				t.Fatal(err)
			}
			for _, packed := range []bool{false, true} {
				copyFrame := func() {
					var f *av.Frame
					var err error
					if packed {
						d.buffer = buffer
						f, err = d.copyContiguousBuffer(12300)
					} else {
						f, err = d.copySample(sample, 123)
					}
					if err != nil {
						panic(err)
					}
					if f == nil || f.PTS.Value != 12300 {
						panic("native frame timestamp")
					}
					defer releaseFrame(f)
					for row := 0; row < c.Height; row++ {
						for col := 0; col < c.Width; col++ {
							if f.Planes[0].Buffer.Bytes[row*c.Width+col] != byte(row*c.Width+col+1) {
								panic("native luma pitch")
							}
						}
					}
					for row := 0; row < c.Height/2; row++ {
						for col := 0; col < c.Width/2; col++ {
							u, v := byte(40+row+col), byte(80+row+col)
							if !nv12 {
								u, v = byte(41+row*4+col), byte(81+row*4+col)
							}
							i := row*c.Width/2 + col
							if f.Planes[1].Buffer.Bytes[i] != u || f.Planes[2].Buffer.Bytes[i] != v {
								panic("native chroma pitch")
							}
						}
					}
				}
				copyFrame()
				if !raceEnabled {
					if n := testing.AllocsPerRun(1000, copyFrame); n != 0 {
						t.Fatalf("packed=%t: sample copy=%g allocations", packed, n)
					}
				}
			}
		})
	}
}

func sampleReferences(sample uintptr) uintptr {
	n := com(sample, 1)
	comRelease(sample)
	return n - 1
}

func TestMediaFoundationCallbackCloseReleasesSamples(t *testing.T) {
	startTestMF(t)
	sample := newTestMFSample(t)
	baseline := sampleReferences(sample)
	obj, cb, err := newCallback()
	if err != nil {
		t.Fatal(err)
	}
	defer comRelease(obj)
	com(obj, 3, 0, firstVideo, 0, 123, sample)
	if got := sampleReferences(sample); got != baseline+1 {
		t.Fatalf("queued references=%d, want %d", got, baseline+1)
	}
	cb.close()
	// This is a borrowed pointer. A closed callback must neither retain it nor
	// release the caller's reference, and must still acknowledge OnFlush.
	com(obj, 3, 0, firstVideo, 0, 456, sample)
	com(obj, 4, firstVideo)
	select {
	case <-cb.flushed:
	default:
		t.Fatal("closed callback suppressed OnFlush")
	}
	if len(cb.samples) != 0 || sampleReferences(sample) != baseline {
		t.Fatal("closed callback leaked a sample or consumed the borrowed reference")
	}
}

func TestMediaFoundationFinalCallbackReleaseDrainsSamples(t *testing.T) {
	startTestMF(t)
	sample := newTestMFSample(t)
	baseline := sampleReferences(sample)
	obj, cb, err := newCallback()
	if err != nil {
		t.Fatal(err)
	}
	com(obj, 3, 0, firstVideo, 0, 123, sample)
	comRelease(obj)
	if len(cb.samples) != 0 || sampleReferences(sample) != baseline || callbackAt(obj) != nil {
		t.Fatal("final callback Release did not retire its queued sample")
	}
}

func TestMediaFoundationCallbackCloseConcurrentEnqueue(t *testing.T) {
	startTestMF(t)
	sample := newTestMFSample(t)
	baseline := sampleReferences(sample)
	obj, cb, err := newCallback()
	if err != nil {
		t.Fatal(err)
	}
	defer comRelease(obj)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				cb.offer(winSample{sample: sample})
			}
		})
	}
	cb.close()
	wg.Wait()
	if len(cb.samples) != 0 || sampleReferences(sample) != baseline {
		t.Fatal("sample enqueue raced past callback closure")
	}
}

func TestMediaFoundationSampleCallbackZeroAllocs(t *testing.T) {
	startTestMF(t)
	sample := newTestMFSample(t)
	obj, cb, err := newCallback()
	if err != nil {
		t.Fatal(err)
	}
	defer comRelease(obj)
	d := &windowsCapture{}
	var pins runtime.Pinner
	pins.Pin(d)
	defer pins.Unpin()
	deliver := func() {
		d.call(obj, 3, 0, firstVideo, 0, 123, sample)
		s := <-cb.samples
		if s.sample != sample || s.pts != 123 {
			panic("sample callback ABI")
		}
		d.release(s.sample)
	}
	deliver()
	if !raceEnabled {
		if n := testing.AllocsPerRun(1000, deliver); n != 0 {
			t.Fatalf("sample callback=%g allocations", n)
		}
	}
}

func TestMediaFoundationFlushIsBoundedAndReportedByClose(t *testing.T) {
	for _, notify := range []bool{false, true} {
		name := "missing notification"
		if notify {
			name = "notification after closure"
		}
		t.Run(name, func(t *testing.T) {
			obj, cb, err := newCallback()
			if err != nil {
				t.Fatal(err)
			}
			defer comRelease(obj)
			// A tiny native reader vtable makes Flush return immediately while
			// optionally issuing the real OnFlush callback after closure.
			word := unsafe.Sizeof(uintptr(0))
			reader, err := windows.LocalAlloc(0, uint32(12*word))
			if err != nil {
				t.Fatal(err)
			}
			defer windows.LocalFree(windows.Handle(reader))
			words := unsafe.Slice((*uintptr)(nativePointer(reader)), 12)
			words[0] = reader + word
			words[11] = syscall.NewCallback(func(self, stream uintptr) uintptr {
				cb.mu.Lock()
				closed := cb.closed
				cb.mu.Unlock()
				if !closed || stream != firstVideo {
					panic("Flush before callback closure")
				}
				if notify {
					com(obj, 4, firstVideo)
				}
				return 0
			})
			d := &windowsCapture{reader: reader, cb: cb, stop: make(chan struct{}), done: make(chan struct{})}
			var pins runtime.Pinner
			pins.Pin(d)
			defer pins.Unpin()
			started := time.Now()
			err = d.flush(10 * time.Millisecond)
			if notify && err != nil || !notify && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("notify=%t: Flush=%v", notify, err)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("Flush wait was not bounded: %s", elapsed)
			}
			if notify {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := d.capture(ctx, nil); err != context.Canceled {
					t.Fatalf("successful flush changed cancellation sentinel: %v", err)
				}
			}
			close(d.done)
			closeErr := d.Close()
			if notify && closeErr != nil || !notify && !errors.Is(closeErr, context.DeadlineExceeded) {
				t.Fatalf("notify=%t: Close=%v", notify, closeErr)
			}
		})
	}
}

func TestMediaFoundationPermissionError(t *testing.T) {
	if err := hr(eAccessDenied); !errors.Is(err, ErrPermission) {
		t.Fatalf("E_ACCESSDENIED=%v, want ErrPermission", err)
	}
}

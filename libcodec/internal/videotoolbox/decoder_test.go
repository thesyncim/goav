package videotoolbox

import (
	"errors"
	"slices"
	"testing"
	"unsafe"

	"github.com/thesyncim/goav/libcodec/kit"
)

func boundFrameworks(t *testing.T) *frameworks {
	t.Helper()
	f := &frameworks{}
	for _, library := range f.libraries() {
		if err := library.Bind(Name); err != nil {
			t.Skip(err)
		}
	}
	return f
}

// TestDrainReleasesEachImageOnce queues decoded, dropped, and damaged
// pictures the way the output callback does and pins that drain releases
// every queued image exactly once: lent pictures through their owner, the
// rest directly. A double release would free the buffers the test still
// holds.
func TestDrainReleasesEachImageOnce(t *testing.T) {
	f := boundFrameworks(t)
	images := make([]uintptr, 4)
	for i := range images {
		if s := f.cv.create(0, 16, 16, pixelI420, 0, &images[i]); s != 0 {
			t.Fatalf("CVPixelBufferCreate: %d", s)
		}
		defer f.cf.release(images[i])
	}
	queued := func() []picture {
		for _, image := range images {
			f.cf.retain(image) // the callback's reference, the queue's to release
		}
		return []picture{
			{image: images[0], pts: 0},
			{pts: 1}, // dropped: no image
			{image: images[1], pts: 2},
			{status: -12909, image: images[2], pts: 3},
			{image: images[3], pts: 4},
		}
	}
	held := func() bool {
		return !slices.ContainsFunc(images, func(image uintptr) bool { return f.cf.retainCount(image) != 1 })
	}

	d := &decoder{f: f, queue: queued()}
	var lent []int64
	err := d.drain(func(p *kit.Picture) error { lent = append(lent, p.PTS); return nil })
	if !errors.Is(err, kit.ErrDamaged) || !slices.Equal(lent, []int64{0, 2, 4}) || !held() {
		t.Fatalf("drain lent %v, returned %v; each image must be released once", lent, err)
	}

	d.queue = queued()
	stop := errors.New("stop")
	if err := d.drain(func(*kit.Picture) error { return stop }); err != stop || !held() {
		t.Fatalf("drain = %v; an emit failure must release the rest once", err)
	}
}

// Hardware decoders produce NV12. Keep its plane geometry and ownership
// intact so the kit can convert it to pooled I420 without a platform scaler.
func TestDecoderLendsNativeNV12(t *testing.T) {
	f := boundFrameworks(t)
	for _, format := range []uint32{pixelNV12Video, pixelNV12Full} {
		t.Run(fourCCString(format), func(t *testing.T) {
			var image uintptr
			if status := f.cv.create(0, 16, 8, format, 0, &image); status != 0 {
				t.Fatalf("CVPixelBufferCreate: %d", status)
			}
			defer f.cf.release(image)
			if status := f.cv.lock(image, 0); status != 0 {
				t.Fatalf("CVPixelBufferLockBaseAddress: %d", status)
			}
			for plane := range 2 {
				stride, rows := f.cv.planeStride(image, plane), f.cv.planeHeight(image, plane)
				data := unsafe.Slice((*byte)(f.cv.planeBase(image, plane)), stride*rows)
				for row := range rows {
					for x := range 16 {
						value := byte(17)
						if plane == 1 {
							value = byte(31 + x%2)
						}
						data[row*stride+x] = value
					}
				}
			}
			f.cv.unlock(image, 0)
			f.cf.retain(image) // the decode callback's queued reference
			d := &decoder{f: f}
			var held *kit.Picture
			err := d.lend(picture{image: image, pts: 99}, func(pic *kit.Picture) error {
				if pic.Format != kit.NV12 || pic.Width != 16 || pic.Height != 8 || pic.PTS != 99 {
					t.Fatalf("unexpected native picture: %+v", pic)
				}
				copy := *pic
				held = &copy
				held.Owner.Retain()
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if held == nil {
				t.Fatal("no picture was emitted")
			}
			for y := range 8 {
				for x := range 16 {
					if held.Planes[0][y*held.Strides[0]+x] != 17 {
						t.Fatal("luma changed")
					}
				}
			}
			for y := range 4 {
				for x := range 16 {
					if held.Planes[1][y*held.Strides[1]+x] != byte(31+x%2) {
						t.Fatal("chroma changed")
					}
				}
			}
			held.Owner.Release()
			if refs := f.cf.retainCount(image); refs != 1 {
				t.Fatalf("pixel buffer kept %d references, want 1", refs)
			}
		})
	}
}

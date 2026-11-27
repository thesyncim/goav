package screenav

import (
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestSourceShapeAndNormalize(t *testing.T) {
	in := New(Config{Width: 1280, Height: 720, FPS: 30, CaptureCursor: true})
	sh := in.SourceShape()
	if sh.Width != 1280 || sh.Height != 720 || sh.PixelFormat != av.PixelFormatYUV420P || !sh.Realtime {
		t.Fatalf("unexpected source shape: %+v", sh)
	}
	if _, err := normalize(Config{Width: 1279, Height: 720}); err == nil {
		t.Fatal("expected odd width to fail normalization")
	}
}

func TestCopyNV12ExactAndScaled(t *testing.T) {
	c, err := normalize(Config{Width: 4, Height: 4, FPS: 30, Queue: 2})
	if err != nil {
		t.Fatal(err)
	}
	c.buffers = newFramePool(c)
	defer c.buffers.close()

	// Exact 4x4 NV12 with stride 6.
	y := make([]byte, 6*4)
	uv := make([]byte, 6*2)
	for r := range 4 {
		for col := range 4 {
			y[r*6+col] = byte(10 + r*4 + col)
		}
	}
	for r := range 2 {
		for col := range 2 {
			uv[r*6+col*2] = 0x55
			uv[r*6+col*2+1] = 0xAA
		}
	}
	f := copyNV12(c, 1234, 4, 4, y, uv, 6, 6)
	if f == nil {
		t.Fatal("expected frame")
	}
	if f.Planes[0].Buffer.Bytes[0] != 10 || f.Planes[1].Buffer.Bytes[0] != 0x55 || f.Planes[2].Buffer.Bytes[0] != 0xAA {
		t.Fatalf("unexpected pixel values: Y=%d U=%d V=%d", f.Planes[0].Buffer.Bytes[0], f.Planes[1].Buffer.Bytes[0], f.Planes[2].Buffer.Bytes[0])
	}
	releaseFrame(f)

	// Scaled 8x8 -> 4x4.
	y8 := make([]byte, 8*8)
	uv8 := make([]byte, 8*4)
	for i := range y8 {
		y8[i] = 200
	}
	for i := 0; i < len(uv8); i += 2 {
		uv8[i] = 64
		uv8[i+1] = 192
	}
	f2 := copyNV12(c, 5678, 8, 8, y8, uv8, 8, 8)
	if f2 == nil {
		t.Fatal("expected scaled frame")
	}
	if f2.Planes[0].Buffer.Bytes[0] != 200 || f2.Planes[1].Buffer.Bytes[0] != 64 || f2.Planes[2].Buffer.Bytes[0] != 192 {
		t.Fatalf("unexpected scaled values: Y=%d U=%d V=%d", f2.Planes[0].Buffer.Bytes[0], f2.Planes[1].Buffer.Bytes[0], f2.Planes[2].Buffer.Bytes[0])
	}
	releaseFrame(f2)
}

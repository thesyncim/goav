package kit

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

// PixelFormat is the 4:2:0 layout a video backend reads or writes: goav
// frames are planar I420, and the kit converts for a backend that wants
// semi-planar NV12.
type PixelFormat int

// The pixel formats.
const (
	I420 PixelFormat = iota
	NV12
)

// Picture is one 8-bit 4:2:0 picture lent to or by a backend for the length
// of one call: never keep its planes. An I420 picture has three planes, an
// NV12 picture two (Y, then interleaved CbCr). PTS and Duration are in the
// stream's time base; Keyframe asks an encoder for a keyframe.
type Picture struct {
	Width, Height int
	Format        PixelFormat
	Planes        [3][]byte
	Strides       [3]int
	PTS           int64
	Duration      int64
	Keyframe      bool
}

func chroma(n int) int { return (n + 1) / 2 }

// framePicture views a goav I420 frame as a Picture without copying.
func framePicture(frame *av.Frame) (Picture, error) {
	if frame.Video == nil || len(frame.Planes) < 3 {
		return Picture{}, fmt.Errorf("%w: kit encoders take 3-plane I420 video frames", codec.ErrUnsupportedFormat)
	}
	switch frame.Video.PixelFormat {
	case "", av.PixelFormatI420, av.PixelFormatYUV420P:
	default:
		return Picture{}, fmt.Errorf("%w: pixel format %s, want i420", codec.ErrUnsupportedFormat, frame.Video.PixelFormat)
	}
	pic := Picture{Width: frame.Video.Width, Height: frame.Video.Height, Format: I420}
	if pic.Width <= 0 || pic.Height <= 0 {
		return Picture{}, fmt.Errorf("%w: frame has no geometry", codec.ErrUnsupportedFormat)
	}
	for i := range 3 {
		plane := frame.Planes[i]
		w, h := pic.Width, pic.Height
		if i > 0 {
			w, h = chroma(w), chroma(h)
		}
		stride := plane.Stride
		if stride == 0 {
			stride = w
		}
		data := plane.Buffer.Bytes
		if plane.Offset < 0 || plane.Offset > len(data) || stride < w || len(data)-plane.Offset < stride*(h-1)+w {
			return Picture{}, fmt.Errorf("%w: plane %d is smaller than %dx%d", codec.ErrUnsupportedFormat, i, w, h)
		}
		pic.Planes[i] = data[plane.Offset:]
		pic.Strides[i] = stride
	}
	return pic, nil
}

// toNV12 converts an I420 picture into scratch, returning the NV12 view.
func toNV12(pic Picture, scratch []byte) (Picture, []byte) {
	cw, ch := chroma(pic.Width), chroma(pic.Height)
	ySize := pic.Width * pic.Height
	need := ySize + 2*cw*ch
	if cap(scratch) < need {
		scratch = make([]byte, need)
	}
	scratch = scratch[:need]
	for row := range pic.Height {
		copy(scratch[row*pic.Width:(row+1)*pic.Width], pic.Planes[0][row*pic.Strides[0]:])
	}
	uv := scratch[ySize:]
	for row := range ch {
		u := pic.Planes[1][row*pic.Strides[1]:]
		v := pic.Planes[2][row*pic.Strides[2]:]
		line := uv[row*2*cw : (row+1)*2*cw]
		for x := range cw {
			line[2*x] = u[x]
			line[2*x+1] = v[x]
		}
	}
	out := pic
	out.Format = NV12
	out.Planes = [3][]byte{scratch[:ySize], uv}
	out.Strides = [3]int{pic.Width, 2 * cw}
	return out, scratch
}

// fillFrame copies a backend's picture into fresh pooled I420 planes of
// frame: one copy at the boundary, after which the backend may reuse its
// memory. frame takes the one reference each plane holds. The caller sets
// frame.Video.
func fillFrame(frame *av.Frame, pic *Picture) error {
	w, h := pic.Width, pic.Height
	cw, ch := chroma(w), chroma(h)
	if w <= 0 || h <= 0 || len(pic.Planes[0]) < pic.Strides[0]*(h-1)+w {
		return fmt.Errorf("%w: decoded picture is smaller than %dx%d", codec.ErrUnsupportedFormat, w, h)
	}
	planes := [3]av.Buffer{av.NewBuffer(w * h), av.NewBuffer(cw * ch), av.NewBuffer(cw * ch)}
	for row := range h {
		copy(planes[0].Bytes[row*w:(row+1)*w], pic.Planes[0][row*pic.Strides[0]:])
	}
	switch pic.Format {
	case NV12:
		if len(pic.Planes[1]) < pic.Strides[1]*(ch-1)+2*cw {
			releaseAll(planes[:])
			return fmt.Errorf("%w: decoded chroma is smaller than %dx%d", codec.ErrUnsupportedFormat, cw, ch)
		}
		for row := range ch {
			src := pic.Planes[1][row*pic.Strides[1]:]
			u := planes[1].Bytes[row*cw : (row+1)*cw]
			v := planes[2].Bytes[row*cw : (row+1)*cw]
			for x := range cw {
				u[x], v[x] = src[2*x], src[2*x+1]
			}
		}
	default:
		for i := 1; i < 3; i++ {
			if len(pic.Planes[i]) < pic.Strides[i]*(ch-1)+cw {
				releaseAll(planes[:])
				return fmt.Errorf("%w: decoded chroma is smaller than %dx%d", codec.ErrUnsupportedFormat, cw, ch)
			}
			for row := range ch {
				copy(planes[i].Bytes[row*cw:(row+1)*cw], pic.Planes[i][row*pic.Strides[i]:])
			}
		}
	}
	if cap(frame.Planes) < 3 {
		frame.Planes = make([]av.Plane, 3)
	}
	frame.Planes = frame.Planes[:3]
	frame.Planes[0] = av.Plane{Buffer: planes[0], Stride: w}
	frame.Planes[1] = av.Plane{Buffer: planes[1], Stride: cw}
	frame.Planes[2] = av.Plane{Buffer: planes[2], Stride: cw}
	frame.Type = av.MediaVideo
	return nil
}

func releaseAll(buffers []av.Buffer) {
	for i := range buffers {
		buffers[i].Release()
	}
}

package kit

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

// NV12 is semi-planar 4:2:0: a luma plane, then one plane of interleaved Cb
// and Cr. Hardware codecs prefer it; goav frames are planar, so the kit
// converts NV12 to and from I420.
const NV12 = "nv12"

// layout is how a pixel format lays out its planes.
type layout struct {
	planes   int
	shiftX   int // chroma subsampling, as a right shift of the luma width
	shiftY   int
	bytes    int // per sample
	planar   string
	interlvd bool // chroma planes interleaved into one (NV12)
}

var layouts = map[string]layout{
	av.PixelFormatI420:    {planes: 3, shiftX: 1, shiftY: 1, bytes: 1},
	av.PixelFormatYUV420P: {planes: 3, shiftX: 1, shiftY: 1, bytes: 1},
	av.PixelFormatI422:    {planes: 3, shiftX: 1, bytes: 1},
	av.PixelFormatYUV422P: {planes: 3, shiftX: 1, bytes: 1},
	av.PixelFormatI444:    {planes: 3, bytes: 1},
	av.PixelFormatYUV444P: {planes: 3, bytes: 1},
	"yuv420p10le":         {planes: 3, shiftX: 1, shiftY: 1, bytes: 2},
	"yuv422p10le":         {planes: 3, shiftX: 1, bytes: 2},
	"yuv444p10le":         {planes: 3, bytes: 2},
	NV12:                  {planes: 2, shiftX: 1, shiftY: 1, bytes: 1, planar: av.PixelFormatI420, interlvd: true},
}

// Picture is one picture lent to or by a backend. Format is a goav pixel
// format name (i420, i422, i444, yuv420p10le) or nv12. The planes are valid
// for the length of the call, unless Owner is set: a decoder that lends its
// own memory (a CVPixelBuffer, a dav1d picture) sets Owner, holding one
// reference, and the kit publishes the planes without a copy, retaining
// Owner once per plane; the decoder drops its own reference after emit
// returns. PTS and Duration are in the stream's time base; Keyframe asks an
// encoder for a keyframe.
type Picture struct {
	Width, Height int
	Format        string
	Planes        [3][]byte
	Strides       [3]int
	Owner         av.BufferOwner
	PTS           int64
	Duration      int64
	Keyframe      bool
}

func layoutOf(format string) (layout, bool) {
	if format == "" {
		format = av.PixelFormatI420
	}
	l, ok := layouts[format]
	return l, ok
}

// extent is a plane's width in bytes and height in rows.
func (l layout) extent(plane, width, height int) (int, int) {
	if plane == 0 {
		return width * l.bytes, height
	}
	w, h := (width+(1<<l.shiftX)-1)>>l.shiftX, (height+(1<<l.shiftY)-1)>>l.shiftY
	if l.interlvd {
		w *= 2
	}
	return w * l.bytes, h
}

func fits(data []byte, stride, rowBytes, rows int) bool {
	return rows <= 0 || (stride >= rowBytes && len(data) >= stride*(rows-1)+rowBytes)
}

// framePicture views a goav frame as a Picture without copying.
func framePicture(frame *av.Frame) (Picture, error) {
	if frame.Video == nil {
		return Picture{}, fmt.Errorf("%w: want a video frame", codec.ErrUnsupportedFormat)
	}
	format := frame.Video.PixelFormat
	if format == "" {
		format = av.PixelFormatI420
	}
	l, ok := layoutOf(format)
	if !ok || l.interlvd || len(frame.Planes) < l.planes {
		return Picture{}, fmt.Errorf("%w: pixel format %s with %d planes", codec.ErrUnsupportedFormat, format, len(frame.Planes))
	}
	pic := Picture{Width: frame.Video.Width, Height: frame.Video.Height, Format: format}
	if pic.Width <= 0 || pic.Height <= 0 {
		return Picture{}, fmt.Errorf("%w: frame has no geometry", codec.ErrUnsupportedFormat)
	}
	for i := range l.planes {
		plane := frame.Planes[i]
		rowBytes, rows := l.extent(i, pic.Width, pic.Height)
		stride := plane.Stride
		if stride == 0 {
			stride = rowBytes
		}
		data := plane.Buffer.Bytes
		if plane.Offset < 0 || plane.Offset > len(data) || !fits(data[plane.Offset:], stride, rowBytes, rows) {
			return Picture{}, fmt.Errorf("%w: plane %d is smaller than %d bytes x %d rows", codec.ErrUnsupportedFormat, i, rowBytes, rows)
		}
		pic.Planes[i] = data[plane.Offset:]
		pic.Strides[i] = stride
	}
	return pic, nil
}

// toNV12 converts an 8-bit I420 picture into scratch, returning the NV12
// view.
func toNV12(pic Picture, scratch []byte) (Picture, []byte) {
	cw, ch := (pic.Width+1)/2, (pic.Height+1)/2
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

// fillFrame publishes a decoded picture as frame's planes and returns the
// goav pixel format they carry. A picture with an Owner in a goav format is
// published in place, one Owner reference per plane; any other is copied
// once into pooled planes (NV12 converted to I420), after which the backend
// may reuse its memory.
func fillFrame(frame *av.Frame, pic *Picture) (string, error) {
	l, ok := layoutOf(pic.Format)
	if !ok || pic.Width <= 0 || pic.Height <= 0 {
		return "", fmt.Errorf("%w: decoded picture %dx%d in %q", codec.ErrUnsupportedFormat, pic.Width, pic.Height, pic.Format)
	}
	for i := range l.planes {
		rowBytes, rows := l.extent(i, pic.Width, pic.Height)
		if !fits(pic.Planes[i], pic.Strides[i], rowBytes, rows) {
			return "", fmt.Errorf("%w: decoded plane %d is smaller than %d bytes x %d rows", codec.ErrUnsupportedFormat, i, rowBytes, rows)
		}
	}
	if cap(frame.Planes) < 3 {
		frame.Planes = make([]av.Plane, 3)
	}
	format := pic.Format
	if format == "" {
		format = av.PixelFormatI420
	}
	if pic.Owner != nil && !l.interlvd {
		frame.Planes = frame.Planes[:l.planes]
		for i := range l.planes {
			pic.Owner.Retain()
			frame.Planes[i] = av.Plane{Buffer: av.Buffer{Bytes: pic.Planes[i], Owner: pic.Owner}, Stride: pic.Strides[i]}
		}
		return format, nil
	}
	if l.interlvd {
		format = l.planar
		l, _ = layoutOf(format)
	}
	frame.Planes = frame.Planes[:l.planes]
	for i := range l.planes {
		rowBytes, rows := l.extent(i, pic.Width, pic.Height)
		buffer := av.NewBuffer(rowBytes * rows)
		switch {
		case i == 0 || pic.Format != NV12:
			for row := range rows {
				copy(buffer.Bytes[row*rowBytes:(row+1)*rowBytes], pic.Planes[i][row*pic.Strides[i]:])
			}
		default:
			// Deinterleave NV12's CbCr plane into Cb (i=1) or Cr (i=2).
			for row := range rows {
				src := pic.Planes[1][row*pic.Strides[1]:]
				dst := buffer.Bytes[row*rowBytes : (row+1)*rowBytes]
				for x := range dst {
					dst[x] = src[2*x+i-1]
				}
			}
		}
		frame.Planes[i] = av.Plane{Buffer: buffer, Stride: rowBytes}
	}
	return format, nil
}

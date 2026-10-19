// Package extbackend proves a codec backend can live outside goav: a
// lossless video codec over the system zlib, bound through libcodec/kit,
// written against public packages only. It is the whole "add a codec" cost.
package extbackend

import (
	"encoding/binary"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/libcodec/kit"
	"github.com/thesyncim/goav/runconfig"
)

// Codec is the codec id this backend serves: each frame, its I420 planes
// deflated, after a 4-byte width and height.
const Codec av.CodecID = "zlib"

// zlib's entry points (uLong is 64-bit on the platforms it declares).
type symbols struct {
	compress   func(dst *byte, dstLen *uint64, src *byte, srcLen uint64, level int32) int32 `sym:"compress2"`
	uncompress func(dst *byte, dstLen *uint64, src *byte, srcLen uint64) int32              `sym:"uncompress"`
	bound      func(srcLen uint64) uint64                                                   `sym:"compressBound"`
}

// zlibPaths are where macOS and the Linux distributions install zlib.
var zlibPaths = []string{"/usr/lib/libz.1.dylib", "/lib/x86_64-linux-gnu/libz.so.1",
	"/usr/lib/x86_64-linux-gnu/libz.so.1", "/lib/aarch64-linux-gnu/libz.so.1", "/usr/lib64/libz.so.1"}

// Option registers the backend as zlib/zlib (lib) on a runtime.
func Option() runconfig.Option { return kit.Register(Declaration()) }

// Declaration is the backend, as a plain value.
func Declaration() kit.Codec {
	sym := &symbols{}
	level := codec.Knob{Name: "level", Type: codec.KnobInt, Min: 0, Max: 9, Default: "6", Help: "deflate level"}
	return kit.Codec{
		ID: Codec, Media: av.MediaVideo, Backend: "zlib", Kind: codec.KindLib,
		Platforms: []string{"darwin", "linux"},
		Realtime:  true,
		Knobs:     []codec.Knob{level},
		Libraries: []kit.Library{{Name: "libz", System: zlibPaths, Fix: "install zlib (zlib1g on Debian)", Symbols: sym}},
		License:   kit.License{Class: kit.LicensePermissive},
		NewVideoEncoder: func(c kit.VideoConfig) (kit.VideoEncoder, error) {
			return &encoder{session{sym: sym}, c.Knobs.Int32("level")}, nil
		},
		NewVideoDecoder: func(kit.VideoConfig) (kit.VideoDecoder, error) { return &decoder{session{sym: sym}}, nil },
	}
}

// session is what an encoder and a decoder share: the symbols and scratch.
type session struct {
	sym      *symbols
	raw, out []byte
}

func (s *session) Close() error { return nil }

type encoder struct {
	session
	level int32
}

type decoder struct{ session }

func (s *encoder) Encode(p *kit.Picture, emit func(*kit.Packet) error) error {
	s.raw = s.raw[:0]
	for i := range 3 {
		w, h := p.Width, p.Height
		if i > 0 {
			w, h = (w+1)/2, (h+1)/2
		}
		for row := range h {
			s.raw = append(s.raw, p.Planes[i][row*p.Strides[i]:row*p.Strides[i]+w]...)
		}
	}
	n := s.sym.bound(uint64(len(s.raw)))
	s.out = append(s.out[:0], make([]byte, 4+n)...)
	binary.BigEndian.PutUint16(s.out, uint16(p.Width))
	binary.BigEndian.PutUint16(s.out[2:], uint16(p.Height))
	if status := s.sym.compress(&s.out[4], &n, unsafe.SliceData(s.raw), uint64(len(s.raw)), s.level); status != 0 {
		return kit.Fail("zlib", "encode", "compress2", int64(status))
	}
	return emit(&kit.Packet{Data: s.out[:4+n], PTS: p.PTS, DTS: p.PTS, Keyframe: true})
}

func (s *decoder) Decode(u *kit.Packet, emit func(*kit.Picture) error) error {
	if len(u.Data) < 5 {
		return kit.Fail("zlib", "decode", "a frame header", -1)
	}
	w, h := int(binary.BigEndian.Uint16(u.Data)), int(binary.BigEndian.Uint16(u.Data[2:]))
	cw, ch := (w+1)/2, (h+1)/2
	n := uint64(w*h + 2*cw*ch)
	s.raw = append(s.raw[:0], make([]byte, n)...)
	if status := s.sym.uncompress(unsafe.SliceData(s.raw), &n, &u.Data[4], uint64(len(u.Data)-4)); status != 0 || n != uint64(len(s.raw)) {
		return kit.Fail("zlib", "decode", "uncompress", int64(status))
	}
	return emit(&kit.Picture{
		Width: w, Height: h, Format: av.PixelFormatI420, PTS: u.PTS,
		Planes:  [3][]byte{s.raw[:w*h], s.raw[w*h : w*h+cw*ch], s.raw[w*h+cw*ch:]},
		Strides: [3]int{w, cw, cw},
	})
}

func (s *encoder) Flush(func(*kit.Packet) error) error { return nil }

func (s *decoder) Flush(func(*kit.Picture) error) error { return nil }

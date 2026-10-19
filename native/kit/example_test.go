package kit_test

import (
	"unsafe"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/native/kit"
)

// A complete backend for a hypothetical C encoder, libfoo, with three entry
// points:
//
//	foo_t *foo_open(int width, int height, int bitrate);
//	int    foo_encode(foo_t *, const uint8_t *planes[3], const int strides[3],
//	                  int keyframe, uint8_t *out, int capacity, int *is_key);
//	void   foo_close(foo_t *);
//
// The declaration says what the backend is and can do; the session calls the
// library. Everything else (frame and packet conversion, timestamps, pooled
// payloads, settings, live controls, Close) is the kit's.

type fooSymbols struct {
	open   func(width, height, bitrate int32) uintptr                                                                         `sym:"foo_open"`
	encode func(foo uintptr, planes *[3]unsafe.Pointer, strides *[3]int32, key int32, out *byte, n int32, isKey *int32) int32 `sym:"foo_encode"`
	close  func(foo uintptr)                                                                                                  `sym:"foo_close"`
}

func fooCodec(path string) kit.Codec {
	sym := &fooSymbols{}
	return kit.Codec{
		ID: av.CodecH264, Media: av.MediaVideo, Backend: "foo", Kind: codec.KindNative,
		Module: "example.com/foo", Package: "example.com/foo",
		Bitstream: kit.AnnexB,
		Knobs:     []codec.Knob{codec.KnobBitrate},
		Libraries: []kit.Library{{Name: "libfoo", Path: path, Fix: "install libfoo", Symbols: sym}},
		NewVideoEncoder: func(config kit.VideoConfig) (kit.VideoEncoder, error) {
			foo := sym.open(int32(config.Width), int32(config.Height), int32(config.Knobs.Int("bitrate")))
			if foo == 0 {
				return nil, &kit.Error{Backend: "foo", Op: "open", Reason: "foo_open failed"}
			}
			return &fooSession{sym: sym, foo: foo, out: make([]byte, config.Width*config.Height)}, nil
		},
	}
}

type fooSession struct {
	sym *fooSymbols
	foo uintptr
	out []byte
}

func (s *fooSession) Encode(pic *kit.Picture, emit func(*kit.Packet) error) error {
	planes := [3]unsafe.Pointer{unsafe.Pointer(&pic.Planes[0][0]), unsafe.Pointer(&pic.Planes[1][0]), unsafe.Pointer(&pic.Planes[2][0])}
	strides := [3]int32{int32(pic.Strides[0]), int32(pic.Strides[1]), int32(pic.Strides[2])}
	force, isKey := int32(0), int32(0)
	if pic.Keyframe {
		force = 1
	}
	n := s.sym.encode(s.foo, &planes, &strides, force, &s.out[0], int32(len(s.out)), &isKey)
	if n < 0 {
		return kit.Fail("foo", "encode", "foo_encode", int64(n))
	}
	return emit(&kit.Packet{Data: s.out[:n], PTS: pic.PTS, DTS: pic.PTS, Keyframe: isKey != 0})
}

func (s *fooSession) Flush(func(*kit.Packet) error) error { return nil }

func (s *fooSession) Close() error {
	s.sym.close(s.foo)
	return nil
}

// Example_backend registers the backend next to the bundle: libfoo binds
// when the runtime is built, and h264 encodes select it by the default
// preference, or by name with codec.Backend("foo"). codectest.Run(t,
// kit.Register(fooCodec(path))) is its whole conformance suite.
func Example_backend() {
	rt, err := bundle.New(kit.Register(fooCodec("/usr/local/lib/libfoo.so")))
	_, _ = rt, err
}

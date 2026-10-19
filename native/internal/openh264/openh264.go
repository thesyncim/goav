// Package openh264 is the OpenH264 backend: Cisco's BSD-licensed H.264
// encoder and decoder, bound at run time through its C API.
package openh264

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/native/kit"
)

// Name is the backend's identity: h264/openh264.
const Name = "openh264"

const fix = "brew install openh264 (macOS), apt install libopenh264-dev (Debian, Ubuntu), or download Cisco's binary from github.com/cisco/openh264/releases; then pass native.Library(path)"

// symbols are the library's C entry points.
type symbols struct {
	createEncoder  func(out *uintptr) int32 `sym:"WelsCreateSVCEncoder"`
	destroyEncoder func(encoder uintptr)    `sym:"WelsDestroySVCEncoder"`
	createDecoder  func(out *uintptr) int32 `sym:"WelsCreateDecoder"`
	destroyDecoder func(decoder uintptr)    `sym:"WelsDestroyDecoder"`
	versionEx      func(v *version)         `sym:"WelsGetCodecVersionEx"`
}

// library is one bound copy of libopenh264: its symbols, the version it
// reported, and the vtables, bound once from the first session.
type library struct {
	symbols
	version version
	layout  int // 25 (2.3 to 2.5) or 26 (2.6)

	encoderOnce sync.Once
	encoder     encoderVtable
	encoderErr  error
	decoderOnce sync.Once
	decoder     decoderVtable
	decoderErr  error
}

func defaultPaths() []string {
	switch runtime.GOOS {
	case "darwin":
		return []string{"libopenh264.dylib", "libopenh264.8.dylib", "libopenh264.7.dylib", "/opt/homebrew/lib/libopenh264.dylib", "/usr/local/lib/libopenh264.dylib"}
	case "linux":
		return []string{"libopenh264.so.8", "libopenh264.so.7", "libopenh264.so"}
	case "windows":
		return []string{"openh264-2.6.0-win64.dll", "openh264.dll"}
	}
	return nil
}

// Codec declares the backend; path, when set, is the only library tried.
func Codec(path string) kit.Codec {
	paths := defaultPaths()
	if path != "" {
		paths = []string{path}
	}
	lib := kit.Library{Name: "libopenh264", Paths: paths, Fix: fix}
	load := sync.OnceValues(func() (*library, error) {
		l := &library{}
		if err := lib.Bind(Name, &l.symbols); err != nil {
			return nil, err
		}
		return l, l.checkVersion()
	})
	return kit.Codec{
		ID:       av.CodecH264,
		Media:    av.MediaVideo,
		Backend:  Name,
		Kind:     codec.KindNative,
		Package:  "github.com/thesyncim/goav/native/internal/openh264",
		Profiles: []string{"baseline", "main", "high"},
		Capabilities: codec.Capabilities{
			MaxWidth:  3840,
			MaxHeight: 2160,
		},
		Realtime:     true,
		Knobs:        encoderKnobs,
		DecoderKnobs: decoderKnobs,
		RawKnobs:     rawKnobs(),
		Bitstream:    kit.AnnexB,
		Pixels:       kit.I420,
		Load:         func() error { _, err := load(); return err },
		NewVideoEncoder: func(config kit.VideoConfig) (kit.VideoEncoder, error) {
			l, err := load()
			if err != nil {
				return nil, err
			}
			return newEncoder(l, config)
		},
		NewVideoDecoder: func(config kit.VideoConfig) (kit.VideoDecoder, error) {
			l, err := load()
			if err != nil {
				return nil, err
			}
			return newDecoder(l, config)
		},
	}
}

// checkVersion keeps to the releases whose structure layouts this backend
// mirrors.
func (l *library) checkVersion() error {
	l.versionEx(&l.version)
	v := l.version
	switch {
	case v.Major == 2 && v.Minor >= 6:
		l.layout = 26
	case v.Major == 2 && v.Minor >= 3:
		l.layout = 25
	default:
		return &kit.Error{Backend: Name, Op: "version", Name: "libopenh264", Reason: fmt.Sprintf("%d.%d.%d is not supported (want 2.3 or later 2.x)", v.Major, v.Minor, v.Revision), Fix: fix}
	}
	return nil
}

// Version reports the bound library's version, for tests and benchmarks.
func (l *library) String() string {
	return fmt.Sprintf("%d.%d.%d", l.version.Major, l.version.Minor, l.version.Revision)
}

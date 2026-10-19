// Package openh264 is the OpenH264 backend: Cisco's BSD-licensed H.264
// encoder and decoder, bound at run time through its C API.
package openh264

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/native/kit"
)

// Name is the backend's identity: h264/openh264.
const Name = "openh264"

const fix = "brew install openh264 (macOS), apt install libopenh264-7 (Debian, Ubuntu), or download Cisco's binary from github.com/cisco/openh264/releases and pass native.Library(path)"

// symbols are the library's C entry points.
type symbols struct {
	createEncoder  func(out *uintptr) int32 `sym:"WelsCreateSVCEncoder"`
	destroyEncoder func(encoder uintptr)    `sym:"WelsDestroySVCEncoder"`
	createDecoder  func(out *uintptr) int32 `sym:"WelsCreateDecoder"`
	destroyDecoder func(decoder uintptr)    `sym:"WelsDestroyDecoder"`
	versionEx      func(v *version)         `sym:"WelsGetCodecVersionEx"`
}

// library is one bound copy of libopenh264: its symbols, its version and
// structure layout, and the vtables, bound from the first session.
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

// SystemPaths are the absolute paths a system install puts libopenh264 at,
// most specific first: Homebrew on macOS, the distribution packages on
// Linux, and on Windows Cisco's DLL beside the executable.
func SystemPaths() []string {
	switch runtime.GOOS {
	case "darwin":
		return []string{"/opt/homebrew/lib/libopenh264.dylib", "/usr/local/lib/libopenh264.dylib"}
	case "linux":
		var paths []string
		for _, dir := range []string{"/usr/lib/x86_64-linux-gnu", "/usr/lib/aarch64-linux-gnu", "/usr/lib64", "/usr/lib", "/usr/local/lib"} {
			for _, name := range []string{"libopenh264.so.8", "libopenh264.so.7", "libopenh264.so"} {
				paths = append(paths, filepath.Join(dir, name))
			}
		}
		return paths
	case "windows":
		if exe, err := os.Executable(); err == nil {
			return []string{filepath.Join(filepath.Dir(exe), "openh264-2.6.0-win64.dll")}
		}
	}
	return nil
}

// Codec declares the backend bound from the absolute path, or "" when none
// is installed.
func Codec(path string) kit.Codec {
	l := &library{}
	return kit.Codec{
		ID:       av.CodecH264,
		Media:    av.MediaVideo,
		Backend:  Name,
		Kind:     codec.KindNative,
		Module:   "github.com/thesyncim/goav/native",
		Package:  "github.com/thesyncim/goav/native/internal/openh264",
		Profiles: []string{"baseline", "main", "high"},
		Capabilities: codec.Capabilities{
			MaxWidth:  3840,
			MaxHeight: 2160,
		},
		Realtime:     true,
		Knobs:        encoderKnobs,
		DecoderKnobs: decoderKnobs,
		RawKnobs:     rawParams.Knobs(),
		Bitstream:    kit.AnnexB,
		Libraries:    []kit.Library{{Name: "libopenh264", Path: path, Fix: fix, Symbols: &l.symbols}},
		Version:      l.checkVersion,
		NewVideoEncoder: func(config kit.VideoConfig) (kit.VideoEncoder, error) {
			return newEncoder(l, config)
		},
		NewVideoDecoder: func(config kit.VideoConfig) (kit.VideoDecoder, error) {
			return newDecoder(l, config)
		},
	}
}

// checkVersion keeps to the releases whose structure layouts this backend
// mirrors.
func (l *library) checkVersion() (string, error) {
	l.versionEx(&l.version)
	v := l.version
	text := fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Revision)
	switch {
	case v.Major == 2 && v.Minor >= 6:
		l.layout = 26
	case v.Major == 2 && v.Minor >= 3:
		l.layout = 25
	default:
		return text, &kit.Error{Backend: Name, Op: "version", Name: "libopenh264", Reason: text + " is not supported (want 2.3 or a later 2.x)", Fix: fix}
	}
	return text, nil
}

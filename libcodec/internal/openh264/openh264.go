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
	"github.com/thesyncim/goav/libcodec/kit"
)

// Name is the backend's identity: h264/openh264.
const Name = "openh264"

const fix = "brew install openh264 (macOS), apt install libopenh264-7 (Debian, Ubuntu), or download Cisco's binary from github.com/cisco/openh264/releases and pass libcodec.Library(path)"

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
		Kind:     codec.KindLib,
		Module:   "github.com/thesyncim/goav/libcodec",
		Package:  "github.com/thesyncim/goav/libcodec/internal/openh264",
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
		Releases:     ciscoReleases,
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

// ciscoReleases are Cisco's prebuilt binaries: the builds Cisco's H.264
// patent license covers when the end user downloads them from Cisco.
var ciscoReleases = []kit.Release{{
	Version: "2.6.0",
	Files: []kit.ReleaseFile{
		{Platform: "darwin/arm64", URL: "http://ciscobinary.openh264.org/libopenh264-2.6.0-mac-arm64.dylib.bz2", SHA256: "6db362ee5abdab572311aeadb96d3f44b0617d9a4a4b9f4db4cb5ac4d968da71", Name: "libopenh264.8.dylib"},
		{Platform: "darwin/amd64", URL: "http://ciscobinary.openh264.org/libopenh264-2.6.0-mac-x64.dylib.bz2", SHA256: "38b2ed6d1d45b6a3e408c734173f2d67ab44a10d0e154ff3489b89877cd60e7e", Name: "libopenh264.8.dylib"},
		{Platform: "linux/amd64", URL: "http://ciscobinary.openh264.org/libopenh264-2.6.0-linux64.8.so.bz2", SHA256: "27ab53323c110b76214c1c72222f459d17febbcd1e252136cadc292b0308d75b", Name: "libopenh264.so.8"},
		{Platform: "linux/arm64", URL: "http://ciscobinary.openh264.org/libopenh264-2.6.0-linux-arm64.8.so.bz2", SHA256: "a78aea7970150f46bcd3bb7994c9e6dd90bd7a9ea785920f5a73f6964e3fcda7", Name: "libopenh264.so.8"},
		{Platform: "windows/amd64", URL: "http://ciscobinary.openh264.org/openh264-2.6.0-win64.dll.bz2", SHA256: "dab5f2a872777f9a58b69bfa9fbcf20d9f82f2d6ec91383fd70bff49bd34ac9f", Name: "openh264-2.6.0-win64.dll"},
		{Platform: "windows/arm64", URL: "http://ciscobinary.openh264.org/openh264-2.6.0-win-arm64.dll.bz2", SHA256: "6b57d3ecd06bd80b5f3ee1abbb31d48798ca6d89ef1bc98cf2e6d1ebddf5cf45", Name: "openh264-2.6.0-win-arm64.dll"},
	},
}}

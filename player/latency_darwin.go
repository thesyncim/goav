//go:build darwin

package player

import (
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
)

// mainElement is kAudioObjectPropertyElementMain.
const mainElement = 0

// audioObjectPropertyAddress mirrors CoreAudio's AudioObjectPropertyAddress.
type audioObjectPropertyAddress struct {
	selector, scope, element uint32
}

func fourCC(s string) uint32 {
	return uint32(s[0])<<24 | uint32(s[1])<<16 | uint32(s[2])<<8 | uint32(s[3])
}

// hardwareLatency is the default output device's presentation latency as the
// HAL reports it: device latency, safety offset, IO buffer, and the output
// stream's latency (Bluetooth outputs report theirs here). Zero when CoreAudio
// cannot be asked.
func hardwareLatency() (latency time.Duration) {
	defer func() {
		if recover() != nil {
			latency = 0
		}
	}()
	lib, err := purego.Dlopen("/System/Library/Frameworks/CoreAudio.framework/CoreAudio", purego.RTLD_LAZY|purego.RTLD_GLOBAL)
	if err != nil {
		return 0
	}
	var get func(object uint32, address *audioObjectPropertyAddress, qualifierSize uint32, qualifier unsafe.Pointer, size *uint32, data unsafe.Pointer) int32
	purego.RegisterLibFunc(&get, lib, "AudioObjectGetPropertyData")
	read := func(object uint32, selector, scope string, data unsafe.Pointer, size uint32) bool {
		address := audioObjectPropertyAddress{selector: fourCC(selector), scope: fourCC(scope), element: mainElement}
		return get(object, &address, 0, nil, &size, data) == 0
	}
	const systemObject = 1
	var device uint32
	if !read(systemObject, "dOut", "glob", unsafe.Pointer(&device), 4) || device == 0 {
		return 0
	}
	var rate float64
	if !read(device, "nsrt", "glob", unsafe.Pointer(&rate), 8) || rate <= 0 {
		return 0
	}
	var frames uint32
	for _, selector := range []string{"ltnc", "saft", "fsiz"} {
		var v uint32
		if read(device, selector, "outp", unsafe.Pointer(&v), 4) {
			frames += v
		}
	}
	var stream uint32 // the first output stream
	if read(device, "stm#", "outp", unsafe.Pointer(&stream), 4) && stream != 0 {
		var v uint32
		if read(stream, "ltnc", "glob", unsafe.Pointer(&v), 4) {
			frames += v
		}
	}
	return time.Duration(float64(frames) / rate * float64(time.Second))
}

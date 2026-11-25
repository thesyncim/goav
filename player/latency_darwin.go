//go:build darwin

package player

import (
	"time"

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
	var getU32 func(object uint32, address *audioObjectPropertyAddress, qualifierSize uint32, qualifier uintptr, size *uint32, data *uint32) int32
	var getF64 func(object uint32, address *audioObjectPropertyAddress, qualifierSize uint32, qualifier uintptr, size *uint32, data *float64) int32
	purego.RegisterLibFunc(&getU32, lib, "AudioObjectGetPropertyData")
	purego.RegisterLibFunc(&getF64, lib, "AudioObjectGetPropertyData")
	readU32 := func(object uint32, selector, scope string, data *uint32) bool {
		address := audioObjectPropertyAddress{selector: fourCC(selector), scope: fourCC(scope), element: mainElement}
		size := uint32(4)
		return getU32(object, &address, 0, 0, &size, data) == 0
	}
	readF64 := func(object uint32, selector, scope string, data *float64) bool {
		address := audioObjectPropertyAddress{selector: fourCC(selector), scope: fourCC(scope), element: mainElement}
		size := uint32(8)
		return getF64(object, &address, 0, 0, &size, data) == 0
	}
	const systemObject = 1
	var device uint32
	if !readU32(systemObject, "dOut", "glob", &device) || device == 0 {
		return 0
	}
	var rate float64
	if !readF64(device, "nsrt", "glob", &rate) || rate <= 0 {
		return 0
	}
	var frames uint32
	for _, selector := range []string{"ltnc", "saft", "fsiz"} {
		var v uint32
		if readU32(device, selector, "outp", &v) {
			frames += v
		}
	}
	var stream uint32 // the first output stream
	if readU32(device, "stm#", "outp", &stream) && stream != 0 {
		var v uint32
		if readU32(stream, "ltnc", "glob", &v) {
			frames += v
		}
	}
	return time.Duration(float64(frames) / rate * float64(time.Second))
}

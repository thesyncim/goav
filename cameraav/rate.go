package cameraav

import "math/big"

// matchesFrameDuration accepts exact rates and native interval quantization
// smaller than one UVC tick (100 ns). A percentage tolerance would incorrectly
// accept distinct fractional rates such as 30000/1001 for a 30 fps request.
// This is only used during mode negotiation; capture keeps native timestamps.
// UVC interval units: https://docs.kernel.org/usb/gadget_uvc.html
func matchesFrameDuration(value, scale int64, fps int) bool {
	if value <= 0 || scale <= 0 || fps < 1 || fps > 240 {
		return false
	}
	var native, requested, difference, tick big.Rat
	native.SetFrac64(value, scale)
	requested.SetFrac64(1, int64(fps))
	difference.Sub(&native, &requested)
	difference.Abs(&difference)
	tick.SetFrac64(1, 10_000_000)
	return difference.Cmp(&tick) < 0
}

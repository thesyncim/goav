package openh264

import (
	"strconv"
	"strings"

	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/libcodec/kit"
)

var encoderKnobs = func() []codec.Knob {
	bitrate := codec.KnobBitrate
	bitrate.Min = 10_000
	rc := codec.KnobRateControl
	rc.Values = []string{"vbr", "cq", "cqp", "buffer", "timestamp"}
	rc.Help = "vbr targets bitrate, cq trades bitrate for quality, cqp holds qp; buffer and timestamp are OpenH264's own; default vbr with a bitrate, else cqp"
	qp := codec.KnobQP
	qp.Max, qp.Default = 51, "26"
	threads := codec.KnobThreads
	threads.Max, threads.Default = 16, "0"
	tune := codec.KnobTune
	tune.Values, tune.Default = []string{"camera", "screen"}, "camera"
	speed := codec.KnobSpeed
	speed.Default = "5"
	speed.Help = "encoder effort: 0 to 3 high complexity, 4 to 6 medium, 7 to 10 low"
	return []codec.Knob{
		bitrate,
		{Name: "max_bitrate", Type: codec.KnobInt, Min: 10_000, Max: 2_000_000_000, Live: true, Help: "peak bitrate cap"},
		rc, qp, speed, threads, tune, codec.KnobGOP,
		{Name: "profile", Type: codec.KnobEnum, Values: []string{"baseline", "main", "high"}, Default: "high"},
		{Name: "level", Type: codec.KnobEnum, Values: []string{"1.0", "1b", "1.1", "1.2", "1.3", "2.0", "2.1", "2.2", "3.0", "3.1", "3.2", "4.0", "4.1", "4.2", "5.0", "5.1", "5.2"}, Help: "default derived from the geometry"},
		{Name: "entropy", Type: codec.KnobEnum, Values: []string{"cavlc", "cabac"}, Help: "entropy coder; default cabac above baseline"},
		{Name: "refs", Type: codec.KnobInt, Min: 1, Max: 16, Help: "reference frames"},
		{Name: "qp_min", Type: codec.KnobInt, Min: 0, Max: 51},
		{Name: "qp_max", Type: codec.KnobInt, Min: 0, Max: 51},
		{Name: "frame_skip", Type: codec.KnobBool, Default: "false", Help: "let rate control drop frames"},
		{Name: "slices", Type: codec.KnobInt, Min: 0, Max: 35, Default: "0", Help: "slices per picture; 0 is one"},
		{Name: "denoise", Type: codec.KnobBool},
		{Name: "scene_detect", Type: codec.KnobBool},
		{Name: "adaptive_quant", Type: codec.KnobBool},
		{Name: "background_detect", Type: codec.KnobBool},
		{Name: "deblock", Type: codec.KnobEnum, Values: []string{"on", "off", "slice"}, Default: "on", Help: "loop filter; slice keeps it inside slice edges"},
		{Name: "log", Type: codec.KnobEnum, Values: []string{"quiet", "error", "warning", "info", "debug"}, Default: "quiet", Help: "library log level, written to stderr"},
	}
}()

var decoderKnobs = func() []codec.Knob {
	threads := codec.KnobThreads
	threads.Max, threads.Default = 16, "0"
	return []codec.Knob{
		{Name: "conceal", Type: codec.KnobEnum, Values: []string{"off", "frame", "slice"}, Default: "off", Help: "error concealment for damaged input"},
		threads,
		{Name: "log", Type: codec.KnobEnum, Values: []string{"quiet", "error", "warning", "info", "debug"}, Default: "quiet"},
	}
}()

// rawParams reaches every scalar field of SEncParamExt and of its first
// spatial layer as a raw parameter by C name: openh264-params="iMaxQp=40".
var rawParams = kit.ParamsOf[encParamExt]()

var logLevels = map[string]int32{"quiet": 0, "error": 1, "warning": 2, "info": 4, "debug": 8}

var rcModes = map[string]int32{"cq": 0, "vbr": 1, "buffer": 2, "timestamp": 3, "cqp": -1}

var profiles = map[string]int32{"baseline": 66, "main": 77, "high": 100}

// complexity maps the common speed scale onto ECOMPLEXITY_MODE.
func complexity(speed int) int32 {
	switch {
	case speed <= 3:
		return 2 // HIGH_COMPLEXITY
	case speed <= 6:
		return 1
	}
	return 0
}

// levelIdc reads a level name: 3.1 is 31, 1b is 9.
func levelIdc(level string) int32 {
	if level == "1b" {
		return 9
	}
	major, minor, _ := strings.Cut(level, ".")
	a, _ := strconv.Atoi(major)
	b, _ := strconv.Atoi(minor)
	return int32(a*10 + b)
}

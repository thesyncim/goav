package openh264

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/codec"
)

var encoderKnobs = []codec.Knob{
	{Name: "bitrate", Type: codec.KnobInt, Min: 10_000, Max: 2_000_000_000, Live: true, Help: "target bitrate in bits per second (2M, 800k); unset encodes for quality"},
	{Name: "max_bitrate", Type: codec.KnobInt, Min: 10_000, Max: 2_000_000_000, Live: true, Help: "peak bitrate cap"},
	{Name: "rc", Type: codec.KnobEnum, Values: []string{"quality", "bitrate", "buffer", "timestamp", "off"}, Help: "rate control; default bitrate when a bitrate is set, else quality"},
	{Name: "gop", Type: codec.KnobInt, Min: 0, Max: 1 << 20, Help: "frames between IDR keyframes; 0 keeps only the first"},
	{Name: "profile", Type: codec.KnobEnum, Values: []string{"baseline", "main", "high"}, Default: "high"},
	{Name: "level", Type: codec.KnobEnum, Values: []string{"1.0", "1b", "1.1", "1.2", "1.3", "2.0", "2.1", "2.2", "3.0", "3.1", "3.2", "4.0", "4.1", "4.2", "5.0", "5.1", "5.2"}, Help: "level; default derived from the geometry"},
	{Name: "threads", Type: codec.KnobInt, Min: 0, Max: 16, Default: "0", Help: "encoder threads; 0 picks per core"},
	{Name: "content", Type: codec.KnobEnum, Values: []string{"camera", "screen"}, Default: "camera", Help: "content tuning"},
	{Name: "complexity", Type: codec.KnobEnum, Values: []string{"low", "medium", "high"}, Default: "medium", Help: "speed against quality"},
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

var decoderKnobs = []codec.Knob{
	{Name: "conceal", Type: codec.KnobEnum, Values: []string{"off", "frame", "slice"}, Default: "off", Help: "error concealment for damaged input"},
	{Name: "threads", Type: codec.KnobInt, Min: 0, Max: 16, Default: "0"},
	{Name: "log", Type: codec.KnobEnum, Values: []string{"quiet", "error", "warning", "info", "debug"}, Default: "quiet"},
}

var logLevels = map[string]int32{"quiet": 0, "error": 1, "warning": 2, "info": 4, "debug": 8}

var rcModes = map[string]int32{"quality": 0, "bitrate": 1, "buffer": 2, "timestamp": 3, "off": -1}

var profiles = map[string]int32{"baseline": 66, "main": 77, "high": 100}

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

// rawField is one SEncParamExt (or first spatial layer) field the raw
// parameters can set, by its C name.
type rawField struct {
	path  []int
	kind  reflect.Kind
	layer bool
}

var rawFields = func() map[string]rawField {
	fields := map[string]rawField{}
	collect := func(t reflect.Type, prefix []int, layer bool) {
		for i := range t.NumField() {
			f := t.Field(i)
			name := f.Tag.Get("c")
			if name == "" {
				continue
			}
			fields[name] = rawField{path: append(append([]int(nil), prefix...), i), kind: f.Type.Kind(), layer: layer}
		}
	}
	collect(reflect.TypeFor[encParamExt](), nil, false)
	collect(reflect.TypeFor[spatialLayer](), nil, true)
	slices, _ := reflect.TypeFor[spatialLayer]().FieldByName("Slices")
	collect(slices.Type, slices.Index, true)
	return fields
}()

// rawKnobs is the raw schema: every scalar field of SEncParamExt and of its
// first spatial layer, by C name, so no encoder parameter is out of reach.
func rawKnobs() []codec.Knob {
	knobs := make([]codec.Knob, 0, len(rawFields))
	for name, field := range rawFields {
		knob := codec.Knob{Name: name, Type: codec.KnobInt}
		switch field.kind {
		case reflect.Bool:
			knob.Type = codec.KnobBool
		case reflect.Float32:
			knob.Type = codec.KnobFloat
		}
		knobs = append(knobs, knob)
	}
	sortKnobs(knobs)
	return knobs
}

func sortKnobs(knobs []codec.Knob) {
	for i := 1; i < len(knobs); i++ {
		for j := i; j > 0 && knobs[j].Name < knobs[j-1].Name; j-- {
			knobs[j], knobs[j-1] = knobs[j-1], knobs[j]
		}
	}
}

// applyRaw sets one already-validated raw parameter.
func applyRaw(param *encParamExt, key, value string) error {
	field, ok := rawFields[key]
	if !ok {
		return fmt.Errorf("openh264: no raw parameter %s", key)
	}
	target := reflect.ValueOf(param).Elem()
	if field.layer {
		target = target.FieldByName("SpatialLayers").Index(0)
	}
	v := target.FieldByIndex(field.path)
	switch field.kind {
	case reflect.Bool:
		v.SetBool(value == "true")
	case reflect.Float32:
		f, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return err
		}
		v.SetFloat(f)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		n, err := strconv.ParseUint(value, 10, v.Type().Bits())
		if err != nil {
			return fmt.Errorf("openh264: raw parameter %s=%s: %w", key, value, err)
		}
		v.SetUint(n)
	default:
		n, err := strconv.ParseInt(value, 10, v.Type().Bits())
		if err != nil {
			return fmt.Errorf("openh264: raw parameter %s=%s: %w", key, value, err)
		}
		v.SetInt(n)
	}
	return nil
}

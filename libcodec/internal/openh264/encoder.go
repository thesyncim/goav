package openh264

import (
	"runtime"
	"strconv"
	"unsafe"

	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/libcodec/kit"
)

// encoder is one ISVCEncoder session. OpenH264 encodes synchronously and
// keeps no reference to the source picture after EncodeFrame returns, so
// the lent planes are read in place.
type encoder struct {
	lib    *library
	handle uintptr
	vt     *encoderVtable
	param  encParamExt
	src    sourcePicture
	info   unsafe.Pointer // frameBSInfo or frameBSInfo26
	keep   any            // keeps info's backing struct reachable
	config []byte
	data   []byte
	base   int64 // TimeBase.Den, for millisecond timestamps
	num    int64
}

func (l *library) encoderTable(handle uintptr) (*encoderVtable, error) {
	l.encoderOnce.Do(func() {
		l.encoderErr = kit.BindVTable(handle, &l.encoder)
	})
	return &l.encoder, l.encoderErr
}

func newEncoder(l *library, config kit.VideoConfig) (kit.VideoEncoder, error) {
	e := &encoder{lib: l, base: config.TimeBase.Den, num: config.TimeBase.Num}
	if l.layout == 26 {
		info := new(frameBSInfo26)
		e.info, e.keep = unsafe.Pointer(info), info
	} else {
		info := new(frameBSInfo)
		e.info, e.keep = unsafe.Pointer(info), info
	}
	if status := l.createEncoder(&e.handle); status != 0 || e.handle == 0 {
		return nil, kit.Fail(Name, "open", "WelsCreateSVCEncoder", int64(status))
	}
	vt, err := l.encoderTable(vtableOf(e.handle))
	if err != nil {
		l.destroyEncoder(e.handle)
		return nil, err
	}
	e.vt = vt
	if err := e.initialize(config); err != nil {
		e.Close()
		return nil, err
	}
	return e, nil
}

// vtableOf reads the vtable pointer an ISVCEncoder or ISVCDecoder starts
// with.
func vtableOf(object uintptr) uintptr {
	return **(**uintptr)(unsafe.Pointer(&object))
}

func (e *encoder) initialize(config kit.VideoConfig) error {
	p := &e.param
	if status := e.vt.GetDefaultParams(e.handle, p); status != 0 {
		return kit.Fail(Name, "open", "GetDefaultParams", int64(status))
	}
	knobs := config.Knobs
	fps := float32(30)
	if config.FrameDuration > 0 && config.TimeBase.Num > 0 {
		fps = float32(float64(config.TimeBase.Den) / float64(config.TimeBase.Num*config.FrameDuration))
	}
	// OpenH264 is a real-time encoder: CAMERA_VIDEO_REAL_TIME and
	// SCREEN_CONTENT_REAL_TIME are the usages it accepts.
	p.UsageType = 0
	if knobs.String("tune") == "screen" {
		p.UsageType = 1
	}
	p.PicWidth, p.PicHeight = int32(config.Width), int32(config.Height)
	p.MaxFrameRate = fps
	p.TemporalLayerNum, p.SpatialLayerNum = 1, 1
	bitrate := knobs.Int32("bitrate")
	p.TargetBitrate = bitrate
	rc := knobs.String("rc")
	if rc == "" {
		rc = "cqp"
		if bitrate > 0 {
			rc = "vbr"
		}
	}
	p.RCMode = rcModes[rc]
	p.MaxBitrate = knobs.Int32("max_bitrate")
	p.IntraPeriod = knobs.Uint32("gop")
	p.SpsPpsIDStrategy = constantID
	p.PrefixNalAddingCtrl = false
	p.EnableFrameSkip = knobs.Bool("frame_skip")
	p.MultipleThreadIdc = knobs.Uint16("threads")
	p.ComplexityMode = complexity(knobs.Int("speed"))
	profile := knobs.String("profile")
	p.EntropyCodingMode = 0
	if entropy := knobs.String("entropy"); entropy == "cabac" || (entropy == "" && profile != "baseline") {
		p.EntropyCodingMode = 1
	}
	if knobs.IsSet("refs") {
		p.NumRefFrame = knobs.Int32("refs")
	}
	if knobs.IsSet("qp_min") {
		p.MinQp = knobs.Int32("qp_min")
	}
	if knobs.IsSet("qp_max") {
		p.MaxQp = knobs.Int32("qp_max")
	}
	for name, field := range map[string]*bool{
		"denoise": &p.EnableDenoise, "scene_detect": &p.EnableSceneChange,
		"adaptive_quant": &p.EnableAdaptiveQuant, "background_detect": &p.EnableBackground,
	} {
		if knobs.IsSet(name) {
			*field = knobs.Bool(name)
		}
	}
	p.LoopFilterDisableIdc = map[string]int32{"on": 0, "off": 1, "slice": 2}[knobs.String("deblock")]
	layer := &p.SpatialLayers[0]
	layer.VideoWidth, layer.VideoHeight = p.PicWidth, p.PicHeight
	layer.FrameRate = fps
	layer.SpatialBitrate = bitrate
	layer.MaxSpatialBitrate = p.MaxBitrate
	layer.DLayerQp = knobs.Int32("qp")
	layer.ProfileIdc = profiles[profile]
	if level := knobs.String("level"); level != "" {
		layer.LevelIdc = levelIdc(level)
	}
	layer.Slices.SliceMode = sliceSingle
	if n := knobs.Uint32("slices"); n > 1 {
		layer.Slices.SliceMode, layer.Slices.SliceNum = sliceFixedNum, n
	}
	if err := rawParams.Apply(p, knobs.Raw()); err != nil {
		return err
	}
	trace := logLevels[knobs.String("log")]
	e.vt.SetOption(e.handle, optionTraceLevel, unsafe.Pointer(&trace))
	if status := e.vt.InitializeExt(e.handle, p); status != 0 {
		return &kit.Error{Backend: Name, Op: "open", Name: "InitializeExt", Reason: "refused the parameters (status " + strconv.Itoa(int(status)) + ")", Fix: "check the knobs and raw parameters against OpenH264's limits for this geometry; log=debug shows its reason"}
	}
	format := int32(videoFormatI420)
	if status := e.vt.SetOption(e.handle, optionDataFormat, unsafe.Pointer(&format)); status != 0 {
		return kit.Fail(Name, "open", "SetOption(DATAFORMAT)", int64(status))
	}
	if status := e.vt.EncodeParameterSets(e.handle, e.info); status != 0 {
		return kit.Fail(Name, "open", "EncodeParameterSets", int64(status))
	}
	e.config = append([]byte(nil), e.bitstream()...)
	return nil
}

// Config is the SPS and PPS, Annex B: the kit derives the avcC record.
func (e *encoder) Config() []byte { return e.config }

// bitstream concatenates the NAL units of the last encode's layers.
func (e *encoder) bitstream() []byte {
	e.data = e.data[:0]
	appendLayer := func(layer *layerInfo) {
		if layer.NalCount <= 0 || layer.BsBuf == nil {
			return
		}
		lengths := unsafe.Slice(layer.NalLengthInByte, layer.NalCount)
		size := 0
		for _, n := range lengths {
			size += int(n)
		}
		e.data = append(e.data, unsafe.Slice(layer.BsBuf, size)...)
	}
	if e.lib.layout == 26 {
		info := (*frameBSInfo26)(e.info)
		for i := range min(int(info.LayerNum), maxLayers) {
			appendLayer(&info.Layers[i].layerInfo)
		}
	} else {
		info := (*frameBSInfo)(e.info)
		for i := range min(int(info.LayerNum), maxLayers) {
			appendLayer(&info.Layers[i])
		}
	}
	return e.data
}

func (e *encoder) frameType() int32 {
	if e.lib.layout == 26 {
		return (*frameBSInfo26)(e.info).FrameType
	}
	return (*frameBSInfo)(e.info).FrameType
}

func (e *encoder) Encode(pic *kit.Picture, emit func(*kit.Packet) error) error {
	if pic.Keyframe {
		e.vt.ForceIntraFrame(e.handle, true, -1)
	}
	src := &e.src
	*src = sourcePicture{ColorFormat: videoFormatI420, PicWidth: int32(pic.Width), PicHeight: int32(pic.Height)}
	var pinner runtime.Pinner
	defer pinner.Unpin()
	for i := range 3 {
		pinner.Pin(&pic.Planes[i][0])
		src.Data[i] = unsafe.Pointer(&pic.Planes[i][0])
		src.Stride[i] = int32(pic.Strides[i])
	}
	if e.base > 0 {
		src.TimeStamp = pic.PTS * 1000 * e.num / e.base
	}
	if status := e.vt.EncodeFrame(e.handle, src, e.info); status != 0 {
		return kit.Fail(Name, "encode", "EncodeFrame", int64(status))
	}
	switch e.frameType() {
	case frameTypeSkip, frameTypeInvalid:
		return nil
	}
	data := e.bitstream()
	if len(data) == 0 {
		return nil
	}
	return emit(&kit.Packet{Data: data, PTS: pic.PTS, DTS: pic.PTS, Keyframe: e.frameType() == frameTypeIDR})
}

// Flush has nothing to drain: OpenH264 holds no frames back.
func (e *encoder) Flush(func(*kit.Packet) error) error { return nil }

// Update applies a bitrate change while running.
func (e *encoder) Update(knob string, values codec.KnobValues) error {
	option, value := int32(optionBitrate), values.Int32(knob)
	switch knob {
	case "bitrate":
	case "max_bitrate":
		option = optionMaxBitrate
	default:
		return &kit.Error{Backend: Name, Op: "control", Name: knob, Reason: "cannot change while running"}
	}
	info := bitrateInfo{Layer: spatialLayerAll, Bitrate: value}
	if status := e.vt.SetOption(e.handle, option, unsafe.Pointer(&info)); status != 0 {
		return kit.Fail(Name, "control", "SetOption("+knob+")", int64(status))
	}
	return nil
}

func (e *encoder) Close() error {
	if e.handle == 0 {
		return nil
	}
	if e.vt != nil {
		e.vt.Uninitialize(e.handle)
	}
	e.lib.destroyEncoder(e.handle)
	e.handle = 0
	return nil
}

package videotoolbox

import (
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/libcodec/kit"
)

func encoderKnobs(id av.CodecID) []codec.Knob {
	rc := codec.KnobRateControl
	rc.Values = []string{"vbr", "cbr", "cq"}
	rc.Help = "vbr averages the bitrate, cbr holds it (macOS 13 and later), cq encodes at quality; default vbr with a bitrate, else the encoder's own"
	speed := codec.KnobSpeed
	speed.Help = "6 and above prioritize encoding speed over quality"
	profile := codec.Knob{Name: "profile", Type: codec.KnobEnum, Values: profilesOf(id), Default: profilesOf(id)[len(profilesOf(id))-1]}
	if id == hevc {
		profile.Default = "main"
	}
	knobs := []codec.Knob{
		codec.KnobBitrate,
		{Name: "max_bitrate", Type: codec.KnobInt, Min: 10_000, Max: 2_000_000_000, Help: "peak bitrate over one second (DataRateLimits)"},
		rc, codec.KnobQuality, speed, codec.KnobGOP, codec.KnobRealtime, profile,
		{Name: "bframes", Type: codec.KnobBool, Default: "false", Help: "allow frame reordering; off for low latency"},
	}
	if id == av.CodecH264 {
		knobs = append(knobs,
			codec.Knob{Name: "level", Type: codec.KnobEnum, Values: []string{"auto", "3.0", "3.1", "3.2", "4.0", "4.1", "4.2", "5.0", "5.1", "5.2"}, Default: "auto"},
			codec.Knob{Name: "entropy", Type: codec.KnobEnum, Values: []string{"cavlc", "cabac"}, Help: "entropy coder; default cabac above baseline"},
		)
	}
	return knobs
}

// encoder is one VTCompressionSession. VideoToolbox encodes asynchronously
// and reports each frame through the output callback, on its own thread:
// the callback retains the sample buffer and queues it, and the session's
// own goroutine turns queued buffers into packets.
type encoder struct {
	f        *frameworks
	id       av.CodecID
	session  uintptr
	handle   uintptr
	pool     uintptr
	keyframe uintptr // frame properties forcing a keyframe
	num, den int64
	realtime bool
	data     []byte

	mu     sync.Mutex
	queue  []output
	closed bool
}

// output is one frame the callback delivered.
type output struct {
	status int32
	sample uintptr
}

// encoded is VTCompressionOutputCallback.
func encoded(refcon, _ uintptr, status int32, _ uint32, sample uintptr) {
	e, ok := encoderSessions.Get(refcon)
	if !ok {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	if sample != 0 {
		e.f.cf.retain(sample)
	}
	e.queue = append(e.queue, output{status: status, sample: sample})
}

func newEncoder(f *frameworks, id av.CodecID, config kit.VideoConfig) (kit.VideoEncoder, error) {
	e := &encoder{f: f, id: id, num: max(config.TimeBase.Num, 1), den: config.TimeBase.Den, realtime: config.Knobs.Bool("realtime")}
	e.handle = encoderSessions.New(e)
	spec := f.dictionary()
	defer f.cf.release(spec)
	f.cf.dictionarySet(spec, f.vt.requireHardwareEncoder, f.cf.booleanTrue)
	if e.realtime && id == av.CodecH264 && f.vt.lowLatencyRateControl != 0 {
		f.cf.dictionarySet(spec, f.vt.lowLatencyRateControl, f.cf.booleanTrue)
	}
	source := f.dictionary()
	defer f.cf.release(source)
	f.setOwned(source, f.cv.formatKey, f.int32Number(pixelI420))
	f.setOwned(source, f.cv.surfaceKey, f.dictionary())
	if s := f.vt.compressionCreate(0, int32(config.Width), int32(config.Height), codecType(id), spec, source, 0, encodedCallback(), e.handle, &e.session); s != 0 {
		encoderSessions.Delete(e.handle)
		return nil, &kit.Error{Backend: Name, Op: "open", Name: "VTCompressionSessionCreate", Reason: "refused " + strconv.Itoa(config.Width) + "x" + strconv.Itoa(config.Height) + " (status " + status(s) + ")", Fix: "VideoToolbox's hardware encoders take even sizes up to 4096x2304"}
	}
	if err := e.configure(config); err != nil {
		e.Close()
		return nil, err
	}
	if s := f.vt.prepare(e.session); s != 0 {
		e.Close()
		return nil, kit.Fail(Name, "open", "VTCompressionSessionPrepareToEncodeFrames", int64(s))
	}
	e.pool = f.vt.pool(e.session)
	e.keyframe = f.dictionary()
	f.cf.dictionarySet(e.keyframe, f.vt.forceKeyFrame, f.cf.booleanTrue)
	return e, nil
}

func (e *encoder) set(name string, key, value uintptr, owned bool) error {
	if owned {
		defer e.f.cf.release(value)
	}
	if key == 0 {
		return &kit.Error{Backend: Name, Op: "open", Name: name, Reason: "is not offered by this macOS", Fix: "leave " + name + " unset, or update macOS"}
	}
	if s := e.f.vt.setProperty(e.session, key, value); s != 0 {
		return &kit.Error{Backend: Name, Op: "open", Name: name, Reason: "was refused (status " + status(s) + ")"}
	}
	return nil
}

func (e *encoder) configure(config kit.VideoConfig) error {
	f, knobs := e.f, config.Knobs
	if err := e.set("realtime", f.vt.keyRealTime, f.boolean(e.realtime), false); err != nil {
		return err
	}
	if err := e.set("bframes", f.vt.keyReordering, f.boolean(knobs.Bool("bframes") && !e.realtime), false); err != nil {
		return err
	}
	level := knobs.String("level")
	if level == "" || level == "auto" {
		level = "AutoLevel"
	}
	profile := knobs.String("profile")
	name := "kVTProfileLevel_H264_" + strings.ToUpper(profile[:1]) + profile[1:] + "_" + strings.ReplaceAll(level, ".", "_")
	if e.id == hevc {
		name = "kVTProfileLevel_HEVC_" + strings.ToUpper(profile[:1]) + profile[1:] + "_AutoLevel"
	}
	profileLevel, ok := f.vtLibrary.Value(name)
	if !ok {
		return &kit.Error{Backend: Name, Op: "open", Name: "profile", Reason: profile + " at level " + level + " is not offered by this macOS (" + name + ")"}
	}
	if err := e.set("profile", f.vt.keyProfileLevel, profileLevel, false); err != nil {
		return err
	}
	bitrate := knobs.Int("bitrate")
	switch rc := knobs.String("rc"); {
	case rc == "cq":
		if err := e.set("quality", f.vt.keyQuality, f.float64Number(float64(knobs.Int("quality"))/100), true); err != nil {
			return err
		}
	case rc == "cbr":
		if bitrate <= 0 {
			return &kit.Error{Backend: Name, Op: "open", Name: "rc", Reason: "cbr needs a bitrate", Fix: "set codec.Bitrate(...)"}
		}
		if err := e.set("rc=cbr", f.vt.keyConstantBitRate, f.int64Number(int64(bitrate)), true); err != nil {
			return err
		}
	case bitrate > 0:
		if err := e.set("bitrate", f.vt.keyAverageBitRate, f.int64Number(int64(bitrate)), true); err != nil {
			return err
		}
	}
	if peak := knobs.Int("max_bitrate"); peak > 0 {
		limits := []uintptr{f.int64Number(int64(peak / 8)), f.float64Number(1)}
		array := f.cf.arrayCreate(0, &limits[0], len(limits), f.cf.arrayCallbacks)
		f.cf.release(limits[0])
		f.cf.release(limits[1])
		if err := e.set("max_bitrate", f.vt.keyDataRateLimits, array, true); err != nil {
			return err
		}
	}
	if gop := knobs.Int32("gop"); gop > 0 {
		if err := e.set("gop", f.vt.keyMaxKeyFrameInterval, f.int32Number(gop), true); err != nil {
			return err
		}
	}
	if config.FrameDuration > 0 {
		fps := float64(config.TimeBase.Den) / float64(e.num*config.FrameDuration)
		if err := e.set("fps", f.vt.keyExpectedFrameRate, f.float64Number(fps), true); err != nil {
			return err
		}
	}
	if knobs.IsSet("speed") {
		if err := e.set("speed", f.vt.keySpeedOverQuality, f.boolean(knobs.Int("speed") >= 6), false); err != nil {
			return err
		}
	}
	if e.id == av.CodecH264 {
		entropy := f.vt.entropyCABAC
		if mode := knobs.String("entropy"); mode == "cavlc" || (mode == "" && profile == "baseline") {
			entropy = f.vt.entropyCAVLC
		}
		if err := e.set("entropy", f.vt.keyEntropyMode, entropy, false); err != nil {
			return err
		}
	}
	return e.raw(knobs.Raw())
}

// raw sets compression properties by name, the tier past the knobs:
// videotoolbox-params="MaxKeyFrameIntervalDuration=2:AllowOpenGOP=false".
// A value is a boolean, a number, a kVT/kCV/kCM constant's name, or text.
func (e *encoder) raw(params [][2]string) error {
	f := e.f
	for _, kv := range params {
		key, ok := f.vtLibrary.Value("kVTCompressionPropertyKey_" + kv[0])
		if !ok {
			return &kit.Error{Backend: Name, Op: "open", Name: kv[0], Reason: "is not a VideoToolbox compression property on this macOS", Fix: "name it as kVTCompressionPropertyKey_<name> without the prefix"}
		}
		var value uintptr
		owned := true
		switch v := kv[1]; {
		case v == "true" || v == "false":
			value, owned = f.boolean(v == "true"), false
		case strings.HasPrefix(v, "kVT") || strings.HasPrefix(v, "kCV") || strings.HasPrefix(v, "kCM"):
			constant, found := f.vtLibrary.Value(v)
			if !found {
				return &kit.Error{Backend: Name, Op: "open", Name: kv[0], Reason: "names " + v + ", which this macOS lacks"}
			}
			value, owned = constant, false
		default:
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				value = f.int64Number(n)
			} else if x, err := strconv.ParseFloat(v, 64); err == nil {
				value = f.float64Number(x)
			} else {
				value = f.cfString(v)
			}
		}
		if err := e.set(kv[0], key, value, owned); err != nil {
			return err
		}
	}
	return nil
}

// Update changes the bitrate while running.
func (e *encoder) Update(knob string, values codec.KnobValues) error {
	if knob != "bitrate" {
		return &kit.Error{Backend: Name, Op: "control", Name: knob, Reason: "cannot change while running"}
	}
	key := e.f.vt.keyAverageBitRate
	if values.String("rc") == "cbr" {
		key = e.f.vt.keyConstantBitRate
	}
	return e.set(knob, key, e.f.int64Number(int64(values.Int(knob))), true)
}

func (e *encoder) Encode(pic *kit.Picture, emit func(*kit.Packet) error) error {
	f := e.f
	if e.session == 0 {
		return codec.ErrClosed
	}
	buffer, err := e.pixelBuffer(pic.Width, pic.Height)
	if err != nil {
		return err
	}
	if err := e.fill(buffer, pic); err != nil {
		f.cf.release(buffer)
		return err
	}
	properties := uintptr(0)
	if pic.Keyframe {
		properties = e.keyframe
	}
	duration := cmTime{}
	if pic.Duration > 0 {
		duration = timeAt(pic.Duration*e.num, e.den)
	}
	var flags uint32
	s := f.vt.encodeFrame(e.session, buffer, timeAt(pic.PTS*e.num, e.den), duration, properties, uintptr(pic.PTS), &flags)
	f.cf.release(buffer)
	if s != 0 {
		return kit.Fail(Name, "encode", "VTCompressionSessionEncodeFrame", int64(s))
	}
	if e.realtime {
		// Low latency: this frame's packet leaves with this call.
		f.vt.complete(e.session, timeAt(pic.PTS*e.num, e.den))
	}
	return e.drain(emit)
}

func (e *encoder) Flush(emit func(*kit.Packet) error) error {
	e.f.vt.complete(e.session, cmTime{})
	return e.drain(emit)
}

// drain turns the frames the callback queued into packets, in the order it
// delivered them: decode order.
func (e *encoder) drain(emit func(*kit.Packet) error) error {
	e.mu.Lock()
	queue := e.queue
	e.queue = nil
	e.mu.Unlock()
	defer func() {
		for _, o := range queue {
			if o.sample != 0 {
				e.f.cf.release(o.sample)
			}
		}
	}()
	for i := range queue {
		o := &queue[i]
		if o.status != 0 {
			return kit.Fail(Name, "encode", "the compression output callback", int64(o.status))
		}
		if o.sample == 0 {
			continue // dropped
		}
		packet, err := e.packet(o.sample)
		if err != nil {
			return err
		}
		if err := emit(&packet); err != nil {
			return err
		}
	}
	return nil
}

func (e *encoder) packet(sample uintptr) (kit.Packet, error) {
	f := e.f
	var timing cmTimingInfo
	f.cm.timing(sample, 0, &timing)
	pts := timing.PTS.Value / e.num
	dts := pts
	if timing.DTS.Flags&timeValid != 0 {
		dts = timing.DTS.Value / e.num
	}
	keyframe := true
	if attachments := f.cm.attachments(sample, false); attachments != 0 && f.cf.arrayCount(attachments) > 0 {
		if notSync := f.cf.dictionaryGet(f.cf.arrayAt(attachments, 0), f.cm.notSyncKey); notSync != 0 && f.cf.booleanValue(notSync) {
			keyframe = false
		}
	}
	block := f.cm.dataBuffer(sample)
	n := f.cm.blockLength(block)
	if cap(e.data) < n {
		e.data = make([]byte, n)
	}
	e.data = e.data[:n]
	if n > 0 {
		if s := f.cm.blockCopy(block, 0, n, unsafe.Pointer(&e.data[0])); s != 0 {
			return kit.Packet{}, kit.Fail(Name, "encode", "CMBlockBufferCopyDataBytes", int64(s))
		}
	}
	packet := kit.Packet{Data: e.data, PTS: pts, DTS: dts, Keyframe: keyframe}
	if keyframe {
		packet.Config = e.configRecord(sample)
	}
	return packet, nil
}

// configRecord is the avcC or hvcC record of the sample's format.
func (e *encoder) configRecord(sample uintptr) []byte {
	f := e.f
	atoms := f.cm.extension(f.cm.formatOf(sample), f.cm.atomsKey)
	if atoms == 0 {
		return nil
	}
	name := "avcC"
	if e.id == hevc {
		name = "hvcC"
	}
	key := f.cfString(name)
	defer f.cf.release(key)
	data := f.cf.dictionaryGet(atoms, key)
	if data == 0 {
		return nil
	}
	return f.bytesOf(data)
}

func (e *encoder) Close() error {
	if e.session == 0 {
		return nil
	}
	f := e.f
	f.vt.complete(e.session, cmTime{})
	f.vt.compressionEnd(e.session)
	f.cf.release(e.session)
	e.session, e.pool = 0, 0
	e.mu.Lock()
	e.closed = true
	queue := e.queue
	e.queue = nil
	e.mu.Unlock()
	for _, o := range queue {
		if o.sample != 0 {
			f.cf.release(o.sample)
		}
	}
	if e.keyframe != 0 {
		f.cf.release(e.keyframe)
	}
	encoderSessions.Delete(e.handle)
	return nil
}

// pixelBuffer takes a buffer from the session's pool, or makes one directly
// when VideoToolbox has none to give.
func (e *encoder) pixelBuffer(width, height int) (uintptr, error) {
	f := e.f
	// The session can replace its pool, so it is asked for each frame.
	e.pool = f.vt.pool(e.session)
	var buffer uintptr
	if e.pool != 0 {
		if s := f.cv.poolCreate(0, e.pool, &buffer); s != 0 {
			return 0, kit.Fail(Name, "encode", "CVPixelBufferPoolCreatePixelBuffer", int64(s))
		}
		return buffer, nil
	}
	attributes := f.dictionary()
	defer f.cf.release(attributes)
	f.setOwned(attributes, f.cv.surfaceKey, f.dictionary())
	if s := f.cv.create(0, width, height, pixelI420, attributes, &buffer); s != 0 {
		return 0, kit.Fail(Name, "encode", "CVPixelBufferCreate", int64(s))
	}
	return buffer, nil
}

const (
	pixelNV12Video = 0x34323076 // '420v'
	pixelNV12Full  = 0x34323066 // '420f'
)

// fill copies an I420 picture into a pixel buffer: planar y420 as is, or the
// semi-planar 420v/420f VideoToolbox can switch its pool to mid-session,
// interleaving the chroma.
func (e *encoder) fill(buffer uintptr, pic *kit.Picture) error {
	f := e.f
	format, planes := f.cv.pixelFormat(buffer), f.cv.planeCount(buffer)
	semi := format == pixelNV12Video || format == pixelNV12Full
	if !(format == pixelI420 && planes == 3) && !(semi && planes == 2) || f.cv.width(buffer) < pic.Width || f.cv.height(buffer) < pic.Height {
		return &kit.Error{Backend: Name, Op: "encode", Reason: "the session's pixel buffers are " + fourCCString(format) + ", not y420 or 420v at the frame's size"}
	}
	f.cv.lock(buffer, 0)
	defer f.cv.unlock(buffer, 0)
	cw := (pic.Width + 1) / 2
	for i := range planes {
		rows, stride := f.cv.planeHeight(buffer, i), f.cv.planeStride(buffer, i)
		dst := unsafe.Slice((*byte)(f.cv.planeBase(buffer, i)), stride*rows)
		for row := range rows {
			line := dst[row*stride : (row+1)*stride]
			switch {
			case i == 0:
				copy(line[:pic.Width], pic.Planes[0][row*pic.Strides[0]:])
			case semi:
				u, v := pic.Planes[1][row*pic.Strides[1]:], pic.Planes[2][row*pic.Strides[2]:]
				for x := range cw {
					line[2*x], line[2*x+1] = u[x], v[x]
				}
			default:
				copy(line[:cw], pic.Planes[i][row*pic.Strides[i]:])
			}
		}
	}
	return nil
}

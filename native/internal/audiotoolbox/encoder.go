package audiotoolbox

import (
	"runtime"
	"strconv"
	"unsafe"

	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/native/kit"
)

// encoder is one AudioConverter from interleaved s16 PCM to AAC-LC. The
// converter pulls its input through the input procedure, which hands it
// whole 1024-sample frames out of a FIFO; each pull returns one packet.
type encoder struct {
	sym      *symbols
	conv     uintptr
	handle   uintptr
	channels int
	config   []byte
	fifo     []int16
	feed     []int16
	out      []byte
	list     bufferList
	desc     packetDesc
	priming  int64
	next     int64 // PTS of the next packet
	started  bool
	draining bool
}

func newEncoder(sym *symbols, config kit.AudioConfig) (kit.AudioEncoder, error) {
	if config.Channels < 1 || config.Channels > 2 {
		return nil, &kit.Error{Backend: Name, Op: "open", Reason: "encodes mono or stereo, not " + strconv.Itoa(config.Channels) + " channels", Fix: "insert .Resample(rate, 2) before .Encode(...)"}
	}
	src, dst := pcmDescription(config.SampleRate, config.Channels), aacDescription(config.SampleRate, config.Channels)
	e := &encoder{sym: sym, channels: config.Channels, config: audioSpecificConfig(config.SampleRate, config.Channels)}
	if s := sym.newConverter(&src, &dst, &e.conv); s != 0 {
		return nil, &kit.Error{Backend: Name, Op: "open", Name: "AudioConverterNew", Reason: "refused " + strconv.Itoa(config.SampleRate) + " Hz x" + strconv.Itoa(config.Channels) + " (status " + status(s) + ")"}
	}
	if err := e.configure(config.Knobs); err != nil {
		sym.dispose(e.conv)
		return nil, err
	}
	var prime [2]uint32
	size := uint32(unsafe.Sizeof(prime))
	if sym.getProperty(e.conv, propertyPrimeInfo, &size, unsafe.Pointer(&prime)) == 0 {
		e.priming = int64(prime[0])
	}
	maxPacket := uint32(0)
	size = 4
	sym.getProperty(e.conv, propertyMaxPacket, &size, unsafe.Pointer(&maxPacket))
	e.out = make([]byte, max(int(maxPacket), 6144*config.Channels))
	e.feed = make([]int16, frames*config.Channels)
	e.handle = sessions.New(e)
	return e, nil
}

func (e *encoder) set(id uint32, value uint32, name string) error {
	if s := e.sym.setProperty(e.conv, id, 4, unsafe.Pointer(&value)); s != 0 {
		return &kit.Error{Backend: Name, Op: "open", Name: name, Reason: "was refused (status " + status(s) + ")", Fix: "the AAC encoder takes bitrates its AudioConverter lists for this rate and layout"}
	}
	return nil
}

func (e *encoder) configure(knobs codec.KnobValues) error {
	mode := map[string]uint32{"cbr": 0, "abr": 1, "vbr": 2, "cq": 3}[knobs.String("rc")]
	if err := e.set(propertyRateControl, mode, "rc"); err != nil {
		return err
	}
	if err := e.set(propertyQuality, uint32(127-knobs.Int("speed")*127/10), "speed"); err != nil {
		return err
	}
	if mode == 3 {
		if err := e.set(propertyVBRQuality, uint32(knobs.Int("quality")*127/100), "quality"); err != nil {
			return err
		}
	} else if bitrate := knobs.Int("bitrate"); bitrate > 0 {
		if err := e.set(propertyBitrate, uint32(bitrate), "bitrate"); err != nil {
			return err
		}
	}
	// The raw tier names AudioConverter properties by four-character code:
	// audiotoolbox-params="cdqu=96".
	for _, kv := range knobs.Raw() {
		if len(kv[0]) != 4 {
			return &kit.Error{Backend: Name, Op: "open", Name: kv[0], Reason: "is not a four-character AudioConverter property", Fix: "name properties by code, such as brat or cdqu"}
		}
		value, err := strconv.ParseUint(kv[1], 10, 32)
		if err != nil {
			return &kit.Error{Backend: Name, Op: "open", Name: kv[0], Reason: "takes an unsigned 32-bit value, not " + kv[1]}
		}
		if err := e.set(fourCC(kv[0]), uint32(value), kv[0]); err != nil {
			return err
		}
	}
	return nil
}

// Config is the AudioSpecificConfig of the stream.
func (e *encoder) Config() []byte { return e.config }

// Update changes the bitrate while running.
func (e *encoder) Update(knob string, values codec.KnobValues) error {
	if knob != "bitrate" {
		return &kit.Error{Backend: Name, Op: "control", Name: knob, Reason: "cannot change while running"}
	}
	return e.set(propertyBitrate, uint32(values.Int(knob)), knob)
}

func (e *encoder) Encode(s *kit.Samples, emit func(*kit.Packet) error) error {
	if !e.started {
		// A packet's PTS is its first decoded sample's, so the priming the
		// encoder adds shows before the first input sample.
		e.next, e.started = s.PTS-e.priming, true
	}
	e.fifo = append(e.fifo, s.PCM...)
	for len(e.fifo) >= frames*e.channels {
		produced, err := e.pull(emit)
		if err != nil || !produced {
			return err
		}
	}
	return nil
}

func (e *encoder) Flush(emit func(*kit.Packet) error) error {
	e.draining = true
	for range 64 {
		produced, err := e.pull(emit)
		if err != nil || !produced {
			return err
		}
	}
	return nil
}

// pull asks the converter for one packet and emits it.
func (e *encoder) pull(emit func(*kit.Packet) error) (bool, error) {
	var pinner runtime.Pinner
	pinner.Pin(&e.out[0])
	pinner.Pin(&e.feed[0])
	defer pinner.Unpin()
	e.list = bufferList{NumberBuffers: 1, NumberChannels: uint32(e.channels), DataByteSize: uint32(len(e.out)), Data: unsafe.Pointer(&e.out[0])}
	packets := uint32(1)
	s := e.sym.fill(e.conv, inputProc(), e.handle, &packets, &e.list, &e.desc)
	if s != 0 && s != noData {
		return false, kit.Fail(Name, "encode", "AudioConverterFillComplexBuffer", int64(s))
	}
	if packets == 0 {
		return false, nil
	}
	pts := e.next
	e.next += frames
	return true, emit(&kit.Packet{Data: e.out[:e.list.DataByteSize], PTS: pts, DTS: pts, Duration: frames, Keyframe: true})
}

// supply hands the converter whole frames from the FIFO, nothing until a
// whole frame is there, and at the end the remainder, then end of stream.
func (e *encoder) supply(packets *uint32, data *bufferList, desc **packetDesc) int32 {
	if desc != nil {
		*desc = nil
	}
	want := min(int(*packets), frames) * e.channels
	have := len(e.fifo)
	switch {
	case have >= want:
	case e.draining && have > 0:
		want = have
	case e.draining:
		*packets = 0
		data.DataByteSize = 0
		return 0
	default:
		*packets = 0
		return noData
	}
	n := copy(e.feed, e.fifo[:want])
	e.fifo = append(e.fifo[:0], e.fifo[n:]...)
	*packets = uint32(n / e.channels)
	data.NumberBuffers = 1
	data.NumberChannels = uint32(e.channels)
	data.DataByteSize = uint32(n * 2)
	data.Data = unsafe.Pointer(&e.feed[0])
	return 0
}

func (e *encoder) Close() error {
	if e.conv == 0 {
		return nil
	}
	sessions.Delete(e.handle)
	e.sym.dispose(e.conv)
	e.conv = 0
	return nil
}

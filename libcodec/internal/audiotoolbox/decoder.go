package audiotoolbox

import (
	"runtime"
	"slices"
	"unsafe"

	"github.com/thesyncim/goav/libcodec/kit"
)

// decoder is one AudioConverter from AAC-LC to interleaved s16 PCM. Each
// Decode hands the converter one packet through the input procedure and
// pulls the PCM it decodes to.
type decoder struct {
	sym      *symbols
	conv     uintptr
	handle   uintptr
	rate     int
	channels int
	packet   []byte
	pending  bool
	pcm      []int16
	list     bufferList
	desc     packetDesc
}

func newDecoder(sym *symbols, config kit.AudioConfig) (kit.AudioDecoder, error) {
	rate, channels := config.SampleRate, config.Channels
	if len(config.ExtraData) >= 2 {
		var err error
		if rate, channels, err = parseConfig(config.ExtraData); err != nil {
			return nil, err
		}
	}
	if rate <= 0 || channels <= 0 {
		return nil, &kit.Error{Backend: Name, Op: "open", Reason: "needs the stream's AudioSpecificConfig or its rate and channels"}
	}
	src, dst := aacDescription(rate, channels), pcmDescription(rate, channels)
	d := &decoder{sym: sym, rate: rate, channels: channels, pcm: make([]int16, frames*channels)}
	if s := sym.newConverter(&src, &dst, &d.conv); s != 0 {
		return nil, kit.Fail(Name, "open", "AudioConverterNew", int64(s))
	}
	asc := slices.Clone(config.ExtraData)
	if len(asc) == 0 {
		asc = audioSpecificConfig(rate, channels)
	}
	if cookie := esds(asc); sym.setProperty(d.conv, propertyMagicCookie, uint32(len(cookie)), unsafe.Pointer(&cookie[0])) != 0 {
		// The stream description alone describes AAC-LC; the cookie only
		// refines it, so a converter that refuses one decodes without it.
		_ = cookie
	}
	d.handle = sessions.New(d)
	return d, nil
}

func (d *decoder) Decode(p *kit.Packet, emit func(*kit.Samples) error) error {
	d.packet = append(d.packet[:0], p.Data...)
	d.pending = true
	var pinner runtime.Pinner
	pinner.Pin(&d.pcm[0])
	pinner.Pin(&d.packet[0])
	pinner.Pin(&d.desc)
	defer pinner.Unpin()
	d.list = bufferList{NumberBuffers: 1, NumberChannels: uint32(d.channels), DataByteSize: uint32(len(d.pcm) * 2), Data: unsafe.Pointer(&d.pcm[0])}
	n := uint32(frames)
	s := d.sym.fill(d.conv, inputProc(), d.handle, &n, &d.list, nil)
	if s != 0 && s != noData {
		return kit.Fail(Name, "decode", "AudioConverterFillComplexBuffer", int64(s))
	}
	if n == 0 {
		return nil
	}
	return emit(&kit.Samples{PCM: d.pcm[:int(n)*d.channels], SampleRate: d.rate, Channels: d.channels, PTS: p.PTS})
}

// supply hands the converter the pending packet once.
func (d *decoder) supply(packets *uint32, data *bufferList, desc **packetDesc) int32 {
	if !d.pending {
		*packets = 0
		return noData
	}
	d.pending = false
	d.desc = packetDesc{DataByteSize: uint32(len(d.packet))}
	if desc != nil {
		*desc = &d.desc
	}
	*packets = 1
	data.NumberBuffers = 1
	data.NumberChannels = uint32(d.channels)
	data.DataByteSize = uint32(len(d.packet))
	data.Data = unsafe.Pointer(&d.packet[0])
	return 0
}

// Flush has nothing to drain: each packet decodes as it arrives.
func (d *decoder) Flush(func(*kit.Samples) error) error { return nil }

func (d *decoder) Close() error {
	if d.conv == 0 {
		return nil
	}
	sessions.Delete(d.handle)
	d.sym.dispose(d.conv)
	d.conv = 0
	return nil
}

// esds wraps an AudioSpecificConfig in the MPEG-4 elementary stream
// descriptor AudioToolbox reads as an AAC magic cookie.
func esds(asc []byte) []byte {
	descriptor := func(tag byte, body []byte) []byte {
		out := []byte{tag, 0x80, 0x80, 0x80, byte(len(body))}
		return append(out, body...)
	}
	specific := descriptor(0x05, asc)
	config := descriptor(0x04, append([]byte{0x40, 0x15, 0, 0x18, 0, 0, 0, 0, 0, 0, 0, 0, 0}, specific...))
	return descriptor(0x03, append(append([]byte{0, 0, 0}, config...), descriptor(0x06, []byte{0x02})...))
}

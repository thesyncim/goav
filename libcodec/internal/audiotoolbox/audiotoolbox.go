// Package audiotoolbox is the AudioToolbox backend: Apple's AAC-LC encoder
// and decoder, driven through AudioConverter.
package audiotoolbox

import (
	"encoding/binary"
	"fmt"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/libcodec/kit"
)

// Name is the backend's identity: aac/audiotoolbox.
const Name = "audiotoolbox"

// Path is the framework's install name; macOS serves it from the dyld
// shared cache.
const Path = "/System/Library/Frameworks/AudioToolbox.framework/AudioToolbox"

type symbols struct {
	newConverter func(src, dst *streamDescription, out *uintptr) int32                                                         `sym:"AudioConverterNew"`
	dispose      func(converter uintptr) int32                                                                                 `sym:"AudioConverterDispose"`
	setProperty  func(converter uintptr, id uint32, size uint32, data unsafe.Pointer) int32                                    `sym:"AudioConverterSetProperty"`
	getProperty  func(converter uintptr, id uint32, size *uint32, data unsafe.Pointer) int32                                   `sym:"AudioConverterGetProperty"`
	fill         func(converter uintptr, proc uintptr, user uintptr, packets *uint32, out *bufferList, desc *packetDesc) int32 `sym:"AudioConverterFillComplexBuffer"`
}

// streamDescription is AudioStreamBasicDescription.
type streamDescription struct {
	SampleRate       float64
	FormatID         uint32
	FormatFlags      uint32
	BytesPerPacket   uint32
	FramesPerPacket  uint32
	BytesPerFrame    uint32
	ChannelsPerFrame uint32
	BitsPerChannel   uint32
	Reserved         uint32
}

// bufferList is an AudioBufferList with its one AudioBuffer.
type bufferList struct {
	NumberBuffers  uint32
	_              uint32
	NumberChannels uint32
	DataByteSize   uint32
	Data           unsafe.Pointer
}

// packetDesc is AudioStreamPacketDescription.
type packetDesc struct {
	StartOffset            int64
	VariableFramesInPacket uint32
	DataByteSize           uint32
}

func fourCC(s string) uint32 { return binary.BigEndian.Uint32([]byte(s)) }

var (
	formatAAC  = fourCC("aac ")
	formatLPCM = fourCC("lpcm")

	propertyBitrate     = fourCC("brat")
	propertyRateControl = fourCC("acbf")
	propertyVBRQuality  = fourCC("vbrq")
	propertyQuality     = fourCC("cdqu")
	propertyPrimeInfo   = fourCC("prim")
	propertyMagicCookie = fourCC("dmgc")
	propertyMaxPacket   = fourCC("xops")
)

const (
	pcmFlags = 1<<2 | 1<<3 // signed integer, packed, native endian
	aacLC    = 2           // kMPEG4Object_AAC_LC
	frames   = 1024        // AAC-LC frames per packet

	// noData is the status the input procedure returns when the session has
	// no more input until the next call: not an end of stream.
	noData = 0x6e6f6474 // 'nodt'
)

// status formats an OSStatus, as a four-character code when it is one.
func status(s int32) string {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(s))
	for _, c := range b {
		if c < 32 || c > 126 {
			return fmt.Sprint(s)
		}
	}
	return fmt.Sprintf("%d '%s'", s, b)
}

// sessions route the converter's input procedure, a C callback, back to the
// session it belongs to. One callback serves every session: purego
// callbacks are a fixed budget that is never freed.
var (
	sessions  kit.Handles[inputSource]
	inputProc = sync.OnceValue(func() uintptr { return purego.NewCallback(input) })
)

// inputSource supplies a converter's input on demand.
type inputSource interface {
	supply(packets *uint32, data *bufferList, desc **packetDesc) int32
}

// input is AudioConverterComplexInputDataProc.
func input(_ uintptr, packets *uint32, data *bufferList, desc **packetDesc, user uintptr) int32 {
	source, ok := sessions.Get(user)
	if !ok {
		*packets = 0
		return noData
	}
	return source.supply(packets, data, desc)
}

var sampleRates = []int{8000, 11025, 12000, 16000, 22050, 24000, 32000, 44100, 48000}

// Codec declares the backend.
func Codec(path string) kit.Codec {
	l := &symbols{}
	rc := codec.KnobRateControl
	rc.Values = []string{"cbr", "abr", "vbr", "cq"}
	rc.Default = "abr"
	rc.Help = "cbr constant, abr long-term average, vbr constrained variable, cq variable at quality"
	quality := codec.KnobQuality
	quality.Default = "75"
	speed := codec.KnobSpeed
	speed.Default = "2"
	speed.Help = "encoder effort, 0 best to 10 fastest (AudioConverter codec quality)"
	bitrate := codec.KnobBitrate
	bitrate.Min, bitrate.Max = 8_000, 320_000
	return kit.Codec{
		ID:        av.CodecAAC,
		Media:     av.MediaAudio,
		Backend:   Name,
		Kind:      codec.KindLib,
		Module:    "github.com/thesyncim/goav/libcodec",
		Package:   "github.com/thesyncim/goav/libcodec/internal/audiotoolbox",
		Platforms: []string{"darwin"},
		Profiles:  []string{"aac_lc"},
		Capabilities: codec.Capabilities{
			SampleRates: sampleRates,
		},
		Realtime:     true,
		Knobs:        []codec.Knob{bitrate, rc, quality, speed},
		DecoderKnobs: []codec.Knob{},
		Libraries:    []kit.Library{{Name: "AudioToolbox.framework", Path: path, Fix: "AudioToolbox ships with macOS", Symbols: l}},
		NewAudioEncoder: func(config kit.AudioConfig) (kit.AudioEncoder, error) {
			return newEncoder(l, config)
		},
		NewAudioDecoder: func(config kit.AudioConfig) (kit.AudioDecoder, error) {
			return newDecoder(l, config)
		},
	}
}

func pcmDescription(rate, channels int) streamDescription {
	return streamDescription{
		SampleRate:       float64(rate),
		FormatID:         formatLPCM,
		FormatFlags:      pcmFlags,
		BytesPerPacket:   uint32(2 * channels),
		FramesPerPacket:  1,
		BytesPerFrame:    uint32(2 * channels),
		ChannelsPerFrame: uint32(channels),
		BitsPerChannel:   16,
	}
}

func aacDescription(rate, channels int) streamDescription {
	return streamDescription{
		SampleRate:       float64(rate),
		FormatID:         formatAAC,
		FormatFlags:      aacLC,
		FramesPerPacket:  frames,
		ChannelsPerFrame: uint32(channels),
	}
}

// audioSpecificConfig is the two-byte AAC-LC AudioSpecificConfig.
func audioSpecificConfig(rate, channels int) []byte {
	index := 15
	for i, r := range []int{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350} {
		if r == rate {
			index = i
		}
	}
	v := uint16(aacLC)<<11 | uint16(index)<<7 | uint16(channels)<<3
	return []byte{byte(v >> 8), byte(v)}
}

// parseConfig reads the rate and channels of an AudioSpecificConfig.
func parseConfig(asc []byte) (rate, channels int, err error) {
	if len(asc) < 2 {
		return 0, 0, fmt.Errorf("%w: aac audiotoolbox: AudioSpecificConfig of %d bytes", codec.ErrUnsupportedFormat, len(asc))
	}
	v := binary.BigEndian.Uint16(asc)
	object, index, channelConfig := int(v>>11), int(v>>7)&15, int(v>>3)&15
	rates := []int{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}
	if object != aacLC || index >= len(rates) || channelConfig == 0 {
		return 0, 0, fmt.Errorf("%w: aac audiotoolbox decodes AAC-LC with a sample-rate index and channel configuration (object %d, index %d, channels %d)", codec.ErrUnsupportedFormat, object, index, channelConfig)
	}
	return rates[index], channelConfig, nil
}

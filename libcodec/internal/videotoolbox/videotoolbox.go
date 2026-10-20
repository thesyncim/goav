// Package videotoolbox is the VideoToolbox backend: the hardware H.264 and
// HEVC encoders and decoders of macOS, driven through VideoToolbox,
// CoreMedia, CoreVideo, and CoreFoundation.
package videotoolbox

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ebitengine/purego"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/libcodec/kit"
)

// Name is the backend's identity: h264/videotoolbox, hevc/videotoolbox.
const Name = "videotoolbox"

const (
	pixelI420 = 0x79343230 // 'y420': kCVPixelFormatType_420YpCbCr8Planar
	hevc      = av.CodecHEVC
)

// codecType is the CMVideoCodecType of a codec.
func codecType(id av.CodecID) uint32 {
	if id == hevc {
		return fourCC("hvc1")
	}
	return fourCC("avc1")
}

// status formats an OSStatus with its meaning where VideoToolbox documents
// one a caller can act on.
func status(s int32) string {
	switch s {
	case -12908:
		return "-12908 (no hardware codec for this format)"
	case -12900:
		return "-12900 (property not supported)"
	case -12902:
		return "-12902 (parameter error)"
	case -12909:
		return "-12909 (bad data)"
	case -12911:
		return "-12911 (malfunction)"
	}
	return fmt.Sprint(s)
}

// Codecs declares the backend: an encoder and a decoder for H.264 and for
// HEVC. Each checks at registration that this Mac has the hardware it needs,
// so a Mac without (a virtual machine, say) registers it unavailable and
// selection uses another backend.
func Codecs() []kit.Codec {
	f := &frameworks{}
	libraries := f.libraries()
	var out []kit.Codec
	for _, id := range []av.CodecID{av.CodecH264, hevc} {
		base := kit.Codec{
			ID:        id,
			Media:     av.MediaVideo,
			Backend:   Name,
			Kind:      codec.KindHardware,
			Platforms: []string{"darwin"},
			Profiles:  profilesOf(id),
			Capabilities: codec.Capabilities{
				MaxWidth:  4096,
				MaxHeight: 2304,
			},
			Realtime:  true,
			Bitstream: kit.AVCC,
			Reorder:   true,
			Libraries: libraries,
			License:   kit.License{Class: kit.LicenseSystem},
		}
		encoder := base
		encoder.Knobs = encoderKnobs(id)
		encoder.Version = func() (string, error) { return "", probe("encode", id, f.probeEncoder) }
		encoder.NewVideoEncoder = func(config kit.VideoConfig) (kit.VideoEncoder, error) {
			return newEncoder(f, id, config)
		}
		decoder := base
		decoder.Version = func() (string, error) { return "", probe("decode", id, f.probeDecoder) }
		decoder.NewVideoDecoder = func(config kit.VideoConfig) (kit.VideoDecoder, error) {
			return newDecoder(f, id, config)
		}
		out = append(out, encoder, decoder)
	}
	return out
}

func profilesOf(id av.CodecID) []string {
	if id == hevc {
		return []string{"main"} // 8-bit hardware sessions
	}
	return []string{"baseline", "main", "high"}
}

// probes are the hardware checks, run once per process for each codec and
// mode: the hardware does not change while the process runs.
var probes sync.Map // "encode h264" -> *probed

type probed struct {
	once sync.Once
	err  error
}

func probe(mode string, id av.CodecID, check func(av.CodecID) error) error {
	value, _ := probes.LoadOrStore(mode+" "+string(id), &probed{})
	p := value.(*probed)
	p.once.Do(func() { p.err = check(id) })
	return p.err
}

// probeDecoder checks for a hardware decoder of the codec.
func (f *frameworks) probeDecoder(id av.CodecID) error {
	if f.vt.hardwareDecode != nil && !f.vt.hardwareDecode(codecType(id)) {
		return &kit.Error{Backend: Name, Op: "platform", Name: string(id), Reason: "this Mac has no hardware decoder", Fix: "decode " + string(id) + " with another backend"}
	}
	return nil
}

// probeEncoder opens and closes a small hardware session.
func (f *frameworks) probeEncoder(id av.CodecID) error {
	spec := f.dictionary()
	defer f.cf.release(spec)
	f.cf.dictionarySet(spec, f.vt.requireHardwareEncoder, f.cf.booleanTrue)
	var session uintptr
	if s := f.vt.compressionCreate(0, 256, 144, codecType(id), spec, 0, 0, 0, 0, &session); s != 0 {
		return &kit.Error{Backend: Name, Op: "platform", Name: string(id), Reason: "this Mac has no hardware encoder (status " + status(s) + ")", Fix: "encode " + string(id) + " with another backend"}
	}
	f.vt.compressionEnd(session)
	f.cf.release(session)
	return nil
}

// pixelOwner keeps a decoded CVPixelBuffer locked and retained while any
// plane published from it is: the last Release unlocks and releases it.
type pixelOwner struct {
	f      *frameworks
	buffer uintptr
	refs   atomic.Int32
}

func (o *pixelOwner) Retain() { o.refs.Add(1) }

func (o *pixelOwner) Release() {
	if o.refs.Add(-1) == 0 {
		o.f.cv.unlock(o.buffer, 1) // kCVPixelBufferLock_ReadOnly
		o.f.cf.release(o.buffer)
	}
}

// Refs reports at least two: the buffer is locked read-only, so a holder that
// wants to write a plane copies it first (av.Buffer.MakeWritable).
func (o *pixelOwner) Refs() int { return max(int(o.refs.Load()), 2) }

// Output callbacks reach their session through a handle: one callback per
// signature serves every session.
var (
	encoderSessions kit.Handles[*encoder]
	decoderSessions kit.Handles[*decoder]
	encodedCallback = sync.OnceValue(func() uintptr { return purego.NewCallback(encoded) })
	decodedCallback = sync.OnceValue(func() uintptr { return purego.NewCallback(decoded) })
)

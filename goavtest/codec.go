// The passthrough codec fake: encode wraps frames into packets, decode
// unwraps them — byte-faithful, so sample sums and pixel values survive a
// full encode/decode round trip and topology tests never need a real codec.

package goavtest

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	goavruntime "github.com/thesyncim/goav/runtime"
)

// Codec returns a runtime option registering a deterministic fake
// encoder+decoder pair for id, so chains can .Encode(...) and .Decode()
// without a real codec implementation. The encoder wraps each frame —
// headers, planes, and bytes — into one keyframe packet carrying the frame's
// PTS and duration; the decoder unwraps it byte-faithfully. Decoding a packet
// that was NOT produced by this encoder (a Packets fixture, say) yields one
// deterministic silent/black frame shaped by the stream's declared codec
// parameters, so packet-domain fixtures still flow through decode chains.
//
// The registered descriptor is media-agnostic (it matches audio and video
// alike) and capability-open, so it never fails a compatibility check.
func Codec(id av.CodecID) goavruntime.Option {
	desc := codecDescriptor(id)
	return goavruntime.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterEncoder(desc, passthroughCodecFactory{desc: desc})
		registry.RegisterDecoder(desc, passthroughCodecFactory{desc: desc})
	})
}

func codecDescriptor(id av.CodecID) codec.Descriptor {
	return codec.Descriptor{
		ID:   id,
		Name: "goavtest passthrough " + string(id),
		Backend: codec.Backend{
			Name:    "goavtest",
			Module:  "github.com/thesyncim/goav",
			Package: "goavtest",
			Status:  "test fake",
		},
	}
}

// passthroughCodecFactory opens one encoder or decoder instance per stream;
// each instance is driven by that stream's serial worker, so no locking is
// needed.
type passthroughCodecFactory struct {
	desc codec.Descriptor
}

func (f passthroughCodecFactory) NewEncoder(_ context.Context, _ codec.EncodeConfig) (codec.Encoder, error) {
	return &passthroughEncoder{desc: f.desc}, nil
}

func (f passthroughCodecFactory) NewDecoder(_ context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	return &passthroughDecoder{desc: f.desc, stream: config.Stream}, nil
}

type passthroughEncoder struct {
	desc codec.Descriptor
}

func (e *passthroughEncoder) Descriptor() codec.Descriptor { return e.desc }

// Open accepts every encode config: the passthrough encoder has no knobs.
func (e *passthroughEncoder) Open(context.Context, codec.EncodeConfig) error { return nil }

// EncodeInto wraps the frame into exactly one keyframe packet carrying the
// frame's stream, PTS, and duration, with the serialized frame as payload.
func (e *passthroughEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if frame == nil {
		return nil
	}
	if len(out.Packets) == cap(out.Packets) {
		return codec.ErrResultFull
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	packet := &out.Packets[index]
	packet.Reset()
	packet.StreamID = frame.StreamID
	packet.Type = frame.Type
	packet.CodecEpoch = frame.CodecEpoch
	packet.PTS = frame.PTS
	packet.DTS = frame.PTS
	packet.Duration = frame.Duration
	packet.Keyframe = true
	packet.Payload.Bytes = appendFramePayload(packet.Payload.Bytes[:0], frame)
	return nil
}

// FlushInto is a no-op: the passthrough encoder buffers nothing.
func (e *passthroughEncoder) FlushInto(context.Context, *codec.EncodeResult) error { return nil }

// HandleEvent accepts every control (keyframe requests, bitrate retargets)
// silently: every passthrough packet is already a keyframe.
func (e *passthroughEncoder) HandleEvent(context.Context, *av.Event) error { return nil }

// Close is a no-op: the passthrough encoder holds no resources.
func (e *passthroughEncoder) Close() error { return nil }

type passthroughDecoder struct {
	desc   codec.Descriptor
	stream av.Stream
	audio  av.AudioFrame
	video  av.VideoFrame
}

func (d *passthroughDecoder) Descriptor() codec.Descriptor { return d.desc }

// Open records the stream's declared parameters, which shape the fallback
// frame for packets the passthrough encoder did not produce.
func (d *passthroughDecoder) Open(_ context.Context, config codec.DecodeConfig) error {
	d.stream = config.Stream
	return nil
}

// DecodeInto unwraps one passthrough packet into the byte-identical frame it
// was encoded from, or fabricates the deterministic fallback frame for
// foreign payloads.
func (d *passthroughDecoder) DecodeInto(_ context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if packet == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	if !parseFramePayload(packet.Payload.Bytes, frame, &d.audio, &d.video) {
		frame.Reset()
		if err := fallbackFrame(packet, d.stream, frame); err != nil {
			out.Frames = out.Frames[:index]
			return err
		}
	}
	frame.StreamID = packet.StreamID
	frame.CodecEpoch = packet.CodecEpoch
	frame.PTS = packet.PTS
	frame.Duration = packet.Duration
	return nil
}

// FlushInto is a no-op: the passthrough decoder buffers nothing.
func (d *passthroughDecoder) FlushInto(context.Context, *codec.DecodeResult) error { return nil }

// HandleEvent accepts every in-band event; the passthrough decoder keeps no
// state to reset.
func (d *passthroughDecoder) HandleEvent(context.Context, *av.Event) error { return nil }

// Close is a no-op: the passthrough decoder holds no resources.
func (d *passthroughDecoder) Close() error { return nil }

const framePayloadMagic = "gAVf"

const (
	frameKindAudio byte = 1
	frameKindVideo byte = 2
)

// appendFramePayload serializes one frame — header plus every plane — into
// the packet payload.
func appendFramePayload(b []byte, frame *av.Frame) []byte {
	b = append(b, framePayloadMagic...)
	switch {
	case frame.Audio != nil:
		b = appendU8(b, frameKindAudio)
		b = appendU32(b, uint32(frame.Audio.SampleRate))
		b = appendU32(b, uint32(frame.Audio.Channels))
		b = appendWireString(b, frame.Audio.ChannelLayout)
		b = appendWireString(b, frame.Audio.SampleFormat)
		b = appendU32(b, uint32(frame.Audio.Samples))
	default:
		var video av.VideoFrame
		if frame.Video != nil {
			video = *frame.Video
		}
		b = appendU8(b, frameKindVideo)
		b = appendU32(b, uint32(video.Width))
		b = appendU32(b, uint32(video.Height))
		b = appendWireString(b, video.PixelFormat)
	}
	b = appendU32(b, uint32(len(frame.Planes)))
	for i := range frame.Planes {
		b = appendU32(b, uint32(frame.Planes[i].Stride))
		b = appendU32(b, uint32(frame.Planes[i].Offset))
		b = appendWireBytes(b, frame.Planes[i].Buffer.Bytes)
	}
	return b
}

// parseFramePayload restores a frame serialized by appendFramePayload into
// frame (reusing its plane capacity), reporting false when the payload was
// not produced by the passthrough encoder.
func parseFramePayload(payload []byte, frame *av.Frame, audioScratch *av.AudioFrame, videoScratch *av.VideoFrame) bool {
	if len(payload) < len(framePayloadMagic) || string(payload[:len(framePayloadMagic)]) != framePayloadMagic {
		return false
	}
	audio, video := resetFrameForPayload(frame)
	r := newWireReader(payload[len(framePayloadMagic):])
	switch r.u8() {
	case frameKindAudio:
		if audio == nil {
			audio = audioScratch
		}
		if audio == nil {
			audio = &av.AudioFrame{}
		}
		*audio = av.AudioFrame{
			SampleRate:    int(r.u32()),
			Channels:      int(r.u32()),
			ChannelLayout: r.internedString(),
			SampleFormat:  r.internedString(),
			Samples:       int(r.u32()),
		}
		frame.Type = av.MediaAudio
		frame.Audio = audio
	case frameKindVideo:
		if video == nil {
			video = videoScratch
		}
		if video == nil {
			video = &av.VideoFrame{}
		}
		*video = av.VideoFrame{
			Width:       int(r.u32()),
			Height:      int(r.u32()),
			PixelFormat: r.internedString(),
		}
		frame.Type = av.MediaVideo
		frame.Video = video
	default:
		return false
	}
	planes := int(r.u32())
	if !r.ok || planes < 0 || planes > 16 {
		return false
	}
	frame.Planes = resizePlanes(frame.Planes, planes)
	for i := 0; i < planes; i++ {
		frame.Planes[i].Stride = int(r.u32())
		frame.Planes[i].Offset = int(r.u32())
		frame.Planes[i].Buffer.Bytes = append(frame.Planes[i].Buffer.Bytes[:0], r.blob()...)
	}
	if !r.ok {
		frame.Reset()
		return false
	}
	return true
}

func resetFrameForPayload(frame *av.Frame) (*av.AudioFrame, *av.VideoFrame) {
	audio := frame.Audio
	video := frame.Video
	frame.StreamID = ""
	frame.CodecEpoch = 0
	frame.Type = ""
	frame.PTS = av.Timestamp{}
	frame.Duration = av.Duration{}
	frame.Audio = nil
	frame.Video = nil
	for i := range frame.Planes {
		frame.Planes[i].Reset()
	}
	frame.Planes = frame.Planes[:0]
	frame.Metadata = nil
	return audio, video
}

func resizePlanes(planes []av.Plane, n int) []av.Plane {
	if cap(planes) < n {
		return make([]av.Plane, n)
	}
	return planes[:n]
}

// fallbackFrame fills one deterministic frame for a packet the passthrough
// encoder did not produce: silence shaped by the stream's declared audio
// parameters (defaulting to 48kHz mono S16, 480 samples), or a black I420
// frame at the declared geometry for video.
func fallbackFrame(packet *av.Packet, stream av.Stream, frame *av.Frame) error {
	media := packet.Type
	if media == "" {
		media = stream.Type
	}
	if media == "" {
		media = stream.Codec.Type
	}
	switch media {
	case av.MediaVideo:
		width := max(stream.Codec.Width, 2)
		height := max(stream.Codec.Height, 2)
		pixelFormat := stream.Codec.PixelFormat
		if pixelFormat == "" {
			pixelFormat = av.PixelFormatI420
		}
		frame.Type = av.MediaVideo
		frame.Video = &av.VideoFrame{Width: width, Height: height, PixelFormat: pixelFormat}
		chromaW := (width + 1) / 2
		chromaH := (height + 1) / 2
		frame.Planes = resizePlanes(frame.Planes, 3)
		frame.Planes[0] = av.Plane{Buffer: av.Buffer{Bytes: make([]byte, width*height)}, Stride: width}
		frame.Planes[1] = av.Plane{Buffer: av.Buffer{Bytes: make([]byte, chromaW*chromaH)}, Stride: chromaW}
		frame.Planes[2] = av.Plane{Buffer: av.Buffer{Bytes: make([]byte, chromaW*chromaH)}, Stride: chromaW}
		return nil
	case av.MediaAudio, av.MediaUnknown:
		sampleRate := stream.Codec.SampleRate
		if sampleRate <= 0 {
			sampleRate = 48000
		}
		channels := max(stream.Codec.Channels, 1)
		frame.Type = av.MediaAudio
		frame.Audio = &av.AudioFrame{
			SampleRate:   sampleRate,
			Channels:     channels,
			SampleFormat: av.SampleFormatS16,
			Samples:      480,
		}
		frame.Planes = resizePlanes(frame.Planes, 1)
		frame.Planes[0] = av.Plane{
			Buffer: av.Buffer{Bytes: make([]byte, 480*channels*2)},
			Stride: channels * 2,
		}
		return nil
	default:
		return fmt.Errorf("goavtest: cannot fabricate a fallback %s frame for stream %q", media, packet.StreamID)
	}
}

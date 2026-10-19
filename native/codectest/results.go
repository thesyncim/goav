package codectest

import (
	"context"
	"slices"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

// newEncodeResult allocates a result the way the runtime does: packet slots
// with payload capacity an adapter may write into.
func newEncodeResult(media av.MediaType) codec.EncodeResult {
	slots, size := 1, 1<<20
	if media == av.MediaAudio {
		slots, size = 8, 4096
	}
	packets := make([]av.Packet, slots)
	for i := range packets {
		packets[i].Payload = av.Buffer{Bytes: make([]byte, 0, size)}
	}
	return codec.EncodeResult{Packets: packets[:0], Events: make([]av.Event, 0, 1)}
}

// newDecodeResult allocates a result the way the runtime does: frame slots,
// with sample capacity for audio decoders that write into it.
func newDecodeResult(media av.MediaType) codec.DecodeResult {
	frames := make([]av.Frame, 1)
	if media == av.MediaAudio {
		frames[0].Planes = []av.Plane{{Buffer: av.Buffer{Bytes: make([]byte, 0, 64<<10)}}}
	} else {
		frames[0].Planes = make([]av.Plane, 3)
	}
	return codec.DecodeResult{Frames: frames[:0], Events: make([]av.Event, 0, 1), Requests: make([]codec.ControlRequest, 0, 1)}
}

// keepPacket holds a result packet past the call: pooled payloads by
// reference, an adapter's own scratch by copy, which it reuses next call.
func keepPacket(p *av.Packet) av.Packet {
	kept := *p.Ref()
	if p.Payload.Owner == nil {
		kept.Payload = av.Buffer{Bytes: slices.Clone(p.Payload.Bytes)}
	}
	return kept
}

// keepFrame holds a result frame past the call, copying planes the decoder
// owns and reuses.
func keepFrame(f *av.Frame) *av.Frame {
	kept := f.Ref()
	for i := range kept.Planes {
		if kept.Planes[i].Buffer.Owner == nil {
			kept.Planes[i].Buffer = av.Buffer{Bytes: slices.Clone(kept.Planes[i].Buffer.Bytes)}
		}
	}
	return kept
}

// decodeConfig opens a decoder the way the runtime does, with adapter state
// from a factory that provides it.
func decodeConfig(ctx context.Context, b backend, stream av.Stream) (codec.DecodeConfig, error) {
	config := codec.DecodeConfig{
		Stream:     stream,
		Resilience: codec.ResiliencePolicy{RequestKeyframes: true, ConcealAudio: stream.Type == av.MediaAudio},
		Bounds:     codec.DecodeBounds{MaxFramesPerInput: 1, MaxEventsPerInput: 1, MaxRequestsPerInput: 1, MaxWidth: stream.Codec.Width, MaxHeight: stream.Codec.Height},
	}
	if stateful, ok := b.decoder.(codec.DecodeStateFactory); ok {
		state, err := stateful.NewDecodeState(ctx, config)
		if err != nil {
			return config, err
		}
		config.OpaqueState = state
	}
	return config, nil
}

// drainPackets hands each result packet to keep (nil drops it), releases the
// result's own references, and resets the result for the next call with its
// scratch capacity intact.
func drainPackets(result *codec.EncodeResult, keep func(av.Packet)) int {
	n := len(result.Packets)
	for i := range result.Packets {
		p := &result.Packets[i]
		if keep != nil {
			keep(keepPacket(p))
		}
		if p.Payload.Owner != nil {
			p.Payload.Release()
		}
	}
	result.Reset()
	return n
}

// drainFrames is drainPackets for decoded frames, and also counts and clears
// the result's requests.
func drainFrames(result *codec.DecodeResult, keep func(*av.Frame)) (frames, requests int) {
	frames, requests = len(result.Frames), len(result.Requests)
	for i := range result.Frames {
		f := &result.Frames[i]
		if keep != nil {
			keep(keepFrame(f))
		}
		for j := range f.Planes {
			if f.Planes[j].Buffer.Owner != nil {
				f.Planes[j].Buffer.Release()
			}
		}
	}
	result.Reset()
	return frames, requests
}

// merged overlays the parameters an opened encoder reports onto the
// stream's, as the runtime does before a muxer writes its header.
func merged(base, reported av.CodecParameters) av.CodecParameters {
	out := base
	set := func(dst *string, v string) {
		if v != "" {
			*dst = v
		}
	}
	set((*string)(&out.ID), string(reported.ID))
	set((*string)(&out.Type), string(reported.Type))
	set(&out.Profile, reported.Profile)
	set(&out.Level, reported.Level)
	set(&out.ChannelLayout, reported.ChannelLayout)
	set(&out.PixelFormat, reported.PixelFormat)
	set(&out.SampleFormat, reported.SampleFormat)
	for _, pair := range [][2]*int{{&out.SampleRate, &reported.SampleRate}, {&out.Channels, &reported.Channels}, {&out.Width, &reported.Width}, {&out.Height, &reported.Height}} {
		if *pair[1] > 0 {
			*pair[0] = *pair[1]
		}
	}
	if reported.ClockRate != 0 {
		out.ClockRate = reported.ClockRate
	}
	if len(reported.ExtraData.Bytes) != 0 {
		out.ExtraData = reported.ExtraData
	}
	return out
}

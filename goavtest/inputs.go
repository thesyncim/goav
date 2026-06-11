// Deterministic pipeline inputs: PTS-stamped, shape-declared sources built on
// goav.Source, so the planner treats them exactly like production inputs.

package goavtest

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/shape"
)

// Audio returns a deterministic S16 frame source: one interleaved audio frame
// per sample slice, then end of stream. Frames are PTS-stamped in sample time
// (frame n starts at the samples already played, timebase 1/sampleRate) with
// matching durations, so PTS-aligned joins and pacing behave coherently.
// Each call gets a distinct generated name (and stream id), so several Audio
// inputs can stand side by side as Mix/Composite/Select arms.
func Audio(sampleRate, channels int, frames ...[]int16) goav.InputSpec {
	name := nextName("audio")
	return goav.Source(name,
		shape.Frame(av.MediaAudio, shape.Audio(sampleRate, channels, av.SampleFormatS16)),
		func(_ context.Context, push goav.SourcePush) error {
			var elapsed int64
			for _, samples := range frames {
				frame := s16Frame(name, sampleRate, channels, samples, elapsed)
				elapsed += frame.Duration.Value
				if _, err := push.Frame(frame); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// LiveAudio returns an S16 frame source that never ends: it replays the given
// frames in a loop with continuously advancing PTS until the task stops — the
// input control-plane tests need (SelectActive, Rebranch, live Control),
// where the stream must outlive the assertion. The caller names it because
// the name is the stream id controls target (goav.SelectActive(name)). With
// no frames it loops a single one-sample silent frame. The source retries on
// backpressure and returns cleanly when the task stops.
func LiveAudio(name string, sampleRate, channels int, frames ...[]int16) goav.InputSpec {
	if len(frames) == 0 {
		frames = [][]int16{make([]int16, max(channels, 1))}
	}
	return goav.Source(name,
		shape.Frame(av.MediaAudio, shape.Audio(sampleRate, channels, av.SampleFormatS16), shape.Realtime(true)),
		func(ctx context.Context, push goav.SourcePush) error {
			var elapsed int64
			for i := 0; ; i = (i + 1) % len(frames) {
				frame := s16Frame(name, sampleRate, channels, frames[i], elapsed)
				elapsed += frame.Duration.Value
				for {
					if ctx.Err() != nil {
						return nil
					}
					_, err := push.Frame(frame)
					if err == nil {
						break
					}
					if !errors.Is(err, goav.ErrBackpressure) {
						return nil
					}
				}
			}
		})
}

// videoFPS is the fixed cadence Video frames are stamped at: PTS n and
// duration 1 in a 1/30 timebase.
const videoFPS = 30

// Video returns a deterministic I420 video source emitting the given number
// of frames, then end of stream. Frame n is identifiable from its pixels: the
// luma plane is filled with byte(n) and both chroma planes with the neutral
// 128, so a test can tell which source frame any output pixel came from.
// Frames are stamped at 30fps (PTS n, duration 1, timebase 1/30). Each call
// gets a distinct generated name (and stream id).
func Video(width, height, frames int) goav.InputSpec {
	name := nextName("video")
	return goav.Source(name,
		shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420)),
		func(_ context.Context, push goav.SourcePush) error {
			for n := 0; n < frames; n++ {
				if _, err := push.Frame(i420Frame(name, width, height, n)); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// Packets returns a deterministic packet-domain source: it pushes the given
// packets in order, then ends the stream. Keyframe flags, PTS/DTS, durations,
// and payload bytes are honored verbatim — never restamped. Each packet's
// StreamID is rewritten to the source's generated stream id (a custom source
// carries exactly one stream, so foreign ids would route nowhere); an empty
// Type inherits the source's media kind, which is taken from the first packet
// that declares one, else inferred from a well-known codecID, else video.
func Packets(codecID av.CodecID, packets ...av.Packet) goav.InputSpec {
	name := nextName("packets")
	media := packetsMedia(codecID, packets)
	return goav.Source(name,
		shape.Packet(media, codecID),
		func(_ context.Context, push goav.SourcePush) error {
			for i := range packets {
				packet := packets[i]
				packet.StreamID = av.StreamID(name)
				if packet.Type == "" {
					packet.Type = media
				}
				if _, err := push.Packet(&packet); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// packetsMedia resolves the declared media kind of a Packets source: the
// first packet that sets Type wins, then the well-known codec ids, then video
// (the common packet-domain test case).
func packetsMedia(codecID av.CodecID, packets []av.Packet) av.MediaType {
	for i := range packets {
		if packets[i].Type != "" {
			return packets[i].Type
		}
	}
	switch codecID {
	case av.CodecOpus, av.CodecVorbis, av.CodecFLAC, av.CodecAAC, av.CodecPCM:
		return av.MediaAudio
	case av.CodecVP8, av.CodecVP9, av.CodecH264, av.CodecAV1:
		return av.MediaVideo
	case av.CodecTextUTF8:
		return av.MediaSubtitle
	}
	return av.MediaVideo
}

// s16Frame builds one interleaved S16 frame stamped at elapsed samples on the
// 1/sampleRate timebase. The payload is freshly allocated and never written
// again, so it is published immutable.
func s16Frame(name string, sampleRate, channels int, samples []int16, elapsed int64) *av.Frame {
	payload := make([]byte, len(samples)*2)
	for i := range samples {
		binary.LittleEndian.PutUint16(payload[i*2:], uint16(samples[i]))
	}
	base := av.TimeBase{Num: 1, Den: int64(max(sampleRate, 1))}
	perChannel := len(samples) / max(channels, 1)
	return &av.Frame{
		StreamID: av.StreamID(name),
		Type:     av.MediaAudio,
		PTS:      av.Timestamp{Value: elapsed, Base: base},
		Duration: av.Duration{Value: int64(perChannel), Base: base},
		Audio: &av.AudioFrame{
			SampleRate:   sampleRate,
			Channels:     channels,
			SampleFormat: av.SampleFormatS16,
			Samples:      perChannel,
		},
		Planes: []av.Plane{{Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}}},
	}
}

// i420Frame builds one tightly-packed I420 frame whose luma plane is filled
// with byte(n) and whose chroma planes are neutral, stamped at 30fps.
func i420Frame(name string, width, height, n int) *av.Frame {
	chromaW := (width + 1) / 2
	chromaH := (height + 1) / 2
	luma := make([]byte, width*height)
	for i := range luma {
		luma[i] = byte(n)
	}
	cb := make([]byte, chromaW*chromaH)
	cr := make([]byte, chromaW*chromaH)
	for i := range cb {
		cb[i] = 128
		cr[i] = 128
	}
	base := av.TimeBase{Num: 1, Den: videoFPS}
	return &av.Frame{
		StreamID: av.StreamID(name),
		Type:     av.MediaVideo,
		PTS:      av.Timestamp{Value: int64(n), Base: base},
		Duration: av.Duration{Value: 1, Base: base},
		Video:    &av.VideoFrame{Width: width, Height: height, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: luma, Ownership: av.BufferImmutable}, Stride: width},
			{Buffer: av.Buffer{Bytes: cb, Ownership: av.BufferImmutable}, Stride: chromaW},
			{Buffer: av.Buffer{Bytes: cr, Ownership: av.BufferImmutable}, Stride: chromaW},
		},
	}
}

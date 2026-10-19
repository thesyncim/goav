package codectest

import (
	"context"
	"slices"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/runconfig"
	goh264 "github.com/thesyncim/goh264"
)

// referenceOptions are the pure-Go codecs every backend is checked against:
// the bundle, plus goh264 as an H.264 decoder (the bundle keeps it behind a
// build tag).
func referenceOptions() []runconfig.Option {
	return []runconfig.Option{bundle.Codecs(), runconfig.WithDecoder(goh264Descriptor(), goh264Factory{})}
}

func goh264Descriptor() codec.Descriptor {
	return codec.Descriptor{
		ID:      av.CodecH264,
		Type:    av.MediaVideo,
		Backend: codec.BackendInfo{Name: "goh264", Kind: codec.KindGo, Module: "github.com/thesyncim/goh264"},
	}
}

type goh264Factory struct{}

func (goh264Factory) NewDecoder(_ context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	d := &goh264Decoder{stream: config.Stream, decoder: goh264.NewDecoder()}
	if extra := config.Stream.Codec.ExtraData.Bytes; len(extra) != 0 {
		if _, err := d.decoder.DecodeFrames(annexBConfig(extra)); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// goh264Decoder decodes with goh264, which returns pictures in presentation
// order without timestamps: each picture takes the earliest pending PTS.
type goh264Decoder struct {
	stream  av.Stream
	decoder *goh264.Decoder
	pending []av.Packet
}

func (d *goh264Decoder) Descriptor() codec.Descriptor                   { return goh264Descriptor() }
func (d *goh264Decoder) Open(context.Context, codec.DecodeConfig) error { return nil }
func (d *goh264Decoder) HandleEvent(context.Context, *av.Event) error   { return nil }
func (d *goh264Decoder) Close() error                                   { return nil }

func (d *goh264Decoder) DecodeInto(_ context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if packet == nil || len(packet.Payload.Bytes) == 0 {
		return nil
	}
	d.pending = append(d.pending, av.Packet{PTS: packet.PTS, Duration: packet.Duration})
	frames, err := d.decoder.DecodeFrames(packet.Payload.Bytes)
	if err != nil {
		return err
	}
	return d.emit(frames, out)
}

func (d *goh264Decoder) FlushInto(_ context.Context, out *codec.DecodeResult) error {
	frames, err := d.decoder.FlushDelayedFrames()
	if err != nil {
		return err
	}
	return d.emit(frames, out)
}

func (d *goh264Decoder) emit(frames []*goh264.Frame, out *codec.DecodeResult) error {
	for _, f := range frames {
		if f == nil {
			continue
		}
		var timing av.Packet
		if len(d.pending) != 0 {
			best := 0
			for i := range d.pending {
				if d.pending[i].PTS.Value < d.pending[best].PTS.Value {
					best = i
				}
			}
			timing = d.pending[best]
			d.pending = slices.Delete(d.pending, best, best+1)
		}
		out.Frames = append(out.Frames, av.Frame{
			StreamID: d.stream.ID,
			Type:     av.MediaVideo,
			PTS:      timing.PTS,
			Duration: timing.Duration,
			Video:    &av.VideoFrame{Width: f.Width, Height: f.Height, PixelFormat: av.PixelFormatI420},
			Planes: []av.Plane{
				{Buffer: av.Buffer{Bytes: f.Y}, Stride: f.YStride},
				{Buffer: av.Buffer{Bytes: f.Cb}, Stride: f.CStride},
				{Buffer: av.Buffer{Bytes: f.Cr}, Stride: f.CStride},
			},
		})
	}
	return nil
}

// annexBConfig turns an avcC record into Annex B parameter sets; other
// configurations pass through.
func annexBConfig(extra []byte) []byte {
	if len(extra) < 7 || extra[0] != 1 {
		return extra
	}
	var out []byte
	data := extra[5:]
	count := int(data[0] & 0x1f)
	data = data[1:]
	for pass := 0; pass < 2; pass++ {
		for range count {
			if len(data) < 2 {
				return out
			}
			n := int(data[0])<<8 | int(data[1])
			if len(data) < 2+n {
				return out
			}
			out = append(out, 0, 0, 0, 1)
			out = append(out, data[2:2+n]...)
			data = data[2+n:]
		}
		if len(data) == 0 {
			return out
		}
		count, data = int(data[0]), data[1:]
	}
	return out
}

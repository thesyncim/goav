package webm

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/matroska"
	"github.com/thesyncim/goav/format"
)

type Prober struct{}

type DemuxerFactory struct{}

type MuxerFactory struct{}

// FormatMuxer adapts the WebM muxer to format.Muxer: the Matroska adapter
// writing through WebM's profile-checked Muxer.
type FormatMuxer struct {
	*matroska.FormatMuxer
}

// FormatDemuxer adapts the WebM demuxer to format.Demuxer: the Matroska
// adapter reading through WebM's profile-checked Demuxer.
type FormatDemuxer struct {
	*matroska.FormatDemuxer
}

func Register(registry *format.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterProber(Prober{})
	registry.RegisterDemuxerDescriptor(Descriptor(), DemuxerFactory{})
	registry.RegisterMuxerDescriptor(Descriptor(), MuxerFactory{})
}

func Descriptor() format.Descriptor {
	return format.Descriptor{
		Format: av.FormatWebM,
		Media:  []av.MediaType{av.MediaAudio, av.MediaVideo},
		Codecs: []av.CodecID{
			av.CodecOpus,
			av.CodecVorbis,
			av.CodecVP8,
			av.CodecVP9,
			av.CodecAV1,
		},
		MinStreams: 1,
		Metadata: av.Metadata{
			"summary": "WebM supports Opus/Vorbis audio and VP8/VP9/AV1 video packets without codec conversion",
		},
	}
}

func (Prober) Probe(ctx context.Context, request format.ProbeRequest) (format.ProbeResult, error) {
	if err := ctx.Err(); err != nil {
		return format.ProbeResult{}, err
	}
	if matchWebMMIME(request.MIMEType) || matchWebMMIME(request.Input.MIMEType) {
		return format.ProbeResult{Format: av.FormatWebM, Score: 95, Reason: "webm mime type"}, nil
	}
	if hasWebMExtension(request.Name) || hasWebMExtension(request.Input.Name) || hasWebMExtension(request.Input.URI) {
		return format.ProbeResult{Format: av.FormatWebM, Score: 90, Reason: "webm extension"}, nil
	}
	if len(request.Header) >= 4 && bytes.Equal(request.Header[:4], []byte{0x1a, 0x45, 0xdf, 0xa3}) &&
		bytes.Contains(request.Header, []byte("webm")) {
		return format.ProbeResult{Format: av.FormatWebM, Score: 110, Reason: "webm doctype"}, nil
	}
	return format.ProbeResult{}, format.ErrNotFound
}

func (DemuxerFactory) NewDemuxer(ctx context.Context, result format.ProbeResult) (format.Demuxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result.Format != "" && result.Format != av.FormatWebM {
		return nil, format.ErrNotFound
	}
	return NewFormatDemuxer(), nil
}

func (MuxerFactory) NewMuxer(ctx context.Context, id av.FormatID) (format.Muxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id != "" && id != av.FormatWebM {
		return nil, format.ErrNotFound
	}
	return NewFormatMuxer(), nil
}

// NewFormatMuxer returns the WebM format.Muxer.
func NewFormatMuxer() *FormatMuxer {
	return &FormatMuxer{matroska.NewFormatMuxer(av.FormatWebM, func(w io.Writer) (matroska.TrackMuxer, error) {
		muxer, err := NewMuxer(w, MuxerOptions{})
		if err != nil {
			return nil, err
		}
		return muxer, nil
	})}
}

// NewFormatDemuxer returns the WebM format.Demuxer.
func NewFormatDemuxer() *FormatDemuxer {
	return &FormatDemuxer{matroska.NewFormatDemuxer(av.FormatWebM, func(r io.Reader) (matroska.TrackDemuxer, error) {
		demuxer, err := NewDemuxer(r, DemuxerOptions{})
		if err != nil {
			return nil, err
		}
		return demuxer, nil
	})}
}

func matchWebMMIME(value string) bool {
	return strings.EqualFold(value, "video/webm") || strings.EqualFold(value, "audio/webm")
}

func hasWebMExtension(name string) bool {
	return strings.EqualFold(filepath.Ext(name), ".webm")
}

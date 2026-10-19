package ts

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// Register installs the transport stream prober, demuxer, and muxer (with
// default MuxerOptions) for av.FormatTS. For a constant bitrate, register
// MuxerFactory{Options: MuxerOptions{MuxRate: rate}} for Descriptor().
func Register(registry *format.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterProber(Prober{})
	registry.RegisterDemuxerDescriptor(Descriptor(), DemuxerFactory{})
	registry.RegisterMuxerDescriptor(Descriptor(), MuxerFactory{})
}

// Descriptor declares what a transport stream carries.
func Descriptor() format.Descriptor {
	return format.Descriptor{
		Format:     av.FormatTS,
		Media:      []av.MediaType{av.MediaAudio, av.MediaVideo},
		Codecs:     []av.CodecID{av.CodecH264, av.CodecHEVC, av.CodecAAC, av.CodecOpus},
		MinStreams: 1,
		MaxStreams: 64,
		Metadata: av.Metadata{
			"summary": "MPEG-TS carries one program of H.264, HEVC, AAC, and Opus with PCR, PAT/PMT repetition, and optional constant-bitrate padding; the demuxer follows PMT changes and reports loss and discontinuities",
		},
	}
}

// Prober detects a transport stream by MIME type, extension, or sync bytes
// every 188 bytes.
type Prober struct{}

// DemuxerFactory opens transport stream demuxers.
type DemuxerFactory struct{}

// MuxerFactory opens transport stream muxers with its Options.
type MuxerFactory struct {
	Options MuxerOptions
}

func (Prober) Probe(ctx context.Context, request format.ProbeRequest) (format.ProbeResult, error) {
	if err := ctx.Err(); err != nil {
		return format.ProbeResult{}, err
	}
	if syncs := syncCount(request.Header); syncs >= 3 {
		return format.ProbeResult{Format: av.FormatTS, Score: 100, Reason: "transport stream sync bytes"}, nil
	}
	if isTSMIME(request.MIMEType) || isTSMIME(request.Input.MIMEType) {
		return format.ProbeResult{Format: av.FormatTS, Score: 90, Reason: "transport stream mime type"}, nil
	}
	if hasTSExtension(request.Name) || hasTSExtension(request.Input.Name) || hasTSExtension(request.Input.URI) {
		return format.ProbeResult{Format: av.FormatTS, Score: 80, Reason: "transport stream extension"}, nil
	}
	return format.ProbeResult{}, format.ErrNotFound
}

// syncCount counts the sync bytes at 0, 188, and 376.
func syncCount(header []byte) int {
	n := 0
	for at := 0; at < 3*packetSize && at < len(header) && header[at] == syncByte; at += packetSize {
		n++
	}
	return n
}

func isTSMIME(value string) bool {
	return strings.EqualFold(value, "video/mp2t") || strings.EqualFold(value, "audio/mp2t")
}

func hasTSExtension(name string) bool {
	if i := strings.IndexAny(name, "?#"); i >= 0 && strings.Contains(name, "://") {
		name = name[:i]
	}
	switch strings.ToLower(filepath.Ext(name)) {
	case ".ts", ".m2t", ".trp":
		return true
	}
	return false
}

func (DemuxerFactory) NewDemuxer(ctx context.Context, result format.ProbeResult) (format.Demuxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result.Format != "" && result.Format != av.FormatTS {
		return nil, format.ErrNotFound
	}
	return NewFormatDemuxer(), nil
}

func (f MuxerFactory) NewMuxer(ctx context.Context, id av.FormatID) (format.Muxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id != "" && id != av.FormatTS {
		return nil, format.ErrNotFound
	}
	return NewFormatMuxer(f.Options), nil
}

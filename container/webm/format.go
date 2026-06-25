package webm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/matroska"
	"github.com/thesyncim/goav/format"
)

type Prober struct{}

type DemuxerFactory struct{}

type MuxerFactory struct{}

type FormatMuxer struct {
	muxer  *Muxer
	tracks []formatTrack
	closed bool
}

type FormatDemuxer struct {
	demuxer *Demuxer
	streams []av.Stream
	tracks  []formatTrack
	closed  bool
}

type formatTrack struct {
	streamID av.StreamID
	trackID  uint32
	media    av.MediaType
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
	return &FormatDemuxer{}, nil
}

func (MuxerFactory) NewMuxer(ctx context.Context, id av.FormatID) (format.Muxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id != "" && id != av.FormatWebM {
		return nil, format.ErrNotFound
	}
	return &FormatMuxer{}, nil
}

func (m *FormatMuxer) Format() av.FormatID {
	return av.FormatWebM
}

func (m *FormatMuxer) Open(ctx context.Context, output format.Output, streams []av.Stream, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if output.Writer == nil {
		return matroska.ErrNilWriter
	}
	muxer, err := NewMuxer(output.Writer, MuxerOptions{})
	if err != nil {
		return err
	}
	m.muxer = muxer
	m.tracks = m.tracks[:0]
	for i := range streams {
		track, err := trackFromAVStream(streams[i], uint32(i+1))
		if err != nil {
			return err
		}
		trackID, err := muxer.AddTrack(track)
		if err != nil {
			return err
		}
		m.tracks = append(m.tracks, formatTrack{streamID: streams[i].ID, trackID: trackID})
	}
	m.closed = false
	return nil
}

func (m *FormatMuxer) Write(ctx context.Context, packet *av.Packet, _ *format.WriteResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if packet == nil {
		return format.ErrNilPacket
	}
	if m.closed {
		return io.ErrClosedPipe
	}
	if m.muxer == nil {
		return matroska.ErrNilWriter
	}
	trackID, ok := m.trackID(packet.StreamID)
	if !ok {
		return nil
	}
	timeValue, ok := packet.PTS.ToDuration()
	if !ok {
		return matroska.ErrInvalidData
	}
	var durationNS int64
	if packet.Duration.Base.Valid() {
		durationValue, ok := packet.Duration.ToDuration()
		if !ok {
			return matroska.ErrInvalidData
		}
		durationNS = int64(durationValue)
	}
	return m.muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     int64(timeValue),
		DurationNS: durationNS,
		Keyframe:   packet.Keyframe,
		Data:       packet.Payload.Bytes,
	})
}

func (m *FormatMuxer) Close() error {
	if m == nil || m.closed {
		return nil
	}
	m.closed = true
	if m.muxer == nil {
		return nil
	}
	return m.muxer.Close()
}

func (m *FormatMuxer) trackID(streamID av.StreamID) (uint32, bool) {
	for i := range m.tracks {
		if m.tracks[i].streamID == streamID {
			return m.tracks[i].trackID, true
		}
	}
	return 0, false
}

func (d *FormatDemuxer) Format() av.FormatID {
	return av.FormatWebM
}

func (d *FormatDemuxer) Open(ctx context.Context, input format.Input, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if input.Reader == nil {
		return matroska.ErrNilReader
	}
	demuxer, err := NewDemuxer(input.Reader, DemuxerOptions{})
	if err != nil {
		return err
	}
	d.demuxer = demuxer
	d.streams = d.streams[:0]
	d.tracks = d.tracks[:0]
	tracks := demuxer.Tracks()
	for i := range tracks {
		stream := streamFromTrack(tracks[i], i)
		d.streams = append(d.streams, stream)
		d.tracks = append(d.tracks, formatTrack{streamID: stream.ID, trackID: tracks[i].ID, media: stream.Type})
	}
	d.closed = false
	return nil
}

func (d *FormatDemuxer) Streams() []av.Stream {
	if len(d.streams) == 0 {
		return nil
	}
	streams := make([]av.Stream, len(d.streams))
	for i := range d.streams {
		streams[i] = cloneAVStream(d.streams[i])
	}
	return streams
}

func cloneAVStream(stream av.Stream) av.Stream {
	if len(stream.Codec.ExtraData.Bytes) != 0 {
		stream.Codec.ExtraData.Bytes = append([]byte(nil), stream.Codec.ExtraData.Bytes...)
	}
	return stream
}

func (d *FormatDemuxer) ReadInto(ctx context.Context, out *format.ReadResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil || out.Packet == nil {
		return format.ErrNilPacket
	}
	if d.closed {
		return io.EOF
	}
	if d.demuxer == nil {
		return matroska.ErrNilReader
	}
	packet := Packet{Data: out.Packet.Payload.Bytes[:0]}
	if err := d.demuxer.ReadPacket(&packet); err != nil {
		return err
	}
	track, ok := d.track(packet.TrackID)
	if !ok {
		return matroska.ErrUnknownTrack
	}
	out.Packet.Reset()
	out.Packet.StreamID = track.streamID
	out.Packet.Type = track.media
	out.Packet.Payload.Bytes = packet.Data
	out.Packet.Payload.Ownership = av.BufferOwned
	out.Packet.PTS = av.Timestamp{Value: packet.TimeNS, Base: av.TimeBase{Num: 1, Den: 1000000000}}
	if packet.DurationNS > 0 {
		out.Packet.Duration = av.Duration{Value: packet.DurationNS, Base: av.TimeBase{Num: 1, Den: 1000000000}}
	}
	out.Packet.Keyframe = packet.Keyframe
	out.PacketReady = true
	return nil
}

var _ format.Seeker = (*FormatDemuxer)(nil)

// Seek implements format.Seeker: it repositions the demuxer to the nearest
// cue at or before position (falling back to the cluster index when the file
// carries no Cues), so the next ReadInto resumes at a keyframe-aligned point
// at or before position. The demuxer's monotonic-timecode validation restarts
// at the new position. Input opened from a reader that cannot seek reports an
// error wrapping format.ErrNotSeekable.
func (d *FormatDemuxer) Seek(ctx context.Context, position time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed {
		return matroska.ErrClosed
	}
	if d.demuxer == nil {
		return matroska.ErrNilReader
	}
	if position < 0 {
		return matroska.ErrInvalidData
	}
	if err := d.demuxer.SeekToTime(int64(position)); err != nil {
		if errors.Is(err, matroska.ErrNonSeekableReader) {
			return fmt.Errorf("%w: %w", format.ErrNotSeekable, err)
		}
		return err
	}
	return nil
}

func (d *FormatDemuxer) Close() error {
	if d == nil {
		return nil
	}
	d.closed = true
	d.demuxer = nil
	return nil
}

func (d *FormatDemuxer) track(trackID uint32) (formatTrack, bool) {
	for i := range d.tracks {
		if d.tracks[i].trackID == trackID {
			return d.tracks[i], true
		}
	}
	return formatTrack{}, false
}

func trackFromAVStream(stream av.Stream, fallbackID uint32) (Track, error) {
	codec := codecFromAV(stream.Codec.ID)
	if codec == 0 {
		return Track{}, ErrUnsupportedWebMCodec
	}
	track := Track{
		ID:            fallbackID,
		Name:          stream.Name,
		Language:      stream.Language,
		LanguageBCP47: stream.Language,
		TimebaseNum:   stream.TimeBase.Num,
		TimebaseDen:   stream.TimeBase.Den,
		Codec:         codec,
		CodecPrivate:  stream.Codec.ExtraData.Bytes,
	}
	if track.Language == "" {
		track.Language = "und"
	}
	switch stream.Type {
	case av.MediaAudio:
		track.Type = TrackAudio
	case av.MediaVideo:
		track.Type = TrackVideo
	default:
		return Track{}, ErrUnsupportedWebMCodec
	}
	track.Audio = AudioConfig{SampleRate: stream.Codec.SampleRate, Channels: stream.Codec.Channels}
	track.Video = VideoConfig{Width: stream.Codec.Width, Height: stream.Codec.Height}
	if err := validateTrack(track); err != nil {
		return Track{}, err
	}
	return track, nil
}

func streamFromTrack(track Track, index int) av.Stream {
	streamType := av.MediaUnknown
	switch track.Type {
	case TrackAudio:
		streamType = av.MediaAudio
	case TrackVideo:
		streamType = av.MediaVideo
	}
	stream := av.Stream{
		ID:       av.StreamID(strconv.FormatUint(uint64(track.ID), 10)),
		Index:    index,
		Type:     streamType,
		TimeBase: av.TimeBase{Num: 1, Den: 1000000000},
		Language: trackLanguage(track),
		Name:     track.Name,
		Codec: av.CodecParameters{
			ID:         codecToAV(track.Codec),
			Type:       streamType,
			SampleRate: track.Audio.SampleRate,
			Channels:   track.Audio.Channels,
			Width:      track.Video.Width,
			Height:     track.Video.Height,
			ExtraData:  av.Buffer{Bytes: track.CodecPrivate, Ownership: av.BufferImmutable},
		},
	}
	if stream.Codec.SampleRate > 0 {
		stream.Codec.ClockRate = uint32(stream.Codec.SampleRate)
	}
	return stream
}

func trackLanguage(track Track) string {
	if track.LanguageBCP47 != "" {
		return track.LanguageBCP47
	}
	return track.Language
}

func matchWebMMIME(value string) bool {
	return strings.EqualFold(value, "video/webm") || strings.EqualFold(value, "audio/webm")
}

func hasWebMExtension(name string) bool {
	return strings.EqualFold(filepath.Ext(name), ".webm")
}

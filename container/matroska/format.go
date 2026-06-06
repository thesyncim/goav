package matroska

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
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
}

func Register(registry *format.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterProber(Prober{})
	registry.RegisterDemuxer(av.FormatMatroska, DemuxerFactory{})
	registry.RegisterMuxer(av.FormatMatroska, MuxerFactory{})
}

func (Prober) Probe(ctx context.Context, request format.ProbeRequest) (format.ProbeResult, error) {
	if err := ctx.Err(); err != nil {
		return format.ProbeResult{}, err
	}
	if len(request.Header) >= 4 && bytes.Equal(request.Header[:4], []byte{0x1a, 0x45, 0xdf, 0xa3}) {
		return format.ProbeResult{Format: av.FormatMatroska, Score: 100, Reason: "ebml header"}, nil
	}
	if hasMatroskaExtension(request.Name) || hasMatroskaExtension(request.Input.Name) || hasMatroskaExtension(request.Input.URI) {
		return format.ProbeResult{Format: av.FormatMatroska, Score: 80, Reason: "matroska extension"}, nil
	}
	return format.ProbeResult{}, format.ErrNotFound
}

func (DemuxerFactory) NewDemuxer(ctx context.Context, result format.ProbeResult) (format.Demuxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result.Format != "" && result.Format != av.FormatMatroska {
		return nil, format.ErrNotFound
	}
	return &FormatDemuxer{}, nil
}

func (MuxerFactory) NewMuxer(ctx context.Context, id av.FormatID) (format.Muxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id != "" && id != av.FormatMatroska {
		return nil, format.ErrNotFound
	}
	return &FormatMuxer{}, nil
}

func (m *FormatMuxer) Format() av.FormatID {
	return av.FormatMatroska
}

func (m *FormatMuxer) Open(ctx context.Context, output format.Output, streams []av.Stream, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if output.Writer == nil {
		return ErrNilWriter
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
		return ErrClosed
	}
	if m.muxer == nil {
		return ErrNilWriter
	}
	trackID, ok := m.trackID(packet.StreamID)
	if !ok {
		return nil
	}
	timeValue, ok := packet.PTS.ToDuration()
	if !ok {
		return ErrInvalidData
	}
	var durationNS int64
	if packet.Duration.Base.Valid() {
		durationValue, ok := packet.Duration.ToDuration()
		if !ok {
			return ErrInvalidData
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
	return av.FormatMatroska
}

func (d *FormatDemuxer) Open(ctx context.Context, input format.Input, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if input.Reader == nil {
		return ErrNilReader
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
		d.tracks = append(d.tracks, formatTrack{streamID: stream.ID, trackID: tracks[i].ID})
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
		return ErrNilReader
	}
	packet := Packet{Data: out.Packet.Payload.Bytes[:0]}
	if err := d.demuxer.ReadPacket(&packet); err != nil {
		return err
	}
	streamID, ok := d.streamID(packet.TrackID)
	if !ok {
		return ErrUnknownTrack
	}
	out.Packet.Reset()
	out.Packet.StreamID = streamID
	out.Packet.Payload.Bytes = packet.Data
	out.Packet.Payload.Ownership = av.BufferOwned
	out.Packet.PTS = av.Timestamp{Value: packet.TimeNS, Base: av.TimeBase{Num: 1, Den: timeNS}}
	if packet.DurationNS > 0 {
		out.Packet.Duration = av.Duration{Value: packet.DurationNS, Base: av.TimeBase{Num: 1, Den: timeNS}}
	}
	out.Packet.Keyframe = packet.Keyframe
	out.PacketReady = true
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

func (d *FormatDemuxer) streamID(trackID uint32) (av.StreamID, bool) {
	for i := range d.tracks {
		if d.tracks[i].trackID == trackID {
			return d.tracks[i].streamID, true
		}
	}
	return "", false
}

func trackFromAVStream(stream av.Stream, fallbackID uint32) (Track, error) {
	codec := codecFromAV(stream.Codec.ID)
	if codec == CodecUnknown {
		return Track{}, ErrUnsupportedCodec
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
		switch stream.Codec.Type {
		case av.MediaAudio:
			track.Type = TrackAudio
		case av.MediaVideo:
			track.Type = TrackVideo
		default:
			return Track{}, ErrInvalidTrack
		}
	}
	track.Audio = AudioConfig{
		SampleRate: stream.Codec.SampleRate,
		Channels:   stream.Codec.Channels,
	}
	track.Video = VideoConfig{
		Width:  stream.Codec.Width,
		Height: stream.Codec.Height,
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
		TimeBase: av.TimeBase{Num: 1, Den: timeNS},
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

func hasMatroskaExtension(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".mkv", ".mka", ".mks", ".mk3d", ".webm":
		return true
	default:
		return false
	}
}

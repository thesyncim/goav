package matroska

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

type Prober struct{}

type DemuxerFactory struct{}

type MuxerFactory struct{}

// TrackMuxer is the packet muxer the av adapter writes through: *Muxer, or a
// profile's muxer, such as WebM's, that checks tracks against the profile.
type TrackMuxer interface {
	AddTrack(Track) (uint32, error)
	WritePacket(Packet) error
	Close() error
}

// TrackDemuxer is the packet demuxer the av adapter reads through: *Demuxer,
// or a profile's demuxer.
type TrackDemuxer interface {
	Tracks() []Track
	ReadPacket(*Packet) error
	SeekToTime(int64) error
}

// FormatMuxer adapts a Matroska-family muxer to format.Muxer. The zero value
// writes Matroska; NewFormatMuxer writes a profile through its own muxer.
type FormatMuxer struct {
	format av.FormatID
	open   func(io.Writer) (TrackMuxer, error)
	muxer  TrackMuxer
	tracks []formatTrack
	closed bool
}

// FormatDemuxer adapts a Matroska-family demuxer to format.Demuxer. The zero
// value reads Matroska; NewFormatDemuxer reads a profile through its own
// demuxer.
type FormatDemuxer struct {
	format  av.FormatID
	open    func(io.Reader) (TrackDemuxer, error)
	demuxer TrackDemuxer
	streams []av.Stream
	tracks  []formatTrack
	closed  bool
}

// NewFormatMuxer returns the adapter writing format through the muxer open
// returns for each output.
func NewFormatMuxer(format av.FormatID, open func(io.Writer) (TrackMuxer, error)) *FormatMuxer {
	return &FormatMuxer{format: format, open: open}
}

// NewFormatDemuxer returns the adapter reading format through the demuxer
// open returns for each input.
func NewFormatDemuxer(format av.FormatID, open func(io.Reader) (TrackDemuxer, error)) *FormatDemuxer {
	return &FormatDemuxer{format: format, open: open}
}

// formatTrack maps a goav stream to its Matroska track. codecDelayNS is the
// track's CodecDelay: block timestamps run that far ahead of presentation
// time (the spec has readers subtract it), so the demuxer subtracts it and the
// muxer adds it back, and a packet's PTS is the time of its first decoded
// sample either way. For Opus that puts the pre-skip before zero.
type formatTrack struct {
	streamID     av.StreamID
	trackID      uint32
	media        av.MediaType
	codecDelayNS int64
	// opus marks an Opus track: a final packet shorter than its frames
	// carries the rest as DiscardPadding, the Matroska end-trim signal.
	opus bool
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
		Format: av.FormatMatroska,
		Media:  []av.MediaType{av.MediaAudio, av.MediaVideo, av.MediaSubtitle},
		Codecs: []av.CodecID{
			av.CodecOpus,
			av.CodecVorbis,
			av.CodecFLAC,
			av.CodecAAC,
			av.CodecVP8,
			av.CodecVP9,
			av.CodecAV1,
			av.CodecH264,
			av.CodecPCM,
			av.CodecTextUTF8,
		},
		MinStreams: 1,
		Metadata: av.Metadata{
			"summary": "Matroska supports multi-track audio, video, and subtitle packets without codec conversion",
		},
	}
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
	return cmp.Or(m.format, av.FormatMatroska)
}

func (m *FormatMuxer) Open(ctx context.Context, output format.Output, streams []av.Stream, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if output.Writer == nil {
		return ErrNilWriter
	}
	open := m.open
	if open == nil {
		open = openMatroskaMuxer
	}
	muxer, err := open(output.Writer)
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
		codecDelayNS, _, err := trackCodecTiming(track, track.CodecPrivate)
		if err != nil {
			return err
		}
		trackID, err := muxer.AddTrack(track)
		if err != nil {
			return err
		}
		m.tracks = append(m.tracks, formatTrack{streamID: streams[i].ID, trackID: trackID, codecDelayNS: codecDelayNS, opus: track.Codec == CodecOpus})
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
	track, ok := m.track(packet.StreamID)
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
	out := Packet{
		TrackID:    track.trackID,
		TimeNS:     int64(timeValue) + track.codecDelayNS,
		DurationNS: durationNS,
		Keyframe:   packet.Keyframe,
		Data:       packet.Payload.Bytes,
	}
	if track.opus {
		if padding := opusDiscardPaddingNS(packet.Payload.Bytes, durationNS); padding > 0 {
			out.DurationNS += padding
			out.DiscardPaddingNS = padding
		}
	}
	return m.muxer.WritePacket(out)
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

func (m *FormatMuxer) track(streamID av.StreamID) (formatTrack, bool) {
	for i := range m.tracks {
		if m.tracks[i].streamID == streamID {
			return m.tracks[i], true
		}
	}
	return formatTrack{}, false
}

func (d *FormatDemuxer) Format() av.FormatID {
	return cmp.Or(d.format, av.FormatMatroska)
}

func (d *FormatDemuxer) Open(ctx context.Context, input format.Input, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if input.Reader == nil {
		return ErrNilReader
	}
	open := d.open
	if open == nil {
		open = openMatroskaDemuxer
	}
	demuxer, err := open(input.Reader)
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
		d.tracks = append(d.tracks, formatTrack{streamID: stream.ID, trackID: tracks[i].ID, media: stream.Type, codecDelayNS: tracks[i].CodecDelayNS, opus: tracks[i].Codec == CodecOpus})
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
	track, ok := d.track(packet.TrackID)
	if !ok {
		return ErrUnknownTrack
	}
	out.Packet.Reset()
	out.Packet.StreamID = track.streamID
	out.Packet.Type = track.media
	out.Packet.Payload.Bytes = packet.Data
	out.Packet.PTS = av.Timestamp{Value: packet.TimeNS - track.codecDelayNS, Base: av.TimeBase{Num: 1, Den: timeNS}}
	durationNS := packet.DurationNS
	if track.opus && packet.DiscardPaddingNS > 0 {
		if durationNS <= 0 {
			durationNS, _ = opusPacketDurationNS(packet.Data)
		}
		durationNS -= packet.DiscardPaddingNS
	}
	if durationNS > 0 {
		out.Packet.Duration = av.Duration{Value: durationNS, Base: av.TimeBase{Num: 1, Den: timeNS}}
	}
	out.Packet.Keyframe = packet.Keyframe
	out.PacketReady = true
	return nil
}

var _ format.Seeker = (*FormatDemuxer)(nil)

// CanSeek reports whether the opened input can reposition (format.Seeker):
// the track demuxer's CanSeek when it has one.
func (d *FormatDemuxer) CanSeek() bool {
	if d == nil || d.demuxer == nil {
		return false
	}
	checker, ok := d.demuxer.(interface{ CanSeek() bool })
	return !ok || checker.CanSeek()
}

// Seek implements format.Seeker: it repositions the demuxer to the nearest
// cue at or before position (falling back to the demuxer's cluster index when
// the file carries no Cues), so the next ReadInto resumes at a keyframe-aligned
// point at or before position. Input opened from a reader that cannot seek
// reports an error wrapping format.ErrNotSeekable.
func (d *FormatDemuxer) Seek(ctx context.Context, position time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed {
		return ErrClosed
	}
	if d.demuxer == nil {
		return ErrNilReader
	}
	if position < 0 {
		return ErrInvalidData
	}
	if err := d.demuxer.SeekToTime(int64(position)); err != nil {
		if errors.Is(err, ErrNonSeekableReader) {
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
	case av.MediaSubtitle:
		track.Type = TrackSubtitle
	default:
		switch stream.Codec.Type {
		case av.MediaAudio:
			track.Type = TrackAudio
		case av.MediaVideo:
			track.Type = TrackVideo
		case av.MediaSubtitle:
			track.Type = TrackSubtitle
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
	case TrackSubtitle:
		streamType = av.MediaSubtitle
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
			ExtraData:  av.Buffer{Bytes: track.CodecPrivate},
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

func openMatroskaMuxer(w io.Writer) (TrackMuxer, error) {
	muxer, err := NewMuxer(w, MuxerOptions{})
	if err != nil {
		return nil, err
	}
	return muxer, nil
}

func openMatroskaDemuxer(r io.Reader) (TrackDemuxer, error) {
	demuxer, err := NewDemuxer(r, DemuxerOptions{})
	if err != nil {
		return nil, err
	}
	return demuxer, nil
}

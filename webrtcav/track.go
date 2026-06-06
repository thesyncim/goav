package webrtcav

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/rtpav"
)

var (
	ErrClosed         = errors.New("webrtcav: closed")
	ErrNilTrack       = errors.New("webrtcav: nil track")
	ErrTrackQueueFull = errors.New("webrtcav: track queue full")
)

type TrackRemoteAdapter struct{}

func NewTrackRemoteAdapter() TrackRemoteAdapter {
	return TrackRemoteAdapter{}
}

func (TrackRemoteAdapter) AdaptTrack(ctx context.Context, remote RemoteTrack) (TrackReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if remote.Track == nil {
		return nil, ErrNilTrack
	}
	if remote.Codec.PayloadType == 0 && remote.Codec.MimeType == "" {
		remote.Codec = remote.Track.Codec()
	}
	if remote.Stream.ID == "" {
		remote.Stream.ID = av.StreamID(remote.Track.ID())
	}
	if remote.Stream.Name == "" {
		remote.Stream.Name = remote.Track.ID()
	}
	remote.Stream.Metadata = mergeMetadata(remote.Stream.Metadata, trackMetadata(remote.Track, remote.Metadata))
	return newTrackReader(remote, trackRemoteRTPReader{track: remote.Track}), nil
}

type trackRTPReader interface {
	ReadRTP() (*rtp.Packet, interceptor.Attributes, error)
}

type trackRemoteRTPReader struct {
	track *webrtc.TrackRemote
}

func (r trackRemoteRTPReader) ReadRTP() (*rtp.Packet, interceptor.Attributes, error) {
	return r.track.ReadRTP()
}

type trackReader struct {
	remote  RemoteTrack
	reader  trackRTPReader
	events  chan av.Event
	closed  bool
	streams []av.Stream
}

var _ rtpav.PacketReader = (*trackReader)(nil)
var _ rtpav.Receiver = (*trackReader)(nil)

func newTrackReader(remote RemoteTrack, reader trackRTPReader) *trackReader {
	stream := streamFromRemoteTrack(remote)
	payloads := remote.Payloads
	if payloads == nil {
		payloads = rtpav.NewStaticPayloadMap(stream.Epoch, []rtpav.PayloadCodec{
			payloadCodecFromWebRTC(remote.Codec, stream.Codec),
		})
	}
	remote.Stream = stream
	remote.Payloads = payloads

	return &trackReader{
		remote:  remote,
		reader:  reader,
		events:  make(chan av.Event, 1),
		streams: []av.Stream{stream},
	}
}

func (r *trackReader) ReadRTP(ctx context.Context) (*rtp.Packet, error) {
	if r.closed {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	packet, _, err := r.reader.ReadRTP()
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.emit(av.Event{Type: av.EventEndOfStream, StreamID: r.remote.Stream.ID, Epoch: r.remote.Stream.Epoch})
		}
		return nil, err
	}
	return packet, ctx.Err()
}

func (r *trackReader) Events() <-chan av.Event {
	return r.events
}

func (r *trackReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.events)
	return nil
}

func (r *trackReader) Streams(context.Context) ([]av.Stream, error) {
	streams := make([]av.Stream, len(r.streams))
	copy(streams, r.streams)
	return streams, nil
}

func (r *trackReader) PayloadMap() rtpav.PayloadMap {
	return r.remote.Payloads
}

func (r *trackReader) WriteRTCP(ctx context.Context, packets []rtcp.Packet) error {
	if r.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(packets) == 0 || r.remote.Feedback == nil {
		return nil
	}
	return r.remote.Feedback.WriteRTCP(ctx, packets)
}

func (r *trackReader) emit(event av.Event) {
	select {
	case r.events <- event:
	default:
	}
}

func streamFromRemoteTrack(remote RemoteTrack) av.Stream {
	stream := remote.Stream
	stream.Metadata = mergeMetadata(stream.Metadata, remote.Metadata)
	codec := codecParametersFromWebRTC(remote.Codec)
	if stream.Codec.ID == "" {
		stream.Codec.ID = codec.ID
	}
	if stream.Codec.Type == "" {
		stream.Codec.Type = codec.Type
	}
	if stream.Type == "" {
		stream.Type = codec.Type
	}
	if stream.Codec.ClockRate == 0 {
		stream.Codec.ClockRate = codec.ClockRate
	}
	if stream.Codec.SampleRate == 0 {
		stream.Codec.SampleRate = codec.SampleRate
	}
	if stream.Codec.Channels == 0 {
		stream.Codec.Channels = codec.Channels
	}
	if stream.Codec.Attributes == nil {
		stream.Codec.Attributes = codec.Attributes
	}
	if stream.TimeBase == (av.TimeBase{}) && stream.Codec.ClockRate != 0 {
		stream.TimeBase = av.RTPTimeBase(stream.Codec.ClockRate)
	}
	return stream
}

func codecParametersFromWebRTC(codec webrtc.RTPCodecParameters) av.CodecParameters {
	id, media := codecIDFromMIME(codec.MimeType)
	params := av.CodecParameters{
		ID:         id,
		Type:       media,
		ClockRate:  codec.ClockRate,
		SampleRate: int(codec.ClockRate),
		Channels:   int(codec.Channels),
		Attributes: av.Metadata{
			"mime_type": codec.MimeType,
			"fmtp":      codec.SDPFmtpLine,
		},
	}
	return params
}

func payloadCodecFromWebRTC(codec webrtc.RTPCodecParameters, params av.CodecParameters) rtpav.PayloadCodec {
	return rtpav.PayloadCodec{
		PayloadType: uint8(codec.PayloadType),
		Parameters:  params,
		MIMEType:    codec.MimeType,
		ClockRate:   codec.ClockRate,
		Channels:    codec.Channels,
		FMTP:        codec.SDPFmtpLine,
		Attributes:  params.Attributes,
	}
}

func codecIDFromMIME(mimeType string) (av.CodecID, av.MediaType) {
	switch {
	case strings.EqualFold(mimeType, webrtc.MimeTypeOpus):
		return av.CodecOpus, av.MediaAudio
	case strings.EqualFold(mimeType, webrtc.MimeTypeVP8):
		return av.CodecVP8, av.MediaVideo
	case strings.EqualFold(mimeType, webrtc.MimeTypeVP9):
		return av.CodecVP9, av.MediaVideo
	case strings.EqualFold(mimeType, webrtc.MimeTypeH264):
		return av.CodecH264, av.MediaVideo
	case strings.EqualFold(mimeType, webrtc.MimeTypeAV1):
		return av.CodecAV1, av.MediaVideo
	default:
		return av.CodecUnknown, av.MediaUnknown
	}
}

func trackMetadata(track *webrtc.TrackRemote, extra av.Metadata) av.Metadata {
	metadata := mergeMetadata(nil, extra)
	metadata["track_id"] = track.ID()
	metadata["stream_id"] = track.StreamID()
	metadata["rid"] = track.RID()
	metadata["ssrc"] = strconv.FormatUint(uint64(track.SSRC()), 10)
	if rtx := track.RtxSSRC(); rtx != 0 {
		metadata["rtx_ssrc"] = strconv.FormatUint(uint64(rtx), 10)
	}
	return metadata
}

func mergeMetadata(base av.Metadata, overlay av.Metadata) av.Metadata {
	if len(base) == 0 && len(overlay) == 0 {
		return av.Metadata{}
	}
	out := make(av.Metadata, len(base)+len(overlay))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range overlay {
		out[key] = value
	}
	return out
}

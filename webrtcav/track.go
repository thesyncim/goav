package webrtcav

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/rtpav"
)

var (
	ErrClosed             = errors.New("webrtcav: closed")
	ErrInvalidCodecUpdate = errors.New("webrtcav: invalid codec update")
	ErrNilTrack           = errors.New("webrtcav: nil track")
	ErrNilSession         = errors.New("webrtcav: nil session")
	ErrStreamExists       = errors.New("webrtcav: stream already exists")
	ErrTrackQueueFull     = errors.New("webrtcav: track queue full")
	ErrUnknownStream      = errors.New("webrtcav: unknown stream")
)

const defaultTrackEventBuffer = 4

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
	mu      sync.RWMutex
	remote  RemoteTrack
	reader  trackRTPReader
	events  chan av.Event
	closed  bool
	streams []av.Stream
}

var _ rtpav.PacketReader = (*trackReader)(nil)
var _ rtpav.FeedbackWriter = (*trackReader)(nil)

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
		events:  make(chan av.Event, defaultTrackEventBuffer),
		streams: []av.Stream{stream},
	}
}

func (r *trackReader) ReadRTP(ctx context.Context) (*rtp.Packet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reader, stream, err := r.snapshotReader()
	if err != nil {
		return nil, err
	}
	packet, _, err := reader.ReadRTP()
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.emit(av.Event{Type: av.EventEndOfStream, StreamID: stream.ID, Epoch: stream.Epoch})
		}
		return nil, err
	}
	return packet, ctx.Err()
}

func (r *trackReader) Events() <-chan av.Event {
	return r.events
}

func (r *trackReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.events)
	return nil
}

func (r *trackReader) Streams(context.Context) ([]av.Stream, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	streams := make([]av.Stream, len(r.streams))
	copy(streams, r.streams)
	return streams, nil
}

func (r *trackReader) PayloadMap() rtpav.PayloadMap {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.remote.Payloads
}

func (r *trackReader) UpdateCodec(ctx context.Context, update TrackCodecUpdate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !hasCodecUpdate(update) {
		return ErrInvalidCodecUpdate
	}
	next, _, err := r.applyCodecUpdate(update, nil)
	if err != nil {
		return err
	}
	r.emitCodecChanged(next)
	return nil
}

func (r *trackReader) UpdateTrack(ctx context.Context, remote RemoteTrack) error {
	return r.updateTrack(ctx, remote, nil)
}

func (r *trackReader) updateTrack(ctx context.Context, remote RemoteTrack, replacement trackRTPReader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	remote = normalizeRemoteTrack(remote)
	if replacement == nil && remote.Track != nil {
		replacement = trackRemoteRTPReader{track: remote.Track}
	}
	update := TrackCodecUpdate{
		Codec:    remote.Codec,
		Stream:   remote.Stream,
		Payloads: remote.Payloads,
		Metadata: remote.Metadata,
	}
	if !hasCodecUpdate(update) && replacement == nil {
		return ErrInvalidCodecUpdate
	}
	next, _, err := r.applyCodecUpdate(update, replacement)
	if err != nil {
		return err
	}
	r.mergeRemoteTrack(remote)
	r.emitCodecChanged(next)
	return nil
}

func (r *trackReader) snapshotReader() (trackRTPReader, av.Stream, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, av.Stream{}, ErrClosed
	}
	if r.reader == nil {
		return nil, av.Stream{}, ErrNilTrack
	}
	return r.reader, r.remote.Stream, nil
}

func (r *trackReader) applyCodecUpdate(update TrackCodecUpdate, replacement trackRTPReader) (av.Stream, rtpav.PayloadMap, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return av.Stream{}, nil, ErrClosed
	}
	current := r.remote.Stream
	if len(r.streams) != 0 {
		current = r.streams[0]
	}
	next := streamFromCodecUpdate(current, update)
	payloads := update.Payloads
	if payloads == nil {
		codec := update.Codec
		if !hasWebRTCCodec(codec) {
			codec = r.remote.Codec
		}
		payloads = rtpav.NewStaticPayloadMap(next.Epoch, []rtpav.PayloadCodec{
			payloadCodecFromWebRTC(codec, next.Codec),
		})
	}
	r.remote.Stream = next
	if hasWebRTCCodec(update.Codec) {
		r.remote.Codec = update.Codec
	}
	if replacement != nil {
		r.reader = replacement
	}
	r.remote.Payloads = payloads
	r.remote.Metadata = mergeMetadata(r.remote.Metadata, update.Metadata)
	r.streams = []av.Stream{next}
	return next, payloads, nil
}

func (r *trackReader) WriteRTCP(ctx context.Context, packets []rtcp.Packet) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return ErrClosed
	}
	feedback := r.remote.Feedback
	r.mu.RUnlock()
	if len(packets) == 0 || feedback == nil {
		return nil
	}
	return feedback.WriteRTCP(ctx, packets)
}

func (r *trackReader) emit(event av.Event) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}
	select {
	case r.events <- event:
	default:
	}
}

func (r *trackReader) emitCodecChanged(stream av.Stream) {
	r.emit(av.Event{
		Type:     av.EventCodecChanged,
		StreamID: stream.ID,
		Epoch:    stream.Epoch,
		Stream:   &stream,
		Codec:    &stream.Codec,
		Reason:   "webrtc track codec changed",
	})
}

func normalizeRemoteTrack(remote RemoteTrack) RemoteTrack {
	if remote.Track != nil {
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
	}
	return remote
}

func (r *trackReader) mergeRemoteTrack(remote RemoteTrack) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if remote.Track != nil {
		r.remote.Track = remote.Track
	}
	if remote.Receiver != nil {
		r.remote.Receiver = remote.Receiver
	}
	if remote.Transceiver != nil {
		r.remote.Transceiver = remote.Transceiver
	}
	if remote.Feedback != nil {
		r.remote.Feedback = remote.Feedback
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

func streamFromCodecUpdate(current av.Stream, update TrackCodecUpdate) av.Stream {
	next := mergeStream(current, update.Stream)
	if update.Metadata != nil {
		next.Metadata = mergeMetadata(next.Metadata, update.Metadata)
	}
	if hasWebRTCCodec(update.Codec) {
		mergeWebRTCCodec(&next, update.Codec)
	}
	if next.Type == "" {
		next.Type = next.Codec.Type
	}
	if next.TimeBase == (av.TimeBase{}) && next.Codec.ClockRate != 0 {
		next.TimeBase = av.RTPTimeBase(next.Codec.ClockRate)
	}
	if update.Stream.Epoch == 0 && update.Payloads != nil && update.Payloads.Epoch() != 0 {
		next.Epoch = update.Payloads.Epoch()
	}
	if next.Epoch == current.Epoch {
		next.Epoch = current.Epoch + 1
		if next.Epoch == 0 {
			next.Epoch = 1
		}
	}
	return next
}

func mergeStream(base av.Stream, update av.Stream) av.Stream {
	out := base
	if update.ID != "" {
		out.ID = update.ID
	}
	if update.Type != "" {
		out.Type = update.Type
	}
	if update.TimeBase != (av.TimeBase{}) {
		out.TimeBase = update.TimeBase
	}
	if update.Language != "" {
		out.Language = update.Language
	}
	if update.Name != "" {
		out.Name = update.Name
	}
	if update.Epoch != 0 {
		out.Epoch = update.Epoch
	}
	out.Metadata = mergeMetadata(out.Metadata, update.Metadata)
	mergeCodecParameters(&out.Codec, update.Codec)
	return out
}

func mergeCodecParameters(base *av.CodecParameters, update av.CodecParameters) {
	if update.ID != "" {
		base.ID = update.ID
	}
	if update.Type != "" {
		base.Type = update.Type
	}
	if update.Profile != "" {
		base.Profile = update.Profile
	}
	if update.Level != "" {
		base.Level = update.Level
	}
	if update.ClockRate != 0 {
		base.ClockRate = update.ClockRate
	}
	if update.SampleRate != 0 {
		base.SampleRate = update.SampleRate
	}
	if update.Channels != 0 {
		base.Channels = update.Channels
	}
	if update.ChannelLayout != "" {
		base.ChannelLayout = update.ChannelLayout
	}
	if update.Width != 0 {
		base.Width = update.Width
	}
	if update.Height != 0 {
		base.Height = update.Height
	}
	if update.PixelFormat != "" {
		base.PixelFormat = update.PixelFormat
	}
	if update.SampleFormat != "" {
		base.SampleFormat = update.SampleFormat
	}
	if len(update.ExtraData.Bytes) != 0 {
		base.ExtraData = update.ExtraData
	}
	base.Attributes = mergeMetadata(base.Attributes, update.Attributes)
}

func mergeWebRTCCodec(stream *av.Stream, codec webrtc.RTPCodecParameters) {
	params := codecParametersFromWebRTC(codec)
	mergeCodecParameters(&stream.Codec, params)
	if stream.Type == "" || params.Type != "" {
		stream.Type = params.Type
	}
	if params.ClockRate != 0 {
		stream.TimeBase = av.RTPTimeBase(params.ClockRate)
	}
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

func hasCodecUpdate(update TrackCodecUpdate) bool {
	return hasWebRTCCodec(update.Codec) || update.Payloads != nil || update.Stream.Codec.ID != "" ||
		update.Stream.Epoch != 0 || update.Stream.Type != "" || update.Stream.TimeBase != (av.TimeBase{})
}

func hasWebRTCCodec(codec webrtc.RTPCodecParameters) bool {
	return codec.PayloadType != 0 || codec.MimeType != "" || codec.ClockRate != 0 || codec.SDPFmtpLine != ""
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

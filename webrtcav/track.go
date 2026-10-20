package webrtcav

import (
	"context"
	"errors"
	"io"
	"maps"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/rtpav"
)

// WebRTC sentinels, matched with errors.Is.
var (
	// ErrClosed reports a call on a closed session, reader, or track set.
	ErrClosed = errors.New("webrtcav: closed")
	// ErrInvalidCodecUpdate reports a codec update missing the facts a live
	// reader needs to adopt it.
	ErrInvalidCodecUpdate = errors.New("webrtcav: invalid codec update")
	// ErrNilTrack reports a remote track without a Pion track handle.
	ErrNilTrack = errors.New("webrtcav: nil track")
	// ErrNilSession reports a nil session handed to a constructor.
	ErrNilSession = errors.New("webrtcav: nil session")
	// ErrStreamExists reports a second track claiming an existing stream id
	// where a replacement was not requested.
	ErrStreamExists = errors.New("webrtcav: stream already exists")
	// ErrTrackQueueFull reports an accepted track arriving faster than the
	// session consumer drains them.
	ErrTrackQueueFull = errors.New("webrtcav: track queue full")
	// ErrUnknownStream reports an update naming a stream no reader owns.
	ErrUnknownStream = errors.New("webrtcav: unknown stream")
)

const defaultTrackEventBuffer = 4

// TrackRemoteAdapter is the default TrackAdapter: it reads RTP straight from
// the Pion remote track, deriving the stream identity from the track.
type TrackRemoteAdapter struct{}

// NewTrackRemoteAdapter returns the stateless default TrackAdapter used when
// a session declares no custom one; provide your own TrackAdapter to wrap or
// replace how remote tracks become TrackReaders.
func NewTrackRemoteAdapter() TrackRemoteAdapter {
	return TrackRemoteAdapter{}
}

// AdaptTrack wraps the remote track in a TrackReader, filling codec and
// stream identity from the track when undeclared.
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
	// SetReadDeadline bounds a blocked ReadRTP: a deadline at or before now
	// makes it return, and the zero time clears the bound.
	SetReadDeadline(time.Time) error
}

type trackRemoteRTPReader struct {
	track *webrtc.TrackRemote
}

func (r trackRemoteRTPReader) ReadRTP() (*rtp.Packet, interceptor.Attributes, error) {
	return r.track.ReadRTP()
}

func (r trackRemoteRTPReader) SetReadDeadline(deadline time.Time) error {
	return r.track.SetReadDeadline(deadline)
}

type trackReader struct {
	mu      sync.RWMutex
	remote  RemoteTrack
	reader  trackRTPReader
	events  chan av.Event
	closed  bool
	streams []av.Stream

	// Read interruption. The reader owns its transport read: Close, or the
	// read's context finishing, moves the blocked read's deadline to now so
	// it returns. One context.AfterFunc hook is kept per distinct read
	// context (a source reads with one), so steady reads register nothing.
	inflight        trackRTPReader // the transport reader a ReadRTP is blocked in
	expired         trackRTPReader // the transport reader whose deadline was moved
	armedDone       <-chan struct{}
	stopArm         func() bool
	armGen          uint64
	readerGen       uint64
	configGen       uint64
	deliveredConfig uint64
}

var _ rtpav.PacketReader = (*trackReader)(nil)
var _ rtpav.FeedbackWriter = (*trackReader)(nil)

func newTrackReader(remote RemoteTrack, reader trackRTPReader) *trackReader {
	stream := streamFromRemoteTrack(remote)
	stream = cloneReadStream(stream)
	payloads := remote.Payloads
	if payloads == nil {
		payloads = rtpav.NewStaticPayloadMap(stream.Epoch, []rtpav.PayloadCodec{
			payloadCodecFromWebRTC(remote.Codec, stream.Codec),
		})
	}
	remote.Stream = stream
	remote.Payloads = trackPayloadMap(stream, payloads)

	return &trackReader{
		remote:  remote,
		reader:  reader,
		events:  make(chan av.Event, defaultTrackEventBuffer),
		streams: []av.Stream{stream},
	}
}

type trackReadState struct {
	reader               trackRTPReader
	stream               av.Stream
	payloads             rtpav.PayloadMap
	readerGen, configGen uint64
}

// ReadRTP pairs each packet with the configuration captured when its
// read began. This is independent of the bounded observer notification channel.
func (r *trackReader) ReadRTP(ctx context.Context) (*rtp.Packet, *av.Event, rtpav.PayloadMap, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	for {
		state, err := r.beginRead(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		packet, _, err := state.reader.ReadRTP()
		replaced, stopped := r.endRead(ctx, &state, packet)
		if stopped != nil {
			return nil, nil, nil, stopped
		}
		if replaced {
			continue // an old packet or replacement wake-up cannot escape
		}
		var event *av.Event
		if err == nil || errors.Is(err, io.EOF) {
			r.mu.Lock()
			if state.configGen != r.deliveredConfig {
				e := trackCodecChangedEvent(state.stream)
				event = &e
				r.deliveredConfig = state.configGen
			}
			r.mu.Unlock()
		}
		if errors.Is(err, io.EOF) {
			r.emit(av.Event{Type: av.EventEndOfStream, StreamID: state.stream.ID, Epoch: state.stream.Epoch})
		}
		return packet, event, state.payloads, err
	}
}

// beginRead claims the transport reader for one read and arms its
// interruption by ctx.
func (r *trackReader) beginRead(ctx context.Context) (trackReadState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return trackReadState{}, ErrClosed
	}
	if r.reader == nil {
		return trackReadState{}, ErrNilTrack
	}
	if done := ctx.Done(); done != r.armedDone {
		r.armGen++
		if r.stopArm != nil {
			r.stopArm()
			r.stopArm = nil
		}
		r.armedDone = done
		if done != nil {
			gen := r.armGen
			r.stopArm = context.AfterFunc(ctx, func() { r.interrupt(gen) })
		}
	}
	// Checked under the lock: a context that finishes after this check runs
	// its hook later, and the hook finds this read in flight.
	if err := ctx.Err(); err != nil {
		return trackReadState{}, err
	}
	if r.expired == r.reader {
		_ = r.reader.SetReadDeadline(time.Time{})
		r.expired = nil
	}
	r.inflight = r.reader
	return trackReadState{r.reader, r.remote.Stream, r.remote.Payloads, r.readerGen, r.configGen}, nil
}

// endRead releases the read and reports why it was interrupted, if it was.
func (r *trackReader) endRead(ctx context.Context, state *trackReadState, packet *rtp.Packet) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	replaced := state.readerGen != r.readerGen
	if replaced && r.expired != nil {
		_ = r.expired.SetReadDeadline(time.Time{})
		r.expired = nil
	}
	r.inflight = nil
	if r.closed {
		return false, ErrClosed
	}
	// An unfamiliar payload type identifies a renegotiated packet returned
	// by the old read. A type the captured map knows keeps the old generation.
	if !replaced && packet != nil && state.payloads != nil && r.remote.Payloads != nil {
		if _, old := state.payloads.Lookup(packet.PayloadType); !old {
			if _, next := r.remote.Payloads.Lookup(packet.PayloadType); next {
				state.stream, state.payloads, state.configGen = r.remote.Stream, r.remote.Payloads, r.configGen
			}
		}
	}
	return replaced, ctx.Err()
}

// interrupt wakes the in-flight read for the context hook of generation gen;
// a hook superseded by a newer read context does nothing.
func (r *trackReader) interrupt(gen uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if gen == r.armGen {
		r.expireLocked()
	}
}

func (r *trackReader) expireLocked() {
	if r.inflight == nil {
		return
	}
	_ = r.inflight.SetReadDeadline(time.Now())
	r.expired = r.inflight
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
	r.expireLocked()
	if r.stopArm != nil {
		r.stopArm()
		r.stopArm = nil
	}
	return nil
}

func (r *trackReader) Streams(context.Context) ([]av.Stream, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	streams := make([]av.Stream, len(r.streams))
	for i, stream := range r.streams {
		streams[i] = cloneReadStream(stream)
	}
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
	_, _, err := r.applyCodecUpdate(update, nil)
	return err
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
	_, _, err := r.applyCodecUpdate(update, replacement)
	if err != nil {
		return err
	}
	r.mergeRemoteTrack(remote)
	return nil
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
	next = cloneReadStream(next)
	if next.Epoch <= current.Epoch {
		return av.Stream{}, nil, ErrInvalidCodecUpdate
	}
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
		r.expireLocked()
		r.readerGen++
		r.reader = replacement
	}
	payloads = trackPayloadMap(next, payloads)
	r.remote.Payloads = payloads
	r.remote.Metadata = mergeMetadata(r.remote.Metadata, update.Metadata)
	r.streams = []av.Stream{next}
	r.configGen++
	// Commit state and its notification under one lock so concurrent updates
	// cannot publish in reverse order. Slow observers retain the newest state.
	r.emitLatestLocked(trackCodecChangedEvent(next))
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

func (r *trackReader) emitLatestLocked(event av.Event) {
	for {
		select {
		case r.events <- event:
			return
		default:
		}
		select {
		case <-r.events: // coalesce an obsolete observer notification
		default: // an observer drained it between the two selects
		}
	}
}

func trackCodecChangedEvent(stream av.Stream) av.Event {
	stream = cloneReadStream(stream)
	return av.Event{
		Type: av.EventCodecChanged, StreamID: stream.ID, Epoch: stream.Epoch,
		Stream: &stream, Codec: &stream.Codec, Reason: "webrtc track codec changed",
	}
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
	if next.Epoch <= current.Epoch {
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
		SampleRate: sampleRateFromClock(codec.ClockRate),
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

// Every atomic map carries transport provenance, even when several tracks use
// the same payload type and codec. Its epoch belongs to this track only.
func trackPayloadMap(stream av.Stream, payloads rtpav.PayloadMap) rtpav.PayloadMap {
	codecs := payloads.Codecs()
	for i := range codecs {
		codecs[i].StreamID = stream.ID
	}
	return rtpav.NewStaticPayloadMap(stream.Epoch, codecs)
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
	maps.Copy(out, base)
	maps.Copy(out, overlay)
	return out
}

// sampleRateFromClock reports an audio clock rate as a sample rate, or 0 when
// the negotiated rate does not fit an int on this platform.
func sampleRateFromClock(clock uint32) int {
	if clock <= math.MaxInt32 {
		return int(clock)
	}
	return 0
}

package flv

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/aac"
	"github.com/thesyncim/goav/internal/av1"
	"github.com/thesyncim/goav/internal/h264"
	"github.com/thesyncim/goav/internal/hevc"
	"github.com/thesyncim/goav/internal/opus"
	"github.com/thesyncim/goav/internal/vpx"
)

// MuxerFactory opens FLV muxers behind the goav format extension point.
type MuxerFactory struct{}

// NewMuxer returns an FLV muxer.
func (MuxerFactory) NewMuxer(ctx context.Context, id av.FormatID) (format.Muxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id != "" && id != av.FormatFLV {
		return nil, format.ErrNotFound
	}
	return &Muxer{}, nil
}

// maxTracks is how many tracks of one media a multitrack FLV can number.
const maxTracks = 256

// Muxer writes FLV. Each Write is one tag in one write to the output, so an
// output that forwards tags (RTMP) sees whole ones; a stream's sequence
// start goes out before its first frame, from its ExtraData or, when the
// stream declared none, from its first keyframe. Video before a stream's
// first keyframe is dropped: nothing can decode it. It implements
// format.Muxer.
type Muxer struct {
	w      io.Writer
	seeker io.WriteSeeker
	tracks []muxTrack
	buf    []byte
	// written counts output bytes; durationAt and sizeAt are where the
	// onMetaData duration and filesize numbers are, to patch.
	written, durationAt, sizeAt int64
	// origin is the first packet's decode time, which becomes zero, and
	// end the latest presentation end, both in milliseconds.
	origin, end int64
	started     bool
	err         error
	closed      bool
}

// muxTrack is one stream's track.
type muxTrack struct {
	id     av.StreamID
	media  av.MediaType
	codec  av.CodecID
	track  uint8
	stream av.Stream
	config []byte
	// configured is set once the sequence start is out.
	configured bool
}

func (m *Muxer) Format() av.FormatID { return av.FormatFLV }

// Open writes the header, onMetaData, and the sequence starts the streams
// declare.
func (m *Muxer) Open(ctx context.Context, output format.Output, streams []av.Stream, options format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.w != nil {
		return errors.New("flv: muxer opened twice")
	}
	if output.Writer == nil {
		return errors.New("flv: nil writer")
	}
	if len(streams) == 0 {
		return fmt.Errorf("%w: no streams", ErrUnsupported)
	}
	counts := map[av.MediaType]int{}
	for _, stream := range streams {
		media := cmp.Or(stream.Type, codecMediaType(stream.Codec.ID))
		t, err := newMuxTrack(stream, media, counts[media])
		if err != nil {
			return err
		}
		counts[media]++
		m.tracks = append(m.tracks, t)
	}
	m.w = output.Writer
	if seeker, ok := output.Writer.(io.WriteSeeker); ok && !output.Realtime && !options.Realtime {
		m.seeker = seeker
	}
	var flags byte
	if counts[av.MediaVideo] > 0 {
		flags |= flagVideo
	}
	if counts[av.MediaAudio] > 0 {
		flags |= flagAudio
	}
	m.buf = append(m.buf[:0], 'F', 'L', 'V', 1, flags, 0, 0, 0, 9, 0, 0, 0, 0)
	if err := m.flush(); err != nil {
		return err
	}
	if err := m.writeMetadata(); err != nil {
		return err
	}
	for i := range m.tracks {
		if t := &m.tracks[i]; t.config != nil {
			if err := m.writeConfig(t, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

// newMuxTrack checks that FLV carries stream and reads the configuration
// it declares.
func newMuxTrack(stream av.Stream, media av.MediaType, index int) (muxTrack, error) {
	codec := stream.Codec.ID
	if _, ok := codecFourCC[codec]; !ok || media != codecMediaType(codec) {
		return muxTrack{}, fmt.Errorf("%w: stream %s: FLV carries H264, HEVC, AV1, VP9, VP8, AAC, Opus, and FLAC, not %s %s", ErrUnsupported, stream.ID, media, codec)
	}
	if index >= maxTracks {
		return muxTrack{}, fmt.Errorf("%w: stream %s: FLV numbers at most %d %s tracks", ErrUnsupported, stream.ID, maxTracks, media)
	}
	t := muxTrack{id: stream.ID, media: media, codec: codec, track: uint8(index), stream: stream}
	extra := stream.Codec.ExtraData.Bytes
	switch codec {
	case av.CodecH264:
		if h264.HasStartCode(extra) {
			t.config, _ = h264.ConfigFromAnnexB(extra)
		} else if _, err := h264.ParseConfig(extra); err == nil {
			t.config = extra
		}
	case av.CodecHEVC:
		if h264.HasStartCode(extra) {
			t.config, _ = hevc.ConfigFromAnnexB(extra)
		} else if _, err := hevc.ParseConfig(extra); err == nil {
			t.config = extra
		}
	case av.CodecAV1:
		if _, err := av1.ParseConfig(extra); err == nil {
			t.config = extra
		}
	case av.CodecVP8, av.CodecVP9:
		if len(extra) >= 12 && extra[0] == 1 {
			t.config = extra // a vpcC payload, version 1
		}
	case av.CodecAAC:
		if _, _, ok := aac.ParseConfig(extra); ok {
			t.config = extra
		}
	case av.CodecOpus:
		t.config = extra
		if len(extra) < 19 || string(extra[:8]) != "OpusHead" {
			t.config = opus.DefaultHead(stream.Codec.Channels)
		}
	case av.CodecFLAC:
		if len(extra) >= 4 && string(extra[:4]) != "fLaC" {
			extra = append([]byte("fLaC"), extra...)
		}
		if len(extra) < 4+4+34 {
			return muxTrack{}, fmt.Errorf("%w: FLAC stream %s needs its STREAMINFO in ExtraData", ErrUnsupported, stream.ID)
		}
		t.config = extra
	}
	return t, nil
}

// writeMetadata writes onMetaData: the codecs, the geometry the streams
// declare, and per-track maps for tracks after the first of their media.
func (m *Muxer) writeMetadata() error {
	body := appendAMFString(nil, "onMetaData")
	body = append(body, amfECMAArray, 0, 0, 0, 0)
	body = appendAMFName(body, "duration")
	durationAt := len(body) + 1
	body = appendAMFNumber(body, 0)
	body = appendAMFName(body, "filesize")
	sizeAt := len(body) + 1
	body = appendAMFNumber(body, 0)
	var videoMap, audioMap []byte
	for i := range m.tracks {
		t := &m.tracks[i]
		var props []byte
		codec := t.stream.Codec
		if t.media == av.MediaVideo {
			props = appendNumberProperty(props, "videocodecid", float64(metadataCodecID(t)))
			if codec.Width > 0 && codec.Height > 0 {
				props = appendNumberProperty(props, "width", float64(codec.Width))
				props = appendNumberProperty(props, "height", float64(codec.Height))
			}
		} else {
			props = appendNumberProperty(props, "audiocodecid", float64(metadataCodecID(t)))
			if codec.SampleRate > 0 {
				props = appendNumberProperty(props, "audiosamplerate", float64(codec.SampleRate))
			}
			props = appendNumberProperty(props, "audiosamplesize", 16)
			if codec.Channels > 0 {
				props = appendAMFName(props, "stereo")
				props = append(props, amfBoolean, b2u(codec.Channels > 1))
			}
		}
		switch {
		case t.track == 0:
			body = append(body, props...)
		case t.media == av.MediaVideo:
			videoMap = appendTrackEntry(videoMap, t.track, props)
		default:
			audioMap = appendTrackEntry(audioMap, t.track, props)
		}
	}
	for _, track := range []struct {
		name    string
		entries []byte
	}{{"videoTrackIdInfoMap", videoMap}, {"audioTrackIdInfoMap", audioMap}} {
		if track.entries != nil {
			body = appendAMFName(body, track.name)
			body = append(body, amfObject)
			body = appendAMFEnd(append(body, track.entries...))
		}
	}
	body = appendAMFName(body, "encoder")
	body = appendAMFEnd(appendAMFString(body, "goav"))
	tagStart := m.written + tagHeaderSize
	m.durationAt, m.sizeAt = tagStart+int64(durationAt), tagStart+int64(sizeAt)
	m.buf = appendTagHeader(m.buf[:0], tagScript, len(body), 0)
	m.buf = binary.BigEndian.AppendUint32(append(m.buf, body...), uint32(tagHeaderSize+len(body)))
	return m.flush()
}

// metadataCodecID is a track's onMetaData codec id: the legacy id for the
// codecs written with legacy headers, the FourCC otherwise.
func metadataCodecID(t *muxTrack) uint32 {
	switch {
	case t.track == 0 && t.codec == av.CodecH264:
		return legacyAVC
	case t.track == 0 && t.codec == av.CodecAAC:
		return legacyAAC
	default:
		return uint32(codecFourCC[t.codec])
	}
}

func appendNumberProperty(dst []byte, name string, v float64) []byte {
	return appendAMFNumber(appendAMFName(dst, name), v)
}

func appendTrackEntry(dst []byte, track uint8, props []byte) []byte {
	dst = appendAMFName(dst, strconv.Itoa(int(track)))
	dst = append(dst, amfObject)
	return appendAMFEnd(append(dst, props...))
}

func b2u(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// Write writes one packet as one tag, after its stream's sequence start.
func (m *Muxer) Write(ctx context.Context, packet *av.Packet, _ *format.WriteResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if packet == nil {
		return format.ErrNilPacket
	}
	if m.closed || m.w == nil {
		return ErrClosed
	}
	if m.err != nil {
		return m.err
	}
	t := m.track(packet.StreamID)
	if t == nil {
		return fmt.Errorf("%w: packet for stream %s the muxer was not opened with", ErrInvalidData, packet.StreamID)
	}
	data := packet.Payload.Bytes
	dts, pts, ok := m.times(packet)
	if !ok {
		return fmt.Errorf("%w: stream %s: packet has no timestamp", ErrInvalidData, t.id)
	}
	if !t.configured {
		if t.config == nil {
			if t.config = learnConfig(t, data, packet.Keyframe); t.config == nil {
				if t.media == av.MediaVideo {
					return nil // nothing decodes video before its first keyframe
				}
				return fmt.Errorf("%w: AAC stream %s declares no AudioSpecificConfig and its packets carry no ADTS header", ErrUnsupported, t.id)
			}
		}
		if err := m.writeConfig(t, dts); err != nil {
			return err
		}
	}
	if t.media == av.MediaVideo {
		cts := int32(min(max(pts-dts, -1<<23), 1<<23-1))
		m.buf = appendTagHeader(m.buf[:0], tagVideo, 0, uint32(dts))
		m.buf = appendVideoHeader(m.buf, t.track, t.codec, itemFrame, packet.Keyframe, cts)
	} else {
		m.buf = appendTagHeader(m.buf[:0], tagAudio, 0, uint32(dts))
		m.buf = appendAudioHeader(m.buf, t.track, t.codec, itemFrame)
	}
	var err error
	switch t.codec {
	case av.CodecH264, av.CodecHEVC:
		if h264.HasStartCode(data) {
			m.buf, err = h264.AppendAVC(m.buf, data)
		} else {
			m.buf = append(m.buf, data...)
		}
	case av.CodecAV1:
		m.buf, err = av1.AppendWithoutTemporalDelimiters(m.buf, data)
	case av.CodecAAC:
		if header, _, ok := aac.ADTS(data); ok {
			data = data[header:]
		}
		m.buf = append(m.buf, data...)
	default:
		m.buf = append(m.buf, data...)
	}
	if err != nil {
		return fmt.Errorf("%w: stream %s: %w", ErrInvalidData, t.id, err)
	}
	if end := pts + m.durationMS(packet); end > m.end {
		m.end = end
	}
	return m.finishTag()
}

// track returns the track of a stream.
func (m *Muxer) track(id av.StreamID) *muxTrack {
	for i := range m.tracks {
		if m.tracks[i].id == id {
			return &m.tracks[i]
		}
	}
	return nil
}

// times returns a packet's decode and presentation times in milliseconds
// after the first packet's decode time. A decode time before it (a stream
// that starts earlier than the first packet written) clamps to zero, its
// presentation time kept. Past 2^32 ms (49.7 days of live publishing) the
// tag timestamps wrap, as RTMP's do.
func (m *Muxer) times(packet *av.Packet) (dts, pts int64, ok bool) {
	ptsTS := packet.PTS
	dtsTS := packet.DTS
	if !dtsTS.Base.Valid() {
		dtsTS = ptsTS
	}
	if !ptsTS.Base.Valid() {
		ptsTS = dtsTS
	}
	if !dtsTS.Base.Valid() {
		return 0, 0, false
	}
	d, ok1 := dtsTS.Rescale(timeBase)
	p, ok2 := ptsTS.Rescale(timeBase)
	if !ok1 || !ok2 {
		return 0, 0, false
	}
	if !m.started {
		m.origin, m.started = d.Value, true
	}
	return max(d.Value-m.origin, 0), p.Value - m.origin, true
}

func (m *Muxer) durationMS(packet *av.Packet) int64 {
	if d, ok := packet.Duration.Rescale(timeBase); ok && d.Value > 0 {
		return d.Value
	}
	return 0
}

// learnConfig reads a codec configuration a stream did not declare from
// its first packet: a keyframe's parameter sets or sequence header, a VP
// keyframe header, or an ADTS header.
func learnConfig(t *muxTrack, data []byte, keyframe bool) []byte {
	var config []byte
	switch t.codec {
	case av.CodecAAC:
		_, config, _ = aac.ADTS(data)
		return config
	}
	if !keyframe {
		return nil
	}
	switch t.codec {
	case av.CodecH264:
		config, _ = h264.ConfigFromAnnexB(data)
	case av.CodecHEVC:
		config, _ = hevc.ConfigFromAnnexB(data)
	case av.CodecAV1:
		config, _ = av1.ConfigFromFrames([][]byte{data})
	case av.CodecVP8, av.CodecVP9:
		if k, ok := vpx.ParseKeyframe(t.codec, data); ok {
			config = vpx.Record(k, k.Width, k.Height)
		}
	}
	return config
}

// writeConfig writes a track's sequence start.
func (m *Muxer) writeConfig(t *muxTrack, dts int64) error {
	if t.media == av.MediaVideo {
		m.buf = appendTagHeader(m.buf[:0], tagVideo, 0, uint32(dts))
		m.buf = appendVideoHeader(m.buf, t.track, t.codec, itemConfig, true, 0)
	} else {
		m.buf = appendTagHeader(m.buf[:0], tagAudio, 0, uint32(dts))
		m.buf = appendAudioHeader(m.buf, t.track, t.codec, itemConfig)
	}
	m.buf = append(m.buf, t.config...)
	t.configured = true
	return m.finishTag()
}

// finishTag fills in the tag's body size, appends PreviousTagSize, and
// writes the tag.
func (m *Muxer) finishTag() error {
	size := len(m.buf) - tagHeaderSize
	if size > maxTagBody {
		return fmt.Errorf("%w: a %d-byte tag body exceeds FLV's 16 MiB", ErrUnsupported, size)
	}
	m.buf[1], m.buf[2], m.buf[3] = byte(size>>16), byte(size>>8), byte(size)
	m.buf = binary.BigEndian.AppendUint32(m.buf, uint32(len(m.buf)))
	return m.flush()
}

func (m *Muxer) flush() error {
	n, err := m.w.Write(m.buf)
	m.written += int64(n)
	if err != nil {
		m.err = err
	}
	return err
}

// Close ends each video track's sequence and, on a seekable output,
// patches onMetaData's duration and file size.
func (m *Muxer) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	if m.w == nil || m.err != nil {
		return m.err
	}
	end := uint32(max(m.end, 0))
	for i := range m.tracks {
		t := &m.tracks[i]
		if t.media != av.MediaVideo || !t.configured {
			continue
		}
		m.buf = appendTagHeader(m.buf[:0], tagVideo, 0, end)
		m.buf = appendVideoHeader(m.buf, t.track, t.codec, itemEnd, true, 0)
		if err := m.finishTag(); err != nil {
			return err
		}
	}
	if m.seeker == nil {
		return nil
	}
	for _, patch := range []struct {
		at    int64
		value float64
	}{{m.durationAt, float64(m.end) / 1000}, {m.sizeAt, float64(m.written)}} {
		var number [8]byte
		binary.BigEndian.PutUint64(number[:], math.Float64bits(patch.value))
		if _, err := m.seeker.Seek(patch.at, io.SeekStart); err != nil {
			return err
		}
		if _, err := m.seeker.Write(number[:]); err != nil {
			return err
		}
	}
	_, err := m.seeker.Seek(0, io.SeekEnd)
	return err
}

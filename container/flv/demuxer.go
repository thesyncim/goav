package flv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/aac"
	"github.com/thesyncim/goav/internal/amf0"
	"github.com/thesyncim/goav/internal/h264"
	"github.com/thesyncim/goav/internal/hevc"
)

// timeBase is FLV's: every timestamp counts milliseconds.
var timeBase = av.TimeBase{Num: 1, Den: 1000}

// The read-ahead that learns the streams at Open stops once every expected
// track is known, or after this much media, or this many tags.
const (
	probeSpan = 2000 // ms
	probeTags = 512
)

// Header flags.
const (
	flagVideo = 0x01
	flagAudio = 0x04
)

// Demuxer reads FLV: a file, a pipe, or a live stream such as RTMP's. It
// needs only an io.Reader, reads forward, and implements format.Demuxer.
type Demuxer struct {
	r      *bufio.Reader
	tracks []*demuxTrack
	// streams are the streams Open announced; a later track is announced
	// by an event.
	streams []av.Stream
	// queue holds what the tags read so far produced and ReadInto has not
	// returned; packet payloads live in arena.
	queue []pending
	head  int
	arena []byte
	body  []byte
	// last and epochs unwrap the 32-bit timestamps; start is the first
	// media tag's, which bounds the read-ahead with tags.
	last, epochs int64
	start        int64
	seen         bool
	started      bool
	framed       bool
	tags         int
	// expect is the tracks onMetaData or the header flags promise.
	expect []trackKey
	hints  amf0.Object
	opened bool
	// ready is set once Open has announced its streams: a track that
	// appears later is announced by an event.
	ready  bool
	closed bool
}

// demuxTrack is one track: a media and a track id.
type demuxTrack struct {
	media      av.MediaType
	id         uint8
	stream     av.Stream
	lengthSize int
}

// trackKey names an expected track; any matches every track of the media.
type trackKey struct {
	media av.MediaType
	id    uint8
	any   bool
}

// pending is one result for ReadInto: an event or a packet whose payload
// is arena[start:end].
type pending struct {
	event      *av.Event
	track      *demuxTrack
	start, end int
	dts        int64
	cts        int32
	keyframe   bool
}

// DemuxerFactory opens FLV demuxers behind the goav format extension point.
type DemuxerFactory struct{}

// NewDemuxer returns an FLV demuxer.
func (DemuxerFactory) NewDemuxer(ctx context.Context, result format.ProbeResult) (format.Demuxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result.Format != "" && result.Format != av.FormatFLV {
		return nil, format.ErrNotFound
	}
	return &Demuxer{}, nil
}

func (d *Demuxer) Format() av.FormatID { return av.FormatFLV }

// Open reads the header and enough tags to learn the streams.
func (d *Demuxer) Open(ctx context.Context, input format.Input, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.opened {
		return errors.New("flv: demuxer opened twice")
	}
	src := input.Reader
	if src == nil && input.ReaderAt != nil {
		src = io.NewSectionReader(input.ReaderAt, 0, max(input.Size, 0))
	}
	if src == nil {
		return fmt.Errorf("%w: no input reader", ErrInvalidData)
	}
	d.r = bufio.NewReaderSize(src, 64<<10)
	var header [9]byte
	if _, err := io.ReadFull(d.r, header[:]); err != nil {
		return fmt.Errorf("%w: header: %w", ErrInvalidData, err)
	}
	if string(header[:3]) != "FLV" {
		return fmt.Errorf("%w: no FLV signature", ErrInvalidData)
	}
	offset := int64(binary.BigEndian.Uint32(header[5:]))
	if offset < 9 || offset > 1<<16 {
		return fmt.Errorf("%w: header size %d", ErrInvalidData, offset)
	}
	// The rest of the header, then PreviousTagSize0.
	if _, err := d.r.Discard(int(offset-9) + 4); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if header[4]&flagVideo != 0 {
		d.expect = append(d.expect, trackKey{media: av.MediaVideo, any: true})
	}
	if header[4]&flagAudio != 0 {
		d.expect = append(d.expect, trackKey{media: av.MediaAudio, any: true})
	}
	d.opened = true
	for !d.probed() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.readTag(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	for _, t := range d.tracks {
		d.streams = append(d.streams, t.stream)
	}
	d.ready = true
	return nil
}

// probed reports that the read-ahead knows the streams, every track
// expected and a frame after the sequence starts that lead them, or has
// spent its budget.
func (d *Demuxer) probed() bool {
	if d.tags >= probeTags || d.started && d.now()-d.start >= probeSpan {
		return true
	}
	if !d.framed {
		return false // the sequence starts lead the first frame
	}
	for _, key := range d.expect {
		if !slices.ContainsFunc(d.tracks, func(t *demuxTrack) bool { return t.media == key.media && (key.any || t.id == key.id) }) {
			return false
		}
	}
	return true
}

func (d *Demuxer) Streams() []av.Stream {
	out := make([]av.Stream, len(d.streams))
	for i := range d.streams {
		out[i] = cloneStream(d.streams[i])
	}
	return out
}

// ReadInto returns the next packet, after any events due before it, until
// io.EOF.
func (d *Demuxer) ReadInto(ctx context.Context, out *format.ReadResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil || out.Packet == nil {
		return format.ErrNilPacket
	}
	if d.closed || !d.opened {
		return ErrClosed
	}
	for d.head == len(d.queue) {
		d.queue, d.head, d.arena = d.queue[:0], 0, d.arena[:0]
		if err := d.readTag(); err != nil {
			return err
		}
	}
	for d.head < len(d.queue) {
		p := &d.queue[d.head]
		if p.event != nil {
			if err := out.AddEvent(*p.event); err != nil && len(out.Events) != 0 {
				return nil // full: the rest waits for the next read
			}
			d.head++
			continue
		}
		d.head++
		t := p.track
		out.Packet.Reset()
		out.Packet.StreamID = t.stream.ID
		out.Packet.Type = t.media
		out.Packet.CodecEpoch = t.stream.Epoch
		out.Packet.Payload.Bytes = d.arena[p.start:p.end]
		out.Packet.DTS = av.Timestamp{Value: p.dts, Base: timeBase}
		out.Packet.PTS = av.Timestamp{Value: p.dts + int64(p.cts), Base: timeBase}
		out.Packet.Keyframe = p.keyframe
		out.PacketReady = true
		return nil
	}
	return nil
}

// readTag reads one tag and queues what it carries.
func (d *Demuxer) readTag() error {
	var header [tagHeaderSize]byte
	if _, err := io.ReadFull(d.r, header[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return io.EOF // a stream cut inside a tag ends there
		}
		return err
	}
	size := int(header[1])<<16 | int(header[2])<<8 | int(header[3])
	// The body, and the PreviousTagSize after it, read in steps so memory
	// follows the bytes that arrive, not the size a header claims.
	d.body = d.body[:0]
	for remaining := size + 4; remaining > 0; {
		step := min(remaining, 64<<10)
		d.body = slices.Grow(d.body, step)
		n, err := io.ReadFull(d.r, d.body[len(d.body):len(d.body)+step])
		d.body = d.body[:len(d.body)+n]
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return io.EOF // a stream cut inside a tag ends there
			}
			return err
		}
		remaining -= step
	}
	body := d.body[:size]
	d.tags++
	if header[0]&0x20 != 0 {
		return nil // encrypted (filtered) tags carry nothing goav reads
	}
	timestamp := d.unwrap(uint32(header[7])<<24 | uint32(header[4])<<16 | uint32(header[5])<<8 | uint32(header[6]))
	var err error
	switch header[0] & 0x1f {
	case tagVideo:
		err = parseVideo(body, func(it item) error { return d.add(it, timestamp) })
	case tagAudio:
		err = parseAudio(body, func(it item) error { return d.add(it, timestamp) })
	case tagScript:
		d.script(body)
	}
	if errors.Is(err, errSkip) {
		return nil
	}
	return err
}

// now is the latest tag's timestamp, unwrapped.
func (d *Demuxer) now() int64 { return d.epochs<<32 | d.last }

// unwrap extends a 32-bit millisecond timestamp to 64 bits: a jump back of
// more than half the range is a wrap.
func (d *Demuxer) unwrap(ts uint32) int64 {
	value := int64(ts)
	if d.seen {
		switch {
		case value < d.last && d.last-value > 1<<31:
			d.epochs++
		case value > d.last && value-d.last > 1<<31 && d.epochs > 0:
			d.epochs--
		}
	}
	d.last, d.seen = value, true
	return d.now()
}

// script reads onMetaData: the media it promises, and stream hints.
func (d *Demuxer) script(body []byte) {
	values, _ := amf0.Decode(body) // what decodes before any damage
	if len(values) > 0 && values[0] == "@setDataFrame" {
		values = values[1:]
	}
	if len(values) < 2 || values[0] != "onMetaData" || d.hints != nil {
		return
	}
	props, ok := values[1].(amf0.Object)
	if !ok {
		return
	}
	d.hints = props
	var expect []trackKey
	for _, media := range []av.MediaType{av.MediaVideo, av.MediaAudio} {
		if _, ok := props.Get(string(media) + "codecid"); ok {
			expect = append(expect, trackKey{media: media})
		}
		entries, _ := props.Object(string(media) + "TrackIdInfoMap")
		for _, entry := range entries {
			if id, err := strconv.ParseUint(entry.Name, 10, 8); err == nil {
				expect = append(expect, trackKey{media: media, id: uint8(id)})
			}
		}
	}
	if expect != nil {
		d.expect = expect
	}
}

// add queues one track's part of a media tag.
func (d *Demuxer) add(it item, dts int64) error {
	if !d.started {
		d.start, d.started = dts, true
	}
	t, added := d.track(it)
	if t.stream.Codec.ID != it.codec {
		return nil // a track changing codec mid-stream is not one goav follows
	}
	if it.kind == itemConfig {
		d.configure(t, it.data, d.ready && !added)
	}
	if added && d.ready {
		// A track after Open is announced before its first frame, with the
		// configuration its first tag carried.
		stream := cloneStream(t.stream)
		d.queue = append(d.queue, pending{event: &av.Event{Type: av.EventStreamAdded, StreamID: stream.ID, Stream: &stream}})
	}
	if it.kind != itemFrame {
		return nil
	}
	d.framed = true
	start := len(d.arena)
	switch {
	case (it.codec == av.CodecH264 || it.codec == av.CodecHEVC) && !h264.HasStartCode(it.data):
		var err error
		if d.arena, err = h264.AppendAnnexB(d.arena, it.data, t.lengthSize); err != nil {
			d.arena = d.arena[:start]
			return nil // a damaged sample is dropped, not the stream
		}
	default:
		d.arena = append(d.arena, it.data...)
	}
	d.queue = append(d.queue, pending{track: t, start: start, end: len(d.arena), dts: dts, cts: it.cts, keyframe: it.keyframe})
	return nil
}

// track returns the track an item belongs to, opening it the first time.
func (d *Demuxer) track(it item) (t *demuxTrack, added bool) {
	for _, t := range d.tracks {
		if t.media == it.media && t.id == it.track {
			return t, false
		}
	}
	t = &demuxTrack{media: it.media, id: it.track, lengthSize: 4}
	id := string(it.media)
	if it.track != 0 {
		id += "-" + strconv.Itoa(int(it.track))
	}
	t.stream = av.Stream{
		ID:       av.StreamID(id),
		Index:    len(d.tracks),
		Type:     it.media,
		TimeBase: timeBase,
		Codec:    av.CodecParameters{ID: it.codec, Type: it.media},
	}
	d.applyHints(t)
	d.tracks = append(d.tracks, t)
	return t, true
}

// applyHints fills a new track's geometry from onMetaData.
func (d *Demuxer) applyHints(t *demuxTrack) {
	props := d.hints
	if t.id != 0 {
		entries, _ := d.hints.Object(string(t.media) + "TrackIdInfoMap")
		props, _ = entries.Object(strconv.Itoa(int(t.id)))
	}
	codec := &t.stream.Codec
	if t.media == av.MediaVideo {
		if w, ok := props.Number("width"); ok && w > 0 && w < 1<<16 {
			codec.Width = int(w)
		}
		if h, ok := props.Number("height"); ok && h > 0 && h < 1<<16 {
			codec.Height = int(h)
		}
		return
	}
	if rate, ok := props.Number("audiosamplerate"); ok && rate > 0 && rate < 1<<20 {
		codec.SampleRate = int(rate)
	}
	if channels, ok := props.Number("channels"); ok && channels > 0 && channels < 256 {
		codec.Channels = int(channels)
	} else if stereo, ok := props.Get("stereo"); ok {
		codec.Channels = 1
		if stereo == true {
			codec.Channels = 2
		}
	}
}

// configure applies a sequence start: the stream's ExtraData, a change to
// which announce publishes with av.EventCodecChanged.
func (d *Demuxer) configure(t *demuxTrack, config []byte, announce bool) {
	codec := &t.stream.Codec
	if bytes.Equal(codec.ExtraData.Bytes, config) {
		return // a repeated sequence start, as after a publisher reconnect
	}
	codec.ExtraData.Bytes = bytes.Clone(config)
	switch codec.ID {
	case av.CodecH264:
		if c, err := h264.ParseConfig(config); err == nil {
			t.lengthSize = c.LengthSize
		}
	case av.CodecHEVC:
		if c, err := hevc.ParseConfig(config); err == nil {
			t.lengthSize = c.LengthSize
		}
	case av.CodecAAC:
		if rate, channels, ok := aac.ParseConfig(config); ok {
			codec.SampleRate = rate
			if channels > 0 {
				codec.Channels = channels
			}
		}
	case av.CodecOpus:
		if len(config) >= 19 && string(config[:8]) == "OpusHead" {
			codec.SampleRate, codec.Channels = 48000, int(config[9])
		}
	}
	if !announce {
		return
	}
	t.stream.Epoch++
	params := cloneStream(t.stream).Codec
	stream := cloneStream(t.stream)
	d.queue = append(d.queue, pending{event: &av.Event{Type: av.EventCodecChanged, StreamID: t.stream.ID, Epoch: t.stream.Epoch, Codec: &params, Stream: &stream}})
}

func (d *Demuxer) Close() error {
	d.closed = true
	return nil
}

func cloneStream(stream av.Stream) av.Stream {
	stream.Codec.ExtraData.Bytes = bytes.Clone(stream.Codec.ExtraData.Bytes)
	return stream
}

package kit

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

// VideoConfig is what a video session opens with. Knobs are its settings,
// already checked against the backend's declaration. A decoder of an AVCC
// bitstream gets the stream's ParameterSets (VPS, SPS, PPS NAL units without
// start codes); the kit opens it once they are known and again when they
// change.
type VideoConfig struct {
	Width, Height int
	TimeBase      av.TimeBase
	// FrameDuration is the nominal frame duration in TimeBase units, 0 when
	// unknown.
	FrameDuration int64
	BitDepth      int
	Realtime      bool
	Knobs         codec.KnobValues
	ParameterSets [][]byte
	// ExtraData is a raw-bitstream decoder's codec configuration, as the
	// stream carries it (an av1C, say).
	ExtraData []byte
}

// VideoEncoder is a backend's encoder session. Encode takes one picture and
// reports finished packets through emit, now or on a later call, in decode
// order, each with the PTS of the picture it carries; Flush drains the rest.
// The kit calls a session from one goroutine at a time.
type VideoEncoder interface {
	Encode(picture *Picture, emit func(*Packet) error) error
	Flush(emit func(*Packet) error) error
	Close() error
}

// VideoDecoder is a backend's decoder session. Decode takes one access unit
// and reports decoded pictures through emit in presentation order, now or on
// a later call, each with the PTS of the access unit it came from; Flush
// drains the rest.
type VideoDecoder interface {
	Decode(unit *Packet, emit func(*Picture) error) error
	Flush(emit func(*Picture) error) error
	Close() error
}

// timing is what a packet or frame carries through a session that does not:
// its duration and metadata, keyed by PTS.
type timing struct {
	pts      int64
	duration int64
	metadata av.Metadata
}

type timings []timing

const maxTimings = 64

func (t *timings) push(entry timing) {
	if len(*t) == maxTimings {
		*t = slices.Delete(*t, 0, 1)
	}
	*t = append(*t, entry)
}

func (t *timings) take(pts int64) (timing, bool) {
	for i := range *t {
		if (*t)[i].pts == pts {
			entry := (*t)[i]
			*t = slices.Delete(*t, i, i+1)
			return entry, true
		}
	}
	return timing{}, false
}

func timeIn(ts av.Timestamp, base av.TimeBase) int64 {
	if !ts.Base.Valid() || ts.Base == base {
		return ts.Value
	}
	if v, ok := ts.Rescale(base); ok {
		return v.Value
	}
	return ts.Value
}

func durationIn(d av.Duration, base av.TimeBase) int64 {
	if !d.Base.Valid() || d.Base == base {
		return d.Value
	}
	if v, ok := d.Rescale(base); ok {
		return v.Value
	}
	return 0
}

type videoEncoder struct {
	c        *Codec
	desc     codec.Descriptor
	settings codec.CodecSettings
	stream   av.Stream
	config   VideoConfig
	session  VideoEncoder
	out      *codec.EncodeResult
	pending  timings
	sets     [][]byte
	scratch  []byte
	forceKey bool
	closed   bool
	emitFn   func(*Packet) error
	// carry holds packets a restart outside EncodeInto drained, until the
	// next call can hand them on.
	carry codec.EncodeResult
}

func (e *videoEncoder) Descriptor() codec.Descriptor { return e.desc }

func (e *videoEncoder) Open(ctx context.Context, config codec.EncodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := refuseControl(e.c, config.Settings); err != nil {
		return err
	}
	stream := config.Stream
	params := config.Parameters
	width := firstPositive(params.Width, stream.Codec.Width)
	height := firstPositive(params.Height, stream.Codec.Height)
	if width <= 0 || height <= 0 {
		return fmt.Errorf("%w: %s %s encode needs the frame geometry", codec.ErrUnsupportedFormat, e.c.ID, e.c.Backend)
	}
	base := stream.TimeBase
	if !base.Valid() {
		base = av.TimeBase{Num: 1, Den: 90000}
	}
	knobs, err := e.desc.ResolveKnobs(config.Settings, config.LowLatency || config.Realtime)
	if err != nil {
		return err
	}
	e.settings = config.Settings
	e.config = VideoConfig{
		Width:         width,
		Height:        height,
		TimeBase:      base,
		FrameDuration: durationIn(config.Settings.Framerate, base),
		BitDepth:      config.Settings.BitDepth,
		Realtime:      config.LowLatency || config.Realtime,
		Knobs:         knobs,
	}
	if err := e.start(); err != nil {
		return err
	}
	params.ID, params.Type = e.c.ID, av.MediaVideo
	params.Width, params.Height = width, height
	params.PixelFormat = ""
	if params.ClockRate == 0 {
		params.ClockRate = 90000
	}
	if e.config.Knobs.String("profile") != "" {
		params.Profile = e.config.Knobs.String("profile")
	}
	stream.Type, stream.TimeBase, stream.Codec = av.MediaVideo, base, params
	e.stream = stream
	e.forceKey = true
	e.emitFn = e.emit
	return nil
}

func (e *videoEncoder) start() error {
	session, err := e.c.NewVideoEncoder(e.config)
	if err != nil {
		return err
	}
	e.session = session
	if configured, ok := session.(Configured); ok {
		e.takeConfig(configured.Config())
	}
	return nil
}

func (e *videoEncoder) takeConfig(config []byte) {
	if nalFramed(e.c.ID, e.c.Bitstream) {
		e.learnConfig(config)
	} else {
		e.learnRaw(config)
	}
}

// learnConfig keeps the parameter sets of a codec configuration for in-band
// keyframes and the stream's extradata.
func (e *videoEncoder) learnConfig(config []byte) {
	sets, err := configParameterSets(e.c.ID, config)
	if err != nil || len(sets) == 0 {
		return
	}
	e.sets = cloneUnits(sets)
	extra := config
	if hasStartCode(config) && e.c.ID == av.CodecH264 {
		extra = avcC(sets)
	}
	e.stream.Codec.ExtraData = av.Buffer{Bytes: slices.Clone(extra)}
}

// learnRaw keeps a non-NAL codec's configuration (an av1C, say) as the
// stream's extradata.
func (e *videoEncoder) learnRaw(config []byte) {
	if len(config) != 0 {
		e.stream.Codec.ExtraData = av.Buffer{Bytes: slices.Clone(config)}
	}
}

func cloneUnits(units [][]byte) [][]byte {
	out := make([][]byte, len(units))
	for i := range units {
		out[i] = slices.Clone(units[i])
	}
	return out
}

func (e *videoEncoder) EncodedParameters() av.CodecParameters { return e.stream.Codec }

func (e *videoEncoder) EncodeInto(ctx context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if err := e.ready(ctx, out); err != nil || frame == nil {
		return err
	}
	pic, err := framePicture(frame)
	if err != nil {
		return err
	}
	if pic.Width != e.config.Width || pic.Height != e.config.Height {
		// A new geometry starts a new session: drain the old one first.
		if err := e.restart(out, func() { e.config.Width, e.config.Height = pic.Width, pic.Height }); err != nil {
			return err
		}
		e.stream.Codec.Width, e.stream.Codec.Height = pic.Width, pic.Height
	}
	base := e.config.TimeBase
	pic.PTS = timeIn(frame.PTS, base)
	pic.Duration = durationIn(frame.Duration, base)
	if pic.Duration == 0 {
		pic.Duration = e.config.FrameDuration
	}
	pic.Keyframe, e.forceKey = e.forceKey, false
	e.pending.push(timing{pts: pic.PTS, duration: pic.Duration, metadata: frame.Metadata})
	switch {
	case e.c.Pixels == NV12 && (pic.Format == av.PixelFormatI420 || pic.Format == av.PixelFormatYUV420P):
		pic, e.scratch = toNV12(pic, e.scratch)
	case e.c.Pixels != "" && e.c.Pixels != NV12 && !samePlanar(e.c.Pixels, pic.Format):
		return fmt.Errorf("%w: %s %s encodes %s, not %s", codec.ErrUnsupportedFormat, e.c.ID, e.c.Backend, e.c.Pixels, pic.Format)
	}
	e.out = out
	return e.session.Encode(&pic, e.emitFn)
}

func (e *videoEncoder) ready(ctx context.Context, out *codec.EncodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if e.closed || e.session == nil {
		return codec.ErrClosed
	}
	out.Packets = takeCarried(out.Packets, &e.carry)
	return nil
}

// restart drains and closes the session, applies change, and opens a new
// one that starts on a keyframe.
func (e *videoEncoder) restart(out *codec.EncodeResult, change func()) error {
	e.out = out
	if err := e.session.Flush(e.emitFn); err != nil {
		return err
	}
	err := e.session.Close()
	e.session = nil
	if err != nil {
		return err
	}
	change()
	e.forceKey = true
	return e.start()
}

func (e *videoEncoder) emit(p *Packet) error {
	if p.Config != nil {
		e.takeConfig(p.Config)
	}
	payload, keyframe, err := e.payload(p)
	if err != nil {
		return err
	}
	entry, _ := e.pending.take(p.PTS)
	duration := p.Duration
	if duration == 0 {
		duration = entry.duration
	}
	base := e.config.TimeBase
	e.out.Packets = append(e.out.Packets, av.Packet{
		StreamID:   e.stream.ID,
		Type:       av.MediaVideo,
		CodecEpoch: e.stream.Epoch,
		Payload:    payload,
		PTS:        av.Timestamp{Value: p.PTS, Base: base},
		DTS:        av.Timestamp{Value: p.DTS, Base: base},
		Duration:   av.Duration{Value: duration, Base: base},
		Keyframe:   keyframe,
		Metadata:   entry.metadata,
	})
	return nil
}

// payload copies a backend's packet into one pooled buffer: as it is for a
// raw bitstream, or as Annex B with the parameter sets ahead of every
// keyframe for H.264 and HEVC.
func (e *videoEncoder) payload(p *Packet) (av.Buffer, bool, error) {
	if !nalFramed(e.c.ID, e.c.Bitstream) {
		payload := av.NewBuffer(len(p.Data))
		copy(payload.Bytes, p.Data)
		return payload, p.Keyframe, nil
	}
	var units [][]byte
	var err error
	if e.c.Bitstream == AVCC {
		units, err = lengthPrefixed(p.Data)
	} else {
		units = nalUnits(p.Data)
	}
	if err != nil {
		return av.Buffer{}, false, fmt.Errorf("%s %s: encoder output: %w", e.c.ID, e.c.Backend, err)
	}
	keyframe := p.Keyframe || slices.ContainsFunc(units, func(u []byte) bool { return randomAccess(e.c.ID, u) })
	inband := len(parameterSetsOf(e.c.ID, units)) != 0
	size := 0
	if keyframe && !inband {
		for _, set := range e.sets {
			size += 4 + len(set)
		}
	}
	for _, unit := range units {
		size += 4 + len(unit)
	}
	payload := av.NewBuffer(size)
	data := payload.Bytes[:0]
	if keyframe && !inband {
		data = appendAnnexB(data, e.sets...)
	}
	payload.Bytes = appendAnnexB(data, units...)
	return payload, keyframe, nil
}

func (e *videoEncoder) FlushInto(ctx context.Context, out *codec.EncodeResult) error {
	if err := e.ready(ctx, out); err != nil {
		return err
	}
	e.out = out
	return e.session.Flush(e.emitFn)
}

func (e *videoEncoder) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil || event == nil {
		return err
	}
	if e.closed {
		return codec.ErrClosed
	}
	switch event.Type {
	case av.EventKeyframeRequired, av.EventDiscontinuity, av.EventCodecChanged:
		e.forceKey = true
	case av.EventBitrateChanged:
		bps, ok := codec.EventBitrate(event)
		if !ok {
			return fmt.Errorf("%w: %s %s: bitrate event needs a positive %s in bits per second", codec.ErrInvalidSetting, e.c.ID, e.c.Backend, av.MetadataBitrate)
		}
		return e.set("bitrate", strconv.Itoa(bps))
	case EventSetting:
		return applySettings(event, e.set)
	}
	return nil
}

// applySettings applies an EventSetting's knobs in name order.
func applySettings(event *av.Event, set func(name, value string) error) error {
	names := make([]string, 0, len(event.Metadata))
	for name := range event.Metadata {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if err := set(name, event.Metadata[name]); err != nil {
			return err
		}
	}
	return nil
}

// samePlanar reports two names of one planar layout (i420 and yuv420p).
func samePlanar(a, b string) bool {
	if a == "" {
		a = av.PixelFormatI420
	}
	if b == "" {
		b = av.PixelFormatI420
	}
	la, okA := layouts[a]
	lb, okB := layouts[b]
	return a == b || (okA && okB && la == lb)
}

// set applies one knob while running: live when the backend declares it so
// and can, otherwise by restarting the session on a keyframe.
func (e *videoEncoder) set(name, value string) error {
	knob, ok := e.desc.Knob(name)
	if !ok {
		return fmt.Errorf("%w: %s %s takes no %s control", codec.ErrInvalidSetting, e.c.ID, e.c.Backend, name)
	}
	parsed, err := knob.Parse(value)
	if err != nil {
		return fmt.Errorf("%s %s: %w", e.c.ID, e.c.Backend, err)
	}
	knobs := e.config.Knobs.With(knob.Name, parsed)
	if updater, ok := e.session.(Updater); ok && knob.Live {
		if err := updater.Update(knob.Name, knobs); err != nil {
			return err
		}
		e.config.Knobs = knobs
		return nil
	}
	return e.restart(&e.carry, func() { e.config.Knobs = knobs })
}

func (e *videoEncoder) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	releasePackets(e.carry.Packets)
	e.carry.Packets = nil
	if e.session == nil {
		return nil
	}
	err := e.session.Close()
	e.session = nil
	return err
}

func firstPositive(values ...int) int {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}

// takeCarried moves carried packets, and their payload references, onto out.
func takeCarried(out []av.Packet, carry *codec.EncodeResult) []av.Packet {
	if len(carry.Packets) == 0 {
		return out
	}
	out = append(out, carry.Packets...)
	clear(carry.Packets)
	carry.Packets = carry.Packets[:0]
	return out
}

func releasePackets(packets []av.Packet) {
	for i := range packets {
		packets[i].Release()
	}
}

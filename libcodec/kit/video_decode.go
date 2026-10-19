package kit

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

type videoDecoder struct {
	c        *Codec
	desc     codec.Descriptor
	stream   av.Stream
	config   VideoConfig
	session  VideoDecoder
	out      *codec.DecodeResult
	pending  timings
	sets     [][]byte
	fedSets  bool
	unit     []byte
	video    *av.VideoFrame
	request  bool
	dropping bool
	healing  bool // a damaged unit asked for the keyframe that heals it
	closed   bool
	emitFn   func(*Picture) error
	// held are decoded pictures a Reorder session returned early, sorted
	// by PTS; decoded is the decode time of the last unit sent.
	held    []av.Frame
	decoded int64
	// carry holds pictures a codec change drained until the next call.
	carry codec.DecodeResult
}

func (d *videoDecoder) Descriptor() codec.Descriptor { return d.desc }

func (d *videoDecoder) Open(ctx context.Context, config codec.DecodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := refuseControl(d.c, config.Settings); err != nil {
		return err
	}
	knobs, err := d.desc.ResolveKnobs(config.Settings, config.LowLatency || config.Realtime)
	if err != nil {
		return err
	}
	stream := config.Stream
	if !stream.TimeBase.Valid() {
		stream.TimeBase = av.TimeBase{Num: 1, Den: 90000}
	}
	d.stream = stream
	d.request = config.Resilience.RequestKeyframes
	d.config = VideoConfig{
		Width:    stream.Codec.Width,
		Height:   stream.Codec.Height,
		TimeBase: stream.TimeBase,
		Realtime: config.LowLatency || config.Realtime,
		Knobs:    knobs,
	}
	d.emitFn = d.emitPicture
	if !nalFramed(d.c.ID, d.c.Bitstream) {
		d.config.ExtraData = slices.Clone(stream.Codec.ExtraData.Bytes)
		return d.start()
	}
	sets, err := configParameterSets(d.c.ID, stream.Codec.ExtraData.Bytes)
	if err != nil {
		return fmt.Errorf("%w: %s %s: extradata: %v", codec.ErrUnsupportedFormat, d.c.ID, d.c.Backend, err)
	}
	d.sets = cloneUnits(sets)
	if d.c.Bitstream == AnnexB || len(d.sets) != 0 {
		return d.start()
	}
	// An AVCC session opens on the first parameter sets the stream carries.
	return nil
}

func (d *videoDecoder) start() error {
	d.config.ParameterSets = d.sets
	session, err := d.c.NewVideoDecoder(d.config)
	if err != nil {
		return err
	}
	d.session, d.fedSets = session, false
	return nil
}

func (d *videoDecoder) stop(out *codec.DecodeResult) error {
	if d.session == nil {
		return nil
	}
	d.out = out
	err := d.session.Flush(d.emitFn)
	d.release(true)
	if closeErr := d.session.Close(); err == nil {
		err = closeErr
	}
	d.session = nil
	return err
}

func (d *videoDecoder) DecodeInto(ctx context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.closed {
		return codec.ErrClosed
	}
	out.Frames = takeCarriedFrames(out.Frames, &d.carry)
	if packet == nil {
		return d.requestKeyframe(out, "packet loss")
	}
	if packet.LossBefore || packet.Discontinuous || (packet.Corrupt && !packet.Keyframe) {
		d.dropping = true
		if err := d.requestKeyframe(out, "discontinuity"); err != nil {
			return err
		}
	}
	data, keyframe, err := d.accessUnit(packet, out)
	if err != nil || data == nil {
		return err
	}
	base := d.stream.TimeBase
	pts := timeIn(packet.PTS, base)
	d.pending.push(timing{pts: pts, duration: durationIn(packet.Duration, base), metadata: packet.Metadata})
	if d.c.Reorder {
		// No later unit presents before this one's decode time: DTS only
		// grows and a unit never presents before it decodes.
		d.decoded = pts
		if packet.DTS.Base.Valid() {
			d.decoded = timeIn(packet.DTS, base)
		}
	}
	d.out = out
	err = d.session.Decode(&Packet{Data: data, PTS: pts, DTS: timeIn(packet.DTS, base), Keyframe: keyframe}, d.emitFn)
	switch {
	case errors.Is(err, ErrDamaged):
		// The library dropped the unit and decodes on. The units that
		// reference it may be dropped too, until a keyframe heals the
		// stream: ask for one once.
		if d.healing {
			return nil
		}
		d.healing = true
		return d.requestKeyframe(out, "damaged unit")
	case err == nil && keyframe:
		d.healing = false
	}
	return err
}

func sameUnits(a, b [][]byte) bool {
	return slices.EqualFunc(a, b, bytes.Equal)
}

func (d *videoDecoder) requestKeyframe(out *codec.DecodeResult, reason string) error {
	if !d.request {
		return nil
	}
	out.Requests = append(out.Requests, codec.ControlRequest{Type: codec.ControlRequestKeyframe, StreamID: d.stream.ID, Reason: string(d.c.ID) + " " + reason})
	return nil
}

func (d *videoDecoder) emitPicture(pic *Picture) error {
	entry, _ := d.pending.take(pic.PTS)
	duration := pic.Duration
	if duration == 0 {
		duration = entry.duration
	}
	var frame av.Frame
	format, err := fillFrame(&frame, pic)
	if err != nil {
		return err
	}
	if d.video == nil || d.video.Width != pic.Width || d.video.Height != pic.Height || d.video.PixelFormat != format {
		// A new header per geometry: frames already published keep theirs.
		d.video = &av.VideoFrame{Width: pic.Width, Height: pic.Height, PixelFormat: format}
	}
	base := d.stream.TimeBase
	frame.StreamID = d.stream.ID
	frame.CodecEpoch = d.stream.Epoch
	frame.Type = av.MediaVideo
	frame.Video = d.video
	frame.PTS = av.Timestamp{Value: pic.PTS, Base: base}
	frame.Duration = av.Duration{Value: duration, Base: base}
	frame.Metadata = entry.metadata
	if !d.c.Reorder {
		d.out.Frames = append(d.out.Frames, frame)
		return nil
	}
	i, _ := slices.BinarySearchFunc(d.held, pic.PTS, func(f av.Frame, pts int64) int { return cmp.Compare(f.PTS.Value, pts) })
	d.held = slices.Insert(d.held, i, frame)
	d.release(false)
	return nil
}

// maxHeld bounds the reorder buffer at the deepest H.264/HEVC picture
// buffer, so a unit that decodes to nothing cannot stall output.
const maxHeld = 16

// release hands held pictures on in presentation order: each once no unit
// still to come can present before it, all of them when forced (at flush).
func (d *videoDecoder) release(force bool) {
	for len(d.held) > 0 {
		if !force && len(d.held) <= maxHeld && d.held[0].PTS.Value > d.decoded {
			return
		}
		d.out.Frames = append(d.out.Frames, d.held[0])
		d.held = slices.Delete(d.held, 0, 1)
	}
}

func (d *videoDecoder) FlushInto(ctx context.Context, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.closed {
		return codec.ErrClosed
	}
	out.Frames = takeCarriedFrames(out.Frames, &d.carry)
	if d.session == nil {
		return nil
	}
	d.out = out
	err := d.session.Flush(d.emitFn)
	d.release(true)
	return err
}

func (d *videoDecoder) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil || event == nil {
		return err
	}
	if d.closed {
		return codec.ErrClosed
	}
	switch event.Type {
	case av.EventPacketLoss:
		d.dropping = true
	case av.EventCodecChanged:
		if err := d.stop(&d.carry); err != nil {
			return err
		}
		if event.Codec != nil {
			d.stream.Codec = *event.Codec
			if sets, err := configParameterSets(d.c.ID, event.Codec.ExtraData.Bytes); err == nil && len(sets) != 0 {
				d.sets = cloneUnits(sets)
			}
		}
		d.dropping = true
		if d.c.Bitstream == AnnexB {
			return d.start()
		}
	}
	return nil
}

func releaseFrames(frames []av.Frame) {
	for i := range frames {
		frames[i].Release()
	}
}

func (d *videoDecoder) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	releaseFrames(d.held)
	d.held = nil
	releaseFrames(d.carry.Frames)
	d.carry.Frames = nil
	if d.session == nil {
		return nil
	}
	err := d.session.Close()
	d.session = nil
	return err
}

// takeCarriedFrames moves carried frames, and their plane references, onto
// out.
func takeCarriedFrames(out []av.Frame, carry *codec.DecodeResult) []av.Frame {
	if len(carry.Frames) == 0 {
		return out
	}
	out = append(out, carry.Frames...)
	clear(carry.Frames)
	carry.Frames = carry.Frames[:0]
	return out
}

// accessUnit prepares a packet for the session: as it is for a raw
// bitstream; for H.264 and HEVC, in the session's framing with the parameter
// sets where it wants them, reopening an AVCC session when they change. It
// returns nil data for a packet dropped while waiting for a keyframe.
func (d *videoDecoder) accessUnit(packet *av.Packet, out *codec.DecodeResult) ([]byte, bool, error) {
	if !nalFramed(d.c.ID, d.c.Bitstream) {
		if d.dropping && !packet.Keyframe {
			return nil, false, nil
		}
		d.dropping = false
		return packet.Payload.Bytes, packet.Keyframe, nil
	}
	units, err := accessUnit(packet.Payload.Bytes)
	if err != nil {
		return nil, false, fmt.Errorf("%w: %s %s: %v", codec.ErrUnsupportedFormat, d.c.ID, d.c.Backend, err)
	}
	keyframe := packet.Keyframe || slices.ContainsFunc(units, func(u []byte) bool { return randomAccess(d.c.ID, u) })
	if d.dropping && !keyframe {
		return nil, false, nil
	}
	d.dropping = false
	if sets := parameterSetsOf(d.c.ID, units); len(sets) != 0 && !sameUnits(sets, d.sets) {
		if d.c.Bitstream == AVCC {
			// New parameter sets need a session built for them.
			if err := d.stop(out); err != nil {
				return nil, false, err
			}
		}
		d.sets = cloneUnits(sets)
		d.fedSets = true
	}
	if d.session == nil {
		if len(d.sets) == 0 {
			return nil, false, d.requestKeyframe(out, "no parameter sets yet")
		}
		if err := d.start(); err != nil {
			return nil, false, err
		}
	}
	d.unit = d.unit[:0]
	if d.c.Bitstream == AVCC {
		for _, unit := range units {
			if !parameterSet(d.c.ID, unit) {
				d.unit = appendAVCC(d.unit, unit)
			}
		}
	} else {
		if !d.fedSets && len(parameterSetsOf(d.c.ID, units)) == 0 {
			d.unit = appendAnnexB(d.unit, d.sets...)
		}
		d.fedSets = true
		d.unit = appendAnnexB(d.unit, units...)
	}
	if len(d.unit) == 0 {
		return nil, false, nil
	}
	return d.unit, keyframe, nil
}

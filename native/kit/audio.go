package kit

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

// AudioConfig is what an audio session opens with: the PCM layout, the
// settings already checked against the backend's declaration, and for a
// decoder the stream's codec configuration (an AudioSpecificConfig).
type AudioConfig struct {
	SampleRate int
	Channels   int
	Realtime   bool
	Knobs      codec.KnobValues
	ExtraData  []byte
}

// Samples is interleaved signed 16-bit PCM lent to or by a backend for one
// call: never keep PCM. PTS counts samples per channel at SampleRate.
type Samples struct {
	PCM        []int16
	SampleRate int
	Channels   int
	PTS        int64
}

// AudioEncoder is a backend's audio encoder session. Encode takes samples
// and reports packets through emit, now or later; a packet's PTS is the
// sample time of its first decoded sample, so codec priming shows before
// zero. Flush drains the rest. Configured reports the codec configuration.
type AudioEncoder interface {
	Encode(samples *Samples, emit func(*Packet) error) error
	Flush(emit func(*Packet) error) error
	Close() error
}

// AudioDecoder is a backend's audio decoder session: Decode takes one packet
// and reports PCM through emit, stamped with the packet's PTS.
type AudioDecoder interface {
	Decode(packet *Packet, emit func(*Samples) error) error
	Flush(emit func(*Samples) error) error
	Close() error
}

type audioEncoder struct {
	c       *Codec
	desc    codec.Descriptor
	stream  av.Stream
	config  AudioConfig
	session AudioEncoder
	out     *codec.EncodeResult
	carry   codec.EncodeResult
	closed  bool
	emitFn  func(*Packet) error
}

func (e *audioEncoder) Descriptor() codec.Descriptor { return e.desc }

func (e *audioEncoder) Open(ctx context.Context, config codec.EncodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := refuseControl(e.c, config.Settings); err != nil {
		return err
	}
	stream, params := config.Stream, config.Parameters
	rate := firstPositive(params.SampleRate, stream.Codec.SampleRate, int(params.ClockRate))
	channels := firstPositive(params.Channels, stream.Codec.Channels)
	if rate <= 0 || channels <= 0 {
		return fmt.Errorf("%w: %s %s encode needs the sample rate and channel count", codec.ErrUnsupportedFormat, e.c.ID, e.c.Backend)
	}
	knobs, err := e.desc.ResolveKnobs(config.Settings, config.LowLatency || config.Realtime)
	if err != nil {
		return err
	}
	if err := e.c.load(); err != nil {
		return err
	}
	e.config = AudioConfig{SampleRate: rate, Channels: channels, Realtime: config.LowLatency || config.Realtime, Knobs: knobs}
	params.ID, params.Type = e.c.ID, av.MediaAudio
	params.SampleRate, params.ClockRate, params.Channels = rate, uint32(rate), channels
	params.SampleFormat = ""
	if params.ChannelLayout == "" {
		params.ChannelLayout = channelLayout(channels)
	}
	stream.Type, stream.Codec = av.MediaAudio, params
	stream.TimeBase = av.TimeBase{Num: 1, Den: int64(rate)}
	e.stream = stream
	e.emitFn = e.emit
	return e.start()
}

func (e *audioEncoder) start() error {
	session, err := e.c.NewAudioEncoder(e.config)
	if err != nil {
		return err
	}
	e.session = session
	if configured, ok := session.(Configured); ok {
		e.stream.Codec.ExtraData = av.Buffer{Bytes: slices.Clone(configured.Config())}
	}
	return nil
}

func (e *audioEncoder) EncodedParameters() av.CodecParameters { return e.stream.Codec }

func (e *audioEncoder) EncodeInto(ctx context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if err := e.ready(ctx, out); err != nil || frame == nil {
		return err
	}
	pcm, err := framePCM(frame, e.config.Channels)
	if err != nil {
		return fmt.Errorf("%s %s: %w", e.c.ID, e.c.Backend, err)
	}
	e.out = out
	return e.session.Encode(&Samples{
		PCM:        pcm,
		SampleRate: e.config.SampleRate,
		Channels:   e.config.Channels,
		PTS:        timeIn(frame.PTS, e.stream.TimeBase),
	}, e.emitFn)
}

// framePCM views a goav s16 audio frame's interleaved samples.
func framePCM(frame *av.Frame, channels int) ([]int16, error) {
	if frame.Audio == nil || len(frame.Planes) == 0 {
		return nil, fmt.Errorf("%w: want an interleaved s16 audio frame", codec.ErrUnsupportedFormat)
	}
	if format := frame.Audio.SampleFormat; format != "" && format != av.SampleFormatS16 {
		return nil, fmt.Errorf("%w: sample format %s, want s16", codec.ErrUnsupportedFormat, format)
	}
	if frame.Audio.Channels != 0 && frame.Audio.Channels != channels {
		return nil, fmt.Errorf("%w: %d channels, the encoder opened with %d", codec.ErrUnsupportedFormat, frame.Audio.Channels, channels)
	}
	data := frame.Planes[0].Buffer.Bytes[frame.Planes[0].Offset:]
	samples := len(data) / 2
	if frame.Audio.Samples > 0 {
		samples = frame.Audio.Samples * channels
	}
	if samples > len(data)/2 || samples%channels != 0 {
		return nil, fmt.Errorf("%w: plane holds %d bytes for %d samples", codec.ErrUnsupportedFormat, len(data), samples)
	}
	if samples == 0 {
		return nil, nil
	}
	return unsafe.Slice((*int16)(unsafe.Pointer(unsafe.SliceData(data))), samples), nil
}

func (e *audioEncoder) ready(ctx context.Context, out *codec.EncodeResult) error {
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

func (e *audioEncoder) emit(p *Packet) error {
	payload := av.NewBuffer(len(p.Data))
	copy(payload.Bytes, p.Data)
	base := e.stream.TimeBase
	e.out.Packets = append(e.out.Packets, av.Packet{
		StreamID:   e.stream.ID,
		Type:       av.MediaAudio,
		CodecEpoch: e.stream.Epoch,
		Payload:    payload,
		PTS:        av.Timestamp{Value: p.PTS, Base: base},
		DTS:        av.Timestamp{Value: p.PTS, Base: base},
		Duration:   av.Duration{Value: p.Duration, Base: base},
		Keyframe:   true,
	})
	return nil
}

func (e *audioEncoder) FlushInto(ctx context.Context, out *codec.EncodeResult) error {
	if err := e.ready(ctx, out); err != nil {
		return err
	}
	e.out = out
	return e.session.Flush(e.emitFn)
}

func (e *audioEncoder) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil || event == nil {
		return err
	}
	if e.closed {
		return codec.ErrClosed
	}
	if event.Type != av.EventBitrateChanged {
		return nil
	}
	bps, ok := codec.EventBitrate(event)
	if !ok {
		return fmt.Errorf("%w: %s %s: bitrate event needs a positive %s in bits per second", codec.ErrInvalidSetting, e.c.ID, e.c.Backend, av.MetadataBitrate)
	}
	knob, ok := e.desc.Knob("bitrate")
	if !ok {
		return fmt.Errorf("%w: %s %s takes no bitrate control", codec.ErrInvalidSetting, e.c.ID, e.c.Backend)
	}
	parsed, err := knob.Parse(strconv.Itoa(bps))
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
	e.out = &e.carry
	if err := e.session.Flush(e.emitFn); err != nil {
		return err
	}
	err = e.session.Close()
	e.session = nil
	if err != nil {
		return err
	}
	e.config.Knobs = knobs
	return e.start()
}

func (e *audioEncoder) Close() error {
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

type audioDecoder struct {
	c       *Codec
	desc    codec.Descriptor
	stream  av.Stream
	config  AudioConfig
	session AudioDecoder
	out     *codec.DecodeResult
	closed  bool
	emitFn  func(*Samples) error
}

func (d *audioDecoder) Descriptor() codec.Descriptor { return d.desc }

func (d *audioDecoder) Open(ctx context.Context, config codec.DecodeConfig) error {
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
	if err := d.c.load(); err != nil {
		return err
	}
	stream := config.Stream
	rate := firstPositive(stream.Codec.SampleRate, int(stream.Codec.ClockRate))
	if !stream.TimeBase.Valid() && rate > 0 {
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(rate)}
	}
	d.stream = stream
	d.config = AudioConfig{
		SampleRate: rate,
		Channels:   stream.Codec.Channels,
		Realtime:   config.LowLatency || config.Realtime,
		Knobs:      knobs,
		ExtraData:  slices.Clone(stream.Codec.ExtraData.Bytes),
	}
	d.emitFn = d.emitSamples
	session, err := d.c.NewAudioDecoder(d.config)
	if err != nil {
		return err
	}
	d.session = session
	return nil
}

func (d *audioDecoder) DecodeInto(ctx context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.closed {
		return codec.ErrClosed
	}
	if packet == nil || len(packet.Payload.Bytes) == 0 {
		return nil
	}
	d.out = out
	return d.session.Decode(&Packet{
		Data:     packet.Payload.Bytes,
		PTS:      timeIn(packet.PTS, d.stream.TimeBase),
		Duration: durationIn(packet.Duration, d.stream.TimeBase),
		Keyframe: true,
	}, d.emitFn)
}

func (d *audioDecoder) emitSamples(s *Samples) error {
	channels := firstPositive(s.Channels, d.config.Channels)
	rate := firstPositive(s.SampleRate, d.config.SampleRate)
	if channels <= 0 || rate <= 0 || len(s.PCM)%channels != 0 {
		return fmt.Errorf("%w: %s %s decoded %d samples for %d channels", codec.ErrUnsupportedFormat, d.c.ID, d.c.Backend, len(s.PCM), channels)
	}
	if len(s.PCM) == 0 {
		return nil
	}
	frames := len(s.PCM) / channels
	n := len(d.out.Frames)
	if n < cap(d.out.Frames) {
		d.out.Frames = d.out.Frames[:n+1]
	} else {
		d.out.Frames = append(d.out.Frames, av.Frame{})
	}
	frame := &d.out.Frames[n]
	frame.Reset()
	buffer := av.NewBuffer(len(s.PCM) * 2)
	copy(unsafe.Slice((*int16)(unsafe.Pointer(unsafe.SliceData(buffer.Bytes))), len(s.PCM)), s.PCM)
	if cap(frame.Planes) < 1 {
		frame.Planes = make([]av.Plane, 1)
	}
	frame.Planes = frame.Planes[:1]
	frame.Planes[0] = av.Plane{Buffer: buffer}
	// Each frame gets its own header: a result holds several, and the
	// stage publishes them by reference.
	header := &av.AudioFrame{SampleRate: rate, Channels: channels, ChannelLayout: channelLayout(channels), SampleFormat: av.SampleFormatS16, Samples: frames}
	base := av.TimeBase{Num: 1, Den: int64(rate)}
	frame.StreamID = d.stream.ID
	frame.CodecEpoch = d.stream.Epoch
	frame.Type = av.MediaAudio
	frame.Audio = header
	frame.PTS = av.Timestamp{Value: s.PTS, Base: base}
	frame.Duration = av.Duration{Value: int64(frames), Base: base}
	return nil
}

func (d *audioDecoder) FlushInto(ctx context.Context, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.closed {
		return codec.ErrClosed
	}
	d.out = out
	return d.session.Flush(d.emitFn)
}

func (d *audioDecoder) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil || event == nil {
		return err
	}
	if d.closed {
		return codec.ErrClosed
	}
	return nil
}

func (d *audioDecoder) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	err := d.session.Close()
	d.session = nil
	return err
}

func channelLayout(channels int) string {
	switch channels {
	case 1:
		return "mono"
	case 2:
		return "stereo"
	}
	return ""
}

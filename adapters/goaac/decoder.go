package goaac

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unsafe"

	aaclib "github.com/thesyncim/goaac"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

const (
	backendName       = "goaac"
	aacTransportKey   = "transport"
	aacTransportRaw   = "raw"
	aacTransportADTS  = "adts"
	aacTransportAuto  = "auto"
	aacSamplesPerUnit = 1024
)

type DecoderFactory struct{}

func NewDecoderFactory() DecoderFactory {
	return DecoderFactory{}
}

func Descriptor() codec.Descriptor {
	return codec.Descriptor{
		ID:       av.CodecAAC,
		Name:     "AAC-LC",
		Type:     av.MediaAudio,
		Modes:    []codec.Mode{codec.ModeDecode},
		Profiles: []string{"aac_lc"},
		Realtime: true,
		Capabilities: codec.Capabilities{
			SampleFormats: []string{av.SampleFormatS16},
		},
		Backend: codec.Backend{
			Name:    backendName,
			Module:  "github.com/thesyncim/goaac",
			Package: "github.com/thesyncim/goav/adapters/goaac",
			Status:  "active",
		},
	}
}

func Register(registry *codec.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterDecoder(Descriptor(), NewDecoderFactory())
}

func (DecoderFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	decoder := &Decoder{}
	if err := decoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return decoder, nil
}

type Decoder struct {
	decoder   *aaclib.Decoder
	stream    av.Stream
	settings  codec.CodecSettings
	transport aaclib.Transport
	audio     av.AudioFrame
	closed    bool
}

func (d *Decoder) Descriptor() codec.Descriptor {
	return Descriptor()
}

func (d *Decoder) Open(ctx context.Context, config codec.DecodeConfig) error {
	state, err := openDecoderState(ctx, config)
	if err != nil {
		return err
	}
	d.closeDecoder()
	d.decoder = state.decoder
	d.stream = state.stream
	d.settings = cloneCodecSettings(config.Settings)
	d.transport = state.transport
	d.audio = audioFrameFromStream(state.stream, 0)
	d.closed = false
	return nil
}

func (d *Decoder) DecodeInto(ctx context.Context, pkt *av.Packet, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.closed {
		return codec.ErrClosed
	}
	if d.decoder == nil {
		return codec.ErrUnsupportedFormat
	}
	if pkt == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}

	required, err := d.requiredOutputSamples(pkt)
	if err != nil {
		return err
	}

	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	pcm, backing, err := prepareOutputFrame(frame, required)
	if err != nil {
		out.Frames = out.Frames[:index]
		return err
	}

	decoded, info, err := d.decoder.Decode(pcm, pkt.Payload.Bytes)
	if err != nil {
		out.Frames = out.Frames[:index]
		return mapDecodeError(err)
	}
	if len(decoded) == 0 {
		out.Frames = out.Frames[:index]
		return nil
	}

	channels := firstPositive(info.Channels, d.stream.Codec.Channels)
	sampleRate := firstPositive(info.SampleRate, d.stream.Codec.SampleRate, int(d.stream.Codec.ClockRate))
	if channels <= 0 || sampleRate <= 0 || len(decoded)%channels != 0 {
		out.Frames = out.Frames[:index]
		return codec.ErrUnsupportedFormat
	}

	samples := len(decoded) / channels
	plane := &frame.Planes[0]
	plane.Buffer.Bytes = backing[:len(decoded)*2]
	plane.Buffer.Ownership = av.BufferOwned
	plane.Stride = channels * 2

	d.stream.Codec.SampleRate = sampleRate
	d.stream.Codec.ClockRate = uint32(sampleRate)
	d.stream.Codec.Channels = channels
	d.stream.Codec.ChannelLayout = audioChannelLayout(channels)
	d.audio = audioFrameFromStream(d.stream, samples)

	frame.StreamID = d.stream.ID
	frame.CodecEpoch = d.stream.Epoch
	if pkt.CodecEpoch != 0 {
		frame.CodecEpoch = pkt.CodecEpoch
	}
	frame.Type = av.MediaAudio
	frame.PTS = pkt.PTS
	frame.Duration = pkt.Duration
	if frame.Duration.Value == 0 {
		frame.Duration = av.SamplesDuration(samples, sampleRate)
	}
	frame.Audio = &d.audio
	frame.Metadata = pkt.Metadata
	return nil
}

func (d *Decoder) FlushInto(ctx context.Context, _ *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed {
		return codec.ErrClosed
	}
	return nil
}

func (d *Decoder) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	if d.closed {
		return codec.ErrClosed
	}
	switch event.Type {
	case av.EventCodecChanged:
		next, ok := codecChangedStream(d.stream, event)
		if !ok {
			return nil
		}
		if next.Codec.ID != "" && next.Codec.ID != av.CodecAAC {
			return codec.ErrUnsupportedCodecSwitch
		}
		return d.reopen(ctx, next)
	case av.EventDiscontinuity:
		return d.reopen(ctx, d.stream)
	}
	return nil
}

func (d *Decoder) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	d.closeDecoder()
	return nil
}

func (d *Decoder) closeDecoder() {
	if d.decoder != nil {
		_ = d.decoder.Close()
		d.decoder = nil
	}
}

func (d *Decoder) reopen(ctx context.Context, stream av.Stream) error {
	state, err := openDecoderState(ctx, codec.DecodeConfig{
		Stream:   stream,
		Settings: d.settings,
	})
	if err != nil {
		return err
	}
	d.closeDecoder()
	d.decoder = state.decoder
	d.stream = state.stream
	d.transport = state.transport
	d.audio = audioFrameFromStream(state.stream, 0)
	return nil
}

func (d *Decoder) requiredOutputSamples(pkt *av.Packet) (int, error) {
	switch d.transport {
	case aaclib.TransportADTS:
		header, err := aaclib.ParseADTSHeader(pkt.Payload.Bytes)
		if err != nil {
			return 0, mapDecodeError(err)
		}
		if header.Channels <= 0 {
			return 0, codec.ErrUnsupportedFormat
		}
		return aacSamplesPerUnit * header.Channels, nil
	case aaclib.TransportRaw:
		channels := d.stream.Codec.Channels
		if channels <= 0 {
			return 0, codec.ErrUnsupportedFormat
		}
		return aacSamplesPerUnit * channels, nil
	default:
		return 0, codec.ErrUnsupportedFormat
	}
}

type decoderState struct {
	decoder   *aaclib.Decoder
	stream    av.Stream
	transport aaclib.Transport
}

func openDecoderState(ctx context.Context, config codec.DecodeConfig) (decoderState, error) {
	if err := ctx.Err(); err != nil {
		return decoderState{}, err
	}
	stream, options, err := normalizeDecodeConfig(config)
	if err != nil {
		return decoderState{}, err
	}
	decoder, err := aaclib.New(options)
	if err != nil {
		return decoderState{}, mapDecodeError(err)
	}
	if control := config.Settings.Control; control != nil {
		if err := control(decoder); err != nil {
			_ = decoder.Close()
			return decoderState{}, mapDecodeError(err)
		}
	}
	return decoderState{decoder: decoder, stream: stream, transport: decoder.Transport()}, nil
}

func normalizeDecodeConfig(config codec.DecodeConfig) (av.Stream, aaclib.Options, error) {
	stream := config.Stream
	if stream.Type == "" {
		stream.Type = av.MediaAudio
	}
	if stream.Type != av.MediaAudio {
		return av.Stream{}, aaclib.Options{}, codec.ErrUnsupportedFormat
	}
	if stream.Codec.ID != "" && stream.Codec.ID != av.CodecAAC {
		return av.Stream{}, aaclib.Options{}, codec.ErrUnsupportedFormat
	}
	stream.Codec.ID = av.CodecAAC
	stream.Codec.Type = av.MediaAudio
	stream.Codec.SampleFormat = av.SampleFormatS16

	sampleRate := firstPositive(stream.Codec.SampleRate, int(stream.Codec.ClockRate))
	if sampleRate > 0 {
		stream.Codec.SampleRate = sampleRate
		stream.Codec.ClockRate = uint32(sampleRate)
	}
	if stream.Codec.Channels > 0 && stream.Codec.ChannelLayout == "" {
		stream.Codec.ChannelLayout = audioChannelLayout(stream.Codec.Channels)
	}
	if stream.TimeBase == (av.TimeBase{}) && stream.Codec.ClockRate != 0 {
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(stream.Codec.ClockRate)}
	}

	transport, err := decodeTransport(config.Settings, stream.Codec)
	if err != nil {
		return av.Stream{}, aaclib.Options{}, err
	}
	options := aaclib.Options{Transport: transport}
	if transport == aaclib.TransportRaw || (transport == aaclib.TransportAuto && streamHasRawConfig(stream)) {
		raw, err := rawConfigFromStream(stream)
		if err != nil {
			return av.Stream{}, aaclib.Options{}, err
		}
		options.Config = raw
		if raw.SampleRate > 0 {
			stream.Codec.SampleRate = raw.SampleRate
			stream.Codec.ClockRate = uint32(raw.SampleRate)
		}
		if raw.Channels > 0 {
			stream.Codec.Channels = raw.Channels
			stream.Codec.ChannelLayout = audioChannelLayout(raw.Channels)
		}
	}
	return stream, options, nil
}

func streamHasRawConfig(stream av.Stream) bool {
	return len(stream.Codec.ExtraData.Bytes) != 0 ||
		(firstPositive(stream.Codec.SampleRate, int(stream.Codec.ClockRate)) > 0 && stream.Codec.Channels > 0)
}

func decodeTransport(settings codec.CodecSettings, parameters av.CodecParameters) (aaclib.Transport, error) {
	value := ""
	if settings.Custom != nil {
		value = settings.Custom[aacTransportKey]
	}
	if value == "" && parameters.Attributes != nil {
		value = parameters.Attributes[aacTransportKey]
	}
	switch strings.ToLower(value) {
	case "":
		if len(parameters.ExtraData.Bytes) != 0 {
			return aaclib.TransportRaw, nil
		}
		return aaclib.TransportADTS, nil
	case aacTransportRaw:
		return aaclib.TransportRaw, nil
	case aacTransportADTS:
		return aaclib.TransportADTS, nil
	case aacTransportAuto:
		return aaclib.TransportAuto, nil
	default:
		return 0, fmt.Errorf("goaac: unsupported AAC transport %q: %w", value, codec.ErrUnsupportedFormat)
	}
}

func rawConfigFromStream(stream av.Stream) (aaclib.Config, error) {
	cfg := aaclib.Config{
		ObjectType: aaclib.AOTAACLC,
		SampleRate: firstPositive(stream.Codec.SampleRate, int(stream.Codec.ClockRate)),
		Channels:   stream.Codec.Channels,
		ExtraData:  append([]byte(nil), stream.Codec.ExtraData.Bytes...),
	}
	if len(cfg.ExtraData) != 0 {
		parsed, err := aaclib.ParseAudioSpecificConfig(cfg.ExtraData)
		if err != nil {
			return aaclib.Config{}, mapDecodeError(err)
		}
		return parsed, nil
	}
	if cfg.SampleRate <= 0 || cfg.Channels <= 0 {
		return aaclib.Config{}, codec.ErrUnsupportedFormat
	}
	return cfg, nil
}

func prepareOutputFrame(frame *av.Frame, requiredSamples int) ([]int16, []byte, error) {
	if cap(frame.Planes) < 1 {
		return nil, nil, codec.ErrOutputBufferTooSmall
	}
	frame.Planes = frame.Planes[:1]
	plane := &frame.Planes[0]
	requiredBytes := requiredSamples * 2
	if requiredSamples <= 0 || cap(plane.Buffer.Bytes) < requiredBytes {
		return nil, nil, codec.ErrOutputBufferTooSmall
	}
	backing := plane.Buffer.Bytes[:cap(plane.Buffer.Bytes)]
	if len(backing)%2 != 0 {
		backing = backing[:len(backing)-1]
	}
	pcm := unsafe.Slice((*int16)(unsafe.Pointer(unsafe.SliceData(backing))), len(backing)/2)
	return pcm[:0], backing, nil
}

func audioFrameFromStream(stream av.Stream, samples int) av.AudioFrame {
	channels := stream.Codec.Channels
	sampleRate := firstPositive(stream.Codec.SampleRate, int(stream.Codec.ClockRate))
	return av.AudioFrame{
		SampleRate:    sampleRate,
		Channels:      channels,
		ChannelLayout: audioChannelLayout(channels),
		SampleFormat:  av.SampleFormatS16,
		Samples:       samples,
	}
}

func audioChannelLayout(channels int) string {
	switch channels {
	case 1:
		return "mono"
	case 2:
		return "stereo"
	default:
		return ""
	}
}

func codecChangedStream(current av.Stream, event *av.Event) (av.Stream, bool) {
	if event == nil || event.Type != av.EventCodecChanged {
		return av.Stream{}, false
	}
	matchesCurrent := event.StreamID == "" || current.ID == "" || event.StreamID == current.ID
	if !matchesCurrent && event.Stream == nil {
		return av.Stream{}, false
	}
	next := current
	if event.Stream != nil {
		next = *event.Stream
	}
	if !matchesCurrent && next.Type != "" && current.Type != "" && next.Type != current.Type {
		return av.Stream{}, false
	}
	if next.ID == "" {
		if event.StreamID != "" {
			next.ID = event.StreamID
		} else {
			next.ID = current.ID
		}
	}
	if next.Type == "" {
		next.Type = current.Type
	}
	if next.Codec.ID == "" {
		next.Codec = current.Codec
	}
	if event.Codec != nil {
		next.Codec = *event.Codec
		if next.Codec.Type != "" {
			next.Type = next.Codec.Type
		}
	}
	if next.TimeBase == (av.TimeBase{}) {
		next.TimeBase = current.TimeBase
	}
	if next.Epoch == 0 && event.Epoch != 0 {
		next.Epoch = event.Epoch
	}
	return next, true
}

func cloneCodecSettings(settings codec.CodecSettings) codec.CodecSettings {
	cloned := settings
	if settings.Custom != nil {
		cloned.Custom = make(av.Metadata, len(settings.Custom))
		for key, value := range settings.Custom {
			cloned.Custom[key] = value
		}
	}
	return cloned
}

func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func mapDecodeError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, aaclib.ErrClosed):
		return codec.ErrClosed
	case errors.Is(err, aaclib.ErrNativeUnavailable):
		return fmt.Errorf("goaac: %v: %w", err, codec.ErrUnavailable)
	case errors.Is(err, aaclib.ErrNeedMoreData),
		errors.Is(err, aaclib.ErrInvalidConfig),
		errors.Is(err, aaclib.ErrInvalidADTS),
		errors.Is(err, aaclib.ErrUnsupportedProfile):
		return fmt.Errorf("goaac: %v: %w", err, codec.ErrUnsupportedFormat)
	default:
		return err
	}
}

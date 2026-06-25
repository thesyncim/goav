// Package adapterproof proves the adapter authoring contract from outside the
// core: one toy implementation of EVERY extension seam — codec
// (descriptor+encoder+decoder), container (muxer+demuxer+prober), filter,
// source provider, and transactional destination provider — defined entirely
// in this test package, registered through the public value options
// (runconfig.WithEncoder/WithDecoder/WithFilter/WithMuxer/WithDemuxer/WithProber)
// or passed as values (goav.Input, goav.Custom), and run end to end through
// the public recipe grammar.
//
// The compile-time claim IS the proof: this package imports only public goav
// packages (goav, av, codec, filter, format, pipeline, shape) and contains no
// core change — a third party can do exactly this from another module. The
// authoring contract each toy follows is documented in
// docs/ADAPTER_AUTHORING.md.
package adapterproof_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
)

// The made-up identities. Nothing in core knows them.
const (
	toyCodecID  av.CodecID  = "toy/pcm"
	toyFormatID av.FormatID = "toyfmt"
)

// --- toy codec: a trivial PCM codec ---------------------------------------
//
// Encode copies the frame's interleaved S16 bytes verbatim into one keyframe
// packet; decode copies them back into one frame shaped by the opened
// stream's codec parameters. Byte-faithful, so the round trip is assertable.

func toyCodecDescriptor() codec.Descriptor {
	return codec.Descriptor{
		ID:   toyCodecID,
		Name: "toy PCM",
		Type: av.MediaAudio,
		Capabilities: codec.Capabilities{
			SampleFormats: []string{av.SampleFormatS16},
		},
		Backend: codec.Backend{Name: "adapterproof", Package: "adapterproof"},
	}
}

// toyCodecFactory returns READY instances: per the authoring contract the
// factory calls Open itself; the runtime never calls Open separately.
type toyCodecFactory struct{}

func (toyCodecFactory) NewEncoder(ctx context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	encoder := &toyEncoder{}
	if err := encoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return encoder, nil
}

func (toyCodecFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	decoder := &toyDecoder{}
	if err := decoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return decoder, nil
}

type toyEncoder struct {
	stream av.Stream
}

func (e *toyEncoder) Descriptor() codec.Descriptor { return toyCodecDescriptor() }

func (e *toyEncoder) Open(_ context.Context, config codec.EncodeConfig) error {
	if format := config.Parameters.SampleFormat; format != "" && format != av.SampleFormatS16 {
		return codec.ErrUnsupportedFormat
	}
	e.stream = config.Stream
	return nil
}

func (e *toyEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if frame == nil || len(frame.Planes) == 0 {
		return nil
	}
	index := len(out.Packets)
	if index == cap(out.Packets) {
		return codec.ErrResultFull
	}
	payload := frame.Planes[0].Buffer.Bytes
	out.Packets = out.Packets[:index+1]
	packet := &out.Packets[index]
	packet.Reset()
	if cap(packet.Payload.Bytes) < len(payload) {
		out.Packets = out.Packets[:index]
		return codec.ErrOutputBufferTooSmall
	}
	packet.Payload.Bytes = append(packet.Payload.Bytes[:0], payload...)
	packet.Payload.Ownership = av.BufferOwned
	packet.StreamID = frame.StreamID
	packet.Type = av.MediaAudio
	packet.PTS = frame.PTS
	packet.DTS = frame.PTS
	packet.Duration = frame.Duration
	packet.Keyframe = true
	return nil
}

func (e *toyEncoder) FlushInto(context.Context, *codec.EncodeResult) error { return nil }

// HandleEvent accepts live controls: every toy packet is a keyframe and the
// codec has no bitrate, so both standard encoder controls are trivially
// satisfied (an encoder that cannot apply one must return an error instead).
func (e *toyEncoder) HandleEvent(context.Context, *av.Event) error { return nil }

func (e *toyEncoder) Close() error { return nil }

type toyDecoder struct {
	stream av.Stream
	audio  av.AudioFrame
}

func (d *toyDecoder) Descriptor() codec.Descriptor { return toyCodecDescriptor() }

func (d *toyDecoder) Open(_ context.Context, config codec.DecodeConfig) error {
	sampleRate := config.Stream.Codec.SampleRate
	channels := config.Stream.Codec.Channels
	if sampleRate <= 0 || channels <= 0 {
		return codec.ErrUnsupportedFormat
	}
	d.stream = config.Stream
	d.audio = av.AudioFrame{
		SampleRate:   sampleRate,
		Channels:     channels,
		SampleFormat: av.SampleFormatS16,
	}
	return nil
}

// DecodeInto unwraps one packet into one frame using the caller's
// preallocated plane storage. A nil packet is the packet-loss concealment
// hook and legal by contract; the toy has nothing to conceal.
func (d *toyDecoder) DecodeInto(_ context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if packet == nil {
		return nil
	}
	index := len(out.Frames)
	if index == cap(out.Frames) {
		return codec.ErrResultFull
	}
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	if cap(frame.Planes) < 1 {
		out.Frames = out.Frames[:index]
		return codec.ErrOutputBufferTooSmall
	}
	frame.Planes = frame.Planes[:1]
	plane := &frame.Planes[0]
	payload := packet.Payload.Bytes
	if cap(plane.Buffer.Bytes) < len(payload) {
		out.Frames = out.Frames[:index]
		return codec.ErrOutputBufferTooSmall
	}
	plane.Buffer.Bytes = append(plane.Buffer.Bytes[:0], payload...)
	plane.Buffer.Ownership = av.BufferOwned
	plane.Stride = d.audio.Channels * 2
	d.audio.Samples = len(payload) / (d.audio.Channels * 2)
	frame.StreamID = packet.StreamID
	frame.CodecEpoch = packet.CodecEpoch
	frame.Type = av.MediaAudio
	frame.PTS = packet.PTS
	frame.Duration = packet.Duration
	frame.Audio = &d.audio
	return nil
}

func (d *toyDecoder) FlushInto(context.Context, *codec.DecodeResult) error { return nil }

func (d *toyDecoder) HandleEvent(context.Context, *av.Event) error { return nil }

func (d *toyDecoder) Close() error { return nil }

// --- toy container: length-prefixed packet records -------------------------
//
// Header: magic, stream count, then per stream id/sample-rate/channels.
// Record: stream id, PTS+duration (value and timebase), keyframe flag,
// payload bytes verbatim. Enough to demux back the exact packets written.

var toyMagic = []byte("TOYF")

type toyContainerFactory struct{}

func (toyContainerFactory) NewMuxer(context.Context, av.FormatID) (format.Muxer, error) {
	return &toyMuxer{}, nil
}

func (toyContainerFactory) NewDemuxer(context.Context, format.ProbeResult) (format.Demuxer, error) {
	return &toyDemuxer{}, nil
}

type toyMuxer struct {
	writer  io.Writer
	scratch []byte
}

func (m *toyMuxer) Format() av.FormatID { return toyFormatID }

func (m *toyMuxer) Open(_ context.Context, output format.Output, streams []av.Stream, _ format.OpenOptions) error {
	if output.Writer == nil {
		return errors.New("adapterproof: toy container output has no writer")
	}
	m.writer = output.Writer
	header := append(m.scratch[:0], toyMagic...)
	header = appendU32(header, uint32(len(streams)))
	for i := range streams {
		header = appendString(header, string(streams[i].ID))
		header = appendU32(header, uint32(streams[i].Codec.SampleRate))
		header = appendU32(header, uint32(streams[i].Codec.Channels))
	}
	m.scratch = header
	_, err := m.writer.Write(header)
	return err
}

func (m *toyMuxer) Write(_ context.Context, packet *av.Packet, _ *format.WriteResult) error {
	if packet == nil {
		return format.ErrNilPacket
	}
	record := appendString(m.scratch[:0], string(packet.StreamID))
	record = appendI64(record, packet.PTS.Value)
	record = appendI64(record, packet.PTS.Base.Num)
	record = appendI64(record, packet.PTS.Base.Den)
	record = appendI64(record, packet.Duration.Value)
	record = appendI64(record, packet.Duration.Base.Num)
	record = appendI64(record, packet.Duration.Base.Den)
	flag := byte(0)
	if packet.Keyframe {
		flag = 1
	}
	record = append(record, flag)
	record = appendU32(record, uint32(len(packet.Payload.Bytes)))
	record = append(record, packet.Payload.Bytes...)
	m.scratch = record
	prefix := appendU32(nil, uint32(len(record)))
	if _, err := m.writer.Write(prefix); err != nil {
		return err
	}
	_, err := m.writer.Write(record)
	return err
}

func (m *toyMuxer) Close() error { return nil }

type toyDemuxer struct {
	reader  io.Reader
	streams []av.Stream
}

func (d *toyDemuxer) Format() av.FormatID { return toyFormatID }

func (d *toyDemuxer) Open(_ context.Context, input format.Input, _ format.OpenOptions) error {
	if input.Reader == nil {
		return errors.New("adapterproof: toy container input has no reader")
	}
	d.reader = input.Reader
	magic := make([]byte, len(toyMagic))
	if _, err := io.ReadFull(d.reader, magic); err != nil || !bytes.Equal(magic, toyMagic) {
		return errors.New("adapterproof: not a toy container")
	}
	count, err := readU32(d.reader)
	if err != nil {
		return err
	}
	d.streams = make([]av.Stream, 0, count)
	for i := uint32(0); i < count; i++ {
		id, err := readString(d.reader)
		if err != nil {
			return err
		}
		sampleRate, err := readU32(d.reader)
		if err != nil {
			return err
		}
		channels, err := readU32(d.reader)
		if err != nil {
			return err
		}
		d.streams = append(d.streams, av.Stream{
			ID:       av.StreamID(id),
			Type:     av.MediaAudio,
			Index:    int(i),
			TimeBase: av.TimeBase{Num: 1, Den: int64(sampleRate)},
			Codec: av.CodecParameters{
				ID:           toyCodecID,
				Type:         av.MediaAudio,
				ClockRate:    sampleRate,
				SampleRate:   int(sampleRate),
				Channels:     int(channels),
				SampleFormat: av.SampleFormatS16,
			},
		})
	}
	return nil
}

func (d *toyDemuxer) Streams() []av.Stream { return d.streams }

func (d *toyDemuxer) ReadInto(_ context.Context, out *format.ReadResult) error {
	length, err := readU32(d.reader)
	if err != nil {
		return err // io.EOF at a record boundary is the clean end of media.
	}
	record := make([]byte, length)
	if _, err := io.ReadFull(d.reader, record); err != nil {
		return io.ErrUnexpectedEOF
	}
	r := bytes.NewReader(record)
	id, err := readString(r)
	if err != nil {
		return err
	}
	packet := out.Packet
	packet.Reset()
	packet.StreamID = av.StreamID(id)
	packet.Type = av.MediaAudio
	values := make([]int64, 6)
	for i := range values {
		if values[i], err = readI64(r); err != nil {
			return err
		}
	}
	packet.PTS = av.Timestamp{Value: values[0], Base: av.TimeBase{Num: values[1], Den: values[2]}}
	packet.Duration = av.Duration{Value: values[3], Base: av.TimeBase{Num: values[4], Den: values[5]}}
	flag := make([]byte, 1)
	if _, err := io.ReadFull(r, flag); err != nil {
		return err
	}
	packet.Keyframe = flag[0] == 1
	payloadLen, err := readU32(r)
	if err != nil {
		return err
	}
	packet.Payload.Bytes = packet.Payload.Bytes[:0]
	packet.Payload.Bytes = append(packet.Payload.Bytes, record[len(record)-int(payloadLen):]...)
	out.PacketReady = true
	return nil
}

func (d *toyDemuxer) Close() error { return nil }

// toyProber implements format.Prober from scratch: the ".toyfmt" name
// extension or the leading magic identifies the format; anything else is
// format.ErrNotFound so other probers stay in play.
type toyProber struct{}

func (toyProber) Probe(_ context.Context, request format.ProbeRequest) (format.ProbeResult, error) {
	names := []string{request.Name, request.Input.Name, request.Input.URI}
	for _, name := range names {
		if len(name) > 7 && name[len(name)-7:] == ".toyfmt" {
			return format.ProbeResult{Format: toyFormatID, Score: 100, Reason: "toyfmt extension"}, nil
		}
	}
	if len(request.Header) >= len(toyMagic) && bytes.Equal(request.Header[:len(toyMagic)], toyMagic) {
		return format.ProbeResult{Format: toyFormatID, Score: 100, Reason: "toyfmt magic"}, nil
	}
	return format.ProbeResult{}, format.ErrNotFound
}

// --- toy filter: integer 2x upsampler in the resample slot ------------------
//
// Registered under filter.FactoryResample, so the grammar's .Resample(...)
// resolves to it. It duplicates every sample (exact 2x rates only), making
// its effect observable in the round-trip assertion.

func toyFilterDescriptor() filter.Descriptor {
	return filter.Descriptor{
		Name:          filter.FactoryResample,
		Input:         av.MediaAudio,
		Output:        av.MediaAudio,
		SampleFormats: []string{av.SampleFormatS16},
		Stateless:     true,
	}
}

type toyFilterFactory struct{}

func (toyFilterFactory) NewFilter(ctx context.Context, config filter.Config) (filter.FrameFilter, error) {
	doubler := &toyUpsampler{}
	if err := doubler.Open(ctx, config); err != nil {
		return nil, err
	}
	return doubler, nil
}

type toyUpsampler struct {
	channels   int
	outputRate int
	audio      av.AudioFrame
}

func (f *toyUpsampler) Descriptor() filter.Descriptor { return toyFilterDescriptor() }

func (f *toyUpsampler) Open(_ context.Context, config filter.Config) error {
	if config.Audio == nil {
		return filter.ErrUnsupportedFormat
	}
	inputRate := config.Stream.Codec.SampleRate
	channels := config.Stream.Codec.Channels
	if inputRate <= 0 || channels <= 0 ||
		config.Audio.SampleRate != inputRate*2 ||
		(config.Audio.Channels != 0 && config.Audio.Channels != channels) {
		return filter.ErrUnsupportedFormat
	}
	f.channels = channels
	f.outputRate = config.Audio.SampleRate
	f.audio = av.AudioFrame{
		SampleRate:   f.outputRate,
		Channels:     channels,
		SampleFormat: av.SampleFormatS16,
	}
	return nil
}

func (f *toyUpsampler) FilterInto(_ context.Context, frame *av.Frame, out *filter.Result) error {
	if frame == nil || len(frame.Planes) == 0 {
		return nil
	}
	index := len(out.Frames)
	if index == cap(out.Frames) {
		return filter.ErrResultFull
	}
	src := frame.Planes[0].Buffer.Bytes
	width := f.channels * 2
	samples := len(src) / width
	out.Frames = out.Frames[:index+1]
	output := &out.Frames[index]
	output.Reset()
	if cap(output.Planes) < 1 || cap(output.Planes[:1][0].Buffer.Bytes) < 2*len(src) {
		out.Frames = out.Frames[:index]
		return filter.ErrOutputBufferTooSmall
	}
	output.Planes = output.Planes[:1]
	dst := output.Planes[0].Buffer.Bytes[:0]
	for sample := 0; sample < samples; sample++ {
		group := src[sample*width : (sample+1)*width]
		dst = append(dst, group...)
		dst = append(dst, group...)
	}
	output.Planes[0].Buffer.Bytes = dst
	output.Planes[0].Buffer.Ownership = av.BufferOwned
	output.Planes[0].Stride = width
	f.audio.Samples = samples * 2
	output.StreamID = frame.StreamID
	output.CodecEpoch = frame.CodecEpoch
	output.Type = av.MediaAudio
	output.PTS = frame.PTS
	output.Duration = av.SamplesDuration(samples*2, f.outputRate)
	output.Audio = &f.audio
	return nil
}

func (f *toyUpsampler) FlushInto(context.Context, *filter.Result) error { return nil }

func (f *toyUpsampler) HandleEvent(context.Context, *av.Event) error { return nil }

func (f *toyUpsampler) Close() error { return nil }

// --- toy source provider ----------------------------------------------------
//
// A provider.Source with the optional Name/Detail/DecodeBounds
// capabilities: it declares its shape up front and opens a pipeline.Source
// that announces its stream, pushes toy-codec packets, and ends with EOS.

type toyProvider struct {
	stream   av.Stream
	payloads [][]byte
	opened   int
}

func newToyProvider(frames ...[]int16) *toyProvider {
	provider := &toyProvider{
		stream: av.Stream{
			ID:       "toy-audio",
			Type:     av.MediaAudio,
			TimeBase: av.TimeBase{Num: 1, Den: 8000},
			Codec: av.CodecParameters{
				ID:           toyCodecID,
				Type:         av.MediaAudio,
				ClockRate:    8000,
				SampleRate:   8000,
				Channels:     1,
				SampleFormat: av.SampleFormatS16,
			},
		},
	}
	for _, samples := range frames {
		payload := make([]byte, len(samples)*2)
		for i, sample := range samples {
			binary.LittleEndian.PutUint16(payload[i*2:], uint16(sample))
		}
		provider.payloads = append(provider.payloads, payload)
	}
	return provider
}

func (p *toyProvider) Name() string   { return "toysrc" }
func (p *toyProvider) Detail() string { return "toy provider" }

func (p *toyProvider) DecodeBounds(av.StreamID) codec.DecodeBounds {
	return codec.DecodeBounds{MaxFramesPerInput: 1, MaxPayloadBytes: 4096}
}

func (p *toyProvider) SourceShape() shape.Spec {
	return shape.Packet(av.MediaAudio, toyCodecID,
		shape.Stream(p.stream.ID),
		shape.Audio(8000, 1, av.SampleFormatS16),
		shape.Realtime(true))
}

func (p *toyProvider) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	p.opened++
	return &toyProviderSource{stream: p.stream, payloads: p.payloads}, []av.Stream{p.stream}, nil
}

type toyProviderSource struct {
	stream   av.Stream
	payloads [][]byte
}

func (s *toyProviderSource) Name() string { return "toysrc" }

func (s *toyProviderSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	announce := av.Event{Type: av.EventStreamAdded, StreamID: s.stream.ID, Stream: &s.stream}
	msg := pipeline.Message{Kind: pipeline.MessageEvent, Event: &announce}
	if err := emitter.Emit(ctx, &msg); err != nil {
		return err
	}
	var packet av.Packet
	var elapsed int64
	for _, payload := range s.payloads {
		packet.Reset()
		packet.StreamID = s.stream.ID
		packet.Type = av.MediaAudio
		packet.PTS = av.Timestamp{Value: elapsed, Base: s.stream.TimeBase}
		packet.Duration = av.Duration{Value: int64(len(payload) / 2), Base: s.stream.TimeBase}
		packet.Keyframe = true
		packet.Payload = av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}
		elapsed += int64(len(payload) / 2)
		msg = pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}
		if err := emitter.Emit(ctx, &msg); err != nil {
			return err
		}
	}
	eos := av.Event{Type: av.EventEndOfStream, StreamID: s.stream.ID}
	msg = pipeline.Message{Kind: pipeline.MessageEvent, Event: &eos}
	return emitter.Emit(ctx, &msg)
}

func (s *toyProviderSource) Close() error { return nil }

// --- toy transactional destination ------------------------------------------
//
// A provider.Destination whose writer stages bytes and publishes them
// only on Commit — the object-store upload pattern. Abort discards.

type toyDestination struct {
	mu        sync.Mutex
	committed []byte
	commits   int
	aborts    int
	info      provider.Info
}

func (d *toyDestination) Name() string { return "toydest" }

func (d *toyDestination) Contract() provider.Contract {
	return provider.Contract{
		ByteStream: true,
		Formats:    []av.FormatID{toyFormatID},
	}
}

func (d *toyDestination) Open(_ context.Context, info provider.Info) (provider.Writer, error) {
	d.mu.Lock()
	d.info = info
	d.mu.Unlock()
	return &toyDestinationWriter{dest: d}, nil
}

type toyDestinationWriter struct {
	dest    *toyDestination
	staging bytes.Buffer
}

func (w *toyDestinationWriter) Write(p []byte) (int, error) { return w.staging.Write(p) }

func (w *toyDestinationWriter) Close() error { return nil }

func (w *toyDestinationWriter) Commit(context.Context) error {
	w.dest.mu.Lock()
	defer w.dest.mu.Unlock()
	w.dest.committed = append([]byte(nil), w.staging.Bytes()...)
	w.dest.commits++
	return nil
}

func (w *toyDestinationWriter) Abort(context.Context) error {
	w.dest.mu.Lock()
	defer w.dest.mu.Unlock()
	w.dest.aborts++
	return nil
}

var _ provider.Source = (*toyProvider)(nil)
var _ provider.Destination = (*toyDestination)(nil)
var _ provider.TransactionalWriter = (*toyDestinationWriter)(nil)
var _ codec.EncoderFactory = toyCodecFactory{}
var _ codec.DecoderFactory = toyCodecFactory{}
var _ format.MuxerFactory = toyContainerFactory{}
var _ format.DemuxerFactory = toyContainerFactory{}
var _ format.Prober = toyProber{}
var _ filter.Factory = toyFilterFactory{}

// --- the proof ---------------------------------------------------------------

// toyRuntime registers one external implementation of every seam on a bare
// runtime — value options only, no registry callbacks, no core knowledge.
func toyRuntime() *goav.Runtime {
	return goav.MustRuntime(
		runconfig.WithRealtime(false),
		runconfig.WithDecoder(toyCodecDescriptor(), toyCodecFactory{}),
		runconfig.WithEncoder(toyCodecDescriptor(), toyCodecFactory{}),
		runconfig.WithFilter(toyFilterDescriptor(), toyFilterFactory{}),
		runconfig.WithMuxer(toyFormatID, toyContainerFactory{}),
		runconfig.WithDemuxer(toyFormatID, toyContainerFactory{}),
		runconfig.WithProber(toyProber{}),
	)
}

// TestExternalAdaptersComposeThroughPublicGrammar runs the five toy seams in
// one job — provider source → decode(toy) → filter(toy) → encode(toy) →
// mux(toy container) → transactional destination, commit asserted — then
// demuxes the committed bytes back (toy prober + demuxer + decoder) and
// asserts the samples survived the full round trip, doubled by the filter.
func TestExternalAdaptersComposeThroughPublicGrammar(t *testing.T) {
	ctx := context.Background()
	dest := &toyDestination{}
	provider := newToyProvider([]int16{100, 200}, []int16{300, 400})

	err := goav.From(goav.Input(provider)).
		Audio().
		Decode().
		Resample(16000, 1).
		Encode(codec.Codec(toyCodecID, av.MediaAudio)).
		To(goav.Custom("out.toyfmt", dest)).
		UseRuntime(toyRuntime()).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if provider.opened != 1 {
		t.Fatalf("provider opened %d times, want 1", provider.opened)
	}
	dest.mu.Lock()
	commits, aborts, info, file := dest.commits, dest.aborts, dest.info, dest.committed
	dest.mu.Unlock()
	if commits != 1 || aborts != 0 {
		t.Fatalf("destination commits=%d aborts=%d, want 1 commit and 0 aborts", commits, aborts)
	}
	if info.Format != toyFormatID {
		t.Fatalf("destination opened with format %q, want %q", info.Format, toyFormatID)
	}
	if len(info.Streams) != 1 || info.Streams[0].Codec.ID != toyCodecID || info.Streams[0].Codec.SampleRate != 16000 {
		t.Fatalf("destination streams = %+v, want one toy/pcm stream at 16kHz", info.Streams)
	}
	if len(file) == 0 {
		t.Fatal("destination committed no bytes")
	}

	// Read the committed container back through the toy prober, demuxer, and
	// decoder, collecting decoded frames through a public sink.
	var mu sync.Mutex
	var frames [][]int16
	collect := component.SinkFunc("collect", func(_ context.Context, msg component.Message) error {
		if msg.Kind != pipeline.MessageFrame || msg.Frame == nil || len(msg.Frame.Planes) == 0 {
			return nil
		}
		data := msg.Frame.Planes[0].Buffer.Bytes
		samples := make([]int16, len(data)/2)
		for i := range samples {
			samples[i] = int16(binary.LittleEndian.Uint16(data[i*2:]))
		}
		mu.Lock()
		frames = append(frames, samples)
		mu.Unlock()
		return nil
	})
	err = goav.From(goav.FileInput("roundtrip.toyfmt", bytes.NewReader(file))).
		Audio().
		Decode().
		To(goav.Sink(collect)).
		UseRuntime(toyRuntime()).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}

	want := [][]int16{{100, 100, 200, 200}, {300, 300, 400, 400}}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(frames, want) {
		t.Fatalf("round-trip frames = %v, want %v (input doubled by the toy upsampler)", frames, want)
	}
}

// TestExternalDestinationAbortsOnFailure proves the other half of the
// transactional contract: a run that fails aborts the destination instead of
// committing it.
func TestExternalDestinationAbortsOnFailure(t *testing.T) {
	ctx := context.Background()
	dest := &toyDestination{}
	provider := newToyProvider([]int16{100, 200})
	failing := component.FrameFunc("boom", func(context.Context, *av.Frame, component.Emit) error {
		return errors.New("adapterproof: induced failure")
	})

	err := goav.From(goav.Input(provider)).
		Audio().
		Decode().
		Do(failing).
		Encode(codec.Codec(toyCodecID, av.MediaAudio)).
		To(goav.Custom("out.toyfmt", dest)).
		UseRuntime(toyRuntime()).
		Run(ctx)
	if err == nil {
		t.Fatal("run should fail through the failing stage")
	}
	dest.mu.Lock()
	defer dest.mu.Unlock()
	if dest.commits != 0 || dest.aborts != 1 {
		t.Fatalf("destination commits=%d aborts=%d, want 0 commits and 1 abort", dest.commits, dest.aborts)
	}
}

// --- little-endian wire helpers ----------------------------------------------

func appendU32(b []byte, v uint32) []byte {
	return binary.LittleEndian.AppendUint32(b, v)
}

func appendI64(b []byte, v int64) []byte {
	return binary.LittleEndian.AppendUint64(b, uint64(v))
}

func appendString(b []byte, s string) []byte {
	b = appendU32(b, uint32(len(s)))
	return append(b, s...)
}

func readU32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func readI64(r io.Reader) (int64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(buf[:])), nil
}

func readString(r io.Reader) (string, error) {
	length, err := readU32(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

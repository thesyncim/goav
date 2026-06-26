package goav

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

type componentEmitter struct {
	messages []pipeline.Message
	err      error
}

func (e *componentEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	e.messages = append(e.messages, cloneComponentMessage(msg))
	return e.err
}

type componentDeliveryEmitter struct {
	componentEmitter
	delivery pipeline.Delivery
}

func (e *componentDeliveryEmitter) EmitDelivery(ctx context.Context, msg *pipeline.Message) (pipeline.Delivery, error) {
	err := e.Emit(ctx, msg)
	return e.delivery, err
}

type componentRetainingEmitter struct {
	messages []*pipeline.Message
}

func (e *componentRetainingEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	e.messages = append(e.messages, msg)
	return nil
}

func cloneComponentMessage(msg *pipeline.Message) pipeline.Message {
	if msg == nil {
		return pipeline.Message{}
	}
	out := *msg
	if msg.Packet != nil {
		packet := *msg.Packet
		out.Packet = &packet
	}
	if msg.Frame != nil {
		frame := *msg.Frame
		out.Frame = &frame
	}
	if msg.Event != nil {
		event := *msg.Event
		out.Event = &event
	}
	return out
}

func TestSourcePushDefaultsAndDeliveryResults(t *testing.T) {
	ctx := context.Background()
	delivery := &componentDeliveryEmitter{delivery: pipeline.Delivery{Delivered: 1, Shed: 1}}
	push := sourcepkg.NewPush(ctx, delivery, "declared")

	if result, err := push.Packet(nil); err != nil || result != (PushResult{}) {
		t.Fatalf("nil Packet result=%+v err=%v, want zero nil", result, err)
	}
	if result, err := push.Frame(nil); err != nil || result != (PushResult{}) {
		t.Fatalf("nil Frame result=%+v err=%v, want zero nil", result, err)
	}
	if len(delivery.messages) != 0 {
		t.Fatalf("nil pushes emitted %d messages", len(delivery.messages))
	}

	packet := &av.Packet{Type: av.MediaAudio}
	result, err := push.Packet(packet)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || !result.Dropped {
		t.Fatalf("packet result=%+v, want accepted and dropped from DeliveryEmitter", result)
	}
	if packet.StreamID != "declared" ||
		delivery.messages[0].Kind != pipeline.MessagePacket ||
		delivery.messages[0].Packet.StreamID != "declared" {
		t.Fatalf("packet defaulting/emission = %+v, message=%+v", packet, delivery.messages[0])
	}

	frame := &av.Frame{Type: av.MediaAudio}
	if _, err := push.Frame(frame); err != nil {
		t.Fatal(err)
	}
	if frame.StreamID != "declared" ||
		delivery.messages[1].Kind != pipeline.MessageFrame ||
		delivery.messages[1].Frame.StreamID != "declared" {
		t.Fatalf("frame defaulting/emission = %+v, message=%+v", frame, delivery.messages[1])
	}

	if _, err := push.Event(av.Event{Type: av.EventStreamAdded, Stream: &av.Stream{ID: "announced"}}); err != nil {
		t.Fatal(err)
	}
	if got := delivery.messages[2].Event.StreamID; got != "announced" {
		t.Fatalf("stream announce event StreamID = %q, want announced", got)
	}
	if _, err := push.Event(av.Event{Type: av.EventStats}); err != nil {
		t.Fatal(err)
	}
	if got := delivery.messages[3].Event.StreamID; got != "declared" {
		t.Fatalf("default event StreamID = %q, want declared", got)
	}
	if err := push.EOS(); err != nil {
		t.Fatal(err)
	}
	if got := delivery.messages[4].Event.StreamID; got != "declared" {
		t.Fatalf("default EOS StreamID = %q, want declared", got)
	}
	if err := push.EOS("other"); err != nil {
		t.Fatal(err)
	}
	if got := delivery.messages[5].Event.StreamID; got != "other" {
		t.Fatalf("explicit EOS StreamID = %q, want other", got)
	}

	plain := &componentEmitter{}
	plainPush := sourcepkg.NewPush(ctx, plain, "plain")
	result, err = plainPush.Packet(&av.Packet{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || result.Dropped {
		t.Fatalf("plain emitter result=%+v, want accepted without dropped", result)
	}
}

func TestCustomSourceShapeDeclaresCodecFactsAndLifecycle(t *testing.T) {
	var ran bool
	input := Source("mic",
		shape.Packet(av.MediaAudio, av.CodecOpus,
			shape.Audio(48_000, codec.Mono, ""),
			shape.Realtime(true),
		),
		func(context.Context, SourcePush) error {
			ran = true
			return nil
		},
	)

	spec, ok := customSourceShape(input)
	if !ok {
		t.Fatal("customSourceShape did not recognize Source input")
	}
	if spec.Domain != shape.DomainPacket || spec.StreamID != "mic" || !spec.Realtime {
		t.Fatalf("normalized source shape = %+v", spec)
	}
	streams := customSourceStreams(input)
	if len(streams) != 1 {
		t.Fatalf("streams = %d, want 1", len(streams))
	}
	stream := streams[0]
	if stream.ID != "mic" ||
		stream.Codec.ID != av.CodecOpus ||
		stream.Codec.SampleRate != 48000 ||
		stream.Codec.Channels != codec.Mono ||
		stream.Codec.ChannelLayout != "mono" ||
		stream.Codec.ClockRate != 48000 {
		t.Fatalf("stream from source shape = %+v", stream)
	}
	probe := customSourceProbeResult(input)
	if probe.Score != 100 || probe.Reason == "" || !reflect.DeepEqual(probe.Streams, streams) {
		t.Fatalf("probe = %+v, streams=%+v", probe, streams)
	}

	source, opened, err := newCustomSource(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(opened, streams) {
		t.Fatalf("opened streams = %+v, want %+v", opened, streams)
	}
	describer, ok := source.(pipeline.NodeDescriber)
	if !ok {
		t.Fatalf("source = %T, want NodeDescriber", source)
	}
	if source.Name() != "mic" || !strings.Contains(describer.DescribeNode().Detail, "custom source") {
		t.Fatalf("source node = %q %+v", source.Name(), describer.DescribeNode())
	}
	if err := source.Start(context.Background(), &componentEmitter{}); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("custom source callback did not run")
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Start(context.Background(), &componentEmitter{}); !errors.Is(err, pipeline.ErrClosed) {
		t.Fatalf("Start after Close err = %v, want ErrClosed", err)
	}

	nilSource, _, err := newCustomSource(Source("nil-fn", shape.Event(), nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := nilSource.Start(context.Background(), &componentEmitter{}); !errors.Is(err, errNilSource) {
		t.Fatalf("nil callback Start err = %v, want errNilSource", err)
	}
	if _, _, err := newCustomSource(InputSpec{}); !errors.Is(err, errNilSource) {
		t.Fatalf("empty custom source err = %v, want errNilSource", err)
	}
	if streams := customSourceStreams(InputSpec{}); streams != nil {
		t.Fatalf("customSourceStreams(empty) = %+v, want nil", streams)
	}
	if got, ok := compileStateCustomSourceShape(nil); ok || got != (shape.Spec{}) {
		t.Fatalf("compileStateCustomSourceShape(nil) = %+v %v", got, ok)
	}

	rawDefault := normalizeCustomSourceShape("raw", shape.Spec{MediaKind: av.MediaAudio})
	if rawDefault.Domain != shape.DomainPacket || rawDefault.StreamID != "raw" {
		t.Fatalf("raw default shape = %+v", rawDefault)
	}
	videoDefault := normalizeCustomSourceShape("", shape.Frame(av.MediaVideo))
	if videoDefault.StreamID != "video" {
		t.Fatalf("video default stream = %q, want video", videoDefault.StreamID)
	}
	eventDefault := normalizeCustomSourceShape("", shape.Event())
	if eventDefault.StreamID != "stream" {
		t.Fatalf("event default stream = %q, want stream", eventDefault.StreamID)
	}
	mismatched := av.Stream{Codec: av.CodecParameters{ID: av.CodecOpus, SampleRate: 48000}}
	fillStreamCodecParameters(&mismatched, codec.VP8(codec.ClockRate(90000)))
	if mismatched.Codec.ID != av.CodecOpus || mismatched.Codec.ClockRate != 0 {
		t.Fatalf("mismatched codec facts changed stream: %+v", mismatched.Codec)
	}
	if got := customSourceNodeName(InputSpec{name: "named"}); got != "named" {
		t.Fatalf("customSourceNodeName without source = %q, want named", got)
	}
}

func TestInputStreamAnchorCarriesRuntimeBranchShape(t *testing.T) {
	input := Source("room",
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Mono, av.SampleFormatS16)),
		func(context.Context, SourcePush) error { return nil },
	)
	stream := av.Stream{
		ID:   "host",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			Type:         av.MediaAudio,
			SampleRate:   48_000,
			Channels:     codec.Mono,
			SampleFormat: av.SampleFormatS16,
		},
	}

	anchor := input.Stream(stream)
	if anchor.Name() != "room" {
		t.Fatalf("anchor name = %q, want room", anchor.Name())
	}
	source := anchor.branchSource()
	if source.from != "room" ||
		source.policy != pipeline.RouteByStream ||
		source.label != "host" ||
		source.stream == nil ||
		source.stream.ID != "host" ||
		source.streamDomain != shape.DomainFrame {
		t.Fatalf("branch source = %+v", source)
	}
	tap := discoveredStreamAnchorTap(source)
	if tap.Node != "room" ||
		tap.Domain != shape.DomainFrame ||
		tap.MediaKind != av.MediaAudio ||
		tap.Shape.StreamID != "host" ||
		tap.Shape.SampleRate != 48_000 ||
		tap.Shape.Channels != codec.Mono ||
		tap.Shape.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("anchor tap = %+v", tap)
	}
}

func TestInputStreamAnchorRequiresStreamID(t *testing.T) {
	input := Source("room",
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Mono, av.SampleFormatS16)),
		func(context.Context, SourcePush) error { return nil },
	)
	spec := Branch("bad").
		From(input.Stream(av.Stream{Type: av.MediaAudio, Codec: av.CodecParameters{Type: av.MediaAudio}})).
		To(Sink(SinkFunc("discard", func(context.Context, Message) error { return nil })))

	err := validateAttachBranchSpec(spec, nil)
	if err == nil || !strings.Contains(err.Error(), "stream id is empty") {
		t.Fatalf("validateAttachBranchSpec err = %v, want empty stream id guidance", err)
	}
}

func TestEmitHelpersNilAndUnscopedEOS(t *testing.T) {
	emitter := &componentEmitter{}
	stage := PacketFunc("", func(_ context.Context, packet *av.Packet, emit Emit) error {
		if err := emit.Packet(nil); err != nil {
			t.Fatalf("Packet(nil): %v", err)
		}
		if err := emit.Frame(nil); err != nil {
			t.Fatalf("Frame(nil): %v", err)
		}
		if len(emitter.messages) != 0 {
			t.Fatalf("nil emits produced %d messages", len(emitter.messages))
		}
		return emit.EOS()
	})
	if stage.Name() != "func" {
		t.Fatalf("default stage name = %q, want func", stage.Name())
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{}}, emitter); err != nil {
		t.Fatal(err)
	}
	if len(emitter.messages) != 1 ||
		emitter.messages[0].Kind != pipeline.MessageEvent ||
		emitter.messages[0].Event.Type != av.EventEndOfStream ||
		emitter.messages[0].Event.StreamID != "" {
		t.Fatalf("unscoped EOS message = %+v", emitter.messages)
	}

	if err := stage.Handle(context.Background(), nil, emitter); err != nil {
		t.Fatal(err)
	}
	sink := SinkFunc("", func(context.Context, Message) error { return nil })
	if sink.Name() != "sink" {
		t.Fatalf("default sink name = %q, want sink", sink.Name())
	}
	if err := sink.Handle(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestEmitMessagesAreIndependentWhenEmitterRetainsPointers(t *testing.T) {
	retained := &componentRetainingEmitter{}
	packet := &av.Packet{StreamID: "packet", Type: av.MediaAudio}
	frame := &av.Frame{StreamID: "frame", Type: av.MediaAudio}
	event := av.Event{Type: av.EventStats, StreamID: "event"}
	stage := PacketFunc("retain", func(_ context.Context, packet *av.Packet, emit Emit) error {
		if err := emit.Packet(packet); err != nil {
			return err
		}
		if err := emit.Frame(frame); err != nil {
			return err
		}
		return emit.Event(event)
	})

	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket, Packet: packet}, retained); err != nil {
		t.Fatal(err)
	}

	if len(retained.messages) != 3 {
		t.Fatalf("retained messages = %d, want 3", len(retained.messages))
	}
	if retained.messages[0] == retained.messages[1] ||
		retained.messages[0] == retained.messages[2] ||
		retained.messages[1] == retained.messages[2] {
		t.Fatalf("retained messages share storage: %p %p %p", retained.messages[0], retained.messages[1], retained.messages[2])
	}
	if retained.messages[0].Kind != pipeline.MessagePacket ||
		retained.messages[0].Packet != packet ||
		retained.messages[0].Frame != nil ||
		retained.messages[0].Event != nil {
		t.Fatalf("first retained message mutated: %+v", retained.messages[0])
	}
	if retained.messages[1].Kind != pipeline.MessageFrame ||
		retained.messages[1].Frame != frame ||
		retained.messages[1].Packet != nil ||
		retained.messages[1].Event != nil {
		t.Fatalf("second retained message mutated: %+v", retained.messages[1])
	}
	if retained.messages[2].Kind != pipeline.MessageEvent ||
		retained.messages[2].Event == nil ||
		retained.messages[2].Event.Type != av.EventStats ||
		retained.messages[2].Event.StreamID != "event" ||
		retained.messages[2].Packet != nil ||
		retained.messages[2].Frame != nil {
		t.Fatalf("third retained message mutated: %+v", retained.messages[2])
	}
}

func TestCustomSourceValidationErrors(t *testing.T) {
	validEvent := Source("events", shape.Event(), func(context.Context, SourcePush) error { return nil })
	if err := validEvent.validate(); err != nil {
		t.Fatalf("event-only source validate: %v", err)
	}

	for _, tt := range []struct {
		name string
		in   InputSpec
		code string
	}{
		{
			name: "nil callback",
			in:   Source("missing", shape.Packet(av.MediaAudio, av.CodecOpus), nil),
			code: "source_callback_missing",
		},
		{
			name: "unsupported domain",
			in: Source("unsupported", shape.New(shape.Domain("samples"), shape.Media(av.MediaAudio)),
				func(context.Context, SourcePush) error { return nil }),
			code: "source_shape_unsupported",
		},
		{
			name: "missing media",
			in: Source("missing-media", shape.Packet("", av.CodecOpus),
				func(context.Context, SourcePush) error { return nil }),
			code: "source_shape_invalid",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var buildErr *BuildError
			if err := tt.in.validate(); !errors.As(err, &buildErr) || string(buildErr.Code) != tt.code {
				t.Fatalf("validate err = %v, want code %s", err, tt.code)
			}
		})
	}
}

type componentWriteCloser struct {
	bytes.Buffer
	closes int
}

func (w *componentWriteCloser) Close() error {
	w.closes++
	return nil
}

type componentDestination struct {
	name     string
	contract provider.Contract
	writer   provider.Writer
	info     provider.Info
	opens    int
	err      error
}

func (d *componentDestination) Name() string { return d.name }

func (d *componentDestination) Contract() provider.Contract { return d.contract }

func (d *componentDestination) Open(_ context.Context, info provider.Info) (provider.Writer, error) {
	d.opens++
	d.info = info
	if d.err != nil {
		return nil, d.err
	}
	return d.writer, nil
}

func TestDestinationContractsAndOpeners(t *testing.T) {
	ctx := context.Background()
	custom := &componentDestination{
		name: "custom-writer",
		contract: provider.Contract{
			ByteStream: true,
			Seekable:   true,
			Realtime:   true,
			Protocol:   av.ProtocolCustom,
			Formats:    []av.FormatID{av.FormatIVF},
			MIMETypes:  []string{"video/ivf"},
		},
		writer: &componentWriteCloser{},
	}

	spec := Custom("", custom).spec
	if spec.Name() != "custom-writer" || spec.output.Name != "custom-writer" || spec.output.URI != "custom-writer" {
		t.Fatalf("custom fallback naming = name:%q output:%q uri:%q", spec.Name(), spec.output.Name, spec.output.URI)
	}
	contract := spec.Contract()
	if !contract.ByteStream || !contract.Seekable || !contract.Realtime ||
		contract.Protocol != av.ProtocolCustom ||
		!reflect.DeepEqual(contract.Formats, []av.FormatID{av.FormatIVF}) ||
		!reflect.DeepEqual(contract.MIMETypes, []string{"video/ivf"}) {
		t.Fatalf("custom contract = %+v", contract)
	}
	overridden := Custom("archive.ogg", custom, Format(av.FormatOgg), MIME("audio/ogg")).spec.Contract()
	if !reflect.DeepEqual(overridden.Formats, []av.FormatID{av.FormatOgg}) ||
		!reflect.DeepEqual(overridden.MIMETypes, []string{"audio/ogg"}) ||
		overridden.Protocol != av.ProtocolCustom ||
		!overridden.Seekable ||
		!overridden.Realtime {
		t.Fatalf("overridden custom contract = %+v", overridden)
	}
	if err := customDestination("broken", nil).validate("record", "fallback"); !errors.Is(err, errNilWriter) {
		t.Fatalf("nil custom validate err = %v, want errNilWriter", err)
	}
	if err := Writer("nil.ogg", nil).spec.validate("record", "fallback"); !errors.Is(err, errNilWriter) {
		t.Fatalf("nil writer validate err = %v, want errNilWriter", err)
	}

	var plain bytes.Buffer
	plainWriter, err := Write("plain.ogg", &plain).spec.Open(ctx, provider.Info{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plainWriter.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := plainWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plain.Bytes(), []byte{1, 2, 3}) {
		t.Fatalf("plain writer bytes = %v", plain.Bytes())
	}

	closer := &componentWriteCloser{}
	openedCloser, err := Write("closer.ogg", closer).spec.Open(ctx, provider.Info{})
	if err != nil {
		t.Fatal(err)
	}
	if openedCloser != closer {
		t.Fatalf("opened closer = %T, want original closer", openedCloser)
	}
	if err := openedCloser.Close(); err != nil {
		t.Fatal(err)
	}
	if closer.closes != 1 {
		t.Fatalf("closer closes = %d, want 1", closer.closes)
	}

	if _, err := (destinationSpec{}).Open(ctx, provider.Info{Name: "empty"}); err == nil {
		t.Fatal("empty destination Open succeeded")
	}

	boom := errors.New("open failed")
	writerCases := []struct {
		name string
		dest Destination
		want error
	}{
		{name: "nil callback", dest: Writer("nil.ogg", nil), want: errNilWriter},
		{name: "callback error", dest: Writer("err.ogg", func(context.Context, provider.Info) (io.WriteCloser, error) {
			return nil, boom
		}), want: boom},
		{name: "nil writer", dest: Writer("nil-writer.ogg", func(context.Context, provider.Info) (io.WriteCloser, error) {
			return nil, nil
		}), want: errNilWriter},
	}
	for _, tt := range writerCases {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.dest.spec.Open(ctx, provider.Info{Name: tt.name}); !errors.Is(err, tt.want) {
				t.Fatalf("Open err = %v, want %v", err, tt.want)
			}
		})
	}

	success := &componentWriteCloser{}
	writerSpec := Writer("upload.ogg", func(context.Context, provider.Info) (io.WriteCloser, error) {
		return success, nil
	}, Format(av.FormatOgg), MIME("audio/ogg")).spec
	if writerSpec.custom.Name() != "upload.ogg" {
		t.Fatalf("writer destination name = %q", writerSpec.custom.Name())
	}
	writerContract := writerSpec.Contract()
	if !writerContract.ByteStream ||
		!reflect.DeepEqual(writerContract.Formats, []av.FormatID{av.FormatOgg}) ||
		!reflect.DeepEqual(writerContract.MIMETypes, []string{"audio/ogg"}) {
		t.Fatalf("writer contract = %+v", writerContract)
	}
	opened, err := writerSpec.Open(ctx, provider.Info{Name: "upload.ogg"})
	if err != nil {
		t.Fatal(err)
	}
	if opened != success {
		t.Fatalf("opened writer = %T, want callback writer", opened)
	}

	fallbackContract := (destinationSpec{custom: custom}).Contract()
	if fallbackContract.Protocol != av.ProtocolCustom ||
		!reflect.DeepEqual(fallbackContract.Formats, []av.FormatID{av.FormatIVF}) ||
		!reflect.DeepEqual(fallbackContract.MIMETypes, []string{"video/ivf"}) {
		t.Fatalf("fallback custom contract = %+v", fallbackContract)
	}
	resolvedContract := (destinationSpec{resolvedFormat: av.FormatWebM}).Contract()
	if !reflect.DeepEqual(resolvedContract.Formats, []av.FormatID{av.FormatWebM}) {
		t.Fatalf("resolved-only contract = %+v", resolvedContract)
	}
}

func TestDestinationOriginContracts(t *testing.T) {
	typ := reflect.TypeOf(Destination{})
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).IsExported() {
			t.Fatalf("Destination field %s is exported; use constructors instead", typ.Field(i).Name)
		}
	}

	var zero Destination
	if zero.origin != destinationOriginZero {
		t.Fatalf("zero Destination origin = %v, want zero", zero.origin)
	}
	if _, err := destinationSpecFromDestination(zero); err == nil ||
		!strings.Contains(err.Error(), "destination is empty") ||
		!strings.Contains(err.Error(), "goav.Write") {
		t.Fatalf("zero Destination error = %v, want constructor guidance", err)
	}
	withOptions := zero.With(Format(av.FormatOgg), Name("archive.ogg"))
	if withOptions.origin != destinationOriginZero {
		t.Fatalf("zero Destination.With origin = %v, want zero", withOptions.origin)
	}
	if _, err := destinationSpecFromDestination(withOptions); err == nil ||
		!strings.Contains(err.Error(), "destination is empty") {
		t.Fatalf("zero Destination.With error = %v, want unconstructed refusal", err)
	}

	custom := &componentDestination{name: "custom", writer: &componentWriteCloser{}}
	writer := func(context.Context, provider.Info) (io.WriteCloser, error) {
		return &componentWriteCloser{}, nil
	}
	for name, destination := range map[string]Destination{
		"write":  Write("out.ogg", io.Discard),
		"uri":    URI("file:///tmp/out.ogg"),
		"sink":   Sink(SinkFunc("frames", func(context.Context, Message) error { return nil })),
		"custom": Custom("custom", custom),
		"writer": Writer("writer.ogg", writer),
		"mux":    Mux("archive", Write("archive.ogg", io.Discard)),
	} {
		if destination.origin != destinationOriginConstructed {
			t.Fatalf("%s destination origin = %v, want constructed", name, destination.origin)
		}
		if _, err := destinationSpecFromDestination(destination); err != nil {
			t.Fatalf("%s destination spec error = %v", name, err)
		}
	}
}

func TestOpenDestinationOutputPassesClonedProviderInfo(t *testing.T) {
	ctx := context.Background()
	writer := &componentWriteCloser{}
	custom := &componentDestination{
		name: "object",
		contract: provider.Contract{
			ByteStream: true,
			Realtime:   true,
			Formats:    []av.FormatID{av.FormatOgg},
		},
		writer: writer,
	}
	metadata := av.Metadata{"kind": "fixture"}
	streams := []av.Stream{{
		ID:       "voice",
		Type:     av.MediaAudio,
		Metadata: av.Metadata{"language": "en"},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			Attributes: av.Metadata{"mode": "speech"},
			ExtraData:  av.Buffer{Bytes: []byte{1, 2, 3}, Ownership: av.BufferImmutable},
		},
	}}
	builder := newTestBuilder(t,
		WithRealtime(false),
		WithProber(format.NewStaticProber(format.ProbeRule{
			Format:     av.FormatOgg,
			Extensions: []string{".ogg"},
			Score:      100,
			Reason:     "test output extension",
		})),
	)
	output, opened, err := builder.openDestinationOutput(ctx,
		Custom("mem://object.ogg", custom, MIME("audio/ogg"), Metadata(metadata)).spec,
		streams,
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if opened != writer || output.Writer != writer {
		t.Fatalf("opened writer=%T output writer=%T, want callback writer", opened, output.Writer)
	}
	if custom.opens != 1 {
		t.Fatalf("opens = %d, want 1", custom.opens)
	}
	if custom.info.Name != "mem://object.ogg" ||
		custom.info.Format != av.FormatOgg ||
		custom.info.MIMEType != "audio/ogg" ||
		!custom.info.Realtime ||
		custom.info.Metadata["kind"] != "fixture" ||
		len(custom.info.Streams) != 1 ||
		custom.info.Streams[0].ID != "voice" ||
		custom.info.Streams[0].Metadata["language"] != "en" ||
		custom.info.Streams[0].Codec.Attributes["mode"] != "speech" ||
		!bytes.Equal(custom.info.Streams[0].Codec.ExtraData.Bytes, []byte{1, 2, 3}) {
		t.Fatalf("provider info = %+v", custom.info)
	}

	metadata["kind"] = "mutated"
	streams[0].Metadata["language"] = "fr"
	streams[0].Codec.Attributes["mode"] = "music"
	streams[0].Codec.ExtraData.Bytes[0] = 9
	if custom.info.Metadata["kind"] != "fixture" ||
		custom.info.Streams[0].Metadata["language"] != "en" ||
		custom.info.Streams[0].Codec.Attributes["mode"] != "speech" ||
		!bytes.Equal(custom.info.Streams[0].Codec.ExtraData.Bytes, []byte{1, 2, 3}) {
		t.Fatalf("provider info was not cloned defensively: %+v", custom.info)
	}

	var plain bytes.Buffer
	plainOutput, plainOpened, err := builder.openDestinationOutput(ctx, Write("plain.ogg", &plain).spec, nil, av.FormatOgg)
	if err != nil {
		t.Fatal(err)
	}
	if plainOutput.Writer != &plain || plainOpened != nil {
		t.Fatalf("plain file output writer=%T opened=%T, want caller writer and nil close hook", plainOutput.Writer, plainOpened)
	}
	closer := &componentWriteCloser{}
	_, closeHook, err := builder.openDestinationOutput(ctx, Write("closer.ogg", closer).spec, nil, av.FormatOgg)
	if err != nil {
		t.Fatal(err)
	}
	if closeHook != closer {
		t.Fatalf("close hook = %T, want original write closer", closeHook)
	}
	uriOutput, uriWriter, err := builder.openDestinationOutput(ctx, URI("mem://plain.ogg").spec, nil, av.FormatOgg)
	if err != nil {
		t.Fatal(err)
	}
	if uriOutput.URI != "mem://plain.ogg" || uriWriter != nil {
		t.Fatalf("uri output=%+v writer=%T, want URI with nil writer", uriOutput, uriWriter)
	}

	openErr := errors.New("destination open failed")
	errorDestination := &componentDestination{err: openErr}
	if _, _, err := builder.openDestinationOutput(ctx, Custom("err.ogg", errorDestination).spec, nil, av.FormatOgg); !errors.Is(err, openErr) {
		t.Fatalf("custom open err = %v, want openErr", err)
	}
	nilDestination := &componentDestination{}
	if _, _, err := builder.openDestinationOutput(ctx, Custom("nil.ogg", nilDestination).spec, nil, av.FormatOgg); !errors.Is(err, errNilWriter) {
		t.Fatalf("custom nil writer err = %v, want errNilWriter", err)
	}
}

type componentMuxer struct {
	writeErr error
	closeErr error
	writes   int
	closes   int
}

func (m *componentMuxer) Format() av.FormatID { return av.FormatOgg }

func (m *componentMuxer) Open(context.Context, format.Output, []av.Stream, format.OpenOptions) error {
	return nil
}

func (m *componentMuxer) Write(context.Context, *av.Packet, *format.WriteResult) error {
	m.writes++
	return m.writeErr
}

func (m *componentMuxer) Close() error {
	m.closes++
	return m.closeErr
}

type componentTransactionalWriter struct {
	componentWriteCloser
	commits   int
	aborts    int
	commitErr error
	abortErr  error
}

func (w *componentTransactionalWriter) Commit(context.Context) error {
	w.commits++
	return w.commitErr
}

func (w *componentTransactionalWriter) Abort(context.Context) error {
	w.aborts++
	return w.abortErr
}

func TestDestinationWriterMuxerTransactionPaths(t *testing.T) {
	success := &componentTransactionalWriter{}
	successMux := &destinationWriterMuxer{
		Muxer:       &componentMuxer{},
		writer:      success,
		transaction: &destinationTransaction{},
	}
	if err := successMux.Close(); err != nil {
		t.Fatal(err)
	}
	if success.commits != 1 || success.aborts != 0 || success.closes != 1 {
		t.Fatalf("success transaction commits=%d aborts=%d closes=%d", success.commits, success.aborts, success.closes)
	}

	boom := errors.New("write failed")
	failed := &componentTransactionalWriter{}
	failedMux := &destinationWriterMuxer{
		Muxer:       &componentMuxer{writeErr: boom},
		writer:      failed,
		transaction: &destinationTransaction{},
	}
	if err := failedMux.Write(context.Background(), &av.Packet{}, &format.WriteResult{}); !errors.Is(err, boom) {
		t.Fatalf("Write err = %v, want boom", err)
	}
	if err := failedMux.Close(); err != nil {
		t.Fatal(err)
	}
	if failed.commits != 0 || failed.aborts != 1 || failed.closes != 1 {
		t.Fatalf("failed transaction commits=%d aborts=%d closes=%d", failed.commits, failed.aborts, failed.closes)
	}

	closeErr := errors.New("mux close failed")
	closeFailed := &componentTransactionalWriter{}
	closeFailedMux := &destinationWriterMuxer{
		Muxer:       &componentMuxer{closeErr: closeErr},
		writer:      closeFailed,
		transaction: &destinationTransaction{},
	}
	if err := closeFailedMux.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close err = %v, want mux close error", err)
	}
	if closeFailed.commits != 0 || closeFailed.aborts != 1 || closeFailed.closes != 1 {
		t.Fatalf("close-failed transaction commits=%d aborts=%d closes=%d", closeFailed.commits, closeFailed.aborts, closeFailed.closes)
	}
}

type componentControllableSource struct {
	name     string
	controls int
}

func (s *componentControllableSource) Name() string { return s.name }

func (s *componentControllableSource) Start(context.Context, pipeline.Emitter) error { return nil }

func (s *componentControllableSource) Close() error { return nil }

func (s *componentControllableSource) Control(context.Context, *pipeline.Message) error {
	s.controls++
	return nil
}

type componentControlWrapper struct {
	inner *componentControllableSource
}

func (w componentControlWrapper) Name() string { return "decorated" }

func (w componentControlWrapper) Start(ctx context.Context, emitter pipeline.Emitter) error {
	return w.inner.Start(ctx, emitter)
}

func (w componentControlWrapper) Close() error { return w.inner.Close() }

func (w componentControlWrapper) Control(ctx context.Context, msg *pipeline.Message) error {
	return w.inner.Control(ctx, msg)
}

func TestApplySourceWrapsPinsIdentityAndPreservesDelegatedControls(t *testing.T) {
	source := &componentControllableSource{name: "source"}
	input := InputSpec{wraps: []func(pipeline.Source) pipeline.Source{
		nil,
		func(pipeline.Source) pipeline.Source { return nil },
		func(pipeline.Source) pipeline.Source {
			return componentControlWrapper{inner: source}
		},
	}}

	wrapped := input.applySourceWraps(source)
	if wrapped.Name() != "source" {
		t.Fatalf("wrapped source name = %q, want source", wrapped.Name())
	}
	controllable, ok := wrapped.(pipeline.ControllableSource)
	if !ok {
		t.Fatalf("wrapped source = %T, want controllable source", wrapped)
	}
	if err := controllable.Control(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent}); err != nil {
		t.Fatal(err)
	}
	if source.controls != 1 {
		t.Fatalf("delegated controls = %d, want 1", source.controls)
	}
}

type componentSourceProvider struct {
	name    string
	detail  string
	source  pipeline.Source
	streams []av.Stream
	spec    shape.Spec
	bounds  codec.DecodeBounds
	err     error
}

func (p *componentSourceProvider) Name() string { return p.name }

func (p *componentSourceProvider) Detail() string { return p.detail }

func (p *componentSourceProvider) SourceShape() shape.Spec { return p.spec }

func (p *componentSourceProvider) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	if p.err != nil {
		return nil, nil, p.err
	}
	return p.source, append([]av.Stream(nil), p.streams...), nil
}

func (p *componentSourceProvider) DecodeBounds(av.StreamID) codec.DecodeBounds {
	return p.bounds
}

func TestInputAndProviderSourceContracts(t *testing.T) {
	typ := reflect.TypeOf(InputSpec{})
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).IsExported() {
			t.Fatalf("InputSpec field %s is exported; use constructors instead", typ.Field(i).Name)
		}
	}

	var zero InputSpec
	if zero.origin != inputSpecOriginZero {
		t.Fatalf("zero InputSpec origin = %v, want zero", zero.origin)
	}
	renamedZero := zero.With(Name("input"))
	if renamedZero.origin != inputSpecOriginZero {
		t.Fatalf("zero InputSpec.With origin = %v, want zero", renamedZero.origin)
	}
	if err := renamedZero.validate(); err == nil ||
		!strings.Contains(err.Error(), "empty input spec") ||
		!strings.Contains(err.Error(), "goav.FileInput") {
		t.Fatalf("zero InputSpec.With error = %v, want constructor guidance", err)
	}
	wrappedZero := WrapSource(zero, func(source pipeline.Source) pipeline.Source { return source })
	if wrappedZero.origin != inputSpecOriginZero {
		t.Fatalf("zero WrapSource origin = %v, want zero", wrappedZero.origin)
	}

	metadata := av.Metadata{"transport": "custom"}
	uri := URIInput("custom://input", MIME("application/x-custom"), Metadata(metadata))
	if uri.origin != inputSpecOriginConstructed {
		t.Fatalf("URIInput origin = %v, want constructed", uri.origin)
	}
	if uri.input.URI != "custom://input" ||
		uri.input.Name != "custom://input" ||
		uri.input.MIMEType != "application/x-custom" ||
		uri.input.Metadata["transport"] != "custom" {
		t.Fatalf("URIInput = %+v", uri.input)
	}
	metadata["transport"] = "mutated"
	if uri.input.Metadata["transport"] != "custom" {
		t.Fatalf("URIInput metadata was not cloned: %+v", uri.input.Metadata)
	}
	if wrapped := WrapSource(uri, nil); len(wrapped.wraps) != 0 {
		t.Fatalf("WrapSource nil added wraps: %+v", wrapped.wraps)
	}
	for name, input := range map[string]InputSpec{
		"file":     FileInput("input.ogg", strings.NewReader("")),
		"uri":      uri,
		"source":   Source("generated", shape.Packet(av.MediaAudio, av.CodecOpus), func(context.Context, sourcepkg.Push) error { return nil }),
		"provider": Input(&componentSourceProvider{spec: shape.Packet(av.MediaAudio, av.CodecOpus)}),
	} {
		if input.origin != inputSpecOriginConstructed {
			t.Fatalf("%s input origin = %v, want constructed", name, input.origin)
		}
	}
	nilProvider := Input(nil)
	if nilProvider.origin != inputSpecOriginConstructed {
		t.Fatalf("nil provider input origin = %v, want constructed", nilProvider.origin)
	}
	if err := nilProvider.validate(); !errors.Is(err, errNilSource) {
		t.Fatalf("nil provider validate error = %v, want errNilSource", err)
	}

	if _, err := openProviderSource(context.Background(), nil, "nil"); !errors.Is(err, errNilSource) {
		t.Fatalf("nil provider err = %v, want errNilSource", err)
	}
	openErr := errors.New("open source failed")
	if _, err := openProviderSource(context.Background(), &componentSourceProvider{err: openErr}, "broken"); !errors.Is(err, openErr) {
		t.Fatalf("provider open err = %v, want openErr", err)
	}

	providerSource := &componentControllableSource{name: "opened"}
	stream := av.Stream{ID: "voice", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}}
	provider := &componentSourceProvider{
		name:    "",
		detail:  "custom receive",
		source:  providerSource,
		streams: []av.Stream{stream},
		spec:    shape.Spec{MediaKind: av.MediaAudio, Codec: av.CodecOpus, Realtime: true},
		bounds:  codec.DecodeBounds{MaxFramesPerInput: 1, MaxPayloadBytes: 128},
	}
	if got := providerNodeName(provider); got != "source" {
		t.Fatalf("providerNodeName empty = %q, want source", got)
	}
	build, err := openProviderSource(context.Background(), provider, "renamed")
	if err != nil {
		t.Fatal(err)
	}
	if build.source.Name() != "renamed" ||
		build.domain != shape.DomainPacket ||
		!build.realtime ||
		len(build.streams) != 1 ||
		build.streams[0].ID != "voice" ||
		build.bounds == nil {
		t.Fatalf("provider build = %+v", build)
	}
	if _, ok := build.source.(pipeline.ControllableSource); !ok {
		t.Fatalf("renamed provider source = %T, want controllable capability", build.source)
	}

	merged := providerDecodeBoundsForStream(stream, []decodeBoundsProvider{
		&componentSourceProvider{bounds: codec.DecodeBounds{MaxFramesPerInput: 1, MaxEventsPerInput: 2, MaxRequestsPerInput: 3}},
		&componentSourceProvider{bounds: codec.DecodeBounds{MaxPayloadBytes: 4, MaxRetainedBytes: 5, MaxWidth: 6, MaxHeight: 7}},
	})
	if merged.MaxFramesPerInput != 1 ||
		merged.MaxEventsPerInput != 2 ||
		merged.MaxRequestsPerInput != 3 ||
		merged.MaxPayloadBytes != 4 ||
		merged.MaxRetainedBytes != 5 ||
		merged.MaxWidth != 6 ||
		merged.MaxHeight != 7 {
		t.Fatalf("merged decode bounds = %+v", merged)
	}
}

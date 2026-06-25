package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
)

// TestProviderRTPDescribePinsLegacyConstructorStrings pins the Describe()
// node name and detail strings the deleted goav.RTP constructor path produced,
// so an RTP job through the provider seam stays byte-identical: the node is
// named by rtpav.WithName and the detail is the rtpav receive summary.
func TestProviderRTPDescribePinsLegacyConstructorStrings(t *testing.T) {
	job := goav.From(goav.Input(rtpav.Receive(&runtimeRTPReceiver{
		streams: []av.Stream{{
			ID:   "audio",
			Type: av.MediaAudio,
			Codec: av.CodecParameters{
				ID:   av.CodecOpus,
				Type: av.MediaAudio,
			},
		}},
	},
		rtpav.WithName("audio"),
		rtpav.WithCodec(codec.Opus()),
		rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}),
		rtpav.WithMaxTimestampGap(av.SamplesDuration(960, 48000)),
	))).Copy().To(goav.File("recording.ogg", io.Discard, goav.Format(av.FormatOgg)))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	var source *pipeline.NodeSpec
	for i := range spec.Nodes {
		if spec.Nodes[i].Kind == pipeline.NodeSource {
			source = &spec.Nodes[i]
		}
	}
	if source == nil {
		t.Fatalf("spec has no source node:\n%s", specText(spec))
	}
	if source.Name != "audio" {
		t.Fatalf("source name = %q, want %q (legacy goav.RTP naming)", source.Name, "audio")
	}
	if source.Detail != "rtp receive, codec=opus, timestamp gap" {
		t.Fatalf("source detail = %q, want %q", source.Detail, "rtp receive, codec=opus, timestamp gap")
	}
}

func TestRecordRecipeRTPAutoCodecRuns(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	runtime := goav.MustNew(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	))

	task, err := goav.From(
		goav.Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(goav.File("recording.ogg", io.Discard)).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 {
		t.Fatalf("muxers=%d, want 1", len(muxers.muxers))
	}
	if muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "audio" {
		t.Fatalf("writes=%d stream=%s", muxers.muxers[0].writes, muxers.muxers[0].lastStream)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecordRecipeCopyToTypedDestinationRuns(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	runtime := goav.MustNew(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	))
	job := goav.From(
		goav.Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(goav.File("recording.ogg", io.Discard, goav.Format(av.FormatOgg))).UseRuntime(runtime)

	report, err := job.Explain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Destinations) != 1 || report.Destinations[0].Name != "recording.ogg" {
		t.Fatalf("report destinations: %+v", report.Destinations)
	}
	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(planned)
	if !strings.Contains(text, "audio -> recording.ogg") {
		t.Fatalf("planned:\n%s", text)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 {
		t.Fatalf("muxers=%d, want 1", len(muxers.muxers))
	}
	if muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "audio" {
		t.Fatalf("writes=%d stream=%s", muxers.muxers[0].writes, muxers.muxers[0].lastStream)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecordRecipeCopyToCustomWriterDestinationRuns(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{7, 8, 9},
		}},
		events: make(chan av.Event),
	}
	state := &writerDestinationState{}
	muxer := &writerDestinationMuxer{}
	runtime := goav.MustNew(withTestFormats(
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))
	target := goav.Writer("s3://bucket/call.ogg", func(_ context.Context, info provider.Info) (io.WriteCloser, error) {
		state.opens++
		state.info = info
		return &writerDestinationWriteCloser{state: state}, nil
	}, goav.Format(av.FormatOgg), goav.MIME("audio/ogg"))

	task, err := goav.From(
		goav.Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(target).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.opens != 1 ||
		state.info.Name != "s3://bucket/call.ogg" ||
		state.info.Format != av.FormatOgg ||
		state.info.MIMEType != "audio/ogg" ||
		len(state.info.Streams) != 1 ||
		state.info.Streams[0].ID != "audio" {
		t.Fatalf("destination info: opens=%d info=%+v", state.opens, state.info)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state.bytes.Bytes(), []byte{7, 8, 9}) {
		t.Fatalf("written bytes = %v", state.bytes.Bytes())
	}
	if muxer.writes != 1 {
		t.Fatalf("muxer writes = %d, want 1", muxer.writes)
	}
	if state.closes != 0 {
		t.Fatalf("writer closed before task close: %d", state.closes)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if state.closes != 1 {
		t.Fatalf("writer closes = %d, want 1", state.closes)
	}
	if state.commits != 1 || state.aborts != 0 {
		t.Fatalf("commits=%d aborts=%d, want commit only", state.commits, state.aborts)
	}
}

// TestRecordRecipeCopyToTransactionalWriterDestinationRuns pins the object-store
// capability through the one Writer constructor: an opened writer that
// implements provider.TransactionalWriter commits after success and closes
// exactly once.
func TestRecordRecipeCopyToTransactionalWriterDestinationRuns(t *testing.T) {
	ctx := context.Background()
	stream := audioOpusTestStream()
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{9, 8, 7},
		}},
		events: make(chan av.Event),
	}
	state := &writerDestinationState{}
	muxer := &writerDestinationMuxer{}
	runtime := goav.MustNew(withTestFormats(
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))
	metadata := av.Metadata{"storage": "hot"}
	target := goav.Writer("s3://bucket/object.ogg", func(_ context.Context, info provider.Info) (io.WriteCloser, error) {
		state.opens++
		state.info = info
		return &writerDestinationWriteCloser{state: state}, nil
	}, goav.Format(av.FormatOgg), goav.MIME("audio/ogg"), goav.Metadata(metadata))

	task, err := goav.From(
		goav.Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(target).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.opens != 1 ||
		state.info.Name != "s3://bucket/object.ogg" ||
		state.info.Format != av.FormatOgg ||
		state.info.MIMEType != "audio/ogg" ||
		state.info.Metadata["storage"] != "hot" ||
		len(state.info.Streams) != 1 ||
		state.info.Streams[0].ID != "audio" {
		t.Fatalf("destination info: opens=%d info=%+v", state.opens, state.info)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state.bytes.Bytes(), []byte{9, 8, 7}) {
		t.Fatalf("written bytes = %v", state.bytes.Bytes())
	}
	if muxer.writes != 1 {
		t.Fatalf("muxer writes = %d, want 1", muxer.writes)
	}
	if state.closes != 1 || state.commits != 1 || state.aborts != 0 {
		t.Fatalf("closes=%d commits=%d aborts=%d, want one close and commit", state.closes, state.commits, state.aborts)
	}
}

// TestFileDestinationClosesCloserWriterOnce pins the goav.File close contract:
// a writer that also implements io.Closer is closed exactly once when the
// destination finalizes, while plain writers stay the caller's to close.
func TestFileDestinationClosesCloserWriterOnce(t *testing.T) {
	ctx := context.Background()
	stream := audioOpusTestStream()
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{4, 5, 6},
		}},
		events: make(chan av.Event),
	}
	muxer := &writerDestinationMuxer{}
	runtime := goav.MustNew(withTestFormats(
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))
	writer := &fileDestinationWriteCloser{}

	task, err := goav.From(
		goav.Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(goav.File("call.ogg", writer, goav.Format(av.FormatOgg))).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if writer.closes != 0 {
		t.Fatalf("writer closed before task close: %d", writer.closes)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(writer.bytes.Bytes(), []byte{4, 5, 6}) {
		t.Fatalf("written bytes = %v", writer.bytes.Bytes())
	}
	if writer.closes != 1 {
		t.Fatalf("writer closes = %d, want exactly one close", writer.closes)
	}
}

func TestRecordRecipeCustomWriterDestinationAbortsOnRunError(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	runErr := errors.New("rtp read failed")
	receiver := &runtimeRTPErrorReceiver{
		runtimeRTPReceiver: runtimeRTPReceiver{
			streams: []av.Stream{stream},
			payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
				PayloadType: 111,
				Parameters:  stream.Codec,
				MIMEType:    av.MIMEOpus,
				ClockRate:   48000,
				Channels:    codec.Stereo,
			}}),
			packets: []*rtp.Packet{{
				Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
				Payload: []byte{7, 8, 9},
			}},
			events: make(chan av.Event),
		},
		err: runErr,
	}
	state := &writerDestinationState{}
	muxer := &writerDestinationMuxer{}
	runtime := goav.MustNew(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))

	task, err := goav.From(
		goav.Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(goav.Writer(
		"s3://bucket/call.ogg",
		func(context.Context, provider.Info) (io.WriteCloser, error) {
			state.opens++
			return &writerDestinationWriteCloser{state: state}, nil
		},
		goav.Format(av.FormatOgg),
		goav.MIME("audio/ogg"),
	)).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = task.Run(ctx)
	if !errors.Is(err, runErr) {
		t.Fatalf("run err = %v, want %v", err, runErr)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if state.opens != 1 || state.closes != 1 {
		t.Fatalf("opens=%d closes=%d, want one open and close", state.opens, state.closes)
	}
	if state.commits != 0 || state.aborts != 1 {
		t.Fatalf("commits=%d aborts=%d, want abort only", state.commits, state.aborts)
	}
}

func TestTaskAttachCustomWriterDestinationRuns(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{4, 5, 6},
		}},
		events: make(chan av.Event),
	}
	state := &writerDestinationState{}
	muxer := &writerDestinationMuxer{}
	runtime := goav.MustNew(withTestFormats(
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))
	task, err := goav.From(goav.Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2})))).
		UseRuntime(runtime).
		Audio().
		Copy().
		Tap(goav.PacketTap("audio.packets")).
		To(goav.Sink(component.SinkFunc("base", func(context.Context, component.Message) error { return nil }))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writer := goav.Writer(
		"s3://bucket/late.ogg",
		func(_ context.Context, info provider.Info) (io.WriteCloser, error) {
			state.opens++
			state.info = info
			return &writerDestinationWriteCloser{state: state}, nil
		},
		goav.Format(av.FormatOgg),
		goav.MIME("audio/ogg"),
	)
	attachment, err := task.Attach(ctx,
		goav.Branch("late").
			From(goav.PacketTap("audio.packets")).
			Copy().
			To(writer),
	)
	if err != nil {
		_ = task.Close()
		t.Fatal(err)
	}
	if attachment == nil {
		_ = task.Close()
		t.Fatal("attachment = nil, want runtime attachment")
	}
	if state.opens != 1 ||
		state.info.Name != "s3://bucket/late.ogg" ||
		state.info.Format != av.FormatOgg ||
		state.info.MIMEType != "audio/ogg" ||
		len(state.info.Streams) != 1 ||
		state.info.Streams[0].ID != "audio" {
		_ = task.Close()
		t.Fatalf("destination info: opens=%d info=%+v", state.opens, state.info)
	}
	if err := task.Run(ctx); err != nil {
		_ = task.Close()
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state.bytes.Bytes(), []byte{4, 5, 6}) {
		t.Fatalf("written bytes = %v", state.bytes.Bytes())
	}
	if muxer.writes != 1 {
		t.Fatalf("muxer writes = %d, want 1", muxer.writes)
	}
	if state.closes != 1 || state.commits != 1 || state.aborts != 0 {
		t.Fatalf("closes=%d commits=%d aborts=%d, want one close and commit", state.closes, state.commits, state.aborts)
	}
}

func TestTaskAttachCustomWriterDestinationAbortsOnPatchFailure(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		events: make(chan av.Event),
	}
	state := &writerDestinationState{}
	muxer := &writerDestinationMuxer{}
	runtime := goav.MustNew(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))
	task, err := goav.From(goav.Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus())))).
		UseRuntime(runtime).
		Audio().
		Copy().
		Tap(goav.PacketTap("audio.packets")).
		To(goav.Sink(component.SinkFunc("base", func(context.Context, component.Message) error { return nil }))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	writer := goav.Writer(
		"s3://bucket/late.ogg",
		func(context.Context, provider.Info) (io.WriteCloser, error) {
			state.opens++
			return &writerDestinationWriteCloser{state: state}, nil
		},
		goav.Format(av.FormatOgg),
		goav.MIME("audio/ogg"),
	)
	_, err = task.Attach(ctx,
		goav.Branch("late").
			From(goav.PacketTap("audio.packets")).
			Do(component.PacketFunc("meter", func(_ context.Context, packet *av.Packet, emit component.Emit) error {
				return emit.Packet(packet)
			})).
			Copy().
			To(goav.Sink(component.SinkFunc("one", func(context.Context, component.Message) error { return nil }))),
		goav.Branch("late").
			From(goav.PacketTap("audio.packets")).
			Do(component.PacketFunc("meter", func(_ context.Context, packet *av.Packet, emit component.Emit) error {
				return emit.Packet(packet)
			})).
			Copy().
			To(writer),
	)
	if err == nil {
		t.Fatal("Attach succeeded, want duplicate node failure")
	}
	if !errors.Is(err, pipeline.ErrNodeExists) {
		t.Fatalf("attach err = %v, want node duplicate", err)
	}
	if state.opens != 1 || state.closes != 1 {
		t.Fatalf("opens=%d closes=%d, want one open and close", state.opens, state.closes)
	}
	if state.commits != 0 || state.aborts != 1 {
		t.Fatalf("commits=%d aborts=%d, want abort only", state.commits, state.aborts)
	}
}

func TestRecordRecipeRTPCodecUsesReaderStreamWhenUnnamed(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	runtime := goav.MustNew(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	))

	task, err := goav.From(
		goav.Input(rtpav.Receive(receiver, rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(goav.File("recording.ogg", io.Discard)).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 {
		t.Fatalf("muxers=%d, want 1", len(muxers.muxers))
	}
	if muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "audio" {
		t.Fatalf("writes=%d stream=%s", muxers.muxers[0].writes, muxers.muxers[0].lastStream)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultRecordRecipeRTPVP8Runs(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     16,
			Height:    16,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x10, 0x00, 0x11, 0x22},
		}},
		events: make(chan av.Event),
	}
	var out bytes.Buffer
	job := goav.From(
		goav.Input(rtpav.Receive(receiver, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).UseRuntime(bundle.MustNew()).Copy().To(goav.File("recording.ivf", &out))

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	stats := task.Stats()
	if stats.Messages < 2 ||
		stats.Packets != 1 ||
		stats.Events == 0 ||
		stats.EventsByType[av.EventEndOfStream] != 1 ||
		stats.Delivered < 2 ||
		stats.Dropped != 0 ||
		!stats.LastEventPresent ||
		stats.LastEvent.Type != av.EventEndOfStream {
		t.Fatalf("stats = %+v", stats)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if out.Len() <= 32 {
		t.Fatalf("output bytes=%d, want IVF header and frame", out.Len())
	}
}

func TestFromAndRecordRecipeMultipleRTPInputsRuns(t *testing.T) {
	ctx := context.Background()
	audio := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	video := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     16,
			Height:    16,
		},
	}
	audioReceiver := &runtimeRTPReceiver{
		streams: []av.Stream{audio},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  audio.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	videoReceiver := &runtimeRTPReceiver{
		streams: []av.Stream{video},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  video.Codec,
			MIMEType:    av.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x10, 0x00, 0x11, 0x22},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	runtime := goav.MustNew(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	))

	task, err := goav.From(
		goav.Input(rtpav.Receive(audioReceiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).UseRuntime(runtime).
		And(goav.Input(rtpav.Receive(videoReceiver, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2})))).
		To(goav.File("recording.ogg", io.Discard)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 {
		t.Fatalf("muxers=%d, want 1", len(muxers.muxers))
	}
	muxer := muxers.muxers[0]
	if muxer.streamCount != 2 || muxer.writes != 2 {
		t.Fatalf("streamCount=%d writes=%d", muxer.streamCount, muxer.writes)
	}
	if len(muxer.writtenStreams) != 2 || muxer.writtenStreams[0] != "audio" || muxer.writtenStreams[1] != "video" {
		t.Fatalf("written streams=%v", muxer.writtenStreams)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !audioReceiver.closed || !videoReceiver.closed || !muxer.closed {
		t.Fatalf("closed audio=%v video=%v mux=%v", audioReceiver.closed, videoReceiver.closed, muxer.closed)
	}
}

func TestRTPInputsSyncFromTimestampsAndDropLatePreview(t *testing.T) {
	ctx := context.Background()
	audio := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	video := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     16,
			Height:    16,
		},
	}
	audioReceiver := &runtimeRTPReceiver{
		streams: []av.Stream{audio},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  audio.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 48000},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	videoReceiver := &runtimeRTPReceiver{
		streams: []av.Stream{video},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  video.Codec,
			MIMEType:    av.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x10, 0x00, 0x11, 0x22},
		}},
		events: make(chan av.Event),
	}

	var delivered atomic.Int64
	sink := goav.Sink(component.SinkFunc("synced-preview", func(_ context.Context, msg component.Message) error {
		if msg.Packet != nil {
			delivered.Add(1)
		}
		return nil
	}))
	policy := goav.Sync("rtp-room", goav.SyncTolerance(5*time.Millisecond), goav.SyncDropLate())
	task, err := goav.From(
		goav.Input(rtpav.Receive(audioReceiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
		goav.Input(rtpav.Receive(videoReceiver, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).
		Audio(goav.InputName("audio")).Sync(policy).Copy().To(sink).
		Video(goav.InputName("video")).Sync(policy).Copy().To(sink).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	stats := task.Stats()
	if got := delivered.Load(); got != 1 {
		t.Fatalf("delivered packets = %d stats=%+v, want only the first RTP stream to pass sync", got, stats)
	}
	if stats.DropReasons[pipeline.DropSync] != 1 {
		t.Fatalf("stats = %+v, want one sync drop", stats)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStreamRecipeCopyTapCanAttachRuntimeMuxDestination(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	runtime := goav.MustNew(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	))

	task, err := goav.From(goav.Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus())))).
		UseRuntime(runtime).
		Audio().
		Copy().
		Tap(goav.PacketTap("audio.copied")).
		To(goav.File("archive.ogg", io.Discard)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	var copiedTap snapshot.Tap
	for _, tap := range task.Taps() {
		if tap.Name == "audio.copied" {
			copiedTap = tap
			break
		}
	}
	if copiedTap.Name == "" ||
		copiedTap.Domain != shape.DomainPacket ||
		copiedTap.MediaKind != av.MediaAudio ||
		copiedTap.Shape.Codec != av.CodecOpus ||
		copiedTap.Shape.StreamID != "audio" ||
		copiedTap.Node != "select-audio" {
		t.Fatalf("copied tap = %+v, want packet Opus audio tap on select-audio", copiedTap)
	}

	recording, err := task.Attach(ctx, goav.Branch("record").
		From(goav.PacketTap("audio.copied")).
		Copy().
		To(goav.File("recording.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 2 ||
		muxers.muxers[0].writes != 1 ||
		muxers.muxers[1].writes != 1 ||
		muxers.muxers[0].lastStream != "audio" ||
		muxers.muxers[1].lastStream != "audio" {
		t.Fatalf("muxers=%d first=%+v second=%+v", len(muxers.muxers), muxers.muxers[0], muxers.muxers[1])
	}
	if err := task.Detach(ctx, recording); err != nil {
		t.Fatal(err)
	}
	if !muxers.muxers[1].closed {
		t.Fatal("late recording muxer was not closed by detach")
	}
}

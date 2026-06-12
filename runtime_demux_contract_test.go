package goav

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

func TestOpenDemuxSourceCustomAdapterContracts(t *testing.T) {
	customFormat := av.FormatID("x-custom")
	probeStreams := []av.Stream{{
		ID:   "probe-audio",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:   av.CodecOpus,
			Type: av.MediaAudio,
		},
	}}
	metadata := av.Metadata{"tenant": "acme", "pipeline": "preview"}
	prober := &demuxContractProber{result: format.ProbeResult{
		Format:  customFormat,
		Score:   100,
		Streams: probeStreams,
	}}
	demuxer := &demuxContractDemuxer{formatID: customFormat}
	factory := &demuxContractFactory{demuxer: demuxer}
	runtime := New(
		WithRealtime(false),
		WithProber(prober),
		WithDemuxer(customFormat, factory),
	).(*runtime)
	input := format.Input{
		Name:     "clip.custom",
		URI:      "file:///tmp/clip.custom",
		Protocol: av.ProtocolFile,
		MIMEType: "application/x-custom",
		Realtime: true,
		Metadata: metadata,
	}

	build, err := (&builder{runtime: runtime}).openDemuxSource(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if factory.calls != 1 || factory.probe.Format != customFormat {
		t.Fatalf("factory calls=%d probe=%+v", factory.calls, factory.probe)
	}
	if prober.request.Name != input.Name ||
		prober.request.MIMEType != input.MIMEType ||
		prober.request.Input.URI != input.URI {
		t.Fatalf("probe request = %+v, want input facts", prober.request)
	}
	if !demuxer.opened || demuxer.closed {
		t.Fatalf("demuxer opened=%v closed=%v", demuxer.opened, demuxer.closed)
	}
	if !demuxer.openOptions.Realtime || !reflect.DeepEqual(demuxer.openOptions.Metadata, metadata) {
		t.Fatalf("open options = %+v, want realtime metadata", demuxer.openOptions)
	}
	if !reflect.DeepEqual(build.streams, probeStreams) {
		t.Fatalf("streams = %+v, want probe streams fallback %+v", build.streams, probeStreams)
	}
	if build.source.Name() != "clip.custom" {
		t.Fatalf("source name = %q, want clip.custom", build.source.Name())
	}
	described, ok := build.source.(pipeline.NodeDescriber)
	if !ok {
		t.Fatalf("source = %T, want pipeline.NodeDescriber", build.source)
	}
	detail := described.DescribeNode().Detail
	for _, fragment := range []string{"demux", "uri=file:///tmp/clip.custom", "protocol=file", "mime=application/x-custom", "realtime"} {
		if !strings.Contains(detail, fragment) {
			t.Fatalf("detail = %q, want fragment %q", detail, fragment)
		}
	}
}

func TestOpenDemuxSourcePrefersDemuxerStreamsAndSizesPacketScratch(t *testing.T) {
	customFormat := av.FormatID("x-video")
	probeStreams := []av.Stream{{ID: "probe-audio", Type: av.MediaAudio}}
	demuxStreams := []av.Stream{{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:     av.CodecVP8,
			Type:   av.MediaVideo,
			Width:  1280,
			Height: 720,
		},
	}}
	demuxer := &demuxContractDemuxer{
		formatID:     customFormat,
		streams:      demuxStreams,
		packetStream: "video",
	}
	runtime := New(
		WithRealtime(false),
		WithProber(&demuxContractProber{result: format.ProbeResult{Format: customFormat, Score: 100, Streams: probeStreams}}),
		WithDemuxer(customFormat, &demuxContractFactory{demuxer: demuxer}),
	).(*runtime)

	build, err := (&builder{runtime: runtime}).openDemuxSource(context.Background(), format.Input{Name: "camera.video"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(build.streams, demuxStreams) {
		t.Fatalf("streams = %+v, want demuxer streams %+v", build.streams, demuxStreams)
	}
	if err := build.source.Start(context.Background(), demuxContractEmitter{}); err != nil {
		t.Fatal(err)
	}
	if demuxer.payloadCap != 4<<20 {
		t.Fatalf("packet scratch cap = %d, want %d", demuxer.payloadCap, 4<<20)
	}
}

func TestOpenDemuxSourceErrorContracts(t *testing.T) {
	ctx := context.Background()
	input := format.Input{Name: "input.custom", MIMEType: "application/x-custom"}
	customFormat := av.FormatID("x-custom")

	t.Run("probe not found becomes structured input format error", func(t *testing.T) {
		runtime := New(WithProber(&demuxContractProber{err: format.ErrNotFound})).(*runtime)
		_, err := (&builder{runtime: runtime}).openDemuxSource(ctx, input)
		buildErr := requireRuntimeFormatBuildError(t, err, errcode.InputFormatUnknown)
		if buildErr.Node != "input.custom" || !errors.Is(buildErr, format.ErrNotFound) {
			t.Fatalf("probe error = %+v", buildErr)
		}
	})

	t.Run("detected format without demuxer is structured", func(t *testing.T) {
		runtime := New(WithProber(&demuxContractProber{result: format.ProbeResult{Format: customFormat, Score: 100}})).(*runtime)
		_, err := (&builder{runtime: runtime}).openDemuxSource(ctx, input)
		buildErr := requireRuntimeFormatBuildError(t, err, errcode.InputDemuxerMissing)
		if !strings.Contains(buildErr.Reason, string(customFormat)) || !errors.Is(buildErr, format.ErrNotFound) {
			t.Fatalf("missing demuxer error = %+v", buildErr)
		}
	})

	t.Run("factory error passes through", func(t *testing.T) {
		factoryErr := errors.New("custom demux factory failed")
		runtime := New(
			WithProber(&demuxContractProber{result: format.ProbeResult{Format: customFormat, Score: 100}}),
			WithDemuxer(customFormat, &demuxContractFactory{err: factoryErr}),
		).(*runtime)
		_, err := (&builder{runtime: runtime}).openDemuxSource(ctx, input)
		if err != factoryErr {
			t.Fatalf("factory error = %v, want original", err)
		}
	})

	t.Run("nil factory result returns format nil demuxer error", func(t *testing.T) {
		runtime := New(
			WithProber(&demuxContractProber{result: format.ProbeResult{Format: customFormat, Score: 100}}),
			WithDemuxer(customFormat, &demuxContractFactory{}),
		).(*runtime)
		_, err := (&builder{runtime: runtime}).openDemuxSource(ctx, input)
		if !errors.Is(err, format.ErrNilDemuxer) {
			t.Fatalf("nil demuxer error = %v, want format.ErrNilDemuxer", err)
		}
	})

	t.Run("open failure closes demuxer", func(t *testing.T) {
		openErr := errors.New("custom open failed")
		demuxer := &demuxContractDemuxer{formatID: customFormat, openErr: openErr}
		runtime := New(
			WithProber(&demuxContractProber{result: format.ProbeResult{Format: customFormat, Score: 100}}),
			WithDemuxer(customFormat, &demuxContractFactory{demuxer: demuxer}),
		).(*runtime)
		_, err := (&builder{runtime: runtime}).openDemuxSource(ctx, input)
		if err != openErr {
			t.Fatalf("open error = %v, want original", err)
		}
		if !demuxer.closed {
			t.Fatal("demuxer was not closed after open failure")
		}
	})
}

type demuxContractProber struct {
	result  format.ProbeResult
	err     error
	request format.ProbeRequest
}

func (p *demuxContractProber) Probe(_ context.Context, request format.ProbeRequest) (format.ProbeResult, error) {
	p.request = request
	if p.err != nil {
		return format.ProbeResult{}, p.err
	}
	return p.result, nil
}

type demuxContractFactory struct {
	demuxer format.Demuxer
	err     error
	calls   int
	probe   format.ProbeResult
}

func (f *demuxContractFactory) NewDemuxer(_ context.Context, probe format.ProbeResult) (format.Demuxer, error) {
	f.calls++
	f.probe = probe
	if f.err != nil {
		return nil, f.err
	}
	return f.demuxer, nil
}

type demuxContractDemuxer struct {
	formatID     av.FormatID
	streams      []av.Stream
	openErr      error
	opened       bool
	closed       bool
	openInput    format.Input
	openOptions  format.OpenOptions
	packetStream av.StreamID
	read         int
	payloadCap   int
}

func (d *demuxContractDemuxer) Format() av.FormatID {
	return d.formatID
}

func (d *demuxContractDemuxer) Open(_ context.Context, input format.Input, options format.OpenOptions) error {
	d.opened = true
	d.openInput = input
	d.openOptions = options
	return d.openErr
}

func (d *demuxContractDemuxer) Streams() []av.Stream {
	return d.streams
}

func (d *demuxContractDemuxer) ReadInto(_ context.Context, out *format.ReadResult) error {
	if out != nil && out.Packet != nil {
		d.payloadCap = cap(out.Packet.Payload.Bytes)
	}
	if d.read > 0 {
		return io.EOF
	}
	d.read++
	if d.packetStream == "" {
		return io.EOF
	}
	out.PacketReady = true
	out.Packet.StreamID = d.packetStream
	out.Packet.Payload.Bytes = append(out.Packet.Payload.Bytes, 1)
	return nil
}

func (d *demuxContractDemuxer) Close() error {
	d.closed = true
	return nil
}

type demuxContractEmitter struct{}

func (demuxContractEmitter) Emit(context.Context, *pipeline.Message) error {
	return nil
}

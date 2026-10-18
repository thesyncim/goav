package integration

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/rtpav"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
)

// recipeAPIRTPReader and recipeAPIVideoRTPReader are minimal
// rtpav.PacketReaders: declared streams and an immediately ended packet feed.
type recipeAPIRTPReader struct{}

type recipeAPIVideoRTPReader struct {
	recipeAPIRTPReader
}

func (recipeAPIRTPReader) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{{ID: "audio", Type: "audio"}}, nil
}

func (recipeAPIVideoRTPReader) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:   av.CodecVP8,
			Type: av.MediaVideo,
		},
	}}, nil
}

func (recipeAPIVideoRTPReader) PayloadMap() rtpav.PayloadMap {
	return rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
		PayloadType: 96,
		Parameters: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
		},
	}})
}

func (recipeAPIRTPReader) PayloadMap() rtpav.PayloadMap {
	return nil
}

func (recipeAPIRTPReader) ReadRTP(context.Context) (*rtp.Packet, error) {
	return nil, io.EOF
}

func (recipeAPIRTPReader) Events() <-chan av.Event {
	return nil
}

func (recipeAPIRTPReader) Close() error {
	return nil
}

type recipeAPIDecoderFactory struct{}

func (recipeAPIDecoderFactory) NewDecoder(context.Context, codec.DecodeConfig) (codec.Decoder, error) {
	return nil, errors.New("decoder should not open during explain")
}

type recipeAPICustomDestination struct{}

func (recipeAPICustomDestination) Name() string {
	return "custom"
}

func (recipeAPICustomDestination) Contract() provider.Contract {
	return provider.Contract{
		ByteStream: true,
		Formats:    []av.FormatID{av.FormatIVF},
		MIMETypes:  []string{"video/ivf"},
	}
}

func (recipeAPICustomDestination) Open(context.Context, provider.Info) (provider.Writer, error) {
	return recipeAPICustomWriter{}, nil
}

type recipeAPICustomWriter struct{}

func (recipeAPICustomWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (recipeAPICustomWriter) Close() error {
	return nil
}

func recordOutput(input goav.InputSpec, outputs ...goav.Destination) goav.Output {
	return input.To(outputs...)
}

func decodeOutput(input goav.InputSpec, output goav.Destination) goav.Output {
	return input.Stream().Decode().To(output)
}

func branchByName(branches []plan.Branch, name string) (plan.Branch, bool) {
	for i := range branches {
		if branches[i].Name == name {
			return branches[i], true
		}
	}
	return plan.Branch{}, false
}

func tapReportByName(taps []plan.Tap, name string) (plan.Tap, bool) {
	for i := range taps {
		if taps[i].Name == name {
			return taps[i], true
		}
	}
	return plan.Tap{}, false
}

func hasRequirement(requirements []plan.AdapterRequirement, kind string, codecID av.CodecID, formatID av.FormatID) bool {
	for i := range requirements {
		requirement := requirements[i]
		if requirement.Kind != kind {
			continue
		}
		if codecID != "" && requirement.Codec != codecID {
			continue
		}
		if formatID != "" && requirement.Format != formatID {
			continue
		}
		return true
	}
	return false
}

func adapterRequirementByKind(requirements []plan.AdapterRequirement, kind string, name string) (plan.AdapterRequirement, bool) {
	for i := range requirements {
		requirement := requirements[i]
		if requirement.Kind != kind {
			continue
		}
		if name != "" && requirement.Name != name && string(requirement.Format) != name && string(requirement.Codec) != name && requirement.Transform != name {
			continue
		}
		return requirement, true
	}
	return plan.AdapterRequirement{}, false
}

func operationKinds(operations []plan.Operation) []plan.OperationKind {
	kinds := make([]plan.OperationKind, 0, len(operations))
	for i := range operations {
		kinds = append(kinds, operations[i].Kind)
	}
	return kinds
}

func equalOperationKinds(a []plan.OperationKind, b []plan.OperationKind) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStrings(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRecordRecipeExplainReturnsStructuredPlan(t *testing.T) {
	out := recordOutput(
		goav.Input(rtpav.Receive(recipeAPIVideoRTPReader{}, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8()))),
		goav.Write("recording.ivf", io.Discard),
	)
	report, err := bundle.Explain(context.Background(), out)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary == "" || report.Operation != "build job" {
		t.Fatalf("report summary=%q operation=%q", report.Summary, report.Operation)
	}
	if len(report.Graph.Nodes) == 0 || len(report.Graph.Edges) == 0 {
		t.Fatalf("empty graph: %+v", report.Graph)
	}
	if len(report.Inputs) != 1 || report.Inputs[0].Name != "video" ||
		report.Inputs[0].Format != "" || report.Inputs[0].Codec != av.CodecVP8 ||
		!report.Inputs[0].Realtime {
		t.Fatalf("inputs=%+v", report.Inputs)
	}
	if len(report.Destinations) != 1 || report.Destinations[0].Name != "recording.ivf" ||
		report.Destinations[0].Format != av.FormatIVF || report.Destinations[0].Kind != "mux" {
		t.Fatalf("destinations=%+v", report.Destinations)
	}
	if !equalStrings(report.Destinations[0].Branches, []string{"video"}) {
		t.Fatalf("destination branches=%+v", report.Destinations[0].Branches)
	}
	if len(report.Branches) != 1 || report.Branches[0].Name != "video" ||
		!equalOperationKinds(operationKinds(report.Branches[0].Operations), []plan.OperationKind{plan.OpDepacketize, plan.OpCopy}) {
		t.Fatalf("branches=%+v", report.Branches)
	}
	if !hasRequirement(report.RequiredAdapters, "depacketizer", av.CodecVP8, "") ||
		!hasRequirement(report.RequiredAdapters, "muxer", "", av.FormatIVF) {
		t.Fatalf("requirements=%+v", report.RequiredAdapters)
	}
	muxer, ok := adapterRequirementByKind(report.RequiredAdapters, "muxer", string(av.FormatIVF))
	if !ok ||
		muxer.MaxStreams != 1 ||
		len(muxer.Media) != 1 ||
		muxer.Media[0] != av.MediaVideo ||
		len(muxer.Codecs) != 3 ||
		muxer.Codecs[0] != av.CodecVP8 ||
		muxer.Codecs[1] != av.CodecVP9 ||
		muxer.Codecs[2] != av.CodecAV1 ||
		muxer.Metadata["summary"] == "" {
		t.Fatalf("ivf requirement=%+v, want container capability details", muxer)
	}
	if len(report.Warnings) != 0 {
		t.Fatalf("warnings=%+v", report.Warnings)
	}
}

func TestExplainReportsBranchShapeFromLiveCodecIntent(t *testing.T) {
	rt := mustGoAVRuntime(runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, recipeAPIDecoderFactory{})
	}))

	report, err := rt.Explain(context.Background(), goav.Input(rtpav.Receive(recipeAPIRTPReader{}, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()))).
		Audio().
		Decode().
		Tap("audio.decoded").
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	if err != nil {
		t.Fatal(err)
	}
	branch, ok := branchByName(report.Branches, "audio")
	if !ok {
		t.Fatalf("branches=%+v, want audio", report.Branches)
	}
	if branch.Shape.Domain != shape.DomainPacket ||
		branch.Shape.MediaKind != av.MediaAudio ||
		branch.Shape.StreamID != "audio" ||
		branch.Shape.Codec != av.CodecOpus ||
		branch.Shape.SampleRate != 48000 ||
		branch.Shape.Channels != codec.Stereo ||
		!branch.Shape.Realtime {
		t.Fatalf("branch shape=%+v, want live Opus packet shape", branch.Shape)
	}
	tap, ok := tapReportByName(report.Taps, "audio.decoded")
	if !ok {
		t.Fatalf("taps=%+v, want audio.decoded", report.Taps)
	}
	if tap.Shape.Domain != shape.DomainFrame ||
		tap.Shape.MediaKind != av.MediaAudio ||
		tap.Shape.Codec != av.CodecOpus ||
		tap.Shape.SampleRate != 48000 ||
		tap.Shape.Channels != codec.Stereo ||
		!tap.Shape.Realtime {
		t.Fatalf("tap shape=%+v, want live decoded audio frame shape", tap.Shape)
	}
}

func TestExternalCustomDestinationCanBeDestined(t *testing.T) {
	dest := recipeAPICustomDestination{}
	target := goav.Custom("custom", dest)

	out := goav.Input(rtpav.Receive(recipeAPIVideoRTPReader{}, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8()))).
		Video().
		To(target)

	report, err := bundle.Explain(context.Background(), out)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Destinations) != 1 ||
		report.Destinations[0].Name != "custom" ||
		report.Destinations[0].Format != av.FormatIVF ||
		report.Destinations[0].MIMEType != "video/ivf" {
		t.Fatalf("destinations: %+v", report.Destinations)
	}
	if len(report.Streams) != 1 || !equalStrings(report.Streams[0].Destinations, []string{"custom"}) {
		t.Fatalf("streams: %+v", report.Streams)
	}
}

func TestFlowBranchesDescribeLiveInputBranches(t *testing.T) {
	// A reusable operation list is a Go function over Streams.
	voice := func(s goav.Stream) goav.Stream {
		return s.Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono))).As("voice")
	}
	archive := func(s goav.Stream) goav.Stream {
		return s.Encode(codec.Opus(codec.Bitrate(128_000), codec.Channels(codec.Stereo))).As("archive")
	}
	voiceOut := goav.Write("voice.ogg", io.Discard)
	archiveOut := goav.Write("archive.ogg", io.Discard)

	decoded := goav.Input(rtpav.Receive(recipeAPIRTPReader{}, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()))).
		Audio().
		Decode()

	spec, err := mustGoAVRuntime().Describe(voice(decoded).To(voiceOut), archive(decoded).To(archiveOut))
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	for _, want := range []string{
		"audio -> select-audio",
		"select-audio -> decode-audio",
		"decode-audio -> encode-voice",
		"decode-audio -> encode-archive",
		"encode-voice -> voice.ogg",
		"encode-archive -> archive.ogg",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
}

func TestReadmeDecodeShortcutUsesSinkDestination(t *testing.T) {
	sink := component.SinkFunc("frames", func(context.Context, component.Message) error {
		return nil
	})
	out := decodeOutput(
		goav.Input(rtpav.Receive(recipeAPIRTPReader{}, rtpav.WithCodec(codec.Opus()))),
		goav.Sink(sink),
	)

	spec, err := bundle.Describe(out)
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "rtp -> select") ||
		!strings.Contains(text, "select -> decode") ||
		!strings.Contains(text, "decode -> frames") {
		t.Fatalf("spec:\n%s", text)
	}
	report, err := bundle.Explain(context.Background(), out)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Streams) != 1 || !report.Streams[0].Decode {
		t.Fatalf("report streams: %+v", report.Streams)
	}
}

func TestRecipeAndRejectsDuplicateRealtimeInputNames(t *testing.T) {
	audio := goav.Input(rtpav.Receive(recipeAPIRTPReader{}, rtpav.WithName("media"), rtpav.WithCodec(codec.Opus())))
	video := goav.Input(rtpav.Receive(recipeAPIRTPReader{}, rtpav.WithName("media"), rtpav.WithCodec(codec.VP8())))
	recording := goav.Write("recording.webm", io.Discard)
	_, err := mustGoAVRuntime().Build(context.Background(), audio.Audio().To(recording), video.Video().To(recording))

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_duplicate" {
		t.Fatalf("err = %v, want input_duplicate with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), `realtime input name "media"`) ||
		!strings.Contains(err.Error(), "second input index: 1") ||
		!strings.Contains(err.Error(), "distinct goav.Name") {
		t.Fatalf("err = %v, want duplicate input guidance", err)
	}
}

func TestPacketCopyRecipeAcceptsSinkDestination(t *testing.T) {
	spec, err := mustGoAVRuntime().Describe(goav.Input(rtpav.Receive(recipeAPIRTPReader{}, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()))).
		To(goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
			return nil
		}))))
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "audio -> packets") {
		t.Fatalf("spec:\n%s", text)
	}
}

func TestProviderRecipeSurfacesOpenSourceError(t *testing.T) {
	// Provider inputs validate themselves: a nil reader is the provider's
	// error, surfaced when the source opens at build time.
	_, err := bundle.Build(context.Background(), recordOutput(
		goav.Input(rtpav.Receive(nil, rtpav.WithName("audio"))),
		goav.Write("recording.ivf", io.Discard),
	))
	if !errors.Is(err, rtpav.ErrNilReceiver) {
		t.Fatalf("err = %v, want rtpav.ErrNilReceiver", err)
	}
}

func TestStreamRecipeReportsMissingDecodeAdapterBeforeOpeningLiveInput(t *testing.T) {
	rt := mustGoAVRuntime(runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDescriptor(codec.Descriptor{
			ID:    av.CodecH264,
			Name:  "h264",
			Modes: []codec.Mode{codec.ModeDecode},
			Capabilities: codec.Capabilities{
				BuildTags: []string{"goav_goh264"},
			},
			Backend: codec.Backend{
				Name:   "goh264",
				Status: "planned-build-tagged",
			},
		})
	}))
	_, err := rt.Build(context.Background(), goav.Input(rtpav.Receive(recipeAPIRTPReader{}, rtpav.WithName("video"), rtpav.WithCodec(codec.H264()))).
		Video().
		Decode().
		To(goav.Sink(component.SinkFunc("frames", func(context.Context, component.Message) error {
			return nil
		}))))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "decode_adapter_unavailable" || !errors.Is(err, codec.ErrUnavailable) {
		t.Fatalf("err = %v, want decode_adapter_unavailable wrapping codec.ErrUnavailable", err)
	}
	if !strings.Contains(err.Error(), "codec=h264") ||
		!strings.Contains(err.Error(), "input.To(output)") ||
		strings.Contains(err.Error(), "stream") ||
		strings.Contains(err.Error(), "cannot open input") {
		t.Fatalf("err = %v, want decode adapter guidance before live input diagnostics", err)
	}
}

func TestBranchCompositionAcceptsRTPInputThenReportsMissingMuxer(t *testing.T) {
	decoded := goav.Input(rtpav.Receive(recipeAPIRTPReader{}, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()))).
		Audio().
		Decode().
		Tap("audio.decoded")
	_, err := mustGoAVRuntime().Build(context.Background(),
		decoded.Encode(codec.Opus(codec.Bitrate(96_000))).As("main").To(goav.Write("archive.ogg", io.Discard)),
	)

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_muxer_missing" {
		t.Fatalf("err = %v, want destination_muxer_missing", err)
	}
	if buildErr.Node != "archive.ogg" {
		t.Fatalf("node = %q, want archive.ogg destination", buildErr.Node)
	}
	if !strings.Contains(err.Error(), "ogg muxer") {
		t.Fatalf("err = %v, want Ogg adapter guidance", err)
	}
}

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	goav "github.com/thesyncim/goav"
	av1adapter "github.com/thesyncim/goav/adapters/goav1"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	matroskaadapter "github.com/thesyncim/goav/container/matroska"
	"github.com/thesyncim/goav/ctl"
	"github.com/thesyncim/goav/goavtest"
)

func TestCLIInvokesCustomControlCommand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type customCommand struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"custom CLI value"`
	}
	var applied string
	command := ctl.NewCommand[customCommand](
		"vendor.cli",
		"custom CLI command",
		func(_ context.Context, _ goav.Task, args customCommand) (ctl.ControlResponse, error) {
			applied = args.Value
			return ctl.ControlResponse{Operation: "control vendor.cli", Result: applied}, nil
		},
	)
	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-cli-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ctl.ServeUnixWithOptions(ctx, task, "unix://"+socket, ctl.WithCapabilities(ctl.CapabilitySet{
			Commands: []ctl.CommandSpec{command},
		}))
	}()
	waitForCLISocket(t, socket, errC)

	cmd := exec.Command("go", "run", ".", "ctl", "--control", "unix://"+socket, "control", "vendor.cli", "value=via-cli")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("goav ctl failed: %v\n%s", err, output)
	}
	if applied != "via-cli" || !strings.Contains(string(output), `"via-cli"`) {
		t.Fatalf("applied=%q output=%s", applied, output)
	}
}

func TestCLIHelpListsCustomPipelineRegistry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	type meterSettings struct {
		Window time.Duration `goavctl:"window,duration" usage:"[window=<duration>]" help:"observation window"`
	}
	meter := ctl.NewBranchStep[meterSettings](
		"meter",
		"custom level meter",
		func(*ctl.BranchPipeline, meterSettings) error { return nil },
		ctl.Aliases("levelmeter"),
	)
	type acmeSettings struct {
		Bitrate int    `goavctl:"bitrate,required,rate" usage:"bitrate=<rate>" help:"target bitrate"`
		Quality string `goavctl:"quality" usage:"[quality=<name>]" help:"native quality"`
	}
	acme := ctl.NewEncoderSpec[acmeSettings](
		"acmeenc",
		"ACME native encoder",
		func(args acmeSettings) (codec.CodecSpec, error) {
			return codec.Codec("acme", av.MediaAudio, codec.Bitrate(args.Bitrate), codec.Profile(args.Quality)), nil
		},
		ctl.Aliases("acme"),
	)
	capabilities := ctl.CapabilitySet{
		Pipeline: ctl.PipelineRegistry{
			Steps:    []ctl.BranchPipelineStepSpec{meter},
			Encoders: []ctl.EncoderSpec{acme},
		},
	}

	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-cli-help-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ctl.ServeUnixWithOptions(ctx, task, "unix://"+socket, ctl.WithCapabilities(capabilities))
	}()
	waitForCLISocket(t, socket, errC)

	cmd := exec.Command("go", "run", ".", "ctl", "--control", "unix://"+socket, "help", "attach")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("goav ctl help attach failed: %v\n%s", err, output)
	}
	text := string(output)
	for _, fragment := range []string{
		"Built-in steps:",
		"Custom steps:",
		"meter [window=<duration>]",
		"(aliases: levelmeter)",
		"custom level meter",
		"Custom encoders:",
		"acmeenc bitrate=<rate> [quality=<name>]",
		"(aliases: acme)",
		"ACME native encoder",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("help missing %q:\n%s", fragment, text)
		}
	}

	caps := runCLI(t, "--control", "unix://"+socket, "capabilities")
	for _, fragment := range []string{
		`"custom_branch_steps"`,
		`"name":"meter"`,
		`"name":"acmeenc"`,
		`"runtime_encoders"`,
	} {
		if !strings.Contains(caps, fragment) {
			t.Fatalf("capabilities missing %q:\n%s", fragment, caps)
		}
	}
}

func TestCLIPrintsGraphAsRawText(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-cli-graph-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ctl.ServeUnixWithOptions(ctx, task, "unix://"+socket)
	}()
	waitForCLISocket(t, socket, errC)

	cmd := exec.Command("go", "run", ".", "ctl", "--control", "unix://"+socket, "graph")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("goav ctl graph failed: %v\n%s", err, output)
	}
	text := string(output)
	if !strings.HasPrefix(text, "flowchart LR\n") || strings.HasPrefix(text, `"`) {
		t.Fatalf("graph output = %q", text)
	}
}

func TestCLIAttachRebranchDetachAndDotGraph(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	task, err := goav.From(goavtest.Packets(av.CodecOpus, packet)).
		Audio().Copy().Tap(goav.PacketTap("pkts")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-cli-mutate-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ctl.ServeUnixWithOptions(ctx, task, "unix://"+socket)
	}()
	waitForCLISocket(t, socket, errC)

	first := filepath.Join(t.TempDir(), "first output.ogg")
	output := runCLI(t, "--control", "unix://"+socket, "attach", "pkts", "as", "cli",
		fmt.Sprintf("copy ! filesink location=%q format=ogg", first))
	if !strings.Contains(output, `"Name":"cli"`) {
		t.Fatalf("attach output = %s", output)
	}

	dot := runCLI(t, "--control", "unix://"+socket, "graph", "format=dot")
	if !strings.HasPrefix(dot, "digraph") || !strings.Contains(dot, "cli") {
		t.Fatalf("dot graph = %s", dot)
	}

	second := filepath.Join(t.TempDir(), "second output.ogg")
	output = runCLI(t, "--control", "unix://"+socket, "rebranch", "cli",
		"--switch", "next_frame",
		"--keep-old-on-failure",
		fmt.Sprintf("copy ! filesink location=%q format=ogg", second))
	if !strings.Contains(output, `"Name":"cli"`) {
		t.Fatalf("rebranch output = %s", output)
	}

	output = runCLI(t, "--control", "unix://"+socket, "detach", "cli")
	if !strings.Contains(output, `"Name":"cli"`) {
		t.Fatalf("detach output = %s", output)
	}
}

func TestCLIWatchFollowPrintsStreamingResponses(t *testing.T) {
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-cli-watch-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := serveOneShot(t, socket, func(conn net.Conn) error {
		var request ctl.Request
		if err := json.NewDecoder(conn).Decode(&request); err != nil {
			return err
		}
		if request.Op != "watch" || request.Args["follow"] != "true" || request.Args["type"] != "stats" {
			return fmt.Errorf("request = %+v", request)
		}
		encoder := json.NewEncoder(conn)
		for i := 0; i < 2; i++ {
			if err := encoder.Encode(ctl.SuccessResponse(map[string]string{"type": "stats"})); err != nil {
				return err
			}
		}
		return nil
	})

	output := runCLI(t, "--control", "unix://"+socket, "watch", "type=stats", "--follow")
	if got := strings.Count(output, `{"type":"stats"}`); got != 2 {
		t.Fatalf("output = %q, want two streamed events", output)
	}
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func TestParseCtlArgs(t *testing.T) {
	control, args, err := parseCtlArgs([]string{"--control", "unix:///tmp/live.sock", "graph", "format=dot"})
	if err != nil {
		t.Fatal(err)
	}
	if control != "unix:///tmp/live.sock" || strings.Join(args, " ") != "graph format=dot" {
		t.Fatalf("control=%q args=%v", control, args)
	}

	control, args, err = parseCtlArgs([]string{"--control=unix:///tmp/live.sock", "help", "graph"})
	if err != nil {
		t.Fatal(err)
	}
	if control != "unix:///tmp/live.sock" || strings.Join(args, " ") != "help graph" {
		t.Fatalf("control=%q args=%v", control, args)
	}

	if _, _, err := parseCtlArgs([]string{"--control"}); err == nil {
		t.Fatal("expected missing control value error")
	}
}

func TestRunPrintsLocalHelp(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"ctl", "help", "graph"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Mermaid") || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunPipelineHelpBootstrapsGeneratedControlFlow(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"run", "--help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d stderr=%q", code, stderr.String())
	}
	text := stdout.String()
	for _, want := range []string{
		"examples:",
		"/tmp/goav-av1.ivf format=ivf",
		"min_qindex=20 max_qindex=180 tune=zerolatency",
		"tap name=<tap-name>",
		"size=<w>x<h>",
		"fps=<n[/d]|decimal>",
		"format=i420|yuv420p",
		"goav ctl --control unix:///tmp/goav-live.sock taps",
		"control seek position=2s source=fixture",
		"attach frames as preview",
		"/tmp/goav-preview.ivf format=ivf",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("help missing %q:\n%s", want, text)
		}
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestRunGeneratedVideoAV1RealtimeString(t *testing.T) {
	out := filepath.Join(t.TempDir(), "generated av1.mkv")
	pipeline := fmt.Sprintf(
		`testsrc video name=fixture width=32 height=18 fps=30 frames=2 realtime=true pattern=bars ! resize width=16 height=16 ! av1enc bitrate=400k fps=30 keyframe_interval=1 min_qindex=20 max_qindex=180 tune=zerolatency ! filesink location=%q format=matroska`,
		out,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"run", pipeline}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var result runPipelineResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("result JSON: %v\n%s", err, stdout.String())
	}
	if result.Runtime != "demo" || result.Codec != "av1" || result.Frames != 2 || !result.Realtime || result.Output != out || result.Format != string(av.FormatMatroska) {
		t.Fatalf("result = %+v", result)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	assertDecodableAV1Matroska(t, data)
}

func TestRunGeneratedVideoAV1IVFString(t *testing.T) {
	out := filepath.Join(t.TempDir(), "generated av1.ivf")
	pipeline := fmt.Sprintf(
		`testsrc video name=fixture width=32 height=18 fps=30 frames=2 realtime=true pattern=bars ! resize width=16 height=16 ! av1enc bitrate=400k fps=30 keyframe_interval=1 min_qindex=20 max_qindex=180 tune=zerolatency ! filesink location=%q format=ivf`,
		out,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"run", pipeline}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var result runPipelineResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("result JSON: %v\n%s", err, stdout.String())
	}
	if result.Runtime != "demo" || result.Codec != "av1" || result.Frames != 2 || !result.Realtime || result.Output != out {
		t.Fatalf("result = %+v", result)
	}
	header, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(header) < 4 || string(header[:4]) != "DKIF" {
		t.Fatalf("output header = %q, want IVF", header[:min(len(header), 8)])
	}
	if got := binary.LittleEndian.Uint32(header[16:20]); got != 30 {
		t.Fatalf("IVF timebase denominator = %d, want 30", got)
	}
	if got := binary.LittleEndian.Uint32(header[24:28]); got != 2 {
		t.Fatalf("IVF frame count = %d, want 2", got)
	}
	assertDecodableAV1IVF(t, header)
}

func TestRunGeneratedVideoWithControlSocket(t *testing.T) {
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-run-control-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	out := filepath.Join(t.TempDir(), "controlled.ivf")
	pipeline := fmt.Sprintf(
		`testsrc video name=fixture width=32 height=18 fps=30 frames=900 realtime=true pattern=bars ! tap name=frames ! av1enc bitrate=200k fps=30 keyframe_interval=1 ! filesink location=%q format=ivf`,
		out,
	)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run([]string{"run", "--runtime", "test", "--control", "unix://" + socket, pipeline}, &stdout, &stderr)
	}()
	waitForRunSocket(t, socket, done, &stdout, &stderr)

	taps := runLocalCtl(t, socket, "taps")
	if !strings.Contains(taps, `"Name":"frames"`) {
		t.Fatalf("taps output = %s", taps)
	}
	graph := runLocalCtl(t, socket, "graph")
	if !strings.Contains(graph, "flowchart LR") || !strings.Contains(graph, "frames") {
		t.Fatalf("graph output = %s", graph)
	}
	runLocalCtl(t, socket, "control", "rate", "value=0.5", "source=fixture")
	runLocalCtl(t, socket, "control", "seek", "position=100ms", "source=fixture")

	branchOut := filepath.Join(t.TempDir(), "preview.ivf")
	attach := runLocalCtl(t, socket, "attach", "frames", "as", "preview",
		fmt.Sprintf(`resize 16x16 ! av1enc bitrate=120k fps=30 keyframe_interval=1 ! filesink location=%q format=ivf`, branchOut))
	if !strings.Contains(attach, `"Name":"preview"`) {
		t.Fatalf("attach output = %s", attach)
	}
	graph = runLocalCtl(t, socket, "graph")
	if !strings.Contains(graph, "branch=preview (attached)") {
		t.Fatalf("graph after attach = %s", graph)
	}
	runLocalCtl(t, socket, "detach", "preview")
	runLocalCtl(t, socket, "stop")

	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("goav run code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("goav run did not stop; stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	var result runPipelineResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("run result JSON: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	if result.Control != "unix://"+socket || result.Runtime != "test" || result.Codec != "av1" {
		t.Fatalf("run result = %+v", result)
	}
}

func assertDecodableAV1Matroska(t *testing.T, data []byte) {
	t.Helper()
	if len(data) < 4 || !bytes.Equal(data[:4], []byte{0x1a, 0x45, 0xdf, 0xa3}) {
		t.Fatalf("not a Matroska EBML file")
	}
	demuxer, err := matroskaadapter.NewDemuxer(bytes.NewReader(data), matroskaadapter.DemuxerOptions{})
	if err != nil {
		t.Fatalf("matroska demuxer: %v", err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	track := tracks[0]
	if track.Codec != matroskaadapter.CodecAV1 || track.Type != matroskaadapter.TrackVideo || track.Video.Width != 16 || track.Video.Height != 16 {
		t.Fatalf("track = %+v, want 16x16 AV1 video", track)
	}

	stream := av.Stream{
		ID:       "fixture",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: int64(time.Second)},
		Codec: av.CodecParameters{
			ID:          av.CodecAV1,
			Type:        av.MediaVideo,
			Width:       track.Video.Width,
			Height:      track.Video.Height,
			PixelFormat: av.PixelFormatI420,
			ClockRate:   uint32(time.Second),
		},
	}
	decoder := newAV1TestDecoder(t, stream, len(data))
	defer decoder.Close()

	packet := matroskaadapter.Packet{Data: make([]byte, 0, len(data))}
	var frames int
	for {
		packet.Data = packet.Data[:0]
		err := demuxer.ReadPacket(&packet)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read matroska packet: %v", err)
		}
		frames += decodeAV1TestPacket(t, decoder, av.Packet{
			StreamID: "fixture",
			Type:     av.MediaVideo,
			Payload:  av.Buffer{Bytes: packet.Data, Ownership: av.BufferBorrowed},
			PTS:      av.Timestamp{Value: packet.TimeNS, Base: stream.TimeBase},
			Duration: av.Duration{Value: packet.DurationNS, Base: stream.TimeBase},
			Keyframe: packet.Keyframe,
		})
	}
	if frames == 0 {
		t.Fatal("decoded zero AV1 frames from generated Matroska")
	}
}

func assertDecodableAV1IVF(t *testing.T, data []byte) {
	t.Helper()
	if len(data) < 32 || string(data[:4]) != "DKIF" || string(data[8:12]) != "AV01" {
		t.Fatalf("not an AV1 IVF file")
	}
	width := int(binary.LittleEndian.Uint16(data[12:14]))
	height := int(binary.LittleEndian.Uint16(data[14:16]))
	timebase := av.TimeBase{
		Num: int64(binary.LittleEndian.Uint32(data[20:24])),
		Den: int64(binary.LittleEndian.Uint32(data[16:20])),
	}
	stream := av.Stream{
		ID:       "fixture",
		Type:     av.MediaVideo,
		TimeBase: timebase,
		Codec: av.CodecParameters{
			ID:          av.CodecAV1,
			Type:        av.MediaVideo,
			Width:       width,
			Height:      height,
			PixelFormat: av.PixelFormatI420,
			ClockRate:   uint32(timebase.Den),
		},
	}
	decoder := newAV1TestDecoder(t, stream, len(data))
	defer decoder.Close()

	var frames int
	for offset := 32; offset < len(data); {
		if offset+12 > len(data) {
			t.Fatalf("truncated IVF frame header at offset %d", offset)
		}
		size := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
		pts := int64(binary.LittleEndian.Uint64(data[offset+4 : offset+12]))
		offset += 12
		if size < 0 || offset+size > len(data) {
			t.Fatalf("invalid IVF frame size %d at offset %d", size, offset)
		}
		packet := av.Packet{
			StreamID: "fixture",
			Type:     av.MediaVideo,
			Payload:  av.Buffer{Bytes: data[offset : offset+size], Ownership: av.BufferBorrowed},
			PTS:      av.Timestamp{Value: pts, Base: timebase},
			Duration: av.Duration{Value: 1, Base: timebase},
			Keyframe: frames == 0,
		}
		frames += decodeAV1TestPacket(t, decoder, packet)
		offset += size
	}
	if frames == 0 {
		t.Fatal("decoded zero AV1 frames from generated IVF")
	}
}

func newAV1TestDecoder(t *testing.T, stream av.Stream, maxPayloadBytes int) codec.Decoder {
	t.Helper()
	factory := av1adapter.NewDecoderFactory()
	state, err := factory.NewDecodeState(context.Background(), codec.DecodeConfig{
		Stream: stream,
		Bounds: codec.DecodeBounds{
			MaxFramesPerInput:   2,
			MaxEventsPerInput:   2,
			MaxRequestsPerInput: 2,
			MaxPayloadBytes:     maxPayloadBytes,
			MaxWidth:            stream.Codec.Width,
			MaxHeight:           stream.Codec.Height,
		},
	})
	if err != nil {
		t.Fatalf("decode state: %v", err)
	}
	decoder, err := factory.NewDecoder(context.Background(), codec.DecodeConfig{
		Stream:      stream,
		OpaqueState: state,
	})
	if err != nil {
		t.Fatalf("decoder: %v", err)
	}
	return decoder
}

func decodeAV1TestPacket(t *testing.T, decoder codec.Decoder, packet av.Packet) int {
	t.Helper()
	frameScratch := make([]av.Frame, 2)
	for i := range frameScratch {
		frameScratch[i].Planes = make([]av.Plane, 0, 3)
	}
	decoded := codec.DecodeResult{
		Frames:   frameScratch[:0],
		Events:   make([]av.Event, 0, 2),
		Requests: make([]codec.ControlRequest, 0, 2),
	}
	if err := decoder.DecodeInto(context.Background(), &packet, &decoded); err != nil {
		t.Fatalf("decode AV1 packet: %v", err)
	}
	return len(decoded.Frames)
}

func TestRunPipelineParserCoversGeneratedTestSourceForms(t *testing.T) {
	plan, err := parseRunPipeline(`testsrc video name=fixture size=1920x1080 fps=30000/1001 duration=100ms realtime=off pixel_format=yuv420p pattern=solid ! tap frames ! resize 640x360 ! encode av01 bitrate=2M fps=29.97 keyframe_interval=30 profile=main level=5.1 tune=zerolatency ! filesink "/tmp/generated test.ivf" format=ivf`)
	if err != nil {
		t.Fatal(err)
	}
	source := plan.source
	if source.name != "fixture" ||
		source.width != 1920 ||
		source.height != 1080 ||
		source.fps != (fpsValue{num: 30000, den: 1001}) ||
		source.frames != 3 ||
		source.realtime ||
		source.pixelFormat != av.PixelFormatI420 ||
		source.pattern != "solid" {
		t.Fatalf("source = %+v", source)
	}
	if plan.destination.location != "/tmp/generated test.ivf" || plan.destination.format != av.FormatIVF {
		t.Fatalf("destination = %+v", plan.destination)
	}
	if len(plan.ops) != 3 {
		t.Fatalf("ops = %+v", plan.ops)
	}
	if plan.ops[0].kind != "tap" || plan.ops[0].name != "frames" {
		t.Fatalf("tap op = %+v", plan.ops[0])
	}
	if plan.ops[1].kind != "resize" || plan.ops[1].width != 640 || plan.ops[1].height != 360 {
		t.Fatalf("resize op = %+v", plan.ops[1])
	}
	encoded := plan.ops[2].codec
	if encoded.ID != av.CodecAV1 ||
		encoded.Type != av.MediaVideo ||
		encoded.Settings.Bitrate != 2_000_000 ||
		encoded.Settings.KeyframeInterval != 30 ||
		encoded.Settings.Profile != "main" ||
		encoded.Settings.Level != "5.1" ||
		encoded.Settings.Framerate.Value != 100 ||
		encoded.Settings.Framerate.Base.Den != 2997 ||
		encoded.Settings.Custom["tune"] != "zerolatency" {
		t.Fatalf("encoded = %+v", encoded)
	}
}

func TestRunPipelineParserHelperEdges(t *testing.T) {
	width, height, err := parseSize("640X360")
	if err != nil || width != 640 || height != 360 {
		t.Fatalf("parseSize = %dx%d, %v", width, height, err)
	}
	fps, err := parseFPS("29.97")
	if err != nil || fps != (fpsValue{num: 2997, den: 100}) || fps.String() != "2997/100" {
		t.Fatalf("parseFPS decimal = %+v, %v", fps, err)
	}
	fps, err = parseFPS("60000/2002")
	if err != nil || fps != (fpsValue{num: 30000, den: 1001}) {
		t.Fatalf("parseFPS fraction = %+v, %v", fps, err)
	}
	if got := gcd(-18, 24); got != 6 {
		t.Fatalf("gcd = %d, want 6", got)
	}
	if frames := framesForDuration(time.Millisecond, fpsValue{num: 30000, den: 1001}); frames != 1 {
		t.Fatalf("framesForDuration = %d, want minimum one frame", frames)
	}
	pixelFormat, err := parseGeneratedPixelFormat("YUV420P")
	if err != nil || pixelFormat != av.PixelFormatI420 {
		t.Fatalf("pixel format = %q, %v", pixelFormat, err)
	}
	if _, err := parseGeneratedPixelFormat("rgba"); err == nil || !strings.Contains(err.Error(), "i420/yuv420p") {
		t.Fatalf("invalid pixel format err = %v", err)
	}
	for _, text := range []string{
		`testsrc video duration=0s ! av1enc ! filesink location=/tmp/out.ivf format=ivf`,
		`testsrc video size=640 ! av1enc ! filesink location=/tmp/out.ivf format=ivf`,
		`testsrc video pix_fmt=rgba ! av1enc ! filesink location=/tmp/out.ivf format=ivf`,
	} {
		if _, err := parseRunPipeline(text); err == nil {
			t.Fatalf("parseRunPipeline(%q) succeeded unexpectedly", text)
		}
	}
}

func TestRunPipelineParserCarriesCustomEncoderSettings(t *testing.T) {
	plan, err := parseRunPipeline(`testsrc video width=16 height=16 frames=1 ! encode codec=x_acme media=video bitrate=1.5M lookahead=deep aq-mode=cyclic ! filesink location=/tmp/acme.bin format=ivf`)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.ops) != 1 || plan.ops[0].kind != "encode" {
		t.Fatalf("ops = %+v", plan.ops)
	}
	spec := plan.ops[0].codec
	if spec.ID != "x_acme" || spec.Type != av.MediaVideo || spec.Settings.Bitrate != 1_500_000 {
		t.Fatalf("codec spec = %+v", spec)
	}
	if spec.Settings.Custom["lookahead"] != "deep" || spec.Settings.Custom["aq-mode"] != "cyclic" {
		t.Fatalf("custom settings = %+v", spec.Settings.Custom)
	}
}

func TestRunReportsUsageErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		argv []string
		want string
		code int
	}{
		{name: "missing command", argv: nil, want: "usage: goav", code: 2},
		{name: "wrong command", argv: []string{"probe"}, want: "usage: goav", code: 2},
		{name: "missing control value", argv: []string{"ctl", "--control"}, want: "--control needs unix://PATH", code: 2},
		{name: "unknown local help", argv: []string{"ctl", "help", "nope"}, want: "unknown help topic", code: 2},
		{name: "missing remote control", argv: []string{"ctl", "graph"}, want: "missing --control", code: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := run(tc.argv, &stdout, &stderr)
			if code != tc.code || !strings.Contains(stderr.String(), tc.want) || stdout.Len() != 0 {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestRunSendsRawTextRequest(t *testing.T) {
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-run-graph-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := serveOneShot(t, socket, func(conn net.Conn) error {
		var request ctl.Request
		if err := json.NewDecoder(conn).Decode(&request); err != nil {
			return err
		}
		if request.Op != "graph" {
			return fmt.Errorf("request = %+v", request)
		}
		return json.NewEncoder(conn).Encode(ctl.SuccessResponse("flowchart LR\n"))
	})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"ctl", "--control", "unix://" + socket, "graph"}, &stdout, &stderr)
	if code != 0 || stdout.String() != "flowchart LR\n" || stderr.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func TestRawTextRequests(t *testing.T) {
	for _, op := range []string{"help", "graph", "flowchart"} {
		if !rawText(ctl.Request{Op: op}) {
			t.Fatalf("%s should be raw text", op)
		}
	}
	if rawText(ctl.Request{Op: "control"}) {
		t.Fatal("control response should stay JSON encoded")
	}
}

func TestSendRejectsUnsupportedAddress(t *testing.T) {
	err := send("tcp://127.0.0.1:9", ctl.Request{Op: "graph"})
	if err == nil || !strings.Contains(err.Error(), "expected unix://PATH") {
		t.Fatalf("send error = %v", err)
	}
}

func TestSendReturnsStructuredError(t *testing.T) {
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-error-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := serveOneShot(t, socket, func(conn net.Conn) error {
		var request ctl.Request
		if err := json.NewDecoder(conn).Decode(&request); err != nil {
			return err
		}
		return json.NewEncoder(conn).Encode(ctl.ErrorResponse(request.Op, fmt.Errorf("boom")))
	})

	err := send("unix://"+socket, ctl.Request{Op: "graph"})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("send error = %v", err)
	}
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func TestSendRawTextRejectsNonStringResult(t *testing.T) {
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-raw-type-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := serveOneShot(t, socket, func(conn net.Conn) error {
		var request ctl.Request
		if err := json.NewDecoder(conn).Decode(&request); err != nil {
			return err
		}
		return json.NewEncoder(conn).Encode(ctl.SuccessResponse(12))
	})

	err := send("unix://"+socket, ctl.Request{Op: "graph"})
	if err == nil || !strings.Contains(err.Error(), "want text") {
		t.Fatalf("send error = %v", err)
	}
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func TestSendFollowReturnsDecodeError(t *testing.T) {
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-follow-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	errC := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errC <- err
			return
		}
		defer conn.Close()
		var request ctl.Request
		if err := json.NewDecoder(conn).Decode(&request); err != nil {
			errC <- err
			return
		}
		if request.Op != "watch" || request.Args["follow"] != "true" {
			errC <- fmt.Errorf("request = %+v", request)
			return
		}
		if _, err := fmt.Fprintln(conn, `{"ok":true,"result":{"type":"stats"}}`); err != nil {
			errC <- err
			return
		}
		if _, err := fmt.Fprint(conn, `{"ok":`); err != nil {
			errC <- err
			return
		}
		errC <- nil
	}()

	err = send("unix://"+socket, ctl.Request{Op: "watch", Args: map[string]string{"follow": "true"}})
	if err == nil || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("send error = %v, want unexpected EOF", err)
	}
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func serveOneShot(t *testing.T, socket string, handle func(net.Conn) error) <-chan error {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	errC := make(chan error, 1)
	go func() {
		defer listener.Close()
		conn, err := listener.Accept()
		if err != nil {
			errC <- err
			return
		}
		defer conn.Close()
		errC <- handle(conn)
	}()
	return errC
}

func runCLI(t *testing.T, args ...string) string {
	t.Helper()
	cmdArgs := append([]string{"run", ".", "ctl"}, args...)
	cmd := exec.Command("go", cmdArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("goav ctl %v failed: %v\n%s", args, err, output)
	}
	return string(output)
}

func runLocalCtl(t *testing.T, socket string, args ...string) string {
	t.Helper()
	argv := append([]string{"ctl", "--control", "unix://" + socket}, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run(argv, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("goav %v failed with code %d\nstdout=%s\nstderr=%s", argv, code, stdout.String(), stderr.String())
	}
	return stdout.String()
}

func waitForRunSocket(t *testing.T, socket string, done <-chan int, stdout *bytes.Buffer, stderr *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.Dial("unix", socket)
		if err == nil {
			_ = conn.Close()
			return
		}
		select {
		case code := <-done:
			t.Fatalf("goav run stopped before creating socket with code %d\nstdout=%s\nstderr=%s", code, stdout.String(), stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %s was not ready\nstdout=%s\nstderr=%s", socket, stdout.String(), stderr.String())
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForCLISocket(t *testing.T, socket string, errC <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.Dial("unix", socket)
		if err == nil {
			_ = conn.Close()
			return
		}
		select {
		case err := <-errC:
			t.Fatalf("server stopped before creating socket: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %s was not ready", socket)
		}
		time.Sleep(time.Millisecond)
	}
}

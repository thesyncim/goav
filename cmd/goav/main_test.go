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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	av1adapter "github.com/thesyncim/goav/adapters/goav1"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	matroskaadapter "github.com/thesyncim/goav/container/matroska"
)

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

func TestRunGeneratedVideoAV1RealtimeString(t *testing.T) {
	out := filepath.Join(t.TempDir(), "generated av1.mkv")
	pipeline := fmt.Sprintf(
		`testsrc video name=fixture size=32x18 fps=30 frames=2 live pattern=bars ! resize 16x16 ! encode av1 bitrate=400k fps=30 keyframe_interval=1 min_qindex=20 max_qindex=180 tune=zerolatency ! file %q format=matroska`,
		out,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"run", pipeline}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	result := decodeRunSummary(t, stdout.Bytes())
	if result.Runtime != "default" || len(result.Wrote) != 1 || result.Wrote[0] != out {
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
		`testsrc video name=fixture size=32x18 fps=30 frames=2 live pattern=bars ! resize 16x16 ! encode av1 bitrate=400k fps=30 keyframe_interval=1 min_qindex=20 max_qindex=180 tune=zerolatency ! file %q format=ivf`,
		out,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"run", pipeline}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	result := decodeRunSummary(t, stdout.Bytes())
	if result.Runtime != "default" || len(result.Wrote) != 1 || result.Wrote[0] != out {
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

func TestRunGeneratedVideoAV1IVFExtensionInfersFormat(t *testing.T) {
	out := filepath.Join(t.TempDir(), "generated av1 inferred.ivf")
	pipeline := fmt.Sprintf(
		`testsrc video name=fixture size=32x18 fps=30 frames=2 pattern=bars ! resize 16x16 ! encode av1 bitrate=400k fps=30 keyframe_interval=1 min_qindex=20 max_qindex=180 tune=zerolatency ! file %q`,
		out,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"run", pipeline}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if result := decodeRunSummary(t, stdout.Bytes()); len(result.Wrote) != 1 || result.Wrote[0] != out {
		t.Fatalf("result = %+v, want the inferred IVF output", result)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 32 || string(data[:4]) != "DKIF" || string(data[8:12]) != "AV01" {
		t.Fatalf("output header = % x, want AV1 IVF", data[:min(len(data), 32)])
	}
	if got := binary.LittleEndian.Uint32(data[24:28]); got != 2 {
		t.Fatalf("IVF frame count = %d, want 2", got)
	}
	assertDecodableAV1IVF(t, data)
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
			Payload:  av.Buffer{Bytes: packet.Data},
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
			Payload:  av.Buffer{Bytes: data[offset : offset+size]},
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
		{name: "missing remote control", argv: []string{"ctl", "stats"}, want: "missing --control", code: 2},
		{name: "a value that is not key=value", argv: []string{"ctl", "--control", "unix:///x", "stats", "oops"}, want: `"oops" is not key=value`, code: 2},
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

type runSummary struct {
	Runtime string   `json:"runtime"`
	Wrote   []string `json:"wrote"`
	Control string   `json:"control"`
	Stopped bool     `json:"stopped"`
}

func decodeRunSummary(t *testing.T, data []byte) runSummary {
	t.Helper()
	var s runSummary
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("run summary JSON: %v\n%s", err, data)
	}
	return s
}

// TestProgramCommands pins describe, explain, elements, help, -f, and the
// refusal path of the program commands.
func TestProgramCommands(t *testing.T) {
	invoke := func(argv ...string) (int, string, string) {
		var stdout, stderr bytes.Buffer
		code := run(argv, &stdout, &stderr)
		return code, stdout.String(), stderr.String()
	}
	program := `v := file in.webm ! video ! decode
v ! resize 160x90 ! encode vp8 ! as small ! file small.webm
v ! encode vp8 ! as full ! file full.webm`
	code, out, errText := invoke("describe", program)
	if code != 0 || !strings.Contains(out, "stage resize-small [resize, 160x90, exact]") || !strings.Contains(out, "decode-video -> encode-full") {
		t.Fatalf("describe code=%d out=%s err=%s", code, out, errText)
	}
	if code, out, _ := invoke("describe", "--format", "mermaid", program); code != 0 || !strings.Contains(out, "flowchart") {
		t.Fatalf("describe mermaid code=%d out=%s", code, out)
	}
	file := filepath.Join(t.TempDir(), "ladder.goav")
	if err := os.WriteFile(file, []byte(program), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, out, errText := invoke("explain", "-f", file); code != 0 || !strings.Contains(out, `"destinations"`) {
		t.Fatalf("explain -f code=%d out=%s err=%s", code, out, errText)
	}
	if code, out, _ := invoke("elements"); code != 0 || !strings.Contains(out, "testsrc <video|audio>") || !strings.Contains(out, "joins\n  composite") {
		t.Fatalf("elements code=%d out=%s", code, out)
	}
	if code, out, _ := invoke("run", "--help"); code != 0 || !strings.Contains(out, "goav run [flags] '<program>'") || !strings.Contains(out, "--control unix://PATH") {
		t.Fatalf("run help code=%d out=%s", code, out)
	}
	code, out, errText = invoke("run", "file in.webm ! video ! decod ! file x.webm")
	if code != 2 || out != "" || !strings.Contains(errText, `unknown element "decod"`) || !strings.Contains(errText, `did you mean "decode"?`) {
		t.Fatalf("refusal code=%d out=%q err=%q", code, out, errText)
	}
}

// TestRunWritesAudioAndVideoIntoOneFile pins the README clip: generated
// audio and video ending at one path mux into one file.
func TestRunWritesAudioAndVideoIntoOneFile(t *testing.T) {
	out := filepath.Join(t.TempDir(), "clip.webm")
	var stdout, stderr bytes.Buffer
	program := fmt.Sprintf("testsrc video size=64x48 frames=3 ! encode vp8 ! file %[1]q\ntestsrc audio frames=3 ! encode opus ! file %[1]q", out)
	if code := run([]string{"run", program}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	demuxer, err := matroskaadapter.NewDemuxer(bytes.NewReader(data), matroskaadapter.DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(demuxer.Tracks()); got != 2 {
		t.Fatalf("clip has %d tracks, want video and audio", got)
	}
}

// TestRunServesTheControlPlane drives a live goav run over its control
// socket with goav ctl: inspect, controls, attach a program at a tap, see it
// in the graph, detach it, and stop the task.
func TestRunServesTheControlPlane(t *testing.T) {
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-run-control-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	out := filepath.Join(t.TempDir(), "controlled.ivf")
	// Attached programs touch files only under the control directory.
	dir := t.TempDir()
	program := fmt.Sprintf(`testsrc video name=fixture size=32x18 fps=30 frames=900 live pattern=bars ! tap frames ! encode av1 bitrate=200k fps=30 keyframe_interval=1 ! file %q format=ivf`, out)

	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		// The real-clock runtime keeps the live source running through the
		// whole conversation; a fake clock would finish it at once.
		done <- run([]string{"run", "--runtime", "default", "--control", "unix://" + socket, "--control-dir", dir, program}, &stdout, &stderr)
	}()
	waitForRunSocket(t, socket, done, &stdout, &stderr)

	if taps := runLocalCtl(t, socket, "taps"); !strings.Contains(taps, `"name": "frames"`) {
		t.Fatalf("taps = %s", taps)
	}
	if graph := runLocalCtl(t, socket, "describe", "format=mermaid"); !strings.Contains(graph, "flowchart LR") {
		t.Fatalf("describe = %s", graph)
	}
	runLocalCtl(t, socket, "rate", "value=0.5")
	runLocalCtl(t, socket, "seek", "position=100ms", "source=fixture")

	preview := filepath.Join(dir, "preview.ivf")
	var refused bytes.Buffer
	if code := run([]string{"ctl", "--control", "unix://" + socket, "attach", `tap frames ! encode av1 ! as escape ! file ../escape.ivf format=ivf`}, io.Discard, &refused); code != 1 {
		t.Fatalf("attach outside the control directory: code %d, stderr %s", code, refused.String())
	}
	attached := runLocalCtl(t, socket, "attach",
		`tap frames ! resize 16x16 ! encode av1 bitrate=120k fps=30 keyframe_interval=1 ! as preview ! file preview.ivf format=ivf`)
	var att struct{ Attachment string }
	if err := json.Unmarshal([]byte(attached), &att); err != nil || att.Attachment != "preview" {
		t.Fatalf("attach = %s (%v)", attached, err)
	}
	if graph := runLocalCtl(t, socket, "describe", "format=mermaid"); !strings.Contains(graph, "branch=preview (attached)") {
		t.Fatalf("describe after attach = %s", graph)
	}
	runLocalCtl(t, socket, "detach", att.Attachment)
	runLocalCtl(t, socket, "stop")

	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("goav run code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("goav run did not stop; stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if result := decodeRunSummary(t, stdout.Bytes()); result.Control != "unix://"+socket || !slices.Contains(result.Wrote, preview) || !slices.Contains(result.Wrote, out) {
		t.Fatalf("run summary = %+v, want the control address and both files", result)
	}

	// A refusal comes back with goav's own message: here, the program's.
	var ctlOut, ctlErr bytes.Buffer
	if code := run([]string{"ctl", "--control", "unix:///nonexistent.sock", "stats"}, &ctlOut, &ctlErr); code != 1 {
		t.Fatalf("ctl against a missing socket: code %d", code)
	}
}

// TestRunServesLowLatencyHLS: goav run --serve is an LL-HLS origin in one
// command: a blocking reload for a part of the live program comes back once
// the part exists.
func TestRunServesLowLatencyHLS(t *testing.T) {
	var stdout, stderr syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- run([]string{"run", "--serve", "127.0.0.1:0",
			"testsrc video size=64x48 fps=25 duration=2s live ! encode vp8 keyframe_interval=25 ! hls live.m3u8 segment=1s parts=200ms"}, &stdout, &stderr)
	}()
	var base string
	for deadline := time.Now().Add(5 * time.Second); base == ""; {
		if m := regexp.MustCompile(`serving HLS at (http://\S+/)`).FindStringSubmatch(stderr.String()); m != nil {
			base = m[1]
		} else if time.Now().After(deadline) {
			t.Fatalf("no serving address; stderr=%q", stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	resp, err := http.Get(base + "live.m3u8?_HLS_msn=1&_HLS_part=0")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `#EXT-X-PART:DURATION=0.200,URI="live-00001.0.m4s",INDEPENDENT=YES`) {
		t.Fatalf("blocking reload = %d:\n%s", resp.StatusCode, body)
	}
	if code := <-done; code != 0 {
		t.Fatalf("goav run = %d; stderr=%q", code, stderr.String())
	}
}

// syncBuffer is a bytes.Buffer safe to read while run writes it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestControlAddressDefaultsToUnix(t *testing.T) {
	for in, want := range map[string]string{
		"/run/goav.sock":        "unix:///run/goav.sock",
		"ctl.sock":              "unix://ctl.sock",
		"unix:///run/goav.sock": "unix:///run/goav.sock",
		"tcp://127.0.0.1:7000":  "tcp://127.0.0.1:7000",
		"":                      "",
	} {
		if got := controlAddress(in); got != want {
			t.Errorf("controlAddress(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestRunReadsURLs pins that goav run grants the network schemes: a program
// reads an https-style URL (here plain http) where it would read a file.
func TestRunReadsURLs(t *testing.T) {
	clip := make([]byte, 32, 47)
	copy(clip, "DKIF")
	binary.LittleEndian.PutUint16(clip[6:], 32)
	copy(clip[8:], "VP80")
	binary.LittleEndian.PutUint16(clip[12:], 16)
	binary.LittleEndian.PutUint16(clip[14:], 16)
	binary.LittleEndian.PutUint32(clip[16:], 1000)
	binary.LittleEndian.PutUint32(clip[20:], 1)
	clip = binary.LittleEndian.AppendUint32(clip, 3)
	clip = append(clip, make([]byte, 8)...)
	clip = append(clip, 0x10, 0x20, 0x30)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "clip.ivf", time.Unix(1700000000, 0), bytes.NewReader(clip))
	}))
	defer srv.Close()
	out := filepath.Join(t.TempDir(), "copy.ivf")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"run", fmt.Sprintf("%q ! video ! file %q", srv.URL+"/clip.ivf", out)}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	if data, err := os.ReadFile(out); err != nil || !bytes.HasPrefix(data, []byte("DKIF")) {
		t.Fatalf("copy.ivf = %v", err)
	}
}

// TestRunGrantsWHIP pins that the CLI grants and registers the WHIP client:
// a program publishes test video to a whips URL, encoded to VP8 for it, and
// the token in the URL stays out of the graph.
func TestRunGrantsWHIP(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"describe", `testsrc video size=64x64 ! uri "whips://media.example/live?token=SECRET"`}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	text := stdout.String()
	if !strings.Contains(text, "codec=vp8") || !strings.Contains(text, "sink whip [whip publish]") || strings.Contains(text, "SECRET") {
		t.Fatalf("describe =\n%s", text)
	}
}

// TestRunControlOverTCPNeedsTheToken pins the tcp control plane's defaults:
// loopback only unless --control-remote, and every client presents the
// token goav run writes owner-only to the token file.
func TestRunControlOverTCPNeedsTheToken(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"run", "--control", "tcp://0.0.0.0:1", "testsrc video live ! null"}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "--control-remote") {
		t.Fatalf("all-interfaces tcp: code %d, stderr %q", code, stderr.String())
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := "tcp://" + l.Addr().String()
	l.Close()
	tokenFile := filepath.Join(t.TempDir(), "ctl.token")
	stdout.Reset()
	stderr.Reset()
	done := make(chan int, 1)
	go func() {
		done <- run([]string{"run", "--control", address, "--control-token", tokenFile, "--control-dir", t.TempDir(), "testsrc video size=16x16 live ! tap live ! null"}, &stdout, &stderr)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if info, err := os.Stat(tokenFile); err == nil {
			if info.Mode().Perm() != 0o600 {
				t.Fatalf("token file mode %v, want 0600", info.Mode().Perm())
			}
			if conn, err := net.Dial("tcp", strings.TrimPrefix(address, "tcp://")); err == nil {
				conn.Close()
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("tcp control never came up; stderr=%q", stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	wrong := filepath.Join(t.TempDir(), "wrong.token")
	if err := os.WriteFile(wrong, []byte("0123456789abcdef0123456789abcdef\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var ctlErr bytes.Buffer
	if code := run([]string{"ctl", "--control", address, "--control-token", wrong, "taps"}, io.Discard, &ctlErr); code != 1 || !strings.Contains(ctlErr.String(), "unauthorized") {
		t.Fatalf("wrong token: code %d, stderr %q", code, ctlErr.String())
	}
	var taps bytes.Buffer
	if code := run([]string{"ctl", "--control", address, "--control-token", tokenFile, "taps"}, &taps, io.Discard); code != 0 || !strings.Contains(taps.String(), `"name": "live"`) {
		t.Fatalf("with the token: code %d, taps %s", code, taps.String())
	}
	if code := run([]string{"ctl", "--control", address, "--control-token", tokenFile, "stop"}, io.Discard, io.Discard); code != 0 {
		t.Fatalf("stop: code %d", code)
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("goav run code %d, stderr %q", code, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("goav run did not stop")
	}
	if _, err := os.Stat(tokenFile); !os.IsNotExist(err) {
		t.Fatalf("token file left behind: %v", err)
	}
}

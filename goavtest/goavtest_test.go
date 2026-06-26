package goavtest_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	goavruntime "github.com/thesyncim/goav/runtime"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// TestReadmeMixExample pins the README "Testing Your Pipeline" snippet
// verbatim: two deterministic audio inputs mixed into a collector on the
// goavtest runtime.
func TestReadmeMixExample(t *testing.T) {
	ctx := context.Background()
	out := goavtest.NewCollector()
	task, err := goav.Mix(
		goav.From(goavtest.Audio(48000, 1, []int16{100}, []int16{200})).Audio(),
		goav.From(goavtest.Audio(48000, 1, []int16{50}, []int16{-50})).Audio(),
	).To(out.Sink()).UseRuntime(goavtest.Runtime()).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := out.S16(), [][]int16{{150}, {150}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("out.S16() = %v, want %v", got, want)
	}
}

func TestAudioStampsCoherentPTS(t *testing.T) {
	ctx := context.Background()
	out := goavtest.NewCollector()
	err := goav.From(goavtest.Audio(48000, 2, []int16{1, 2, 3, 4}, []int16{5, 6})).
		Audio().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	frames := out.Frames()
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want 2", len(frames))
	}
	base := av.TimeBase{Num: 1, Den: 48000}
	// Frame 0: two stereo sample pairs starting at 0; frame 1 starts where it ended.
	if frames[0].PTS != (av.Timestamp{Value: 0, Base: base}) || frames[0].Duration != (av.Duration{Value: 2, Base: base}) {
		t.Fatalf("frame 0 timing = %+v/%+v, want 0/2 in 1/48000", frames[0].PTS, frames[0].Duration)
	}
	if frames[1].PTS != (av.Timestamp{Value: 2, Base: base}) || frames[1].Duration != (av.Duration{Value: 1, Base: base}) {
		t.Fatalf("frame 1 timing = %+v/%+v, want 2/1 in 1/48000", frames[1].PTS, frames[1].Duration)
	}
	if got, want := out.S16(), [][]int16{{1, 2, 3, 4}, {5, 6}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("samples = %v, want %v", got, want)
	}
}

func TestVideoFramesIdentifiableFromPixels(t *testing.T) {
	ctx := context.Background()
	out := goavtest.NewCollector()
	err := goav.From(goavtest.Video(4, 4, 3)).
		Video().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	frames := out.Frames()
	if len(frames) != 3 {
		t.Fatalf("frames = %d, want 3", len(frames))
	}
	base := av.TimeBase{Num: 1, Den: 30}
	for n, frame := range frames {
		if frame.Video == nil || frame.Video.Width != 4 || frame.Video.Height != 4 {
			t.Fatalf("frame %d geometry = %+v, want 4x4", n, frame.Video)
		}
		if got := frame.Planes[0].Buffer.Bytes[0]; got != byte(n) {
			t.Fatalf("frame %d luma = %d, want %d (frames are numbered in pixels)", n, got, n)
		}
		if frame.PTS != (av.Timestamp{Value: int64(n), Base: base}) {
			t.Fatalf("frame %d PTS = %+v, want %d in 1/30", n, frame.PTS, n)
		}
	}
}

func TestPacketsHonorKeyframeFlagsAndPTS(t *testing.T) {
	ctx := context.Background()
	base := av.TimeBase{Num: 1, Den: 90000}
	out := goavtest.NewCollector()
	err := goav.From(goavtest.Packets(av.CodecVP8,
		av.Packet{Keyframe: true, PTS: av.Timestamp{Value: 0, Base: base}, Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}},
		av.Packet{Keyframe: false, PTS: av.Timestamp{Value: 3000, Base: base}, Payload: av.Buffer{Bytes: []byte{2}, Ownership: av.BufferImmutable}},
	)).
		Video().Copy().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	packets, err := out.WaitPackets(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(packets) != 2 {
		t.Fatalf("packets = %d, want 2", len(packets))
	}
	if !packets[0].Keyframe || packets[1].Keyframe {
		t.Fatalf("keyframe flags = %v/%v, want true/false", packets[0].Keyframe, packets[1].Keyframe)
	}
	if packets[1].PTS != (av.Timestamp{Value: 3000, Base: base}) {
		t.Fatalf("packet 1 PTS = %+v, want 3000 in 1/90000", packets[1].PTS)
	}
	if packets[0].Payload.Bytes[0] != 1 || packets[1].Payload.Bytes[0] != 2 {
		t.Fatalf("payloads = %v/%v, want 1/2", packets[0].Payload.Bytes, packets[1].Payload.Bytes)
	}
}

// TestCodecRoundTripIsByteFaithful proves the passthrough codec contract:
// encode in one task, feed the collected packets into a second task, decode,
// and the samples come back bit-identical.
func TestCodecRoundTripIsByteFaithful(t *testing.T) {
	ctx := context.Background()
	encoded := goavtest.NewCollector()
	err := goav.From(goavtest.Audio(48000, 1, []int16{100, -200}, []int16{300})).
		Audio().
		Encode(codec.Opus()).
		To(encoded.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	collected := encoded.Packets()
	if len(collected) != 2 {
		t.Fatalf("encoded packets = %d, want 2", len(collected))
	}
	if !collected[0].Keyframe {
		t.Fatal("passthrough packets must be keyframes")
	}

	packets := make([]av.Packet, 0, len(collected))
	for _, packet := range collected {
		packets = append(packets, *packet)
	}
	decoded := goavtest.NewCollector()
	err = goav.From(goavtest.Packets(av.CodecOpus, packets...)).
		Audio().Decode().
		To(decoded.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := decoded.S16(), [][]int16{{100, -200}, {300}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded samples = %v, want %v (byte-faithful round trip)", got, want)
	}
}

// TestFormatRoundTripThroughFile proves the fake container contract: encode
// and mux into a bytes.Buffer through goav.File, then demux and decode it
// back through goav.FileInput — no real container anywhere.
func TestFormatRoundTripThroughFile(t *testing.T) {
	ctx := context.Background()
	var file bytes.Buffer
	err := goav.From(goavtest.Audio(48000, 1, []int16{1, 2, 3}, []int16{4, 5, 6})).
		Audio().
		Encode(codec.Opus()).
		To(goav.Write("roundtrip.ogg", &file)).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if file.Len() == 0 {
		t.Fatal("fake container wrote no bytes")
	}

	out := goavtest.NewCollector()
	err = goav.From(goav.FileInput("roundtrip.ogg", bytes.NewReader(file.Bytes()))).
		Audio().Decode().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime(goavruntime.WithRealtime(false))).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := out.S16(), [][]int16{{1, 2, 3}, {4, 5, 6}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("round-tripped samples = %v, want %v", got, want)
	}
}

func TestCollectorWaitEventsObservesEventSource(t *testing.T) {
	ctx := context.Background()
	out := goavtest.NewCollector()
	input := goav.Source("diagnostics",
		shape.Event(),
		func(_ context.Context, push source.Push) error {
			if _, err := push.Event(av.Event{Type: av.EventStats, Reason: "ready"}); err != nil {
				return err
			}
			return push.EOS()
		},
	)
	if err := goav.From(input).To(out.Sink()).UseRuntime(goavtest.Runtime()).Run(ctx); err != nil {
		t.Fatal(err)
	}
	events, err := out.WaitEvents(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 ||
		events[0].Type != av.EventStats ||
		events[0].Reason != "ready" ||
		events[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v, want stats then EOS", events)
	}
}

func TestClockRecordsSleepsAndAdvances(t *testing.T) {
	clock := goavtest.NewClock()
	if err := clock.Sleep(context.Background(), 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Millisecond)
	if got := clock.Now(); got != 25*time.Millisecond {
		t.Fatalf("Now = %v, want 25ms", got)
	}
	if got, want := clock.Sleeps(), []time.Duration{20 * time.Millisecond}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Sleeps = %v, want %v", got, want)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := clock.Sleep(cancelled, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep on done ctx = %v, want context.Canceled", err)
	}
}

// TestRuntimePacesFileInputOnFakeClock pins the determinism story end to end:
// a realtime file input paced through an injected goavtest.Clock runs
// instantly while the exact media-time sleeps the pacer requested are
// recorded.
func TestRuntimePacesFileInputOnFakeClock(t *testing.T) {
	ctx := context.Background()
	var file bytes.Buffer
	// Two 960-sample frames at 48kHz: media times 0 and 20ms.
	err := goav.From(goavtest.Audio(48000, 1, make([]int16, 960), make([]int16, 960))).
		Audio().
		Encode(codec.Opus()).
		To(goav.Write("paced.ogg", &file)).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}

	clock := goavtest.NewClock()
	out := goavtest.NewCollector()
	err = goav.From(goav.FileInput("paced.ogg", bytes.NewReader(file.Bytes()))).
		Audio().Copy().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime(goavruntime.WithClock(clock))).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Packets()) != 2 {
		t.Fatalf("packets = %d, want 2", len(out.Packets()))
	}
	// The first packet anchors; the second waits its 20ms media-time delta —
	// on the fake clock, recorded instead of slept.
	if got, want := clock.Sleeps(), []time.Duration{20 * time.Millisecond}; !reflect.DeepEqual(got, want) {
		t.Fatalf("paced sleeps = %v, want %v", got, want)
	}
}

// TestCollectorWaitObservesLivePipeline runs a never-ending LiveAudio source
// and uses Wait to stop once enough frames were observed — the
// wait-until-observed pattern for control-plane tests.
func TestCollectorWaitObservesLivePipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out := goavtest.NewCollector()
	task, err := goav.From(goavtest.LiveAudio("live", 48000, 1, []int16{7})).
		Audio().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	frames, err := out.WaitFrames(ctx, 3)
	if err != nil {
		t.Fatalf("WaitFrames: %v", err)
	}
	if len(frames) < 3 {
		t.Fatalf("frames = %d, want at least 3", len(frames))
	}
	for _, samples := range out.S16() {
		if samples[0] != 7 {
			t.Fatalf("live samples = %v, want 7s", samples)
		}
	}
	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}

// TestRuntimeOptionsAreLastWins overrides one default fake with a custom
// encoder through Runtime(opts...): the layered option must win.
func TestRuntimeOptionsAreLastWins(t *testing.T) {
	ctx := context.Background()
	custom := &countingEncoderFactory{}
	out := goavtest.NewCollector()
	err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().
		Encode(codec.Opus()).
		To(out.Sink()).
		UseRuntime(goavtest.Runtime(goavruntime.WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, custom))).
		Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if custom.encoders == 0 {
		t.Fatal("layered encoder option did not override the goavtest default")
	}
}

type countingEncoderFactory struct {
	encoders int
}

func (f *countingEncoderFactory) NewEncoder(_ context.Context, _ codec.EncodeConfig) (codec.Encoder, error) {
	f.encoders++
	return countingEncoder{}, nil
}

type countingEncoder struct{}

func (countingEncoder) Descriptor() codec.Descriptor                   { return codec.Descriptor{ID: av.CodecOpus} }
func (countingEncoder) Open(context.Context, codec.EncodeConfig) error { return nil }
func (countingEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if frame == nil || len(out.Packets) == cap(out.Packets) {
		return nil
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	out.Packets[index].Reset()
	out.Packets[index].StreamID = frame.StreamID
	out.Packets[index].Payload.Bytes = append(out.Packets[index].Payload.Bytes, 9)
	return nil
}
func (countingEncoder) FlushInto(context.Context, *codec.EncodeResult) error { return nil }
func (countingEncoder) HandleEvent(context.Context, *av.Event) error         { return nil }
func (countingEncoder) Close() error                                         { return nil }

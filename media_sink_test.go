package goav_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// mediaSink is a media destination with every optional capability: it
// declares what it accepts, learns its streams, and binds the control path.
type mediaSink struct {
	accepts shape.Set

	mu      sync.Mutex
	streams []av.Stream
	opens   int
	packets []av.Packet
	closes  int
	control func(context.Context, control.Control) error
	arrived chan struct{}
}

func newMediaSink(accepts ...shape.Spec) *mediaSink {
	return &mediaSink{accepts: accepts, arrived: make(chan struct{}, 1)}
}

func (s *mediaSink) Name() string           { return "media-out" }
func (s *mediaSink) InputShapes() shape.Set { return s.accepts }

func (s *mediaSink) OpenStreams(_ context.Context, streams []av.Stream) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams, s.opens = streams, s.opens+1
	return nil
}

func (s *mediaSink) UseControl(fn func(context.Context, control.Control) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.control = fn
}

func (s *mediaSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg.Kind != pipeline.MessagePacket {
		return nil
	}
	s.mu.Lock()
	s.packets = append(s.packets, av.Packet{StreamID: msg.Packet.StreamID, Keyframe: msg.Packet.Keyframe})
	s.mu.Unlock()
	select {
	case s.arrived <- struct{}{}:
	default:
	}
	return nil
}

func (s *mediaSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	return nil
}

// TestMediaSinkGetsTheEncodeItNeeds pins codec negotiation with a media
// destination: frames routed to a sink that takes H264 or VP8 packets get a
// VP8 encode (the first codec the runtime can encode), shown by Describe and
// Explain; the sink learns the encoded stream before its first packet, and
// its control path reaches the encoder.
func TestMediaSinkGetsTheEncodeItNeeds(t *testing.T) {
	rt := bundle.MustNew()
	sink := newMediaSink(shape.Packet(av.MediaVideo, av.CodecH264), shape.Packet(av.MediaVideo, av.CodecVP8))
	out := goavtest.LiveVideo("cam", 64, 64).Video().To(goav.Sink(sink))
	spec, err := rt.Describe(out)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, node := range spec.Nodes {
		found = found || strings.HasPrefix(node.Name, "encode-") && strings.Contains(node.Detail, "codec=vp8")
	}
	if !found {
		t.Fatalf("no vp8 encode in %+v", spec.Nodes)
	}
	report, err := rt.Explain(context.Background(), out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmtJSON(t, report), "inserted encode-vp8 before the destination, which takes h264, vp8") {
		t.Fatalf("explain names no inserted encode: %s", fmtJSON(t, report))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	task, err := rt.Build(ctx, out)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	select {
	case <-sink.arrived:
	case <-ctx.Done():
		t.Fatal("no packet")
	}
	sink.mu.Lock()
	streams, ctl := sink.streams, sink.control
	first := sink.packets[0]
	sink.mu.Unlock()
	if len(streams) != 1 || streams[0].Codec.ID != av.CodecVP8 || first.StreamID != streams[0].ID {
		t.Fatalf("streams = %+v, first packet %+v", streams, first)
	}
	if err := ctl(ctx, control.Keyframe(streams[0].ID)); err != nil {
		t.Fatalf("keyframe control: %v", err)
	}
	cancel()
	<-done
	task.Close()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.opens != 1 || sink.closes != 1 {
		t.Fatalf("opens=%d closes=%d", sink.opens, sink.closes)
	}
}

// TestMediaSinkRefusesWhatItCannotTake pins the refusals with their exact
// fixes: packets of a codec the sink does not take (decode, and goav
// encodes), and frames no registered encoder can turn into a codec it takes.
func TestMediaSinkRefusesWhatItCannotTake(t *testing.T) {
	rt := bundle.MustNew()
	aac := goavtest.Packets(av.CodecAAC, av.Packet{Keyframe: true, Payload: av.Buffer{Bytes: []byte{1}}})
	_, err := rt.Build(context.Background(), aac.Audio().To(goav.Sink(newMediaSink(shape.Packet(av.MediaAudio, av.CodecOpus)))))
	assertSinkRefusal(t, err, "decode the stream so goav encodes it to opus: .Decode() before the destination")

	frames := goavtest.Video(64, 64, 1)
	_, err = rt.Build(context.Background(), frames.Video().To(goav.Sink(newMediaSink(shape.Packet(av.MediaVideo, av.CodecH264)))))
	assertSinkRefusal(t, err, "encode to a codec the destination takes: .Encode(codec.H264())")
}

func assertSinkRefusal(t *testing.T, err error, fix string) {
	t.Helper()
	var refusal *goav.BuildError
	if !errors.As(err, &refusal) || refusal.Code != errcode.Code("destination_shape_mismatch") {
		t.Fatalf("err = %v, want a destination_shape_mismatch refusal", err)
	}
	for _, line := range refusal.FixLines() {
		if line == fix {
			return
		}
	}
	t.Fatalf("fixes = %q, want %q", refusal.FixLines(), fix)
}

func fmtJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestMediaSinkRefusesARemuxItCannotTake pins the refusal of a whole input
// copied into a media sink that does not take one of its streams.
func TestMediaSinkRefusesARemuxItCannotTake(t *testing.T) {
	rt := bundle.MustNew()
	aac := goavtest.Packets(av.CodecAAC, av.Packet{Keyframe: true, Payload: av.Buffer{Bytes: []byte{1}}})
	_, err := rt.Build(context.Background(), aac.To(goav.Sink(newMediaSink(shape.Packet(av.MediaAudio, av.CodecOpus)))))
	assertSinkRefusal(t, err, "route the input's streams one by one, decoding the aac one: input.Audio().Decode().To(destination)")
}

// TestMediaSinkAttachRefusesFramesWithTheEncode pins the attach-time
// refusal: frames from a running task's tap need the encode a media sink
// takes, which the fix names.
func TestMediaSinkAttachRefusesFramesWithTheEncode(t *testing.T) {
	rt := bundle.MustNew()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	task, err := rt.Build(ctx, goavtest.LiveVideo("cam", 64, 64).Video().Tap("raw").To(goav.Sink(newMediaSink())))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	_, err = task.Attach(ctx, task.Tap("raw").To(goav.Sink(newMediaSink(shape.Packet(av.MediaVideo, av.CodecVP8)))))
	assertSinkRefusal(t, err, "encode to a codec the destination takes: .Encode(codec.VP8())")
	cancel()
	<-done
}

// TestMediaSinkTakesARemux pins a whole file copied into a media sink: it
// opens with the file's streams, codecs and H264 extradata included, and
// every packet names one of them.
func TestMediaSinkTakesARemux(t *testing.T) {
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	sink := newMediaSink(shape.Packet(av.MediaVideo, av.CodecH264), shape.Packet(av.MediaAudio, av.CodecAAC))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := offlineRuntime().Run(ctx, goav.FileInput("in.mp4", bytes.NewReader(data)).To(goav.Sink(sink))); err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	ids := map[av.StreamID]av.CodecID{}
	for _, stream := range sink.streams {
		ids[stream.ID] = stream.Codec.ID
		if stream.Codec.ID == av.CodecH264 && len(stream.Codec.ExtraData.Bytes) == 0 {
			t.Fatalf("h264 stream %s opened without its extradata", stream.ID)
		}
	}
	if len(ids) != 2 || len(sink.packets) == 0 {
		t.Fatalf("streams = %v, %d packets", ids, len(sink.packets))
	}
	for _, packet := range sink.packets {
		if _, ok := ids[packet.StreamID]; !ok {
			t.Fatalf("packet for %q, opened %v", packet.StreamID, ids)
		}
	}
}

// liveMediaSink is a media sink that consumes in real time.
type liveMediaSink struct{ *mediaSink }

func (liveMediaSink) Live() bool { return true }

// TestLiveMediaSinkPacesAFile pins pacing for a live destination: an MP4
// copied whole into it on an offline runtime plans realtime and plays at its
// media time, while the same copy into a sink that is not live runs at full
// speed.
func TestLiveMediaSinkPacesAFile(t *testing.T) {
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	accepts := []shape.Spec{shape.Packet(av.MediaVideo, av.CodecH264), shape.Packet(av.MediaAudio, av.CodecAAC)}
	run := func(sink pipeline.Sink) time.Duration {
		t.Helper()
		out := goav.FileInput("in.mp4", bytes.NewReader(data)).To(goav.Sink(sink))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		start := time.Now()
		if err := offlineRuntime().Run(ctx, out); err != nil {
			t.Fatal(err)
		}
		return time.Since(start)
	}
	live := liveMediaSink{newMediaSink(accepts...)}
	spec, err := offlineRuntime().Describe(goav.FileInput("in.mp4", bytes.NewReader(data)).To(goav.Sink(live)))
	if err != nil || !spec.Realtime {
		t.Fatalf("describe realtime=%v err=%v, want a realtime plan", spec.Realtime, err)
	}
	paced, fast := run(liveMediaSink{newMediaSink(accepts...)}), run(newMediaSink(accepts...))
	if paced < 300*time.Millisecond || fast > paced/2 {
		t.Fatalf("live sink took %s, plain sink %s: want the live one paced at the file's media time", paced, fast)
	}
}

// TestMediaSinkLearnsTheConfiguredBitrate pins the bitrate a recipe gives
// an encoder riding its stream (av.MetadataBitrate), the ceiling a media
// sink holds bandwidth estimates to.
func TestMediaSinkLearnsTheConfiguredBitrate(t *testing.T) {
	sink := newMediaSink(shape.Packet(av.MediaVideo, av.CodecVP8))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := bundle.MustNew().Run(ctx, goavtest.Video(64, 64, 2).Video().Encode(codec.VP8(codec.Bitrate(300_000))).To(goav.Sink(sink))); err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.streams) != 1 || sink.streams[0].Metadata[av.MetadataBitrate] != "300000" {
		t.Fatalf("streams = %+v, want the configured bitrate on the encoded stream", sink.streams)
	}
}

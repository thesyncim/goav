package goav_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
)

// flakyLive is a live provider whose first fails sessions each deliver one
// packet (none, when silent) and then drop; the next delivers one and ends.
type flakyLive struct {
	mu    sync.Mutex
	opens int
	fails int
	// silent sessions after the first drop before delivering anything.
	silent bool
	// renamed sessions after the first carry their stream as "audio-b".
	renamed bool
}

func (f *flakyLive) SourceShape() shape.Spec {
	return shape.Spec{Domain: shape.DomainPacket, MediaKind: av.MediaAudio, Codec: av.CodecOpus, Realtime: true}
}

func (f *flakyLive) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opens++
	id := av.StreamID("audio")
	if f.renamed && f.opens > 1 {
		id = "audio-b"
	}
	stream := av.Stream{ID: id, Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio, SampleRate: 48000, Channels: 2}}
	return &flakySession{drop: f.opens <= f.fails, silent: f.silent && f.opens > 1, n: f.opens, id: id}, []av.Stream{stream}, nil
}

type flakySession struct {
	drop, silent bool
	n            int
	id           av.StreamID
}

var errDropped = errors.New("connection dropped")

func (s *flakySession) Name() string { return "live" }

func (s *flakySession) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if s.silent {
		return errDropped
	}
	packet := &av.Packet{StreamID: s.id, Type: av.MediaAudio, Payload: av.Buffer{Bytes: []byte{byte(s.n)}},
		PTS: av.Timestamp{Value: int64(s.n), Base: av.TimeBase{Num: 1, Den: 48000}}}
	if err := emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessagePacket, Packet: packet}); err != nil {
		return err
	}
	if s.drop {
		return errDropped
	}
	return nil
}

func (s *flakySession) Close() error { return nil }

// TestReconnectReopensALiveInput pins the reconnect policy: a live input
// that drops is reopened, each attempt and the recovery are published
// through Watch, and the media from every session reaches the sink.
func TestReconnectReopensALiveInput(t *testing.T) {
	live := &flakyLive{fails: 2}
	sink := goavtest.NewCollector()
	task, err := bundle.Build(context.Background(), goav.Input(live, goav.Reconnect(3, time.Millisecond)).Audio().To(sink.Sink()))
	if err != nil {
		t.Fatal(err)
	}
	watch := task.Watch(goav.WatchTypes(av.EventReconnect))
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	task.Close()
	var events []av.Event
	for event := range watch.Events() {
		events = append(events, event)
	}
	if live.opens != 3 || len(sink.Packets()) != 3 {
		t.Fatalf("opens = %d packets = %d, want three sessions", live.opens, len(sink.Packets()))
	}
	if len(events) != 4 || !errors.Is(events[0].Cause, errDropped) || events[0].Metadata["attempt"] != "1" ||
		events[1].Metadata["state"] != "reconnected" || events[2].Metadata["attempt"] != "1" ||
		!strings.Contains(events[0].Reason, "attempt 1 of 3") {
		t.Fatalf("events = %+v, want attempt and recovery per drop (count reset by delivered media)", events)
	}
}

// TestReconnectGivesUpAndRefusesFiles pins the attempt cap and that only
// live inputs take a reconnect policy.
func TestReconnectGivesUpAndRefusesFiles(t *testing.T) {
	live := &flakyLive{fails: 100, silent: true}
	err := bundle.Run(context.Background(), goav.Input(live, goav.Reconnect(2, time.Millisecond)).Audio().To(goavtest.NewCollector().Sink()))
	if !errors.Is(err, errDropped) || !strings.Contains(err.Error(), "gave up after 2 reconnect attempts") {
		t.Fatalf("err = %v, want the attempt cap", err)
	}

	_, err = bundle.Describe(goav.FileInput("in.ivf", bytes.NewReader(tinyIVF()), goav.Reconnect(1, 0)).Video().To(goavtest.NewCollector().Sink()))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_invalid" || !strings.Contains(buildErr.Reason, "applies to live inputs") {
		t.Fatalf("err = %v, want the live-only refusal", err)
	}
}

// TestReconnectReopensALiveByteInput pins the byte path: a live URL whose
// reader breaks is opened again through its scheme and demuxed afresh.
func TestReconnectReopensALiveByteInput(t *testing.T) {
	custom := &testScheme{data: tinyIVF(), live: true, breakFirst: true}
	rt := bundle.MustNew(runconfig.Scheme("cam", custom))
	sink := goavtest.NewCollector()
	if err := rt.Run(context.Background(), goav.URIInput("cam://door/feed.ivf", goav.Reconnect(1, time.Millisecond)).Video().To(sink.Sink())); err != nil {
		t.Fatal(err)
	}
	if custom.opened != 2 || len(sink.Packets()) != 2 {
		t.Fatalf("opened = %d packets = %d, want the feed read twice", custom.opened, len(sink.Packets()))
	}
}

// TestLiveMarksAnInputLive pins goav.Live: the input plans live before it
// opens, and a URI input marked live takes a reconnect policy.
func TestLiveMarksAnInputLive(t *testing.T) {
	report, err := bundle.Explain(context.Background(), goav.FileInput("pipe.ivf", bytes.NewReader(tinyIVF()), goav.Live()).Video().To(goavtest.NewCollector().Sink()))
	if err != nil || len(report.Inputs) != 1 || !report.Inputs[0].Realtime {
		t.Fatalf("report = %+v err = %v, want a live input", report.Inputs, err)
	}
	custom := &testScheme{data: tinyIVF(), breakFirst: true}
	rt := bundle.MustNew(runconfig.Scheme("cam", custom))
	sink := goavtest.NewCollector()
	if err := rt.Run(context.Background(), goav.URIInput("cam://door/feed.ivf", goav.Live(), goav.Reconnect(1, time.Millisecond)).Video().To(sink.Sink())); err != nil {
		t.Fatal(err)
	}
	if custom.opened != 2 {
		t.Fatalf("opened = %d, want the live feed reopened", custom.opened)
	}
}

// TestReconnectKeepsStreamIDs pins reconciliation: a reopened session whose
// stream comes back under a new id keeps the id downstream knows, and the
// recovery event reports the drift.
func TestReconnectKeepsStreamIDs(t *testing.T) {
	live := &flakyLive{fails: 1, renamed: true}
	sink := goavtest.NewCollector()
	task, err := bundle.Build(context.Background(), goav.Input(live, goav.Reconnect(2, time.Millisecond)).Audio().To(sink.Sink()))
	if err != nil {
		t.Fatal(err)
	}
	watch := task.Watch(goav.WatchTypes(av.EventReconnect))
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	task.Close()
	drift := ""
	for event := range watch.Events() {
		drift += event.Metadata["drift"]
	}
	if len(sink.Packets()) != 2 || sink.Packets()[1].StreamID != "audio" || drift != "audio-b=audio" {
		t.Fatalf("packets = %d, second id = %q, drift = %q", len(sink.Packets()), sink.Packets()[len(sink.Packets())-1].StreamID, drift)
	}
}

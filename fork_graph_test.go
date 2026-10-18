package goav_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// TestForkGraphs pins how forks of one Stream plan: work before a fork runs
// once, each fork's own work runs on its own nodes, and forks writing one
// destination value feed one mux.
func TestForkGraphs(t *testing.T) {
	sink := func(name string) goav.Destination {
		return goav.Sink(component.SinkFunc(name, func(context.Context, component.Message) error { return nil }))
	}
	meter := component.FrameFunc("meter", func(_ context.Context, frame *av.Frame, emit component.Emit) error {
		return emit.Frame(frame)
	})
	file := func(name string) goav.InputSpec { return goav.FileInput(name, strings.NewReader("")) }
	for _, tt := range []struct {
		name    string
		outs    func() []goav.Output
		want    []string
		without []string
	}{
		{
			name: "packet copies share the selected stream",
			outs: func() []goav.Output {
				v := file("input.ivf").Video()
				return []goav.Output{v.As("archive").To(goav.Write("archive.ivf", io.Discard)), v.As("packets").To(sink("packets"))}
			},
			want:    []string{"input.ivf -> select-video", "select-video -> archive.ivf", "select-video -> packets"},
			without: []string{"decode-video", "encode-"},
		},
		{
			name: "forks share the work before them",
			outs: func() []goav.Output {
				r := file("input.webm").Video().Decode().Resize(1280, 720)
				return []goav.Output{
					r.Encode(codec.VP9(codec.Bitrate(2_000_000))).As("web").To(goav.Write("web.webm", io.Discard)),
					r.Resize(320, 180).As("thumb").To(sink("thumbnail")),
				}
			},
			want:    []string{"decode-video -> resize-video", "resize-video -> encode-web", "resize-video -> resize-thumb", "resize-thumb -> thumbnail"},
			without: []string{"decode-video -> resize-web", "decode-video -> resize-thumb"},
		},
		{
			name: "forks split where their work parts",
			outs: func() []goav.Output {
				d := file("input.webm").Video().Decode()
				return []goav.Output{
					d.Resize(320, 180).As("raw-preview").To(sink("thumbnail")),
					d.Resize(1280, 720).Encode(codec.VP9(codec.Bitrate(2_000_000))).As("web").To(goav.Write("web.webm", io.Discard)),
				}
			},
			want: []string{"decode-video -> resize-raw-preview", "decode-video -> resize-web", "resize-web -> encode-web", "resize-raw-preview -> thumbnail"},
		},
		{
			name: "streams of one input fork apart",
			outs: func() []goav.Output {
				in := file("source.webm")
				watch, mobile := goav.Write("watch.webm", io.Discard), goav.Write("mobile.webm", io.Discard)
				v := in.Video().Decode()
				return []goav.Output{
					v.Resize(1920, 1080).Encode(codec.VP9(codec.Bitrate(4_000_000))).As("v1080").To(watch),
					v.Resize(640, 360).Encode(codec.VP8(codec.Bitrate(600_000))).As("v360").To(mobile),
					in.Audio().Decode().Resample(48_000, codec.Stereo).Encode(codec.Opus(codec.Bitrate(96_000))).As("a96").To(watch, mobile),
				}
			},
			want: []string{
				"decode-video -> resize-v1080", "decode-video -> resize-v360", "decode-audio -> resample-a96",
				"encode-v1080 -> watch.webm", "encode-v360 -> mobile.webm", "encode-a96 -> watch.webm", "encode-a96 -> mobile.webm",
			},
		},
		{
			name: "a fork keeps its steps in order",
			outs: func() []goav.Output {
				web := goav.Write("web.webm", io.Discard)
				d := file("source.webm").Video().Decode()
				return []goav.Output{
					d.Do(meter).Resize(640, 360).Encode(codec.VP9(codec.Bitrate(600_000))).As("v360").To(web),
					d.As("raw").To(sink("raw")),
				}
			},
			want: []string{"decode-video -> meter", "meter -> resize-v360", "resize-v360 -> encode-v360"},
		},
		{
			name: "forks writing one destination share its mux",
			outs: func() []goav.Output {
				web := goav.Write("web.ivf", io.Discard, goav.Format(av.FormatIVF))
				d := file("input.ivf").Video().Decode()
				return []goav.Output{
					d.Encode(codec.VP9(codec.Bitrate(2_000_000))).As("v720").To(web),
					d.Resize(640, 360).Encode(codec.VP8(codec.Bitrate(600_000))).As("v360").To(web),
				}
			},
			want: []string{"encode-v720 -> web.ivf", "encode-v360 -> web.ivf"},
		},
		{
			name: "decoded frames reach a sink fork",
			outs: func() []goav.Output {
				d := file("input.webm").Video().Decode()
				return []goav.Output{d.As("preview").To(sink("frames")), d.Encode(codec.VP9()).As("web").To(goav.Write("web.webm", io.Discard))}
			},
			want:    []string{"decode-video -> frames", "decode-video -> encode-web"},
			without: []string{"encode-preview"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := mustRuntime().Describe(tt.outs()...)
			if err != nil {
				t.Fatal(err)
			}
			text := specText(spec)
			for _, want := range tt.want {
				if !strings.Contains(text, want) {
					t.Fatalf("spec missing %q:\n%s", want, text)
				}
			}
			for _, absent := range tt.without {
				if strings.Contains(text, absent) {
					t.Fatalf("spec has %q:\n%s", absent, text)
				}
			}
		})
	}
}

// TestTapAfterEncodeIsAPacketTap pins that a tap's domain follows its
// position: after the encoder it taps packets.
func TestTapAfterEncodeIsAPacketTap(t *testing.T) {
	report, err := mustRuntime().Explain(context.Background(), goav.FileInput("input.ogg", strings.NewReader("")).
		Audio().
		Decode().
		Tap("audio.decoded").
		Encode(codec.Opus(codec.Bitrate(96_000))).
		Tap("audio.encoded").
		To(goav.Write("archive.ogg", io.Discard)))
	if err != nil && len(report.Taps) == 0 {
		t.Fatal(err)
	}
	domains := map[string]shape.MediaDomain{}
	for _, tap := range report.Taps {
		domains[tap.Name] = tap.Domain
	}
	if domains["audio.decoded"] != shape.DomainFrame || domains["audio.encoded"] != shape.DomainPacket {
		t.Fatalf("tap domains = %v, want frames before the encoder and packets after it", domains)
	}
}

// TestTapAnchorsFollowPlannedNodes pins that a tap anchors on the node the
// work before it runs as, shared nodes included: task.Taps() names a node of
// the graph, at the shape the tap carries.
func TestTapAnchorsFollowPlannedNodes(t *testing.T) {
	sink := func(name string) goav.Destination { return goavtest.NewCollector().Sink().With(goav.Name(name)) }
	for _, tt := range []struct {
		name  string
		outs  func() []goav.Output
		tap   string
		node  string
		width int
	}{
		{
			name: "after a shared encoder",
			outs: func() []goav.Output {
				e := goavtest.Audio(48_000, 1, []int16{1, 2}).Audio().Encode(codec.Opus())
				return []goav.Output{e.As("a").To(sink("s1")), e.Tap("enc").As("b").To(sink("s2"))}
			},
			tap:  "enc",
			node: "encode-audio",
		},
		{
			name: "after the second of two shared resizes",
			outs: func() []goav.Output {
				v := goavtest.Video(64, 48, 1).Video()
				r1 := v.Resize(6, 6)
				r2 := r1.Resize(4, 4)
				return []goav.Output{r2.Tap("small").As("a").To(sink("a")), r2.As("b").To(sink("b")), r1.As("c").To(sink("c"))}
			},
			tap:   "small",
			node:  "resize-video-2",
			width: 4,
		},
		{
			name: "after a fork's own resize",
			outs: func() []goav.Output {
				v := goavtest.Video(64, 48, 1).Video()
				return []goav.Output{v.Resize(8, 8).Tap("thumb").As("thumb").To(sink("thumb")), v.As("full").To(sink("full"))}
			},
			tap:   "thumb",
			node:  "resize-thumb",
			width: 8,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task, err := goavtest.Runtime().Build(context.Background(), tt.outs()...)
			if err != nil {
				t.Fatal(err)
			}
			defer task.Close()
			var found bool
			for _, tap := range task.Taps() {
				if tap.Name != tt.tap {
					continue
				}
				found = true
				if string(tap.Node) != tt.node || (tt.width != 0 && tap.Shape.Width != tt.width) {
					t.Fatalf("tap %s = node %s width %d, want node %s width %d", tt.tap, tap.Node, tap.Shape.Width, tt.node, tt.width)
				}
			}
			if !found {
				t.Fatalf("taps = %+v, want %s", task.Taps(), tt.tap)
			}
			if !strings.Contains(specText(task.Describe()), "stage "+tt.node+" ") {
				t.Fatalf("tap node %s is not a graph node:\n%s", tt.node, specText(task.Describe()))
			}
			if _, err := task.Attach(context.Background(), task.Tap(tt.tap).As("late").To(sink("late"))); err != nil {
				t.Fatalf("attach from %s: %v", tt.tap, err)
			}
		})
	}
}

// TestSharedEncoderKeepsItsStreamID pins the naming rule for an encoder forks
// share: As names only a branch's own work, so the encoded stream keeps the
// id it carries, and Explain reports what the packets carry.
func TestSharedEncoderKeepsItsStreamID(t *testing.T) {
	a, b := goavtest.NewCollector(), goavtest.NewCollector()
	meter := component.PacketFunc("meter", func(_ context.Context, p *av.Packet, emit component.Emit) error { return emit.Packet(p) })
	e := goavtest.Audio(48_000, 1, make([]int16, 960)).Audio().Encode(codec.Opus())
	outs := []goav.Output{e.As("a").To(a.Sink()), e.Do(meter).As("b").To(b.Sink())}
	report, err := goavtest.Runtime().Explain(context.Background(), outs...)
	if err != nil {
		t.Fatal(err)
	}
	if err := goavtest.Runtime().Run(context.Background(), outs...); err != nil {
		t.Fatal(err)
	}
	if len(a.Packets()) == 0 || len(b.Packets()) == 0 {
		t.Fatalf("packets a=%d b=%d, want both", len(a.Packets()), len(b.Packets()))
	}
	got := a.Packets()[0].StreamID
	if b.Packets()[0].StreamID != got || got == "a" || got == "b" {
		t.Fatalf("packet stream ids a=%s b=%s, want the encoded stream's own id on both", got, b.Packets()[0].StreamID)
	}
	for _, branch := range report.Branches {
		for _, operation := range branch.Operations {
			if operation.Kind == plan.OpEncode && operation.Shape.StreamID != got {
				t.Fatalf("branch %s encode shape stream id = %s, want %s", branch.Name, operation.Shape.StreamID, got)
			}
		}
	}
}

// TestForksNamedApartAreTwoBranches pins that two names after the same work
// make two branches over the same nodes: the encoder they share runs once
// and keeps its stream id, and one name routed twice stays one branch.
func TestForksNamedApartAreTwoBranches(t *testing.T) {
	a, b := goavtest.NewCollector(), goavtest.NewCollector()
	e := goavtest.Audio(48_000, 1, make([]int16, 960)).Audio().Encode(codec.Opus())
	outs := []goav.Output{e.As("a").To(a.Sink()), e.As("b").To(b.Sink())}
	report, err := goavtest.Runtime().Explain(context.Background(), outs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Branches) != 2 || report.Branches[0].Name != "a" || report.Branches[1].Name != "b" {
		t.Fatalf("branches = %+v, want a and b", report.Branches)
	}
	spec, err := goavtest.Runtime().Describe(outs...)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(specText(spec), "stage encode-"); got != 1 {
		t.Fatalf("encoders = %d, want one shared encoder:\n%s", got, specText(spec))
	}
	if err := goavtest.Runtime().Run(context.Background(), outs...); err != nil {
		t.Fatal(err)
	}
	if len(a.Packets()) == 0 || len(b.Packets()) == 0 || a.Packets()[0].StreamID != b.Packets()[0].StreamID || a.Packets()[0].StreamID == "a" {
		t.Fatalf("packets a=%d b=%d, want both carrying the encoded stream's own id", len(a.Packets()), len(b.Packets()))
	}
	one, err := goavtest.Runtime().Explain(context.Background(), e.As("a").To(goavtest.NewCollector().Sink()), e.As("a").To(goavtest.NewCollector().Sink()))
	if err != nil || len(one.Branches) != 1 {
		t.Fatalf("one name, two destinations: branches=%+v err=%v, want one branch", one.Branches, err)
	}
}

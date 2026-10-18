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

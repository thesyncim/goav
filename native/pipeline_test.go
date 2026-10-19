package native_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/native"
	"github.com/thesyncim/goav/native/kit"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/runconfig"
)

// nativeH264 is the H.264 encoder backend this host runs: VideoToolbox's
// hardware on a Mac that has it, else OpenH264.
func nativeH264(t *testing.T) (string, runconfig.Option) {
	t.Helper()
	both := runconfig.Option(func(config *runconfig.Config) error {
		for _, option := range []runconfig.Option{native.OpenH264(), native.VideoToolbox()} {
			if err := option(config); err != nil {
				return err
			}
		}
		return nil
	})
	config, err := runconfig.NewConfig(both)
	if err != nil {
		t.Fatal(err)
	}
	// Ask selection itself, as the runtime will: VideoToolbox can serve H.264
	// on a Mac whose HEVC encoder is missing.
	choice, err := config.Codecs.Select(codec.Need{ID: av.CodecH264, Mode: codec.ModeEncode})
	if err != nil {
		t.Skipf("no native H.264 encoder here: %v", err)
	}
	return choice.Descriptor.Backend.Name, both
}

func requireAudioToolbox(t *testing.T) {
	t.Helper()
	if err := kit.Probe(native.AudioToolbox()); err != nil {
		t.Skipf("AudioToolbox unavailable: %v", err)
	}
}

// demuxed opens an MP4 with the bundle and collects each stream's packets.
func demuxed(t *testing.T, data []byte) (video, audio []*av.Packet) {
	t.Helper()
	collector := goavtest.NewCollector()
	if err := bundle.Run(context.Background(), goav.FileInput("out.mp4", bytes.NewReader(data)).To(collector.Sink())); err != nil {
		t.Fatalf("demux: %v", err)
	}
	for _, p := range collector.Packets() {
		switch p.Type {
		case av.MediaVideo:
			video = append(video, p)
		case av.MediaAudio:
			audio = append(audio, p)
		}
	}
	return video, audio
}

// TestLaunchProgramMuxesNativeH264AndAACIntoMP4 runs a goav program the way
// goav run does: test video and audio, encoded by native backends, into one
// MP4 that the pure-Go demuxer reads back.
func TestLaunchProgramMuxesNativeH264AndAACIntoMP4(t *testing.T) {
	requireAudioToolbox(t)
	backend, h264 := nativeH264(t)
	dir := t.TempDir()
	files, err := launch.Dir(dir)
	if err != nil {
		t.Fatal(err)
	}
	program := "testsrc video size=320x240 fps=30 duration=1s ! encode h264 backend=" + backend + " bitrate=1M keyframe_interval=30 ! file out.mp4\n" +
		"testsrc audio rate=48000 channels=2 duration=1s ! encode aac media=audio backend=audiotoolbox bitrate=128k ! file out.mp4"
	outputs, err := launch.Compile(program, launch.Files(files))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	rt := bundle.MustNew(h264, native.AudioToolbox())
	if err := rt.Run(context.Background(), outputs...); err != nil {
		t.Fatalf("run: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "out.mp4"))
	if err != nil {
		t.Fatal(err)
	}
	video, audio := demuxed(t, data)
	if len(video) != 30 || !video[0].Keyframe {
		t.Fatalf("demuxed %d video packets (first keyframe %v), want 30 from a keyframe", len(video), len(video) > 0 && video[0].Keyframe)
	}
	if len(audio) < 46 {
		t.Fatalf("demuxed %d AAC packets, want about 48 for a second", len(audio))
	}
}

// TestRecipeChoosesTheNativeBackendAndExplainSaysWhy pins the Go form of the
// same job: the default preference puts the native encoder ahead of the
// pure-Go codecs, Describe names it, and Explain gives the reason.
func TestRecipeChoosesTheNativeBackendAndExplainSaysWhy(t *testing.T) {
	backend, h264 := nativeH264(t)
	rt := bundle.MustNew(h264)
	out := goavtest.Video(320, 240, 10).Video().Encode(codec.H264(codec.Bitrate(1_000_000))).To(goavtest.NewCollector().Sink())
	spec, err := rt.Describe(out)
	if err != nil {
		t.Fatal(err)
	}
	kind := map[string]string{"videotoolbox": "hardware", "openh264": "native"}[backend]
	want := "backend=" + backend + " (" + kind + ")"
	if !slices.ContainsFunc(spec.Nodes, func(n pipeline.NodeSpec) bool { return strings.HasSuffix(n.Detail, want) }) {
		t.Fatalf("Describe nodes %+v, want an encode node naming %s", spec.Nodes, want)
	}
	report, err := rt.Explain(context.Background(), out)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(report.RequiredAdapters, func(r plan.AdapterRequirement) bool { return r.Kind == "encoder" })
	if i < 0 || report.RequiredAdapters[i].Backend != backend+" ("+kind+")" || report.RequiredAdapters[i].BackendReason == "" || len(report.RequiredAdapters[i].Knobs) == 0 {
		t.Fatalf("Explain encoder requirement = %+v", report.RequiredAdapters)
	}
}

// TestMissingLibraryIsATypedRefusalWithTheFix pins that a backend whose
// library is absent never serves and says how to install it, while the
// runtime still builds and other backends still serve.
func TestMissingLibraryIsATypedRefusalWithTheFix(t *testing.T) {
	missing := native.OpenH264(native.Library(filepath.Join(t.TempDir(), "libopenh264.so")))
	err := kit.Probe(missing)
	var failure *kit.Error
	if !errors.As(err, &failure) || !errors.Is(err, kit.ErrUnavailable) || !errors.Is(err, codec.ErrUnavailable) {
		t.Fatalf("Probe = %v, want a *kit.Error that is ErrUnavailable", err)
	}
	if !strings.Contains(err.Error(), "libopenh264 did not load") || !strings.Contains(err.Error(), "brew install openh264") {
		t.Fatalf("Probe = %q, want the library and the fix", err)
	}
	rt := bundle.MustNew(missing)
	task, err := rt.Build(context.Background(), goavtest.Video(320, 240, 2).Video().Encode(codec.H264(codec.Backend("openh264"))).To(goavtest.NewCollector().Sink()))
	if err == nil {
		task.Close()
		t.Fatal("a named backend whose library is missing served")
	}
	if !strings.Contains(err.Error(), "not-installed") {
		t.Fatalf("Build = %v, want the not-installed status", err)
	}
	// The pure-Go VP8 encoder still serves next to it.
	if err := rt.Run(context.Background(), goavtest.Video(64, 48, 2).Video().Encode(codec.VP8()).To(goavtest.NewCollector().Sink())); err != nil {
		t.Fatalf("VP8 next to a missing OpenH264: %v", err)
	}
}

// TestLaunchProgramEncodesH264WithEachNativeBackend runs the video program
// with each native H.264 encoder this host has, by name.
func TestLaunchProgramEncodesH264WithEachNativeBackend(t *testing.T) {
	for name, option := range map[string]runconfig.Option{"openh264": native.OpenH264(), "videotoolbox": native.VideoToolbox()} {
		t.Run(name, func(t *testing.T) {
			if err := kit.Probe(option); err != nil {
				t.Skipf("%s unavailable: %v", name, err)
			}
			dir := t.TempDir()
			files, err := launch.Dir(dir)
			if err != nil {
				t.Fatal(err)
			}
			outputs, err := launch.Compile("testsrc video size=640x360 fps=30 frames=30 ! encode h264 backend="+name+" bitrate=1M ! file out.mp4", launch.Files(files))
			if err != nil {
				t.Fatal(err)
			}
			if err := bundle.MustNew(option).Run(context.Background(), outputs...); err != nil {
				t.Fatalf("run: %v", err)
			}
			data, err := os.ReadFile(filepath.Join(dir, "out.mp4"))
			if err != nil {
				t.Fatal(err)
			}
			if video, _ := demuxed(t, data); len(video) != 30 {
				t.Fatalf("demuxed %d packets, want 30", len(video))
			}
		})
	}
}

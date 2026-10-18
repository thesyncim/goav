package goav_test

import (
	"bytes"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/webm"
	"github.com/thesyncim/goav/goavtest"
)

// mfAudioTrack decodes the one audio track of a WebM and reports its samples.
func mfAudioTrack(t *testing.T, label string, data []byte, wantRate, wantChannels int) int {
	t.Helper()
	samples, rate, channels := mfSamples(mfDecoded(t, data, av.MediaAudio))
	if samples == 0 || rate != wantRate || channels != wantChannels {
		t.Fatalf("%s audio = %d samples at %d Hz %d ch, want %d Hz %d ch", label, samples, rate, channels, wantRate, wantChannels)
	}
	return samples
}

// TestForksAcrossSeveralInputs runs an HLS-style ladder from one camera with
// shared audio: the camera forks into two renditions, a second input's audio
// muxes into both, and each fork reads its own input.
func TestForksAcrossSeveralInputs(t *testing.T) {
	var hd, sd bytes.Buffer
	hdOut, sdOut := goav.Write("hd.webm", &hd), goav.Write("sd.webm", &sd)
	v := goavtest.Video(128, 96, 12).Video()
	mfRun(t,
		v.Encode(codec.VP9(codec.Bitrate(400_000))).As("hd").To(hdOut),
		v.Resize(64, 48).Encode(codec.VP9(codec.Bitrate(80_000))).As("sd").To(sdOut),
		mfTone(48_000, 2, 25).Audio().Encode(codec.Opus()).To(hdOut, sdOut),
	)
	for _, out := range []struct {
		name          string
		data          []byte
		width, height int
	}{{"hd.webm", hd.Bytes(), 128, 96}, {"sd.webm", sd.Bytes(), 64, 48}} {
		if tracks := mfTracks(t, out.data); len(tracks) != 2 {
			t.Fatalf("%s tracks = %+v, want video and the shared audio", out.name, tracks)
		}
		mfAssertVideo(t, out.name, mfDecoded(t, out.data, av.MediaVideo), 12, out.width, out.height)
	}
	if hdSamples, sdSamples := mfAudioTrack(t, "hd.webm", hd.Bytes(), 48_000, 2), mfAudioTrack(t, "sd.webm", sd.Bytes(), 48_000, 2); hdSamples != sdSamples {
		t.Fatalf("shared audio decodes to %d samples in hd.webm and %d in sd.webm, want one encode", hdSamples, sdSamples)
	}
}

// TestForksOfTwoFiles forks two files into renditions in one run: each
// file's forks share that file's decode and first resize, and neither sees
// the other's track 1.
func TestForksOfTwoFiles(t *testing.T) {
	a := goav.FileInput("a.webm", bytes.NewReader(mfWebM(t, 64, 48, 8, 0, 0, 0)))
	b := goav.FileInput("b.webm", bytes.NewReader(mfWebM(t, 32, 24, 5, 0, 0, 0)))
	// Each file's forks share its decode and its first resize.
	av1, bv := a.Video().Decode().Resize(48, 36), b.Video().Decode().Resize(16, 12)
	var aFull, aHalf, bFull, bHalf bytes.Buffer
	outs := []goav.Output{
		av1.Encode(codec.VP8()).As("a-full").To(goav.Write("a-full.webm", &aFull)),
		av1.Resize(24, 18).Encode(codec.VP8()).As("a-half").To(goav.Write("a-half.webm", &aHalf)),
		bv.Encode(codec.VP9()).As("b-full").To(goav.Write("b-full.webm", &bFull)),
		bv.Resize(8, 6).Encode(codec.VP9()).As("b-half").To(goav.Write("b-half.webm", &bHalf)),
	}
	spec, err := bundle.Describe(outs...)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got := countNodesWithPrefix(spec, "decode-"); got != 2 {
		t.Fatalf("decode nodes = %d in %v, want one per file", got, specNodeNames(spec))
	}
	for _, name := range []string{"resize-video@a.webm", "resize-video@b.webm"} {
		if countNodesWithPrefix(spec, name) != 1 {
			t.Fatalf("nodes = %v, want one shared %s", specNodeNames(spec), name)
		}
	}
	mfRun(t, outs...)
	mfAssertVideo(t, "a-full.webm", mfDecoded(t, aFull.Bytes(), av.MediaVideo), 8, 48, 36)
	mfAssertVideo(t, "a-half.webm", mfDecoded(t, aHalf.Bytes(), av.MediaVideo), 8, 24, 18)
	mfAssertVideo(t, "b-full.webm", mfDecoded(t, bFull.Bytes(), av.MediaVideo), 5, 16, 12)
	mfAssertVideo(t, "b-half.webm", mfDecoded(t, bHalf.Bytes(), av.MediaVideo), 5, 8, 6)
}

// TestNamedStreamBesideAnotherInput pins a lone named stream in a run with
// several inputs: its name reaches its nodes beside a second file's copy.
func TestNamedStreamBesideAnotherInput(t *testing.T) {
	a := goav.FileInput("a.webm", bytes.NewReader(mfWebM(t, 64, 48, 6, 0, 0, 0)))
	bData := mfWebM(t, 0, 0, 0, 48_000, 2, 10)
	b := goav.FileInput("b.webm", bytes.NewReader(bData))
	var o bytes.Buffer
	out := goav.Write("o.webm", &o)
	outs := []goav.Output{a.Video().Decode().Resize(32, 24).Encode(codec.VP8()).As("cam").To(out), b.Audio().To(out)}
	spec, err := bundle.Describe(outs...)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	for _, name := range []string{"resize-cam", "encode-cam"} {
		if countNodesWithPrefix(spec, name) != 1 {
			t.Fatalf("nodes = %v, want %s: As names the stream", specNodeNames(spec), name)
		}
	}
	mfRun(t, outs...)
	if tracks := mfTracks(t, o.Bytes()); len(tracks) != 2 {
		t.Fatalf("o.webm tracks = %+v, want the named video and b's audio", tracks)
	}
	mfAssertVideo(t, "cam", mfTrack(t, o.Bytes(), av.MediaVideo, func(tr webm.Track) bool { return tr.Video.Width == 32 }), 6, 32, 24)
	if got, want := mfAudioTrack(t, "o.webm", o.Bytes(), 48_000, 2), mfAudioTrack(t, "b.webm", bData, 48_000, 2); got != want {
		t.Fatalf("copied audio = %d samples, want b's %d", got, want)
	}
}

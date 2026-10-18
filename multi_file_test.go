package goav_test

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/webm"
	"github.com/thesyncim/goav/goavtest"
)

// mfWebM encodes a real WebM through the bundled codecs: frames VP8 frames of
// width x height and audioFrames 20 ms Opus frames of a tone at rate and
// channels; either may be zero.
func mfWebM(t *testing.T, width, height, frames, rate, channels, audioFrames int) []byte {
	t.Helper()
	var out bytes.Buffer
	file := goav.Write("fixture.webm", &out)
	var outs []goav.Output
	if frames > 0 {
		outs = append(outs, goavtest.Video(width, height, frames).Video().Encode(codec.VP8()).To(file))
	}
	if audioFrames > 0 {
		outs = append(outs, mfTone(rate, channels, audioFrames).Audio().Encode(codec.Opus()).To(file))
	}
	mfRun(t, outs...)
	return out.Bytes()
}

// mfTone is a 440 Hz S16 tone in 20 ms frames.
func mfTone(rate, channels, frames int) goav.InputSpec {
	perFrame := rate / 50
	samples := make([][]int16, frames)
	for f := range samples {
		samples[f] = make([]int16, perFrame*channels)
		for i := range perFrame {
			v := int16(8000 * math.Sin(2*math.Pi*440*float64(f*perFrame+i)/float64(rate)))
			for c := range channels {
				samples[f][i*channels+c] = v
			}
		}
	}
	return goavtest.Audio(rate, channels, samples...)
}

func mfRun(t *testing.T, outs ...goav.Output) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := bundle.Run(ctx, outs...); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func mfTracks(t *testing.T, data []byte) []webm.Track {
	t.Helper()
	demuxer, err := webm.NewDemuxer(bytes.NewReader(data), webm.DemuxerOptions{})
	if err != nil {
		t.Fatalf("demux: %v", err)
	}
	return demuxer.Tracks()
}

// mfDecoded decodes one stream of a WebM through the bundled decoders.
func mfDecoded(t *testing.T, data []byte, media av.MediaType, options ...goav.StreamOption) []*av.Frame {
	t.Helper()
	in := goav.FileInput("out.webm", bytes.NewReader(data))
	stream := in.Video(options...)
	if media == av.MediaAudio {
		stream = in.Audio(options...)
	}
	frames := goavtest.NewCollector()
	mfRun(t, stream.Decode().To(frames.Sink()))
	return frames.Frames()
}

// mfTrack decodes the output track that matches the predicate.
func mfTrack(t *testing.T, data []byte, media av.MediaType, match func(webm.Track) bool) []*av.Frame {
	t.Helper()
	for _, track := range mfTracks(t, data) {
		if match(track) {
			return mfDecoded(t, data, media, goav.StreamID(av.StreamID(strconv.FormatUint(uint64(track.ID), 10))))
		}
	}
	t.Fatalf("no track matches in %+v", mfTracks(t, data))
	return nil
}

func mfSamples(frames []*av.Frame) (samples, rate, channels int) {
	for _, f := range frames {
		samples += f.Audio.Samples
		rate, channels = f.Audio.SampleRate, f.Audio.Channels
	}
	return samples, rate, channels
}

func mfAssertVideo(t *testing.T, label string, frames []*av.Frame, count, width, height int) {
	t.Helper()
	if len(frames) != count {
		t.Fatalf("%s decodes to %d frames, want %d", label, len(frames), count)
	}
	for _, f := range frames {
		if f.Video.Width != width || f.Video.Height != height {
			t.Fatalf("%s frame is %dx%d, want %dx%d", label, f.Video.Width, f.Video.Height, width, height)
		}
	}
}

// TestSeveralFileInputsTranscodeInOneRun runs two files in one task, each
// select fed by its own file: neither output sees the other file's track 1.
func TestSeveralFileInputsTranscodeInOneRun(t *testing.T) {
	a := goav.FileInput("a.webm", bytes.NewReader(mfWebM(t, 64, 48, 10, 0, 0, 0)))
	b := goav.FileInput("b.webm", bytes.NewReader(mfWebM(t, 32, 24, 6, 0, 0, 0)))
	var x, y bytes.Buffer
	mfRun(t,
		a.Video().Decode().Encode(codec.VP9()).To(goav.Write("x.webm", &x)),
		b.Video().Decode().Encode(codec.VP9()).To(goav.Write("y.webm", &y)),
	)
	mfAssertVideo(t, "x.webm", mfDecoded(t, x.Bytes(), av.MediaVideo), 10, 64, 48)
	mfAssertVideo(t, "y.webm", mfDecoded(t, y.Bytes(), av.MediaVideo), 6, 32, 24)
}

// TestCopyFromTwoFilesIntoOneMux copies streams of two files into one WebM.
// Both files' video is track 1: the ids are qualified by input, so the mux
// keeps two video tracks instead of interleaving them into one.
func TestCopyFromTwoFilesIntoOneMux(t *testing.T) {
	aData := mfWebM(t, 64, 48, 10, 48_000, 2, 10)
	bData := mfWebM(t, 32, 24, 6, 24_000, 1, 8)
	a := goav.FileInput("a.webm", bytes.NewReader(aData))
	b := goav.FileInput("b.webm", bytes.NewReader(bData))
	var m bytes.Buffer
	out := goav.Write("m.webm", &m)
	mfRun(t, a.Video().To(out), b.Video().To(out), b.Audio().To(out))

	if tracks := mfTracks(t, m.Bytes()); len(tracks) != 3 {
		t.Fatalf("m.webm tracks = %+v, want a's video, b's video, b's audio", tracks)
	}
	mfAssertVideo(t, "a's video", mfTrack(t, m.Bytes(), av.MediaVideo, func(tr webm.Track) bool { return tr.Video.Width == 64 }), 10, 64, 48)
	mfAssertVideo(t, "b's video", mfTrack(t, m.Bytes(), av.MediaVideo, func(tr webm.Track) bool { return tr.Video.Width == 32 }), 6, 32, 24)
	gotSamples, rate, channels := mfSamples(mfDecoded(t, m.Bytes(), av.MediaAudio))
	wantSamples, _, _ := mfSamples(mfDecoded(t, bData, av.MediaAudio))
	if gotSamples != wantSamples || rate != 24_000 || channels != 1 {
		t.Fatalf("copied audio = %d samples %d Hz %d ch, want b's %d samples at 24000 Hz mono", gotSamples, rate, channels, wantSamples)
	}
}

// TestRemuxTwoFilesIntoOneFile copies every stream of two files into one.
func TestRemuxTwoFilesIntoOneFile(t *testing.T) {
	aData := mfWebM(t, 64, 48, 10, 48_000, 2, 10)
	bData := mfWebM(t, 32, 24, 6, 48_000, 2, 5)
	var r bytes.Buffer
	out := goav.Write("r.webm", &r)
	mfRun(t,
		goav.FileInput("a.webm", bytes.NewReader(aData)).To(out),
		goav.FileInput("b.webm", bytes.NewReader(bData)).To(out),
	)
	if tracks := mfTracks(t, r.Bytes()); len(tracks) != 4 {
		t.Fatalf("r.webm tracks = %+v, want both files' video and audio", tracks)
	}
	mfAssertVideo(t, "b's video", mfTrack(t, r.Bytes(), av.MediaVideo, func(tr webm.Track) bool { return tr.Video.Width == 32 }), 6, 32, 24)
}

// TestFileAndCustomSourceInputsShareOneRun keeps mixed input kinds working: a
// file's video and an application source's audio into one WebM.
func TestFileAndCustomSourceInputsShareOneRun(t *testing.T) {
	file := goav.FileInput("a.webm", bytes.NewReader(mfWebM(t, 64, 48, 10, 0, 0, 0)))
	var o bytes.Buffer
	out := goav.Write("o.webm", &o)
	mfRun(t,
		file.Video().Decode().Encode(codec.VP9()).To(out),
		mfTone(48_000, 2, 10).Audio().Encode(codec.Opus()).To(out),
	)
	if tracks := mfTracks(t, o.Bytes()); len(tracks) != 2 {
		t.Fatalf("o.webm tracks = %+v, want video and audio", tracks)
	}
	mfAssertVideo(t, "file video", mfDecoded(t, o.Bytes(), av.MediaVideo), 10, 64, 48)
	if samples, rate, _ := mfSamples(mfDecoded(t, o.Bytes(), av.MediaAudio)); samples == 0 || rate != 48_000 {
		t.Fatalf("source audio = %d samples at %d Hz, want 48 kHz audio", samples, rate)
	}
}

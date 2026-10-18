package goav_test

import (
	"bytes"
	"context"
	"io"
	"math"
	"slices"
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

// TestMixFilesOfDifferentFormats mixes a 48 kHz stereo file with a 24 kHz
// mono one: neither format is known until the files open, and each arm
// converts as it arrives to the first arm's format.
func TestMixFilesOfDifferentFormats(t *testing.T) {
	stereo := mfWebM(t, 0, 0, 0, 48_000, 2, 10)
	mono := mfWebM(t, 0, 0, 0, 24_000, 1, 10)
	file := func(name string, data []byte) goav.Stream {
		return goav.FileInput(name, bytes.NewReader(data)).Audio().Decode()
	}
	for _, tc := range []struct {
		name           string
		mixed          goav.Stream
		encode         codec.Spec
		rate, channels int
	}{
		{"first is stereo", goav.Mix(file("a.webm", stereo), file("b.webm", mono)), codec.Opus(), 48_000, 2},
		{"first is mono", goav.Mix(file("b.webm", mono), file("a.webm", stereo)), codec.Opus(), 24_000, 1},
		{"encoder fixes the format", goav.Mix(file("b.webm", mono), file("a.webm", stereo)), codec.Opus(codec.SampleRate(48_000), codec.Channels(codec.Stereo)), 48_000, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var m bytes.Buffer
			mfRun(t, tc.mixed.Encode(tc.encode).To(goav.Write("m.webm", &m)))
			samples, rate, channels := mfSamples(mfDecoded(t, m.Bytes(), av.MediaAudio))
			if rate != tc.rate || channels != tc.channels {
				t.Fatalf("mix = %d Hz %d ch, want %d Hz %d ch", rate, channels, tc.rate, tc.channels)
			}
			// Ten 20 ms frames: about 200 ms of audio, less the codec delay.
			if want := tc.rate / 5; samples < want*3/4 || samples > want*5/4 {
				t.Fatalf("mix decodes to %d samples, want about %d", samples, want)
			}
		})
	}
}

// TestCompositeFilesOfDifferentSizes composites a 64x48 file with a 32x24
// one placed at (40, 30): the canvas is their bounding box, known only once
// the files open.
func TestCompositeFilesOfDifferentSizes(t *testing.T) {
	big := goav.FileInput("a.webm", bytes.NewReader(mfWebM(t, 64, 48, 6, 0, 0, 0)))
	small := goav.FileInput("b.webm", bytes.NewReader(mfWebM(t, 32, 24, 6, 0, 0, 0)))
	pip := goav.Composite(big.Video().Decode(), goav.At(small.Video().Decode(), 40, 30))
	var out bytes.Buffer
	mfRun(t, pip.Encode(codec.VP8()).To(goav.Write("pip.webm", &out)))
	mfAssertVideo(t, "pip.webm", mfDecoded(t, out.Bytes(), av.MediaVideo), 6, 72, 54)
}

// TestDescribeFileJoinOpensNothing pins that a join over files describes its
// whole graph, each arm's bind, decode, and conversion, without reading them.
func TestDescribeFileJoinOpensNothing(t *testing.T) {
	a := goav.FileInput("a.webm", failingReader{t})
	b := goav.FileInput("b.webm", failingReader{t})
	spec, err := bundle.Describe(goav.Mix(a.Audio().Decode(), b.Audio().Decode()).Encode(codec.Opus()).To(goav.Write("m.webm", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	var nodes []string
	for _, node := range spec.Nodes {
		nodes = append(nodes, node.Name)
	}
	want := []string{"a.webm", "mix-select-a.webm", "mix-decode-a.webm", "mix-conform-a.webm", "b.webm", "mix-select-b.webm", "mix-decode-b.webm", "mix-conform-b.webm", "mix", "encode-mix-encode", "m.webm"}
	if !slices.Equal(nodes, want) {
		t.Fatalf("nodes = %v, want %v", nodes, want)
	}
}

type failingReader struct{ t *testing.T }

func (r failingReader) Read([]byte) (int, error) {
	r.t.Error("describing a join read its file")
	return 0, io.EOF
}

// TestFileMixFeedsForksAndNests runs a mix over files through the rest of the
// grammar: forks off it encode the format it resolved once the files opened,
// it nests in an outer mix, and an application source first in the mix sets
// the format the file converts to.
func TestFileMixFeedsForksAndNests(t *testing.T) {
	stereo := mfWebM(t, 0, 0, 0, 48_000, 2, 10)
	mono := mfWebM(t, 0, 0, 0, 24_000, 1, 10)
	file := func(name string, data []byte) goav.Stream {
		return goav.FileInput(name, bytes.NewReader(data)).Audio().Decode()
	}
	check := func(label string, data []byte, rate, channels int) {
		t.Helper()
		samples, gotRate, gotChannels := mfSamples(mfDecoded(t, data, av.MediaAudio))
		if samples == 0 || gotRate != rate || gotChannels != channels {
			t.Fatalf("%s = %d samples at %d Hz %d ch, want %d Hz %d ch", label, samples, gotRate, gotChannels, rate, channels)
		}
	}

	var low, full bytes.Buffer
	mixed := goav.Mix(file("a.webm", stereo), file("b.webm", mono))
	mfRun(t,
		mixed.Resample(16_000, 1).Encode(codec.Opus()).To(goav.Write("low.webm", &low)),
		mixed.Encode(codec.Opus()).To(goav.Write("full.webm", &full)),
	)
	check("resampled fork", low.Bytes(), 16_000, 1)
	check("full fork", full.Bytes(), 48_000, 2)

	var nested bytes.Buffer
	inner := goav.Mix(file("b.webm", mono), file("a.webm", stereo))
	mfRun(t, goav.Mix(inner, file("c.webm", stereo)).Encode(codec.Opus()).To(goav.Write("nested.webm", &nested)))
	check("nested mix", nested.Bytes(), 24_000, 1)

	var sourced bytes.Buffer
	mfRun(t, goav.Mix(mfTone(16_000, 1, 10).Audio(), file("a.webm", stereo)).Encode(codec.Opus()).To(goav.Write("sourced.webm", &sourced)))
	check("source-first mix", sourced.Bytes(), 16_000, 1)
}

// TestSeveralFileInputsInterleaveDeterministically pins the offline
// multi-source scheduler: several files into one mux, copied or transcoded,
// produce the same bytes on every run, with packets in time order.
func TestSeveralFileInputsInterleaveDeterministically(t *testing.T) {
	// Single-track fixtures: each file is in time order by construction, so
	// any disorder in the mux is the scheduler's.
	aData := mfWebM(t, 64, 48, 10, 0, 0, 0)
	bData := mfWebM(t, 0, 0, 0, 24_000, 1, 8)
	cData := mfWebM(t, 48, 32, 8, 0, 0, 0)
	in := func(name string, data []byte) goav.InputSpec {
		return goav.FileInput(name, bytes.NewReader(data))
	}
	for _, tc := range []struct {
		name string
		// monotonic says the mux sees packets in global time order. A copy
		// does; a transcode holds each track back by its codecs' delay, so
		// only each track is in order.
		monotonic bool
		outputs   func(out goav.Destination) []goav.Output
	}{
		{"copy two files", true, func(out goav.Destination) []goav.Output {
			return []goav.Output{in("a.webm", aData).Video().To(out), in("b.webm", bData).Audio().To(out)}
		}},
		{"copy three files", true, func(out goav.Destination) []goav.Output {
			return []goav.Output{in("a.webm", aData).To(out), in("b.webm", bData).To(out), in("c.webm", cData).To(out)}
		}},
		{"transcode two files", false, func(out goav.Destination) []goav.Output {
			return []goav.Output{
				in("a.webm", aData).Video().Decode().Encode(codec.VP8()).To(out),
				in("b.webm", bData).Audio().Decode().Encode(codec.Opus()).To(out),
			}
		}},
		{"transcode three files", false, func(out goav.Destination) []goav.Output {
			return []goav.Output{
				in("a.webm", aData).Video().Decode().Encode(codec.VP8()).To(out),
				in("b.webm", bData).Audio().Decode().Encode(codec.Opus()).To(out),
				in("c.webm", cData).Video().Decode().Encode(codec.VP8()).To(out),
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var first []byte
			for run := range 5 {
				var m bytes.Buffer
				mfRun(t, tc.outputs(goav.Write("m.webm", &m))...)
				if run == 0 {
					first = m.Bytes()
					mfAssertTimeOrder(t, first, tc.monotonic)
					continue
				}
				if !bytes.Equal(m.Bytes(), first) {
					t.Fatalf("run %d wrote %d bytes that differ from run 0's %d", run, m.Len(), len(first))
				}
			}
		})
	}
}

// mfAssertTimeOrder reads a WebM's packets in file order and checks each
// track's time never goes back and, when global, that no packet precedes an
// earlier one of any track.
func mfAssertTimeOrder(t *testing.T, data []byte, global bool) {
	t.Helper()
	demuxer, err := webm.NewDemuxer(bytes.NewReader(data), webm.DemuxerOptions{})
	if err != nil {
		t.Fatalf("demux: %v", err)
	}
	last := map[uint32]int64{}
	lastAny := int64(math.MinInt64)
	packets := 0
	for {
		packet := webm.Packet{Data: make([]byte, 0, 1<<20)}
		if err := demuxer.ReadPacket(&packet); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("read packet %d: %v", packets, err)
		}
		if prev, ok := last[packet.TrackID]; ok && packet.TimeNS < prev {
			t.Fatalf("packet %d of track %d at %d ns follows %d ns", packets, packet.TrackID, packet.TimeNS, prev)
		}
		if global && packet.TimeNS < lastAny {
			t.Fatalf("packet %d (track %d) at %d ns follows a packet at %d ns", packets, packet.TrackID, packet.TimeNS, lastAny)
		}
		last[packet.TrackID] = packet.TimeNS
		lastAny = max(lastAny, packet.TimeNS)
		packets++
	}
	if packets == 0 {
		t.Fatal("mux wrote no packets")
	}
}

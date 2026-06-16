package mp4

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

func TestFormatDemuxerReadsGoBuiltFixture(t *testing.T) {
	data := buildVideoMP4()
	d := &FormatDemuxer{}
	if err := d.Open(context.Background(), format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	streams := d.Streams()
	if len(streams) != 1 {
		t.Fatalf("streams = %d, want 1", len(streams))
	}
	if streams[0].Type != av.MediaVideo || streams[0].Codec.ID != av.CodecH264 {
		t.Fatalf("stream = %s/%s, want video/h264", streams[0].Type, streams[0].Codec.ID)
	}
	if streams[0].ID != "1" {
		t.Fatalf("stream id = %q, want \"1\"", streams[0].ID)
	}

	out := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 1024)}}}
	wantSizes := []int{10, 20, 30}
	wantDTS := []int64{0, 512, 1024}
	wantKey := []bool{true, false, false}
	for i := 0; i < 3; i++ {
		out.Reset()
		if err := d.ReadInto(context.Background(), &out); err != nil {
			t.Fatalf("ReadInto %d: %v", i, err)
		}
		p := out.Packet
		if p.StreamID != "1" || p.Type != av.MediaVideo {
			t.Fatalf("packet %d stream = %s/%s", i, p.StreamID, p.Type)
		}
		if len(p.Payload.Bytes) != wantSizes[i] {
			t.Fatalf("packet %d size = %d, want %d", i, len(p.Payload.Bytes), wantSizes[i])
		}
		if p.DTS.Value != wantDTS[i] || p.PTS.Value != wantDTS[i] || p.Keyframe != wantKey[i] {
			t.Fatalf("packet %d dts=%d pts=%d key=%v, want dts=%d key=%v", i, p.DTS.Value, p.PTS.Value, p.Keyframe, wantDTS[i], wantKey[i])
		}
	}
	out.Reset()
	if err := d.ReadInto(context.Background(), &out); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadInto after last = %v, want io.EOF", err)
	}
}

// nonSeekableReader is an io.Reader that is deliberately not an io.ReaderAt, so
// resolveReaderAt must buffer it.
type nonSeekableReader struct{ r io.Reader }

func (n nonSeekableReader) Read(p []byte) (int, error) { return n.r.Read(p) }

func TestFormatDemuxerBuffersNonSeekableReader(t *testing.T) {
	data := buildVideoMP4()
	d := &FormatDemuxer{}
	if err := d.Open(context.Background(), format.Input{Reader: nonSeekableReader{bytes.NewReader(data)}}, format.OpenOptions{}); err != nil {
		t.Fatalf("Open (buffered): %v", err)
	}
	defer d.Close()
	if len(d.Streams()) != 1 {
		t.Fatalf("buffered streams = %d, want 1", len(d.Streams()))
	}
}

func TestDemuxRealFile(t *testing.T) {
	assertDemuxesAVFile(t, "testdata/h264_aac.mp4")
}

func TestDemuxFragmentedFile(t *testing.T) {
	assertDemuxesAVFile(t, "testdata/h264_aac_fragmented.mp4")
}

// assertDemuxesAVFile opens an MP4 fixture and checks it demuxes into an H264
// video track (64x48, with avcC) and an AAC audio track (44100, with the esds
// AudioSpecificConfig), reads every sample to EOF, and finds at least one video
// keyframe. It covers both the progressive and fragmented sample paths.
func assertDemuxesAVFile(t *testing.T, path string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	dem, err := NewDemuxer(file, info.Size())
	if err != nil {
		t.Fatalf("NewDemuxer: %v", err)
	}
	tracks := dem.Tracks()
	var video, audio *Track
	for i := range tracks {
		switch tracks[i].Media {
		case av.MediaVideo:
			video = &tracks[i]
		case av.MediaAudio:
			audio = &tracks[i]
		}
	}
	if video == nil || audio == nil {
		t.Fatalf("tracks = %+v, want one video and one audio", tracks)
	}
	if video.Codec.ID != av.CodecH264 || video.Codec.Width != 64 || video.Codec.Height != 48 {
		t.Fatalf("video = %+v, want h264 64x48", video.Codec)
	}
	if len(video.Codec.ExtraData.Bytes) == 0 {
		t.Fatal("video extradata (avcC) is empty")
	}
	if audio.Codec.ID != av.CodecAAC || audio.Codec.SampleRate != 44100 {
		t.Fatalf("audio = %+v, want aac 44100", audio.Codec)
	}
	if len(audio.Codec.ExtraData.Bytes) == 0 {
		t.Fatal("audio extradata (AudioSpecificConfig from esds) is empty")
	}

	counts := map[int]int{}
	keyframes := 0
	buf := make([]byte, 0, 64*1024)
	var sample Sample
	for {
		err := dem.ReadInto(buf, &sample)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadInto: %v", err)
		}
		buf = sample.Data[:0]
		counts[sample.TrackIndex]++
		if len(sample.Data) == 0 {
			t.Fatal("empty sample payload")
		}
		if tracks[sample.TrackIndex].Media == av.MediaVideo && sample.Keyframe {
			keyframes++
		}
	}
	if counts[0] == 0 || counts[1] == 0 {
		t.Fatalf("per-track sample counts = %v, want both tracks to produce samples", counts)
	}
	if keyframes == 0 {
		t.Fatal("no video keyframes found")
	}
}

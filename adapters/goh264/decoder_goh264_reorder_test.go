//go:build goav_goh264

package goh264

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/mp4"
)

// The B-frame fixture: 12 frames of 64x48 testsrc, one GOP, two B-frames
// between references, so decode order differs from presentation order and
// the backend holds frames back until later packets or flush. The framemd5
// file is ffmpeg's decode of the same file, one line per frame in
// presentation order. Regenerate both with GOAV_REGEN_FIXTURES=1.
const (
	bframeFixture    = "testdata/bframes.mp4"
	bframeFixtureMD5 = "testdata/bframes.framemd5"
)

func TestRegenerateBFrameFixture(t *testing.T) {
	if os.Getenv("GOAV_REGEN_FIXTURES") == "" {
		t.Skip("set GOAV_REGEN_FIXTURES=1 to regenerate the committed fixture")
	}
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg unavailable: cannot regenerate the fixture")
	}
	commands := [][]string{
		{"ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "testsrc=size=64x48:rate=25",
			"-frames:v", "12", "-c:v", "libx264", "-preset", "veryfast", "-bf", "2", "-g", "12", "-keyint_min", "12",
			"-sc_threshold", "0", "-b-pyramid", "none", "-pix_fmt", "yuv420p", "-video_track_timescale", "25600",
			"-movflags", "+faststart", "-an", bframeFixture},
		{"ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", bframeFixture, "-f", "framemd5", bframeFixtureMD5},
	}
	for _, args := range commands {
		if out, err := exec.Command(args[0], args[1:]...).CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
}

// Reordered output keeps each picture's own timestamp: the backend emits
// frames in presentation order, one decode call late, with the last one only
// at flush, so stamping a frame from the packet being decoded is wrong. Each
// frame's PTS must match its picture (ffmpeg's per-frame md5) and the
// presentation sequence 0, 1024, ..., 11264, flush frames included.
func TestDecodeBFrameGOPTimestampsFollowPictures(t *testing.T) {
	ctx := context.Background()
	data, err := os.ReadFile(bframeFixture)
	if err != nil {
		t.Fatal(err)
	}
	wantMD5 := readFrameMD5(t, bframeFixtureMD5)
	demuxer, err := mp4.NewDemuxer(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	track := demuxer.Tracks()[0]
	base := av.TimeBase{Num: 1, Den: int64(track.Timescale)}
	stream := av.Stream{ID: "video", Epoch: 2, Type: av.MediaVideo, TimeBase: base, Codec: track.Codec}
	decoder, err := NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: stream})
	if err != nil {
		t.Fatal(err)
	}

	type decoded struct {
		pts      av.Timestamp
		duration av.Duration
		epoch    av.Epoch
		md5      string
	}
	var got []decoded
	collect := func(result *codec.DecodeResult) {
		for _, frame := range result.Frames {
			sum := md5.Sum(appendI420FromFrame(nil, frame))
			got = append(got, decoded{pts: frame.PTS, duration: frame.Duration, epoch: frame.CodecEpoch, md5: hex.EncodeToString(sum[:])})
		}
	}
	result := h264DecodeResult(2)
	var sample mp4.Sample
	for demuxer.ReadInto(nil, &sample) == nil {
		packet := av.Packet{
			StreamID:   "video",
			CodecEpoch: 2,
			Payload:    av.Buffer{Bytes: sample.Data},
			PTS:        av.Timestamp{Value: sample.CTS, Base: base},
			DTS:        av.Timestamp{Value: sample.DTS, Base: base},
			Duration:   av.Duration{Value: 1024, Base: base},
			Keyframe:   sample.Keyframe,
		}
		result.Reset()
		if err := decoder.DecodeInto(ctx, &packet, &result); err != nil {
			t.Fatal(err)
		}
		collect(&result)
	}
	result.Reset()
	if err := decoder.FlushInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	collect(&result)

	if len(got) != 12 || len(wantMD5) != 12 {
		t.Fatalf("frames = %d, framemd5 lines = %d, want 12", len(got), len(wantMD5))
	}
	for i, frame := range got {
		if frame.pts.Value != int64(i)*1024 || frame.pts.Base != base || !frame.pts.Base.Valid() {
			t.Errorf("frame %d pts = %+v, want %d in %v", i, frame.pts, i*1024, base)
		}
		if frame.duration.Value != 1024 || frame.duration.Base != base || frame.epoch != 2 {
			t.Errorf("frame %d duration = %+v epoch = %d", i, frame.duration, frame.epoch)
		}
		if frame.md5 != wantMD5[i] {
			t.Errorf("frame %d md5 = %s, want %s (picture of presentation frame %d)", i, frame.md5, wantMD5[i], i)
		}
	}
}

func readFrameMD5(t *testing.T, path string) []string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var sums []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, ",")
		sums = append(sums, strings.TrimSpace(fields[len(fields)-1]))
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return sums
}

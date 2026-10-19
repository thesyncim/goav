package goav_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
)

// TestInputFormatIsDetectedFromContent pins content sniffing: an input's
// first bytes decide its container and its name is only a hint, so a
// misnamed or unnamed file works, from a seekable reader or a plain stream.
func TestInputFormatIsDetectedFromContent(t *testing.T) {
	mp4, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	webm := opusWebM(t)
	for _, tc := range []struct {
		name   string
		data   []byte
		plain  bool
		tracks int
	}{
		{"misnamed.webm", mp4, false, 2},
		{"misnamed.mp4", webm, false, 1},
		{"no-extension", webm, false, 1},
		{"pipe.mp4", webm, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reader io.Reader = bytes.NewReader(tc.data)
			if tc.plain {
				reader = struct{ io.Reader }{reader}
			}
			var out bytes.Buffer
			if err := offlineRuntime().Run(context.Background(), goav.FileInput(tc.name, reader).To(goav.Write("out.mkv", &out))); err != nil {
				t.Fatalf("run: %v", err)
			}
			if got := matroskaTracks(t, out.Bytes()); got != tc.tracks {
				t.Fatalf("remuxed %d tracks, want %d", got, tc.tracks)
			}
		})
	}
}

// TestInputDetectedWithoutDemuxerIsNamed pins the refusal for content goav
// recognizes but cannot demux: it names the detected format, whatever the
// file is called.
func TestInputDetectedWithoutDemuxerIsNamed(t *testing.T) {
	mp3 := append([]byte("ID3\x04\x00\x00\x00\x00\x00\x00"), make([]byte, 64)...)
	err := offlineRuntime().Run(context.Background(), goav.FileInput("song.webm", bytes.NewReader(mp3)).To(goavtest.NewCollector().Sink()))
	requireBuildError(t, err, errcode.Code("input_demuxer_missing"), "open input", "song.webm", "mp3 demuxer")
	if !strings.Contains(err.Error(), `"mp3"`) {
		t.Fatalf("refusal does not name the detected format:\n%v", err)
	}
}

package mp4

import (
	"bytes"
	"os"
	"testing"
)

// FuzzDemuxerMalformedInputs drives arbitrary bytes through the demuxer to
// prove that parsing and sample reading never panic, hang, or allocate
// unbounded memory on malformed input. Valid files are seeded so the fuzzer can
// mutate from a real structure.
func FuzzDemuxerMalformedInputs(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("\x00\x00\x00\x10ftypisom\x00\x00\x00\x00"))
	f.Add(box("moov")) // empty movie
	f.Add(buildVideoMP4())
	if half := buildVideoMP4(); len(half) > 8 {
		f.Add(half[:len(half)/2]) // truncated mid-file
	}
	if real, err := os.ReadFile("testdata/h264_aac.mp4"); err == nil {
		f.Add(real)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		demuxer, err := NewDemuxer(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return
		}
		_ = demuxer.Tracks()
		buf := make([]byte, 0, 1<<16)
		var sample Sample
		for i := 0; i < 256; i++ {
			// Any error (EOF, ErrInvalidData, a reader io error) ends this input;
			// the fuzz target only asserts the demuxer never panics or hangs.
			if err := demuxer.ReadInto(buf, &sample); err != nil {
				return
			}
			buf = sample.Data[:0]
		}
	})
}

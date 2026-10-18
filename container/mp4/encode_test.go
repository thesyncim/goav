package mp4_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/launch"
)

// memFiles is a launch.FileSystem in memory: seekable outputs, or plain
// writers that cannot seek, as a pipe is.
type memFiles struct {
	pipe  bool
	files map[string]*memFile
}

type memFile struct {
	data []byte
	pos  int64
}

func (f *memFile) Write(p []byte) (int, error) {
	if end := f.pos + int64(len(p)); end > int64(len(f.data)) {
		f.data = append(f.data, make([]byte, end-int64(len(f.data)))...)
	}
	n := copy(f.data[f.pos:], p)
	f.pos += int64(n)
	return n, nil
}

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = int64(len(f.data)) + offset
	}
	return f.pos, nil
}

type pipeWriter struct{ f *memFile }

func (w pipeWriter) Write(p []byte) (int, error) { return w.f.Write(p) }

func (fs *memFiles) Open(name string) (io.Reader, error) {
	if f, ok := fs.files[name]; ok {
		return bytes.NewReader(f.data), nil
	}
	return nil, os.ErrNotExist
}

func (fs *memFiles) Create(name string) (io.Writer, error) {
	f := &memFile{}
	fs.files[name] = f
	if fs.pipe {
		return pipeWriter{f}, nil
	}
	return f, nil
}

// TestEncodedMP4Decodes encodes video and audio with the bundled encoders
// into MP4, progressive and fragmented, and decodes every frame back.
func TestEncodedMP4Decodes(t *testing.T) {
	for _, tc := range []struct {
		name, codec string
		pipe        bool
	}{
		{"vp9 progressive", "vp9", false},
		{"vp8 fragmented", "vp8", true},
		{"av1 progressive", "av1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := &memFiles{pipe: tc.pipe, files: map[string]*memFile{}}
			outs, err := launch.Compile(`testsrc video size=64x48 frames=30 ! encode `+tc.codec+` keyframe_interval=10 ! file out.mp4
testsrc audio duration=1s ! encode opus ! file out.mp4`, launch.Files(files))
			if err != nil {
				t.Fatal(err)
			}
			if err := bundle.Run(context.Background(), outs...); err != nil {
				t.Fatal(err)
			}
			data := files.files["out.mp4"].data
			if fragmented := bytes.Contains(data, []byte("moof")); fragmented != tc.pipe {
				t.Fatalf("fragmented = %v, want %v", fragmented, tc.pipe)
			}
			var frames, samples atomic.Int64
			in := goav.FileInput("out.mp4", bytes.NewReader(data))
			err = bundle.Run(context.Background(),
				in.Video().Decode().To(goav.Sink(component.SinkFunc("frames", func(_ context.Context, m component.Message) error {
					if m.Frame != nil {
						frames.Add(1)
					}
					return nil
				}))),
				in.Audio().Decode().To(goav.Sink(component.SinkFunc("samples", func(_ context.Context, m component.Message) error {
					if m.Frame != nil && m.Frame.Audio != nil {
						samples.Add(int64(m.Frame.Audio.Samples))
					}
					return nil
				}))),
			)
			if err != nil {
				t.Fatal(err)
			}
			if frames.Load() != 30 {
				t.Fatalf("decoded %d video frames, want 30", frames.Load())
			}
			// One second of 48 kHz audio, give or take the last frame's padding.
			if got := samples.Load(); got < 47_000 || got > 49_000 {
				t.Fatalf("decoded %d audio samples, want about 48000", got)
			}
			ffprobe(t, data)
		})
	}
}

// ffprobe checks the file with FFmpeg when it is installed: every packet
// decodes without an error. CI runs without it.
func ffprobe(t *testing.T, data []byte) {
	t.Helper()
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		return
	}
	path := filepath.Join(t.TempDir(), "out.mp4")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(ffmpeg, "-v", "error", "-i", path, "-f", "null", "-").CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) || len(bytes.TrimSpace(out)) != 0 {
		t.Fatalf("ffmpeg found problems: %v\n%s", err, out)
	}
}

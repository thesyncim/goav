package goav

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/scheme"
	"github.com/thesyncim/goav/shape"
)

// TestAttachResolvesURIThroughTheRuntimeSchemes pins the attach path: an
// Output attached to a running task resolves its URI through the same
// registry the build used, and the drained branch commits the file.
func TestAttachResolvesURIThroughTheRuntimeSchemes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	in := Source("cam", shape.Packet(av.MediaVideo, av.CodecVP8, shape.Video(16, 16, ""), shape.Stream("cam"), shape.Realtime(true)),
		func(ctx context.Context, push SourcePush) error {
			for i := int64(0); ctx.Err() == nil; i++ {
				packet := &av.Packet{StreamID: "cam", Type: av.MediaVideo, Keyframe: true, Payload: av.Buffer{Bytes: []byte{0x10, 0x20, 0x30}},
					PTS: av.Timestamp{Value: i, Base: av.TimeBase{Num: 1, Den: 1000}}}
				if _, err := push.Packet(packet); err != nil {
					return err
				}
				time.Sleep(2 * time.Millisecond)
			}
			return nil
		})
	main := Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))
	task, err := testBundleRuntime(runconfig.Scheme("file", scheme.File())).Build(ctx, in.Video().Tap("live").To(main))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	out := filepath.Join(t.TempDir(), "attached.ivf")
	var attachment Attachment
	waitForCondition(t, "file branch attached", func() bool {
		attachment, err = task.Attach(ctx, task.Tap("live").As("record").To(URI(attachFileURL(out))))
		return err == nil
	})
	waitForCondition(t, "file written", func() bool {
		info, err := os.Stat(out)
		return err == nil && info.Size() > 32
	})
	if err := task.Detach(ctx, attachment, DrainBranch()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(out)
	if err != nil || !bytes.HasPrefix(data, []byte("DKIF")) {
		t.Fatalf("attached output = %q, %v", data[:min(4, len(data))], err)
	}
	cancel()
	<-runErr
}

// attachFileURL is the file URL of a local path: file:///C:/x on Windows, whose
// paths have no leading slash.
func attachFileURL(path string) string {
	p := filepath.ToSlash(path)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return "file://" + p
}

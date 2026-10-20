package goav_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/goavtest"
)

// TestSeekOnAContainerThatCannotSeekIsRefused pins the seek refusal's fix
// for a seekable file whose container has no seek (IVF): the refusal is
// format.ErrNotSeekable naming the containers that can, and the task plays
// on.
func TestSeekOnAContainerThatCannotSeekIsRefused(t *testing.T) {
	ctx := context.Background()
	var ivf bytes.Buffer
	if err := offlineRuntime().Run(ctx, goavtest.Video(64, 48, 3).Video().Encode(codec.VP8()).To(goav.Write("in.ivf", &ivf))); err != nil {
		t.Fatal(err)
	}
	out := goavtest.NewCollector()
	task, err := offlineRuntime().Build(ctx, goav.FileInput("in.ivf", bytes.NewReader(ivf.Bytes())).To(out.Sink()))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	err = task.Control(ctx, control.Seek(time.Second))
	if !errors.Is(err, format.ErrNotSeekable) || !strings.Contains(err.Error(), "IVF and Annex B cannot") {
		t.Fatalf("seek err = %v, want format.ErrNotSeekable naming the containers that seek", err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatalf("run after a refused seek: %v", err)
	}
	if got := len(out.Packets()); got != 3 {
		t.Fatalf("delivered %d packets, want all 3", got)
	}
}

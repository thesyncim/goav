package cameraav

import (
	"context"
	"errors"
	"github.com/thesyncim/goav/pipeline"
	"os"
	"testing"
	"time"
)

func TestCameraHardwareOptIn(t *testing.T) {
	device := os.Getenv("GOAV_CAMERA_TEST_DEVICE")
	if device == "" {
		t.Skip("set GOAV_CAMERA_TEST_DEVICE to opt into capture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	src, streams, err := New(Config{Device: device}).OpenSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if len(streams) != 1 {
		t.Fatal(streams)
	}
	count := 0
	var previous int64
	finished := errors.New("captured smoke frames")
	err = src.Start(ctx, cameraEmitter(func(_ context.Context, m *pipeline.Message) error {
		if m.Frame == nil || m.Frame.Video.Width != 640 || m.Frame.Video.Height != 480 || len(m.Frame.Planes) != 3 {
			t.Error("unexpected capture frame")
			return finished
		}
		if count > 0 && m.Frame.PTS.Value < previous {
			t.Error("timestamp regression")
		}
		previous = m.Frame.PTS.Value
		count++
		if count == 6 {
			return finished
		}
		return nil
	}))
	if !errors.Is(err, finished) || count != 6 {
		t.Fatalf("capture count=%d error=%v", count, err)
	}
	if err = src.Close(); err != nil {
		t.Fatal(err)
	}
}

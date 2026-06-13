package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

func TestCustomDestinationReceivesResolvedInfo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dest, info, err := runCustomDestination(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "mem://voice.ogg" || info.Format != av.FormatOgg || info.MIMEType != "audio/ogg" {
		t.Fatalf("provider info = %+v", info)
	}
	if len(info.Streams) != 1 || info.Metadata["kind"] != "demo-destination" {
		t.Fatalf("provider info = %+v", info)
	}
	if dest.closes != 1 || dest.Len() == 0 {
		t.Fatalf("destination closes=%d bytes=%d", dest.closes, dest.Len())
	}
	output := fmt.Sprintln("name:", info.Name) +
		fmt.Sprintln("format:", info.Format, "mime:", info.MIMEType, "streams:", len(info.Streams)) +
		fmt.Sprintln("kind:", info.Metadata["kind"], "closed:", dest.closes, "bytes:", dest.Len() > 0)
	if output != expectedOutput(t) {
		t.Fatalf("output = %q, want %q", output, expectedOutput(t))
	}
}

func TestBrokenDestinationRefusesAtOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := runBrokenDestination(ctx); err == nil {
		t.Fatal("expected nil writer opener to fail")
	}
}

func expectedOutput(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("testdata/expected.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

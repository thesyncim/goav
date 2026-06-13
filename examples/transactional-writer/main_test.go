package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

func TestSuccessfulUploadCommits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	upload, info, err := runSuccessfulUpload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Format != av.FormatOgg || len(info.Streams) != 1 || info.Metadata["kind"] != "call-recording" {
		t.Fatalf("provider info = %+v", info)
	}
	if upload.commits != 1 || upload.aborts != 0 || upload.closes != 1 || upload.Len() == 0 {
		t.Fatalf("upload commits=%d aborts=%d closes=%d bytes=%d", upload.commits, upload.aborts, upload.closes, upload.Len())
	}
	output := fmt.Sprintln("format:", info.Format) +
		fmt.Sprintln("commits:", upload.commits, "aborts:", upload.aborts, "bytes:", upload.Len() > 0)
	if output != expectedOutput(t) {
		t.Fatalf("output = %q, want %q", output, expectedOutput(t))
	}
}

func TestFailedUploadAborts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	upload, err := runFailedUpload(ctx)
	if err == nil {
		t.Fatal("expected induced failure")
	}
	if upload.commits != 0 || upload.aborts != 1 || upload.closes != 1 {
		t.Fatalf("upload commits=%d aborts=%d closes=%d", upload.commits, upload.aborts, upload.closes)
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

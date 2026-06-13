package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest/expect"
)

func TestSuccessfulUploadCommits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	upload, info, err := runSuccessfulUpload(ctx)
	expect.NoError(t, err)
	expect.Equal(t, "provider format", info.Format, av.FormatOgg)
	expect.Equal(t, "provider streams", len(info.Streams), 1)
	expect.Equal(t, "provider metadata kind", info.Metadata["kind"], "call-recording")
	expect.Equal(t, "upload commits", upload.commits, 1)
	expect.Equal(t, "upload aborts", upload.aborts, 0)
	expect.Equal(t, "upload closes", upload.closes, 1)
	expect.Equal(t, "upload wrote bytes", upload.Len() > 0, true)
	output := fmt.Sprintln("format:", info.Format) +
		fmt.Sprintln("commits:", upload.commits, "aborts:", upload.aborts, "bytes:", upload.Len() > 0)
	expect.GoldenString(t, "testdata/expected.txt", output)
}

func TestFailedUploadAborts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	upload, err := runFailedUpload(ctx)
	expect.Error(t, err)
	expect.Equal(t, "upload commits", upload.commits, 0)
	expect.Equal(t, "upload aborts", upload.aborts, 1)
	expect.Equal(t, "upload closes", upload.closes, 1)
}

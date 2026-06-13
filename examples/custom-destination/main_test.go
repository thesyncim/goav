package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest/expect"
)

func TestCustomDestinationReceivesResolvedInfo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dest, info, err := runCustomDestination(ctx)
	expect.NoError(t, err)
	expect.Equal(t, "provider name", info.Name, "mem://voice.ogg")
	expect.Equal(t, "provider format", info.Format, av.FormatOgg)
	expect.Equal(t, "provider MIME type", info.MIMEType, "audio/ogg")
	expect.Len(t, "provider streams", info.Streams, 1)
	expect.Equal(t, "provider metadata kind", info.Metadata["kind"], "demo-destination")
	expect.Equal(t, "destination closes", dest.closes, 1)
	expect.Equal(t, "destination wrote bytes", dest.Len() > 0, true)
	output := fmt.Sprintln("name:", info.Name) +
		fmt.Sprintln("format:", info.Format, "mime:", info.MIMEType, "streams:", len(info.Streams)) +
		fmt.Sprintln("kind:", info.Metadata["kind"], "closed:", dest.closes, "bytes:", dest.Len() > 0)
	expect.GoldenString(t, "testdata/expected.txt", output)
}

func TestBrokenDestinationRefusesAtOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	expect.Error(t, runBrokenDestination(ctx))
}

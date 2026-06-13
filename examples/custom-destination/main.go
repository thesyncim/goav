package main

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/provider"
)

func main() {
	dest, info, err := runCustomDestination(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("name:", info.Name)
	fmt.Println("format:", info.Format, "mime:", info.MIMEType, "streams:", len(info.Streams))
	fmt.Println("kind:", info.Metadata["kind"], "closed:", dest.closes, "bytes:", dest.Len() > 0)
}

type memoryDestination struct {
	bytes.Buffer
	closes int
}

func (d *memoryDestination) Close() error {
	d.closes++
	return nil
}

func runCustomDestination(ctx context.Context) (*memoryDestination, provider.Info, error) {
	dest := &memoryDestination{}
	var opened provider.Info
	err := goav.From(goavtest.Audio(48000, 1, []int16{11, 12, 13, 14})).
		Audio().
		Encode(codec.Opus()).
		To(memoryWriter("mem://voice.ogg", dest, &opened)).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	return dest, opened, err
}

func runBrokenDestination(ctx context.Context) error {
	return goav.From(goavtest.Audio(48000, 1, []int16{11, 12})).
		Audio().
		Encode(codec.Opus()).
		To(goav.Writer("mem://broken.ogg", nil, goav.Format(av.FormatOgg))).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
}

func memoryWriter(name string, dest *memoryDestination, opened *provider.Info) goav.Destination {
	return goav.Writer(name,
		func(_ context.Context, info provider.Info) (io.WriteCloser, error) {
			*opened = info
			return dest, nil
		},
		goav.Format(av.FormatOgg),
		goav.MIME("audio/ogg"),
		goav.Metadata(av.Metadata{"kind": "demo-destination"}),
	)
}

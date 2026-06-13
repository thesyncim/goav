package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/provider"
)

func main() {
	upload, info, err := runSuccessfulUpload(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("format:", info.Format)
	fmt.Println("commits:", upload.commits, "aborts:", upload.aborts, "bytes:", upload.Len() > 0)
}

type memoryUpload struct {
	bytes.Buffer
	commits int
	aborts  int
	closes  int
}

func (u *memoryUpload) Close() error {
	u.closes++
	return nil
}

func (u *memoryUpload) Commit(context.Context) error {
	u.commits++
	return nil
}

func (u *memoryUpload) Abort(context.Context) error {
	u.aborts++
	return nil
}

func runSuccessfulUpload(ctx context.Context) (*memoryUpload, provider.Info, error) {
	upload := &memoryUpload{}
	var opened provider.Info
	err := goav.From(goavtest.Audio(48000, 1, []int16{7, 8})).
		Audio().
		Encode(codec.Opus()).
		To(uploadDestination(upload, &opened)).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	return upload, opened, err
}

func runFailedUpload(ctx context.Context) (*memoryUpload, error) {
	upload := &memoryUpload{}
	var opened provider.Info
	fail := goav.FrameFunc("fail-before-commit", func(context.Context, *av.Frame, goav.Emit) error {
		return errors.New("transactional-writer: induced failure")
	})
	err := goav.From(goavtest.Audio(48000, 1, []int16{7, 8})).
		Audio().
		Do(fail).
		Encode(codec.Opus()).
		To(uploadDestination(upload, &opened)).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	return upload, err
}

func uploadDestination(upload *memoryUpload, opened *provider.Info) goav.Destination {
	return goav.Writer("mem://voice.ogg",
		func(_ context.Context, info provider.Info) (io.WriteCloser, error) {
			*opened = info
			return upload, nil
		},
		goav.Format(av.FormatOgg),
		goav.MIME("audio/ogg"),
		goav.Metadata(av.Metadata{"kind": "call-recording"}),
	)
}

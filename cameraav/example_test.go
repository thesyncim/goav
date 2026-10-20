package cameraav_test

import (
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/cameraav"
	"github.com/thesyncim/goav/codec"
)

func ExampleNew() {
	camera := goav.Input(cameraav.New(cameraav.Config{Width: 1280, Height: 720, FPS: 30}))
	output := camera.Video().Encode(codec.VP8()).To(goav.URI("camera.webm"))
	_ = output // Pass to a configured runtime's Build or Run.
}

package main

import "github.com/thesyncim/goav/av"

type nativeAudio interface {
	HandleFrame(*av.Frame)
	Status() nativeAudioStatus
	Close() error
}

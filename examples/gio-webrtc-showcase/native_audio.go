//go:build !nativeaudio

package main

import "github.com/thesyncim/goav/av"

type disabledNativeAudio struct{}

func newNativeAudio() nativeAudio {
	return disabledNativeAudio{}
}

func (disabledNativeAudio) HandleFrame(*av.Frame) {}

func (disabledNativeAudio) Status() nativeAudioStatus {
	return nativeAudioStatus{
		Available: false,
		Enabled:   false,
		Message:   "native speaker output is available with -tags nativeaudio; browser WebRTC playback remains active",
	}
}

func (disabledNativeAudio) Close() error {
	return nil
}

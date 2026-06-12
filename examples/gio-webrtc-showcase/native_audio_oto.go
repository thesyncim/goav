//go:build nativeaudio

package main

import (
	"io"
	"sync"
	"time"

	"github.com/ebitengine/oto/v3"
	"github.com/thesyncim/goav/av"
)

type otoNativeAudio struct {
	mu      sync.Mutex
	status  nativeAudioStatus
	started bool
	frames  chan []byte
	done    chan struct{}
	writer  *io.PipeWriter
	player  *oto.Player
}

func newNativeAudio() nativeAudio {
	return &otoNativeAudio{
		status: nativeAudioStatus{
			Available: true,
			Enabled:   false,
			Message:   "native speaker output will start on the first compatible S16 frame",
		},
	}
}

func (a *otoNativeAudio) HandleFrame(frame *av.Frame) {
	if frame == nil || frame.Audio == nil || len(frame.Planes) == 0 || frame.Audio.SampleFormat != av.SampleFormatS16 {
		return
	}
	if !a.ensure(frame.Audio.SampleRate, frame.Audio.Channels) {
		return
	}
	payload := append([]byte(nil), frame.Planes[0].Buffer.Bytes...)
	select {
	case a.frames <- payload:
	default:
		a.mu.Lock()
		a.status.Message = "native speaker buffer is full; dropping preview PCM"
		a.mu.Unlock()
	}
}

func (a *otoNativeAudio) ensure(sampleRate, channels int) bool {
	if sampleRate <= 0 {
		sampleRate = 48000
	}
	if channels <= 0 {
		channels = 1
	}
	if channels != 1 && channels != 2 {
		a.mu.Lock()
		a.status.Message = "native speaker supports mono or stereo S16 PCM"
		a.mu.Unlock()
		return false
	}

	a.mu.Lock()
	if a.started {
		enabled := a.status.Enabled
		a.mu.Unlock()
		return enabled
	}
	a.started = true
	a.frames = make(chan []byte, 48)
	a.done = make(chan struct{})
	reader, writer := io.Pipe()
	a.writer = writer
	a.mu.Unlock()

	ctx, ready, err := oto.NewContext(&oto.NewContextOptions{
		SampleRate:   sampleRate,
		ChannelCount: channels,
		Format:       oto.FormatSignedInt16LE,
		BufferSize:   80 * time.Millisecond,
	})
	if err != nil {
		_ = reader.Close()
		_ = writer.Close()
		a.mu.Lock()
		a.status.Enabled = false
		a.status.Message = err.Error()
		a.mu.Unlock()
		return false
	}
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
	}

	player := ctx.NewPlayer(reader)
	player.SetBufferSize(sampleRate * channels * 2 / 5)
	player.Play()

	a.mu.Lock()
	a.player = player
	a.status.Enabled = true
	a.status.Message = "native speaker output enabled through Oto"
	a.mu.Unlock()

	go a.writeLoop(writer)
	return true
}

func (a *otoNativeAudio) writeLoop(writer *io.PipeWriter) {
	for {
		select {
		case frame := <-a.frames:
			if len(frame) == 0 {
				continue
			}
			if _, err := writer.Write(frame); err != nil {
				a.mu.Lock()
				a.status.Enabled = false
				a.status.Message = err.Error()
				a.mu.Unlock()
				return
			}
		case <-a.done:
			return
		}
	}
}

func (a *otoNativeAudio) Status() nativeAudioStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

func (a *otoNativeAudio) Close() error {
	a.mu.Lock()
	done := a.done
	writer := a.writer
	player := a.player
	a.done = nil
	a.writer = nil
	a.player = nil
	a.status.Enabled = false
	a.mu.Unlock()
	if done != nil {
		close(done)
	}
	if writer != nil {
		_ = writer.Close()
	}
	if player != nil {
		_ = player.Close()
	}
	return nil
}

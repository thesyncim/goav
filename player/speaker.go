package player

import (
	"fmt"
	"time"

	"github.com/ebitengine/oto/v3"
	"github.com/thesyncim/goav/player/internal/engine"
)

// speakerBuffer is how much rendered audio the speaker player holds ahead of
// the device: small, so a pause or seek is heard promptly.
const speakerBuffer = 40 * time.Millisecond

// fill adapts the engine's renderer to the io.Reader oto pulls from; it never
// blocks and never ends.
type fill struct{ e *engine.Engine }

func (f fill) Read(p []byte) (int, error) { return f.e.Fill(p), nil }

// openSpeaker opens the default audio device at the engine's output format
// and starts it pulling. The device then plays from the moment it opens —
// silence until media arrives — so its clock runs like hardware does.
func openSpeaker(e *engine.Engine) (stop func(), err error) {
	ctx, ready, err := oto.NewContext(&oto.NewContextOptions{
		SampleRate:      engine.OutputRate,
		ChannelCount:    engine.OutputChannels,
		Format:          oto.FormatSignedInt16LE,
		BufferSize:      speakerBuffer,
		ApplicationName: "goav",
	})
	if err != nil {
		return nil, err
	}
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("audio device did not become ready")
	}
	p := ctx.NewPlayer(fill{e})
	p.SetBufferSize(int(speakerBuffer.Seconds()*engine.OutputRate) * engine.BytesPerFrame)
	// Past the player buffer oto reports, a sample still waits in the device
	// queue (oto keeps it full: speakerBuffer, split into periods on darwin)
	// and then the hardware's own output latency, where the platform
	// reports one. Both delay what is heard, so both shift the heard position.
	e.SetDevice(func() int { return p.BufferedSize() / engine.BytesPerFrame }, speakerBuffer+hardwareLatency())
	p.Play()
	// The engine's device callback keeps the player reachable while it plays
	// (oto closes a collected player).
	return p.Pause, nil
}

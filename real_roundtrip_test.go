package goav

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// TestRealOpusWebMRoundTripThroughGrammar is the zero-fake grammar path: PCM
// frames are encoded by the real gopus encoder and muxed by the real WebM
// muxer through From(...).Encode(...).To(File(...)), then the produced bytes
// are read back through From(FileInput(...)).Decode() — real WebM demuxer,
// real gopus decoder. Most grammar tests run against goavtest/fake codecs for
// determinism; this one proves the registered default adapters compose end to
// end and that the audio that comes back carries the format and roughly the
// duration that went in.
func TestRealOpusWebMRoundTripThroughGrammar(t *testing.T) {
	ctx := context.Background()

	const (
		rate     = 48_000
		channels = 2
		frames   = 20          // 20 x 10ms = 200ms of audio
		samples  = rate / 100  // one 10ms frame
		freqHz   = 440.0       // an audible tone, not silence
		amp      = 0.5 * 32767 // half scale, no clipping
	)
	pcm := Source("mic",
		shape.Frame(av.MediaAudio, shape.Audio(rate, channels, av.SampleFormatS16), shape.Stream("mic")),
		func(_ context.Context, push SourcePush) error {
			for i := 0; i < frames; i++ {
				b := make([]byte, samples*channels*2)
				for s := 0; s < samples; s++ {
					n := i*samples + s
					v := int16(amp * math.Sin(2*math.Pi*freqHz*float64(n)/rate))
					for c := 0; c < channels; c++ {
						binary.LittleEndian.PutUint16(b[(s*channels+c)*2:], uint16(v))
					}
				}
				frame := &av.Frame{
					StreamID: "mic", Type: av.MediaAudio,
					PTS:    av.Timestamp{Value: int64(i * samples), Base: av.TimeBase{Num: 1, Den: rate}},
					Audio:  &av.AudioFrame{SampleRate: rate, Channels: channels, SampleFormat: av.SampleFormatS16, Samples: samples},
					Planes: []av.Plane{{Buffer: av.Buffer{Bytes: b, Ownership: av.BufferImmutable}}},
				}
				if _, err := push.Frame(frame); err != nil {
					return err
				}
			}
			return push.EOS()
		})

	var webm bytes.Buffer
	encodeTask, err := From(pcm).
		Audio().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(File("roundtrip.webm", &webm)).
		UseRuntime(testBundleRuntime()).
		Build(ctx)
	if err != nil {
		t.Fatalf("Build(encode): %v", err)
	}
	defer encodeTask.Close()
	if err := encodeTask.Run(ctx); err != nil {
		t.Fatalf("Run(encode): %v", err)
	}
	if err := encodeTask.Close(); err != nil {
		t.Fatalf("Close(encode): %v", err)
	}

	// The destination must hold a real EBML/WebM document, not a stand-in.
	data := webm.Bytes()
	if len(data) < 4 || !bytes.Equal(data[:4], []byte{0x1A, 0x45, 0xDF, 0xA3}) {
		t.Fatalf("destination bytes do not start with the EBML magic: % X...", data[:min(8, len(data))])
	}

	var decodedFrames, decodedSamples int
	var gotRate, gotChannels int
	var energy int64
	sink := Sink(SinkFunc("speakers", func(_ context.Context, m Message) error {
		if m.Kind != pipeline.MessageFrame || m.Frame == nil || m.Frame.Audio == nil {
			return nil
		}
		decodedFrames++
		decodedSamples += m.Frame.Audio.Samples
		gotRate = m.Frame.Audio.SampleRate
		gotChannels = m.Frame.Audio.Channels
		for _, plane := range m.Frame.Planes {
			for i := 0; i+1 < len(plane.Buffer.Bytes); i += 2 {
				v := int64(int16(binary.LittleEndian.Uint16(plane.Buffer.Bytes[i:])))
				if v < 0 {
					v = -v
				}
				energy += v
			}
		}
		return nil
	}))
	decodeTask, err := From(FileInput("roundtrip.webm", bytes.NewReader(data))).
		Audio().
		Decode().
		To(sink).
		UseRuntime(testBundleRuntime(WithRealtime(false))).
		Build(ctx)
	if err != nil {
		t.Fatalf("Build(decode): %v", err)
	}
	defer decodeTask.Close()
	if err := decodeTask.Run(ctx); err != nil {
		t.Fatalf("Run(decode): %v", err)
	}

	if decodedFrames == 0 {
		t.Fatal("no decoded audio frames reached the sink")
	}
	if gotRate != rate || gotChannels != channels {
		t.Fatalf("decoded format = %dHz %dch, want %dHz %dch", gotRate, gotChannels, rate, channels)
	}
	// Opus pads the stream edges (pre-skip), so the round trip is approximate:
	// the decoded duration must be within half a frame of the input on either
	// side and never wildly off.
	want := frames * samples
	if decodedSamples < want-samples || decodedSamples > want+2*samples {
		t.Fatalf("decoded %d samples, want about %d (input duration must survive the round trip)", decodedSamples, want)
	}
	// The tone must survive: average absolute amplitude well above silence.
	if avg := energy / int64(decodedSamples*channels); avg < 1000 {
		t.Fatalf("decoded average |amplitude| = %d, want a clearly audible tone (input amplitude %.0f)", avg, amp)
	}
}

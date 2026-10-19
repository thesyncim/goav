package kit

import (
	"context"
	"errors"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/runconfig"
)

// Codec declares one native backend of one codec: its identity, what it can
// do, the settings it takes, how it loads, and its sessions. Register turns
// it into encoder and decoder factories; the kit owns everything between
// goav and the sessions — frame and packet conversion, timestamps,
// reference-counted buffers, settings, live controls, and Close.
type Codec struct {
	// ID, Media, and Backend identify the backend: h264/openh264. Kind ranks
	// it for selection (codec.KindHardware or codec.KindNative). Package is
	// the Go package Explain names.
	ID      av.CodecID
	Media   av.MediaType
	Backend string
	Kind    codec.BackendKind
	Package string

	// Profiles, Capabilities, and Realtime declare what the backend can do;
	// selection keeps to backends whose declarations fit a request.
	// Capabilities.PixelFormats defaults to i420 and yuv420p for video, and
	// SampleFormats to s16 for audio.
	Profiles     []string
	Capabilities codec.Capabilities
	Realtime     bool

	// Knobs are the encoder's settings and DecoderKnobs the decoder's (see
	// codec.Knob); RawKnobs check the encoder's raw parameters.
	Knobs        []codec.Knob
	DecoderKnobs []codec.Knob
	RawKnobs     []codec.Knob

	// Bitstream and Pixels say how a video backend frames NAL units and
	// lays out pictures; the kit converts goav's Annex B and I420.
	Bitstream Bitstream
	Pixels    PixelFormat

	// Load binds the library, once; the kit calls it before every open and
	// from Probe. A failure should be an *Error naming the fix.
	Load func() error

	// The session constructors; a nil one leaves that mode unregistered.
	NewVideoEncoder func(VideoConfig) (VideoEncoder, error)
	NewVideoDecoder func(VideoConfig) (VideoDecoder, error)
	NewAudioEncoder func(AudioConfig) (AudioEncoder, error)
	NewAudioDecoder func(AudioConfig) (AudioDecoder, error)
}

// Register returns the runtime option that adds each backend's encoder and
// decoder to the runtime's codec registry, alongside every other backend of
// the same codec. Nothing loads until a session opens or Probe runs.
func Register(codecs ...Codec) runconfig.Option {
	return runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		for i := range codecs {
			c := &codecs[i]
			if c.NewVideoDecoder != nil || c.NewAudioDecoder != nil {
				registry.RegisterDecoder(c.descriptor(codec.ModeDecode), decoderFactory{c})
			}
			if c.NewVideoEncoder != nil || c.NewAudioEncoder != nil {
				registry.RegisterEncoder(c.descriptor(codec.ModeEncode), encoderFactory{c})
			}
		}
	})
}

// Probe loads every kit backend the options register and reports each one
// that cannot run here, with its fix: Probe(native.OpenH264()) is nil only
// when libopenh264 loads and binds.
func Probe(options ...runconfig.Option) error {
	config, err := runconfig.NewConfig(options...)
	if err != nil {
		return err
	}
	var errs []error
	seen := map[string]bool{}
	for _, desc := range config.Codecs.Descriptors() {
		for _, mode := range desc.Modes {
			choice, err := config.Codecs.Select(codec.Need{ID: desc.ID, Mode: mode, Backend: desc.Backend.Name})
			if err != nil {
				continue
			}
			var factory any = choice.Encoder
			if mode == codec.ModeDecode {
				factory = choice.Decoder
			}
			prober, ok := factory.(interface{ probe() error })
			if !ok || seen[desc.Backend.Name] {
				continue
			}
			seen[desc.Backend.Name] = true
			if err := prober.probe(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (c *Codec) descriptor(mode codec.Mode) codec.Descriptor {
	capabilities := c.Capabilities
	if c.Media == av.MediaVideo && capabilities.PixelFormats == nil {
		capabilities.PixelFormats = []string{av.PixelFormatI420, av.PixelFormatYUV420P}
	}
	if c.Media == av.MediaAudio && capabilities.SampleFormats == nil {
		capabilities.SampleFormats = []string{av.SampleFormatS16}
	}
	knobs, raw := c.Knobs, c.RawKnobs
	if mode == codec.ModeDecode {
		knobs, raw = c.DecoderKnobs, nil
		if knobs == nil {
			knobs = []codec.Knob{}
		}
	}
	return codec.Descriptor{
		ID:           c.ID,
		Name:         strings.ToUpper(string(c.ID)) + " (" + c.Backend + ")",
		Type:         c.Media,
		Modes:        []codec.Mode{mode},
		Profiles:     c.Profiles,
		Realtime:     c.Realtime,
		Capabilities: capabilities,
		Backend: codec.BackendInfo{
			Name:    c.Backend,
			Kind:    c.Kind,
			Module:  "github.com/thesyncim/goav/native",
			Package: c.Package,
			Status:  "active",
		},
		Knobs:    knobs,
		RawKnobs: raw,
	}
}

func (c *Codec) load() error {
	if c.Load == nil {
		return nil
	}
	return c.Load()
}

type encoderFactory struct{ c *Codec }

func (f encoderFactory) probe() error { return f.c.load() }

func (f encoderFactory) NewEncoder(ctx context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	var encoder codec.Encoder
	if f.c.Media == av.MediaAudio {
		encoder = &audioEncoder{c: f.c, desc: f.c.descriptor(codec.ModeEncode)}
	} else {
		encoder = &videoEncoder{c: f.c, desc: f.c.descriptor(codec.ModeEncode)}
	}
	if err := encoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return encoder, nil
}

type decoderFactory struct{ c *Codec }

func (f decoderFactory) probe() error { return f.c.load() }

func (f decoderFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	var decoder codec.Decoder
	if f.c.Media == av.MediaAudio {
		decoder = &audioDecoder{c: f.c, desc: f.c.descriptor(codec.ModeDecode)}
	} else {
		decoder = &videoDecoder{c: f.c, desc: f.c.descriptor(codec.ModeDecode)}
	}
	if err := decoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return decoder, nil
}

// Packet is one encoded unit lent to or by a backend for the length of one
// call: never keep Data. PTS, DTS, and Duration are in the stream's time
// base (samples for audio). A video encoder of an AVCC bitstream sets Config
// to its codec configuration record (avcC, hvcC) whenever it is new; the kit
// writes the parameter sets in-band ahead of every keyframe.
type Packet struct {
	Data     []byte
	PTS      int64
	DTS      int64
	Duration int64
	Keyframe bool
	Config   []byte
}

// Configured is an optional encoder capability: the codec configuration
// known at open — an AudioSpecificConfig, an avcC or hvcC record, or Annex B
// parameter sets — which the stream reports as its extradata before any
// muxer writes a header.
type Configured interface {
	Config() []byte
}

// Updater is an optional encoder capability for knobs declared Live: Update
// applies the new value of knob (in values) while running. Without it, or
// for a knob that is not Live, the kit restarts the session with the new
// value and a keyframe.
type Updater interface {
	Update(knob string, values codec.KnobValues) error
}

func refuseControl(c *Codec, settings codec.CodecSettings) error {
	if settings.Control == nil {
		return nil
	}
	return &Error{Backend: c.Backend, Op: "open", Name: "codec.Control", Reason: "is not offered by native backends", Fix: "pass library parameters as codec.Setting(\"" + c.Backend + "-params\", \"key=value:...\")"}
}

package kit

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/runconfig"
)

// Codec declares one native backend of one codec: its identity, what it can
// do, the settings it takes, the libraries it binds, and its sessions.
// Register turns it into encoder and decoder factories; the kit owns
// everything between goav and the sessions: frame and packet conversion,
// timestamps, reference-counted buffers, settings, live controls, and Close.
type Codec struct {
	// ID, Media, and Backend identify the backend: h264/openh264. Kind ranks
	// it for selection (codec.KindHardware or codec.KindNative). Module and
	// Package name its Go code in Explain.
	ID      av.CodecID
	Media   av.MediaType
	Backend string
	Kind    codec.BackendKind
	Module  string
	Package string

	// Platforms lists the GOOS values the backend runs on; empty is all.
	// Profiles, Capabilities, and Realtime declare what it can do: selection
	// keeps to backends whose declarations fit a request.
	// Capabilities.PixelFormats defaults to i420 and yuv420p for video, and
	// SampleFormats to s16 for audio.
	Platforms    []string
	Profiles     []string
	Capabilities codec.Capabilities
	Realtime     bool

	// Knobs are the encoder's settings and DecoderKnobs the decoder's (see
	// codec.Knob and its common knobs); RawKnobs check the encoder's raw
	// parameters (see Params).
	Knobs        []codec.Knob
	DecoderKnobs []codec.Knob
	RawKnobs     []codec.Knob

	// Bitstream says how a video backend frames its payloads, and Pixels the
	// pixel format its encoder reads (default i420; nv12 is converted).
	Bitstream Bitstream
	Pixels    string

	// Libraries are bound, once per process, when Register runs (at
	// goav.New, never in init). Version then reports the bound library's
	// version, refusing one outside the ABI window the backend mirrors. A
	// backend that fails either registers descriptor-only, with a status
	// that says why, and never serves a request; Probe reports the fix.
	Libraries []Library
	Version   func() (string, error)

	// The session constructors; a nil one leaves that mode unregistered.
	NewVideoEncoder func(VideoConfig) (VideoEncoder, error)
	NewVideoDecoder func(VideoConfig) (VideoDecoder, error)
	NewAudioEncoder func(AudioConfig) (AudioEncoder, error)
	NewAudioDecoder func(AudioConfig) (AudioDecoder, error)
}

// The statuses of a backend that cannot run here.
const (
	statusActive       = "active"
	statusNotInstalled = "not-installed"
	statusPlatform     = "unsupported-platform"
	statusVersion      = "unsupported-version"
	statusSymbol       = "missing-symbol"
)

// Register returns the runtime option that adds each backend to the
// runtime's codec registry, alongside every other backend of its codec. It
// binds the libraries then: a backend that cannot run here registers
// descriptor-only, so selection passes it over for one that can, and naming
// it explicitly is refused with the reason.
func Register(codecs ...Codec) runconfig.Option {
	return runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		for i := range codecs {
			c := &codecs[i]
			version, err := c.bind()
			for _, mode := range c.modes() {
				desc := c.descriptor(mode, version)
				if err != nil {
					desc.Backend.Status = statusOf(err)
					registry.RegisterDescriptor(desc)
					continue
				}
				if mode == codec.ModeDecode {
					registry.RegisterDecoder(desc, decoderFactory{c})
				} else {
					registry.RegisterEncoder(desc, encoderFactory{c})
				}
			}
		}
	})
}

func (c *Codec) modes() []codec.Mode {
	var modes []codec.Mode
	if c.NewVideoDecoder != nil || c.NewAudioDecoder != nil {
		modes = append(modes, codec.ModeDecode)
	}
	if c.NewVideoEncoder != nil || c.NewAudioEncoder != nil {
		modes = append(modes, codec.ModeEncode)
	}
	return modes
}

func statusOf(err error) string {
	var e *Error
	if !errors.As(err, &e) {
		return statusNotInstalled
	}
	switch e.Op {
	case "platform":
		return statusPlatform
	case "version":
		return statusVersion
	case "symbol":
		return statusSymbol
	}
	return statusNotInstalled
}

// binding is one backend's libraries bound once per process.
type binding struct {
	once    sync.Once
	version string
	err     error
}

var bindings sync.Map // *Codec -> *binding: each registered declaration binds once

func (c *Codec) bind() (string, error) {
	value, _ := bindings.LoadOrStore(c, &binding{})
	b := value.(*binding)
	b.once.Do(func() {
		if len(c.Platforms) != 0 && !slices.Contains(c.Platforms, runtime.GOOS) {
			b.err = unavailable(c.Backend, "platform", "", "runs only on "+strings.Join(c.Platforms, ", "), "use another "+string(c.ID)+" backend on "+runtime.GOOS, nil)
			return
		}
		for _, lib := range c.Libraries {
			if err := lib.Bind(c.Backend); err != nil {
				b.err = err
				return
			}
		}
		if c.Version != nil {
			b.version, b.err = c.Version()
		}
	})
	failures.Store(string(c.ID)+"/"+c.Backend, b.err)
	return b.version, b.err
}

// failures are the outcome of each backend's last registration, by codec
// and backend, for Probe.
var failures sync.Map

// Probe reports each kit backend the options register that cannot run
// here, with its fix: Probe(native.OpenH264()) is nil only when libopenh264
// loads, binds, and has a supported version.
func Probe(options ...runconfig.Option) error {
	config, err := runconfig.NewConfig(options...)
	if err != nil {
		return err
	}
	var errs []error
	seen := map[string]bool{}
	for _, desc := range config.Codecs.Descriptors() {
		key := string(desc.ID) + "/" + desc.Backend.Name
		if seen[key] {
			continue
		}
		seen[key] = true
		if failure, ok := failures.Load(key); ok && failure != nil {
			errs = append(errs, failure.(error))
		}
	}
	return errors.Join(errs...)
}

func (c *Codec) descriptor(mode codec.Mode, version string) codec.Descriptor {
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
	name := c.Backend
	if version != "" {
		name += " " + version
	}
	return codec.Descriptor{
		ID:           c.ID,
		Name:         name,
		Type:         c.Media,
		Modes:        []codec.Mode{mode},
		Profiles:     c.Profiles,
		Realtime:     c.Realtime,
		Capabilities: capabilities,
		Backend: codec.BackendInfo{
			Name:    c.Backend,
			Kind:    c.Kind,
			Module:  c.Module,
			Package: c.Package,
			Status:  statusActive,
		},
		Knobs:    knobs,
		RawKnobs: raw,
	}
}

type encoderFactory struct{ c *Codec }

func (f encoderFactory) NewEncoder(ctx context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	var encoder codec.Encoder
	if f.c.Media == av.MediaAudio {
		encoder = &audioEncoder{c: f.c, desc: f.c.descriptor(codec.ModeEncode, "")}
	} else {
		encoder = &videoEncoder{c: f.c, desc: f.c.descriptor(codec.ModeEncode, "")}
	}
	if err := encoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return encoder, nil
}

type decoderFactory struct{ c *Codec }

func (f decoderFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	var decoder codec.Decoder
	if f.c.Media == av.MediaAudio {
		decoder = &audioDecoder{c: f.c, desc: f.c.descriptor(codec.ModeDecode, "")}
	} else {
		decoder = &videoDecoder{c: f.c, desc: f.c.descriptor(codec.ModeDecode, "")}
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
// known at open (an AudioSpecificConfig, an avcC or hvcC record, Annex B
// parameter sets, an av1C), which the stream reports as its extradata before
// any muxer writes a header.
type Configured interface {
	Config() []byte
}

// Updater is an optional encoder capability for knobs declared Live: Update
// applies the new value of knob (in values) while running. Without it, or
// for a knob that is not Live, the kit restarts the session with the new
// value on a keyframe.
type Updater interface {
	Update(knob string, values codec.KnobValues) error
}

// EventSetting is the event that changes encoder knobs while running: each
// Metadata entry is a knob and its new value, applied as Updater says. Send
// it with control.Deliver; bitrate also arrives as av.EventBitrateChanged and
// keyframe requests as av.EventKeyframeRequired.
const EventSetting av.EventType = "codec_setting"

func refuseControl(c *Codec, settings codec.CodecSettings) error {
	if settings.Control == nil {
		return nil
	}
	return &Error{Backend: c.Backend, Op: "open", Name: "codec.Control", Reason: "is not offered by native backends", Fix: "pass library parameters as codec.Setting(\"" + c.Backend + "-params\", \"key=value:...\")"}
}

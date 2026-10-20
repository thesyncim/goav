package kit

import (
	"context"
	"errors"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/runconfig"
)

// Codec declares one library backend of one codec: its identity, what it can
// do, the settings it takes, the libraries it binds, its license, and its
// sessions. It is a plain value: reading it loads nothing. Register turns it
// into encoder and decoder factories; the kit owns everything between goav
// and the sessions: frame and packet conversion, timestamps,
// reference-counted buffers, settings, live controls, and Close.
type Codec struct {
	// ID, Media, and Backend identify the backend: h264/openh264. Kind ranks
	// it for selection (codec.KindHardware or codec.KindLib). Explain names
	// the Go module and package it lives in, read from the constructors.
	ID      av.CodecID
	Media   av.MediaType
	Backend string
	Kind    codec.BackendKind

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
	// codec.Knob and its common knobs); a backend takes no setting it does
	// not declare. RawKnobs check the encoder's raw parameters (see Params);
	// nil leaves them to the library, which checks them at open.
	Knobs        []codec.Knob
	DecoderKnobs []codec.Knob
	RawKnobs     []codec.Knob

	// Bitstream says how a video backend frames its payloads, and Pixels the
	// pixel format its encoder reads (default i420; nv12 is converted).
	Bitstream Bitstream
	Pixels    string
	// Reorder says the decoder returns pictures in decode order; the kit
	// holds them and hands them on in presentation order.
	Reorder bool

	// Libraries are bound when Register runs (at goav.New, never in init),
	// each file once per process. Version then reports the bound library's
	// version, which must fall in ABI, the window of releases whose layouts
	// the backend mirrors. A backend that cannot run registers
	// descriptor-only, with a status that says why (not-installed, disabled,
	// outside-window) and the error with the fix; selection passes it over.
	Libraries []Library
	ABI       Window
	Version   func() (string, error)

	// License is the library's license, with the notice and terms it asks a
	// program to carry; Releases are its published builds, for installers
	// and lockfiles.
	License  License
	Releases []Release

	// The session constructors; a nil one leaves that mode unregistered.
	NewVideoEncoder func(VideoConfig) (VideoEncoder, error)
	NewVideoDecoder func(VideoConfig) (VideoDecoder, error)
	NewAudioEncoder func(AudioConfig) (AudioEncoder, error)
	NewAudioDecoder func(AudioConfig) (AudioDecoder, error)

	reg *registration
}

// License is how a backend's library is licensed: its Class, the Notice its
// terms ask a program to show wherever it credits its codecs (Explain prints
// it), and the Terms a program must reproduce with itself.
type License struct {
	Class  LicenseClass
	Notice string
	Terms  string
}

// LicenseClass sorts licenses by what they ask of the program that loads
// the library.
type LicenseClass string

// The license classes.
const (
	// LicensePermissive is BSD, MIT, Apache: keep the notices.
	LicensePermissive LicenseClass = "permissive"
	// LicenseCopyleft is GPL and LGPL: the terms reach the program.
	LicenseCopyleft LicenseClass = "copyleft"
	// LicenseSystem is a library the OS ships: the program carries none.
	LicenseSystem LicenseClass = "system"
)

// The statuses of a backend: active, or why it cannot serve.
const (
	statusActive        = "active"
	statusNotInstalled  = "not-installed"
	statusDisabled      = "disabled"
	statusOutsideWindow = "outside-window"
)

// disableEnv turns backends off for the end user of any goav program: a
// comma-separated list of backend names, or all. A disabled backend never
// loads its library.
const disableEnv = "GOAV_LIBCODEC_DISABLE"

// registration is what Register works out for one declaration, once: where
// its code lives, and whether its libraries bind.
type registration struct {
	once        sync.Once
	module, pkg string
	version     string
	err         error
}

// Register returns the runtime option that adds each backend to the
// runtime's codec registry, alongside every other backend of its codec. It
// binds the libraries then: a backend that cannot run here registers
// descriptor-only, so selection passes it over for one that can, and naming
// it explicitly is refused with the reason and the fix.
func Register(codecs ...Codec) runconfig.Option {
	codecs = slices.Clone(codecs)
	for i := range codecs {
		codecs[i].reg = &registration{}
	}
	return runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		for i := range codecs {
			c := &codecs[i]
			err := c.bind()
			for _, mode := range c.modes() {
				desc := c.descriptor(mode)
				if err != nil {
					desc.Backend.Status = statusOf(err)
					desc.Backend.Unavailable = err
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
	case "disabled":
		return statusDisabled
	case "version", "symbol":
		return statusOutsideWindow
	}
	return statusNotInstalled
}

// bind loads the declaration's libraries and checks their version, once.
func (c *Codec) bind() error {
	r := c.registration()
	r.once.Do(func() {
		r.module, r.pkg = origin(c)
		r.version, r.err = c.load()
	})
	return r.err
}

func (c *Codec) registration() *registration {
	if c.reg == nil {
		c.reg = &registration{}
	}
	return c.reg
}

func (c *Codec) load() (string, error) {
	if disabled(c.Backend) {
		return "", &Error{Backend: c.Backend, Op: "disabled", Reason: "is turned off by " + disableEnv, Fix: "take " + c.Backend + " out of " + disableEnv}
	}
	if len(c.Platforms) != 0 && !slices.Contains(c.Platforms, runtime.GOOS) {
		return "", unavailable(c.Backend, "platform", "", "runs only on "+strings.Join(c.Platforms, ", "), "use another "+string(c.ID)+" backend on "+runtime.GOOS, nil)
	}
	for _, lib := range c.Libraries {
		if err := lib.bind(c.Backend, lib.resolve(), c.installFix(lib.Fix)); err != nil {
			return "", err
		}
	}
	if c.Version == nil {
		return "", nil
	}
	version, err := c.Version()
	if err == nil && !c.ABI.Contains(version) {
		name := c.Backend
		if len(c.Libraries) != 0 {
			name = c.Libraries[0].Name
		}
		err = &Error{Backend: c.Backend, Op: "version", Name: name, Reason: version + " is outside the releases this backend mirrors (" + c.ABI.String() + ")", Fix: c.installFix("install a release in that window")}
	}
	return version, err
}

// resolve picks the file a library binds: the explicit Path, else the
// first System path that exists. Every step yields an absolute path, so
// loading never searches the working directory.
func (l Library) resolve() string {
	if l.Path != "" {
		return l.Path
	}
	// goav codecs install's lockfile, then its cache, will come here.
	return find(l.System...)
}

// installFix leads a backend's fix with the installer when it has releases
// to install.
func (c *Codec) installFix(fix string) string {
	if len(c.Releases) == 0 {
		return fix
	}
	if fix == "" {
		return "goav codecs install " + c.Backend
	}
	return "goav codecs install " + c.Backend + ", or " + fix
}

func disabled(backend string) bool {
	for _, name := range strings.Split(os.Getenv(disableEnv), ",") {
		if name = strings.TrimSpace(name); strings.EqualFold(name, backend) || name == "all" {
			return true
		}
	}
	return false
}

// origin names the Go module and package a backend's code lives in, read
// from its session constructors and the binary's build information.
func origin(c *Codec) (module, pkg string) {
	for _, fn := range []any{c.NewVideoEncoder, c.NewVideoDecoder, c.NewAudioEncoder, c.NewAudioDecoder} {
		if v := reflect.ValueOf(fn); !v.IsNil() {
			if f := runtime.FuncForPC(v.Pointer()); f != nil {
				pkg = packageOf(f.Name())
				break
			}
		}
	}
	module = pkg
	if info, ok := debug.ReadBuildInfo(); ok && pkg != "" {
		best := ""
		for _, m := range append([]*debug.Module{&info.Main}, info.Deps...) {
			if m.Path != "" && len(m.Path) > len(best) && (pkg == m.Path || strings.HasPrefix(pkg, m.Path+"/")) {
				best = m.Path
			}
		}
		if best != "" {
			module = best
		}
	}
	return module, pkg
}

// packageOf is the package path of a function symbol (path/to/pkg.Fn.func1).
func packageOf(symbol string) string {
	slash := strings.LastIndexByte(symbol, '/')
	if dot := strings.IndexByte(symbol[slash+1:], '.'); dot >= 0 {
		return symbol[:slash+1+dot]
	}
	return symbol
}

// Probe reports each kit backend the options register that cannot run
// here, with its fix: Probe(libcodec.OpenH264()) is nil only when libopenh264
// loads, binds, and has a version in the backend's window.
func Probe(options ...runconfig.Option) error {
	config, err := runconfig.NewConfig(options...)
	if err != nil {
		return err
	}
	var errs []error
	seen := map[string]bool{}
	for _, desc := range config.Codecs.Descriptors() {
		key := string(desc.ID) + "/" + desc.Backend.Name
		if failure := desc.Backend.Unavailable; failure != nil && !seen[key] {
			seen[key] = true
			errs = append(errs, failure)
		}
	}
	return errors.Join(errs...)
}

func (c *Codec) descriptor(mode codec.Mode) codec.Descriptor {
	r := c.registration()
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
	}
	if knobs == nil {
		knobs = []codec.Knob{}
	}
	name := c.Backend
	if r.version != "" {
		name += " " + r.version
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
			Module:  r.module,
			Package: r.pkg,
			Status:  statusActive,
			Notice:  c.License.Notice,
		},
		Knobs:    knobs,
		RawKnobs: raw,
	}
}

type encoderFactory struct{ c *Codec }

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
	return &Error{Backend: c.Backend, Op: "open", Name: "codec.Control", Reason: "is not offered by libcodec backends", Fix: "pass library parameters as codec.Setting(\"" + c.Backend + "-params\", \"key=value:...\")"}
}

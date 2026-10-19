package codec

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
)

// namedEncoder is an encoder factory that remembers which backend it is, so
// a test can tell which registration Select returned.
type namedEncoder string

func (namedEncoder) NewEncoder(context.Context, EncodeConfig) (Encoder, error) { return nil, nil }

func backendDescriptor(id av.CodecID, name string, kind BackendKind, mutate ...func(*Descriptor)) Descriptor {
	desc := Descriptor{ID: id, Type: av.MediaVideo, Backend: BackendInfo{Name: name, Kind: kind}}
	for _, fn := range mutate {
		fn(&desc)
	}
	return desc
}

func h264Registry() *SimpleRegistry {
	r := NewRegistry()
	r.RegisterEncoder(backendDescriptor(av.CodecH264, "goh264", KindGo), namedEncoder("goh264"))
	r.RegisterEncoder(backendDescriptor(av.CodecH264, "openh264", KindNative, func(d *Descriptor) {
		d.Profiles = []string{"baseline", "main", "high"}
	}), namedEncoder("openh264"))
	r.RegisterEncoder(backendDescriptor(av.CodecH264, "videotoolbox", KindHardware, func(d *Descriptor) {
		d.Profiles = []string{"baseline", "main", "high"}
		d.Capabilities.BitDepths = []int{8, 10}
		d.Capabilities.MaxWidth, d.Capabilities.MaxHeight = 4096, 2304
	}), namedEncoder("videotoolbox"))
	return r
}

func chosen(t *testing.T, r *SimpleRegistry, need Need) Choice {
	t.Helper()
	choice, err := r.Select(need)
	if err != nil {
		t.Fatalf("Select(%+v): %v", need, err)
	}
	return choice
}

// TestSelectDefaultOrderIsHardwareNativeGo pins the default ranking and that
// every other fitting backend is reported as an alternative, in order.
func TestSelectDefaultOrderIsHardwareNativeGo(t *testing.T) {
	choice := chosen(t, h264Registry(), Need{ID: av.CodecH264, Mode: ModeEncode})
	if choice.Encoder != namedEncoder("videotoolbox") || choice.Descriptor.Backend.Name != "videotoolbox" {
		t.Fatalf("chose %s, want videotoolbox", choice.Descriptor.Backend.Name)
	}
	if got := BackendList(choice.Alternatives); got != "openh264 (native), goh264 (go)" {
		t.Fatalf("alternatives = %q", got)
	}
	if !strings.Contains(choice.Reason, "hardware > native > go") {
		t.Fatalf("reason = %q", choice.Reason)
	}
}

func TestSelectPreferenceByNameAndKind(t *testing.T) {
	r := h264Registry()
	r.Prefer("openh264")
	if choice := chosen(t, r, Need{ID: av.CodecH264, Mode: ModeEncode}); choice.Descriptor.Backend.Name != "openh264" || !strings.Contains(choice.Reason, "runtime preference openh264") {
		t.Fatalf("name preference chose %s (%s)", choice.Descriptor.Backend.Name, choice.Reason)
	}
	r.Prefer(string(KindGo))
	if choice := chosen(t, r, Need{ID: av.CodecH264, Mode: ModeEncode}); choice.Descriptor.Backend.Name != "goh264" {
		t.Fatalf("kind preference chose %s", choice.Descriptor.Backend.Name)
	}
}

// TestSelectFiltersByCapability pins that declared capabilities filter the
// candidates before preference ranks them.
func TestSelectFiltersByCapability(t *testing.T) {
	r := h264Registry()
	if choice := chosen(t, r, Need{ID: av.CodecH264, Mode: ModeEncode, Width: 7680, Height: 4320}); choice.Descriptor.Backend.Name != "openh264" {
		t.Fatalf("8K chose %s, want openh264 (videotoolbox declares 4096x2304)", choice.Descriptor.Backend.Name)
	}
	if choice := chosen(t, r, Need{ID: av.CodecH264, Mode: ModeEncode, Profile: "High"}); choice.Descriptor.Backend.Name != "videotoolbox" {
		t.Fatalf("profile High chose %s", choice.Descriptor.Backend.Name)
	}
	_, err := r.Select(Need{ID: av.CodecH264, Mode: ModeEncode, BitDepth: 12})
	var failure *SelectError
	if !errors.As(err, &failure) || !errors.Is(err, ErrUnsupportedFormat) || len(failure.Rejected) != 3 {
		t.Fatalf("12-bit = %v, want three rejections", err)
	}
	if !strings.Contains(err.Error(), "videotoolbox: bit depth 12 not supported (8, 10)") {
		t.Fatalf("refusal = %q, want each backend's reason", err)
	}
}

// TestSelectNamedBackendNeverSubstitutes pins explicit selection: the named
// backend serves or the request is refused with the backends that can.
func TestSelectNamedBackendNeverSubstitutes(t *testing.T) {
	r := h264Registry()
	if choice := chosen(t, r, Need{ID: av.CodecH264, Mode: ModeEncode, Backend: "OpenH264"}); choice.Encoder != namedEncoder("openh264") || !strings.Contains(choice.Reason, "requested") {
		t.Fatalf("named choice = %+v", choice)
	}
	_, err := r.Select(Need{ID: av.CodecH264, Mode: ModeEncode, Backend: "x264"})
	var failure *SelectError
	if !errors.As(err, &failure) || !errors.Is(err, ErrNotFound) || BackendList(failure.Satisfying) != "videotoolbox (hardware), openh264 (native), goh264 (go)" {
		t.Fatalf("missing backend = %v, want not found listing the satisfying backends", err)
	}
	_, err = r.Select(Need{ID: av.CodecH264, Mode: ModeEncode, Backend: "openh264", BitDepth: 10})
	if !errors.As(err, &failure) || !errors.Is(err, ErrUnsupportedFormat) || BackendList(failure.Satisfying) != "videotoolbox (hardware)" {
		t.Fatalf("unfit named backend = %v, want the backends that can", err)
	}
}

// TestRegisterSameIdentityReplaces pins last-wins per identity and that a
// later registration ranks first among equals.
func TestRegisterSameIdentityReplaces(t *testing.T) {
	r := NewRegistry()
	r.RegisterEncoder(backendDescriptor(av.CodecVP8, "a", KindGo), namedEncoder("a1"))
	r.RegisterEncoder(backendDescriptor(av.CodecVP8, "b", KindGo), namedEncoder("b"))
	r.RegisterEncoder(backendDescriptor(av.CodecVP8, "a", KindGo), namedEncoder("a2"))
	choice := chosen(t, r, Need{ID: av.CodecVP8, Mode: ModeEncode})
	if choice.Encoder != namedEncoder("a2") || len(choice.Alternatives) != 1 {
		t.Fatalf("chose %v with %d alternatives, want the re-registered a2 and one alternative", choice.Encoder, len(choice.Alternatives))
	}
	// An application's own factory (no kind) outranks every kind.
	r.RegisterEncoder(Descriptor{ID: av.CodecVP8}, namedEncoder("app"))
	if choice := chosen(t, r, Need{ID: av.CodecVP8, Mode: ModeEncode}); choice.Encoder != namedEncoder("app") {
		t.Fatalf("chose %v, want the kindless application factory", choice.Encoder)
	}
}

func TestSelectDescriptorOnlyIsUnavailable(t *testing.T) {
	r := NewRegistry()
	r.RegisterDescriptor(Descriptor{ID: av.CodecH264, Modes: []Mode{ModeDecode}, Backend: BackendInfo{Name: "goh264", Kind: KindGo}})
	_, err := r.Select(Need{ID: av.CodecH264, Mode: ModeDecode})
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "goh264: registered without an implementation") {
		t.Fatalf("descriptor-only = %v", err)
	}
	if _, err := r.DecoderFactory(av.CodecH264); err != ErrUnavailable {
		t.Fatalf("DecoderFactory = %v, want the bare ErrUnavailable sentinel", err)
	}
	if _, err := r.EncoderFactory(av.CodecH264); err != ErrNotFound {
		t.Fatalf("EncoderFactory = %v, want the bare ErrNotFound sentinel", err)
	}
}

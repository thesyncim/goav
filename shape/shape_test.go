package shape_test

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/shape"
)

func TestSpecConstructorsMatchingAndString(t *testing.T) {
	customCodec := av.CodecID("vendor.rawcam")
	customFormat := av.FormatID("acme.raw")

	expected := shape.Packet(
		av.MediaVideo,
		customCodec,
		shape.Stream("camera-main"),
		shape.Format(customFormat),
		shape.Video(1920, 1080, "vendor-bayer16"),
		shape.Realtime(true),
	)

	actual := shape.New(
		nil,
		shape.Domain(shape.DomainPacket),
		shape.Media(av.MediaVideo),
		shape.Stream("camera-main"),
		shape.Codec(customCodec),
		shape.Format(customFormat),
		shape.Video(1920, 1080, "vendor-bayer16"),
		shape.Realtime(true),
	)

	if !actual.CompatibleWith(expected) {
		t.Fatalf("actual spec should satisfy expected custom shape:\nactual:   %s\nexpected: %s", actual, expected)
	}
	if !(shape.Set{expected}).Accepts(actual) {
		t.Fatalf("set should accept compatible spec")
	}
	if !(shape.Set{}).Accepts(shape.Event()) {
		t.Fatalf("empty set should accept any spec")
	}
	if actual.CompatibleWith(shape.Frame(av.MediaVideo)) {
		t.Fatalf("packet spec should not satisfy frame-domain expectation")
	}
	if shape.Packet(av.MediaVideo, customCodec).CompatibleWith(expected) {
		t.Fatalf("unpopulated actual facts should not satisfy pinned custom expectations")
	}

	const wantString = "domain=packet media=video stream=camera-main codec=vendor.rawcam format=acme.raw size=1920x1080 pixel_format=vendor-bayer16 realtime=true"
	if got := actual.String(); got != wantString {
		t.Fatalf("String() = %q, want %q", got, wantString)
	}
	if got := (shape.Spec{}).String(); got != "any" {
		t.Fatalf("empty spec String() = %q, want any", got)
	}
}

func TestSetCloneIsIndependent(t *testing.T) {
	original := shape.Set{
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48000, 2, av.SampleFormatF32)),
	}

	cloned := original.Clone()
	cloned[0] = shape.Packet(av.MediaVideo, av.CodecVP9, shape.Video(1280, 720, av.PixelFormatI420))

	if original[0].MediaKind != av.MediaAudio {
		t.Fatalf("clone mutation changed original set: got %s", original[0])
	}
	if got := (shape.Set{}).Clone(); got != nil {
		t.Fatalf("empty set clone = %#v, want nil", got)
	}
}

func TestSetAcceptsScansAlternatives(t *testing.T) {
	actual := shape.Frame(av.MediaAudio, shape.Audio(48000, 2, av.SampleFormatF32))
	alternatives := shape.Set{
		shape.Frame(av.MediaVideo),
		shape.Frame(av.MediaAudio, shape.Audio(44100, 2, av.SampleFormatF32)),
		shape.Frame(av.MediaAudio, shape.Audio(48000, 2, av.SampleFormatF32)),
	}

	if !alternatives.Accepts(actual) {
		t.Fatalf("set should accept the first compatible later alternative")
	}
	if (shape.Set{shape.Packet(av.MediaVideo, av.CodecVP9)}).Accepts(actual) {
		t.Fatalf("set should reject when no alternative is compatible")
	}
}

func TestSpecCompatibilityRejectsEachPinnedMismatch(t *testing.T) {
	actual := shape.Spec{
		Domain:       shape.DomainFrame,
		MediaKind:    av.MediaVideo,
		StreamID:     "v0",
		Codec:        av.CodecVP9,
		Format:       av.FormatWebM,
		Width:        1280,
		Height:       720,
		PixelFormat:  av.PixelFormatI420,
		SampleRate:   48000,
		Channels:     2,
		SampleFormat: av.SampleFormatF32,
		Realtime:     false,
	}

	tests := []struct {
		name     string
		expected shape.Spec
	}{
		{name: "domain", expected: shape.Spec{Domain: shape.DomainPacket}},
		{name: "media", expected: shape.Spec{MediaKind: av.MediaAudio}},
		{name: "stream", expected: shape.Spec{StreamID: "v1"}},
		{name: "codec", expected: shape.Spec{Codec: av.CodecAV1}},
		{name: "format", expected: shape.Spec{Format: av.FormatMP4}},
		{name: "width", expected: shape.Spec{Width: 1920}},
		{name: "height", expected: shape.Spec{Height: 1080}},
		{name: "pixel", expected: shape.Spec{PixelFormat: av.PixelFormatYUV420P}},
		{name: "sample rate", expected: shape.Spec{SampleRate: 44100}},
		{name: "channels", expected: shape.Spec{Channels: 1}},
		{name: "sample format", expected: shape.Spec{SampleFormat: av.SampleFormatS16}},
		{name: "realtime", expected: shape.Spec{Realtime: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if actual.CompatibleWith(tt.expected) {
				t.Fatalf("spec should reject %s mismatch", tt.name)
			}
		})
	}
}

func TestPolicyConversionsMissingAndConstructors(t *testing.T) {
	none := shape.Policy{}
	if !none.Empty() || none.String() != "none" {
		t.Fatalf("zero policy should be empty none, got empty=%v string=%q", none.Empty(), none.String())
	}
	if got := none.Constructors(); len(got) != 0 {
		t.Fatalf("zero policy constructors = %#v, want empty", got)
	}

	audioNeeded := shape.Conversions(
		shape.Frame(av.MediaAudio, shape.Audio(48000, 1, av.SampleFormatS16)),
		shape.Frame(av.MediaAudio, shape.Audio(44100, 2, av.SampleFormatF32)),
	)
	if got, want := audioNeeded.String(), "resample+convert"; got != want {
		t.Fatalf("audio conversions = %q, want %q", got, want)
	}

	videoNeeded := shape.Conversions(
		shape.Frame(av.MediaVideo, shape.Video(1280, 720, av.PixelFormatI420)),
		shape.Frame(av.MediaVideo, shape.Video(1920, 1080, av.PixelFormatYUV420P)),
	)
	if got, want := videoNeeded.String(), "resize+convert"; got != want {
		t.Fatalf("video conversions = %q, want %q", got, want)
	}

	allowed := shape.AllowResample().Union(shape.AllowResize()).Union(shape.AllowConvert())
	if got, want := allowed.String(), "resample+resize+convert"; got != want {
		t.Fatalf("union string = %q, want %q", got, want)
	}
	if got, want := allowed.Constructors(), []string{"shape.AllowResample()", "shape.AllowResize()", "shape.AllowConvert()"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("full policy constructors = %#v, want %#v", got, want)
	}
	if !allowed.Covers(audioNeeded) || !allowed.Covers(videoNeeded) {
		t.Fatalf("full policy should cover audio and video conversion needs")
	}

	missing := shape.AllowResize().Missing(audioNeeded)
	if got, want := missing.String(), "resample+convert"; got != want {
		t.Fatalf("missing policy = %q, want %q", got, want)
	}
	if got, want := missing.Constructors(), []string{"shape.AllowResample()", "shape.AllowConvert()"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("missing constructors = %#v, want %#v", got, want)
	}
	if missing.AllowsResize() || !missing.AllowsResample() || !missing.AllowsConvert() {
		t.Fatalf("missing policy accessors disagree with policy: %s", missing)
	}
	if none.Covers(shape.AllowResample()) {
		t.Fatalf("zero policy should not cover resample")
	}
	if shape.AllowResample().Covers(shape.AllowResize()) {
		t.Fatalf("resample policy should not cover resize")
	}
	if shape.AllowResample().Covers(shape.AllowConvert()) {
		t.Fatalf("resample policy should not cover convert")
	}
	if got := shape.Conversions(shape.Frame(av.MediaAudio), shape.Frame(av.MediaAudio)); !got.Empty() {
		t.Fatalf("open expected facts should not require conversions, got %s", got)
	}
}

func TestAudioVideoOptionsIgnoreNilSpec(t *testing.T) {
	shape.Audio(48000, 2, av.SampleFormatF32)(nil)
	shape.Video(1920, 1080, av.PixelFormatI420)(nil)
}

func TestAudioSpecString(t *testing.T) {
	spec := shape.Frame(av.MediaAudio, shape.Audio(48000, 2, av.SampleFormatF32))
	const want = "domain=frame media=audio sample_rate=48000 channels=2 sample_format=f32"
	if got := spec.String(); got != want {
		t.Fatalf("audio String() = %q, want %q", got, want)
	}
}

func TestDeriveAndMergeSpecs(t *testing.T) {
	parameters := av.CodecParameters{
		ID:           av.CodecAV1,
		Type:         av.MediaVideo,
		Width:        3840,
		Height:       2160,
		PixelFormat:  av.PixelFormatI420,
		SampleRate:   48000,
		Channels:     2,
		SampleFormat: av.SampleFormatF32,
	}

	fromParameters := shape.FromCodecParameters(parameters)
	if got, want := fromParameters, (shape.Spec{
		MediaKind:    av.MediaVideo,
		Codec:        av.CodecAV1,
		Width:        3840,
		Height:       2160,
		PixelFormat:  av.PixelFormatI420,
		SampleRate:   48000,
		Channels:     2,
		SampleFormat: av.SampleFormatF32,
	}); got != want {
		t.Fatalf("FromCodecParameters() = %#v, want %#v", got, want)
	}

	fromStream := shape.FromStream(av.Stream{
		ID:    "v0",
		Type:  av.MediaVideo,
		Codec: parameters,
	}, shape.DomainPacket)
	if !fromStream.CompatibleWith(shape.Packet(av.MediaVideo, av.CodecAV1, shape.Stream("v0"), shape.Video(3840, 2160, av.PixelFormatI420))) {
		t.Fatalf("FromStream() did not derive a packet video shape: %s", fromStream)
	}

	merged := shape.Merge(
		shape.Packet(av.MediaVideo, av.CodecH264, shape.Format(av.FormatRTP), shape.Video(1920, 1080, av.PixelFormatI420)),
		shape.Spec{Codec: av.CodecAV1, PixelFormat: av.PixelFormatYUV420P, Realtime: true},
	)
	want := shape.Packet(av.MediaVideo, av.CodecAV1, shape.Format(av.FormatRTP), shape.Video(1920, 1080, av.PixelFormatYUV420P), shape.Realtime(true))
	if merged != want {
		t.Fatalf("Merge() = %#v, want %#v", merged, want)
	}

	kept := shape.Merge(shape.Frame(av.MediaAudio, shape.Audio(48000, 2, av.SampleFormatF32)), shape.Spec{})
	if !kept.CompatibleWith(shape.Frame(av.MediaAudio, shape.Audio(48000, 2, av.SampleFormatF32))) {
		t.Fatalf("empty overlay should keep base facts: %s", kept)
	}

	overlay := shape.Merge(
		shape.Spec{},
		shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "a0",
			Codec:        av.CodecOpus,
			Format:       av.FormatOgg,
			Width:        1,
			Height:       2,
			PixelFormat:  "ignored-for-audio-but-preserved",
			SampleRate:   48000,
			Channels:     2,
			SampleFormat: av.SampleFormatF32,
			Realtime:     true,
		},
	)
	wantOverlay := shape.Spec{
		Domain:       shape.DomainFrame,
		MediaKind:    av.MediaAudio,
		StreamID:     "a0",
		Codec:        av.CodecOpus,
		Format:       av.FormatOgg,
		Width:        1,
		Height:       2,
		PixelFormat:  "ignored-for-audio-but-preserved",
		SampleRate:   48000,
		Channels:     2,
		SampleFormat: av.SampleFormatF32,
		Realtime:     true,
	}
	if overlay != wantOverlay {
		t.Fatalf("full Merge overlay = %#v, want %#v", overlay, wantOverlay)
	}
}

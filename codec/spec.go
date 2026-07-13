package codec

import "github.com/thesyncim/goav/av"

// Spec is a fully-resolved codec choice: the codec id and media type, the
// structural Parameters, the encoder Settings, and the Copy/Auto modes. Build it
// with the constructors (VP9, VP8, Opus, H264, AV1, Codec) or Copy/Auto.
type Spec struct {
	ID         av.CodecID
	Type       av.MediaType
	Parameters av.CodecParameters
	Settings   CodecSettings
	Copy       bool
	Auto       bool
}

// Auto selects the codec automatically from the resolved stream.
func Auto() Spec { return Spec{Auto: true} }

// Copy preserves packets without re-encoding.
func Copy() Spec { return Spec{Copy: true} }

// Codec builds a spec for an explicit codec id and media type.
func Codec(id av.CodecID, media av.MediaType, options ...Option) Spec {
	return newCodecSpec(id, media, av.CodecParameters{ID: id, Type: media}, options...)
}

// Opus builds an Opus encoder spec (defaults to 48kHz stereo).
func Opus(options ...Option) Spec {
	return newCodecSpec(av.CodecOpus, av.MediaAudio, av.CodecParameters{
		ID:            av.CodecOpus,
		Type:          av.MediaAudio,
		ClockRate:     48000,
		SampleRate:    48000,
		Channels:      Stereo,
		ChannelLayout: "stereo",
	}, options...)
}

// VP8 builds a VP8 encoder spec.
func VP8(options ...Option) Spec {
	return newCodecSpec(av.CodecVP8, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecVP8,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

// VP9 builds a VP9 encoder spec.
func VP9(options ...Option) Spec {
	return newCodecSpec(av.CodecVP9, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecVP9,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

// H264 builds an H.264 codec spec.
func H264(options ...Option) Spec {
	return newCodecSpec(av.CodecH264, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecH264,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

// AV1 builds an AV1 codec spec.
func AV1(options ...Option) Spec {
	return newCodecSpec(av.CodecAV1, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecAV1,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

// applyEncodeCaps maps the audio caps set through options (Channels / SampleRate
// / ClockRate, carried on CodecSettings) onto the structural Parameters the rest
// of the pipeline reads.
func applyEncodeCaps(spec *Spec) {
	s := spec.Settings
	if s.ChannelsSet {
		spec.Parameters.Channels = s.Channels
	}
	if s.ChannelLayout != "" {
		spec.Parameters.ChannelLayout = s.ChannelLayout
	}
	if s.SampleRateSet {
		spec.Parameters.SampleRate = s.SampleRate
	}
	if s.ClockRate != 0 {
		spec.Parameters.ClockRate = s.ClockRate
	}
}

func newCodecSpec(id av.CodecID, media av.MediaType, params av.CodecParameters, options ...Option) Spec {
	spec := Spec{ID: id, Type: media, Parameters: params}
	for i := range options {
		if options[i] != nil {
			options[i](&spec.Settings)
		}
	}
	applyEncodeCaps(&spec)
	if spec.Parameters.ID == "" {
		spec.Parameters.ID = id
	}
	if spec.Parameters.Type == "" {
		spec.Parameters.Type = media
	}
	return spec
}

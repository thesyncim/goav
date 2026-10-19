package compile

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// mixJoinProfile is Mix's entry in the join table: audio arms, explicit
// packet-arm decode, first-arm-wins format solving through the shared shape
// solver (the implicit always-on arm policy — no user opt-in needed),
// audioMixStage convergence, and an encodable S16 output stream derived from
// the first arm's shape.
var mixJoinProfile = joinProfile{
	media:      av.MediaAudio,
	DecodeArms: true,
	graphBuffer: &pipeline.BufferPolicy{
		Capacity: RealtimeRecipeBufferCapacity,
		Drop:     pipeline.DropBlock,
	},
	graphBufferRealtimeOnly: true,
	// The first arm's audio format is the mix target; later arms that differ
	// get a conversion planned by the shape solver under the arm policy.
	armExpected: func(p *JoinPlan, stream av.Stream) shape.Spec {
		if stream.Codec.SampleRate <= 0 {
			return shape.Spec{}
		}
		channels := MaxInt(stream.Codec.Channels, 1)
		if p.targetRate == 0 {
			p.targetRate, p.targetChannels = stream.Codec.SampleRate, channels
			return shape.Spec{}
		}
		return shape.Frame(av.MediaAudio, shape.Audio(p.targetRate, p.targetChannels, ""))
	},
	armPolicy: shape.AllowResample().Union(shape.AllowConvert()),
	// An arm whose format is known only once it opens converts at run time to
	// the first arm's rate and channels in S16, or to the encoder's explicit
	// rate and channels when the first arm is such an arm too.
	ConformTarget: func(p *JoinPlan, first av.Stream) shape.Spec {
		rate, channels := first.Codec.SampleRate, first.Codec.Channels
		if rate > 0 {
			channels = MaxInt(channels, 1)
		}
		if p.lateTarget {
			rate = FirstPositiveInt(p.target.SampleRate, rate)
			channels = FirstPositiveInt(p.target.Channels, channels)
		}
		return shape.Frame(av.MediaAudio, shape.Audio(rate, channels, av.SampleFormatS16))
	},
	conformFollowsFirstArm: true,
	// The output id is the join's planned node name (mix, or mix-2 when
	// nested under another mix); the format facts come from the first arm —
	// a leaf's declared source shape or a sub-join's joined stream.
	joinedStream: func(p *JoinPlan) av.Stream {
		shape := p.firstArmSourceShape()
		return av.Stream{
			ID:   av.StreamID(p.Name),
			Type: av.MediaAudio,
			Codec: av.CodecParameters{
				Type:         av.MediaAudio,
				SampleRate:   shape.SampleRate,
				Channels:     shape.Channels,
				SampleFormat: shape.SampleFormat,
				ClockRate:    uint32(shape.SampleRate),
			},
		}
	},
	sinkOnlyReason: "mix to a non-Sink destination requires .Encode(...)",
	sinkOnlySuggestions: []string{
		"encode the mixed audio first: goav.Mix(a, b).Encode(codec.Opus(codec.Bitrate(96_000))).To(out)",
		"deliver raw mixed frames with .To(goav.Sink(sink)) instead",
	},
}

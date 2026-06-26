// Package integration runs the WebRTC-flavored goav recipe and component
// tests: every test drives the root module's public API with webrtcav tracks
// and sessions, proving the track seam from the webrtcav module's side of
// the boundary.
package integration

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	goavruntime "github.com/thesyncim/goav/runtime"
	"github.com/thesyncim/goav/webrtcav"
)

func specText(spec pipeline.Spec) string {
	out, err := graphrender.RenderURI(spec, "goav:graph")
	if err != nil {
		return err.Error()
	}
	return out
}

// webRTCRemote adapts a fixture RemoteTrack into a provider input the way an
// application would: adapt the track, read its streams, and declare the
// stream's codec as receive intent.
func webRTCRemote(t *testing.T, remote webrtcav.RemoteTrack) goav.InputSpec {
	t.Helper()
	reader, err := webrtcav.NewTrackRemoteAdapter().AdaptTrack(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	streams, err := reader.Streams(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) == 0 {
		t.Fatal(webrtcav.ErrUnknownStream)
	}
	stream := streams[0]
	options := make([]rtpav.ReceiveOption, 0, 2)
	name := string(stream.ID)
	if name == "" {
		name = stream.Name
	}
	if name != "" {
		options = append(options, rtpav.WithName(name))
	}
	if stream.Codec.ID != "" {
		spec := codec.CodecSpec{
			ID:         stream.Codec.ID,
			Type:       stream.Codec.Type,
			Parameters: stream.Codec,
		}
		if spec.Type == "" {
			spec.Type = stream.Type
		}
		if spec.Parameters.Type == "" {
			spec.Parameters.Type = spec.Type
		}
		options = append(options, rtpav.WithCodec(spec))
	}
	return goav.Input(rtpav.Receive(reader, options...))
}

func webRTCTestRuntime() *goav.Runtime {
	return bundle.MustNew(goavruntime.WithRealtime(false))
}

func TestWebRTCTrackRecordRecipeUsesCodecIntent(t *testing.T) {
	job := goav.From(
		webRTCRemote(t, webrtcav.RemoteTrack{
			Track: &webrtc.TrackRemote{},
			Codec: webrtc.RTPCodecParameters{
				RTPCodecCapability: webrtc.RTPCodecCapability{
					MimeType:  webrtc.MimeTypeVP8,
					ClockRate: 90000,
				},
				PayloadType: 96,
			},
			Stream: av.Stream{
				ID:   "video",
				Type: av.MediaVideo,
			},
		}),
	).UseRuntime(webRTCTestRuntime()).
		Copy().
		To(goav.Write("recording.ivf", io.Discard))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "video -> recording.ivf") ||
		!strings.Contains(text, "rtp receive, codec=vp8") ||
		strings.Contains(text, "depacketizers=") {
		t.Fatalf("spec:\n%s", text)
	}
	report, err := job.Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Inputs) != 1 ||
		!report.Inputs[0].Realtime ||
		report.Inputs[0].Codec != av.CodecVP8 {
		t.Fatalf("inputs: %+v", report.Inputs)
	}

	task, err := job.BuildLive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(spec, built) {
		t.Fatalf("planned = %+v, built = %+v", spec, built)
	}
}

func TestWebRTCTrackUnknownCodecMetadataYieldsNoCodecIntent(t *testing.T) {
	// Unknown track codecs are the provider's concern: the derived input
	// carries no codec intent, and depacketization fails inside the provider
	// at run time (rtpav.ErrDepacketizerNotFound) instead of at the root.
	input := webRTCRemote(t, webrtcav.RemoteTrack{
		Track: &webrtc.TrackRemote{},
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  "audio/telephone-event",
				ClockRate: 8000,
			},
			PayloadType: 101,
		},
		Stream: av.Stream{
			ID:   "dtmf",
			Type: av.MediaAudio,
		},
	})
	report, err := goav.From(input).
		UseRuntime(webRTCTestRuntime()).
		Copy().
		To(goav.Write("recording.webm", io.Discard)).
		Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Inputs) != 1 {
		t.Fatalf("inputs: %+v", report.Inputs)
	}
	if report.Inputs[0].Codec != "" {
		t.Fatalf("codec = %q, want no codec intent for an unknown track codec", report.Inputs[0].Codec)
	}
	if !report.Inputs[0].Realtime || report.Inputs[0].Name != "dtmf" {
		t.Fatalf("input = %+v, want realtime input named after the track stream", report.Inputs[0])
	}
}

func TestWebRTCTrackRecordMultiInputRecipeUsesCodecIntent(t *testing.T) {
	job := goav.From(webRTCRemote(t, webrtcav.RemoteTrack{
		Track: &webrtc.TrackRemote{},
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeOpus,
				ClockRate: 48000,
				Channels:  2,
			},
			PayloadType: 111,
		},
		Stream: av.Stream{
			ID:   "audio",
			Type: av.MediaAudio,
		},
	})).UseRuntime(webRTCTestRuntime()).
		And(webRTCRemote(t, webrtcav.RemoteTrack{
			Track: &webrtc.TrackRemote{},
			Codec: webrtc.RTPCodecParameters{
				RTPCodecCapability: webrtc.RTPCodecCapability{
					MimeType:  webrtc.MimeTypeVP8,
					ClockRate: 90000,
				},
				PayloadType: 96,
			},
			Stream: av.Stream{
				ID:   "video",
				Type: av.MediaVideo,
			},
		})).
		To(goav.Write("recording.webm", io.Discard))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "audio -> recording.webm") ||
		!strings.Contains(text, "video -> recording.webm") ||
		!strings.Contains(text, "rtp receive, codec=opus") ||
		!strings.Contains(text, "rtp receive, codec=vp8") ||
		strings.Contains(text, "depacketizers=") {
		t.Fatalf("spec:\n%s", text)
	}
	report, err := job.Explain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Inputs) != 2 ||
		report.Inputs[0].Codec != av.CodecOpus ||
		report.Inputs[1].Codec != av.CodecVP8 {
		t.Fatalf("inputs: %+v", report.Inputs)
	}
}

func TestWebRTCTrackRecipeReportsNilTrack(t *testing.T) {
	// Track adaptation errors are the provider's: they surface when the
	// source opens at build time.
	_, err := goav.From(
		goav.Input(webrtcav.Track(nil)),
	).UseRuntime(webRTCTestRuntime()).
		Copy().
		To(goav.Write("recording.ivf", io.Discard)).
		Build(context.Background())
	if !errors.Is(err, webrtcav.ErrNilTrack) {
		t.Fatalf("err = %v, want webrtcav.ErrNilTrack", err)
	}
}

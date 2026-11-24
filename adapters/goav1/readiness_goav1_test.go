package goav1

import (
	"testing"

	backend "github.com/thesyncim/goav1"
)

func TestBackendRealtimeAPISurface(t *testing.T) {
	desc := Descriptor()
	if desc.Backend.Module != "github.com/thesyncim/goav1" {
		t.Fatalf("backend module = %q", desc.Backend.Module)
	}
	if len(desc.Capabilities.BuildTags) != 0 {
		t.Fatalf("build tags = %v", desc.Capabilities.BuildTags)
	}

	var _ func(backend.VideoEncoderConfig) (*backend.VideoEncoder, error) = backend.NewVideoEncoder
	var _ func(*backend.VideoEncoder, backend.I420Frame, bool) (backend.EncodedFrame, error) = (*backend.VideoEncoder).Encode
	var _ func([][]byte, ...backend.Option) (*backend.Decoder, error) = backend.NewDecoderFromRTPPayloads
	var _ func(*backend.Decoder, []byte) ([]*backend.Frame, error) = (*backend.Decoder).DecodeRTPPayload
	var _ func(*backend.Decoder, []byte) ([]*backend.Frame, error) = (*backend.Decoder).DecodeRTPPayloadAfterLoss
	var _ func(
		[]byte,
		backend.RTPPayloadSizeLimits,
		bool,
		bool,
		[]backend.RTPPacketizerOBU,
		[]backend.RTPPacketPlan,
		[]backend.RTPPacketPlan,
	) (backend.RTPPacketizer, error) = backend.NewRTPPacketizer
}

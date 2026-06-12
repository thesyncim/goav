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
	var _ func(backend.FrameFormat) (backend.FrameLayout, error) = backend.FrameRequiredSize
	var _ func(
		backend.DecoderStream,
		int,
		[]byte,
		int,
		[]byte,
		[]backend.RTPObuSpan,
		[]backend.DecoderEvent,
		[]backend.TileSpan,
		[]backend.TileJob,
		[]backend.TileBatch,
	) (backend.DecoderFrameWorkResidualStreamPlan, error) = backend.DecoderFrameWorkResidualRTPPayloadStreamPlan
	var _ func(
		backend.DecoderStream,
		int,
		[][]byte,
		int,
		[]byte,
		[]backend.RTPObuSpan,
		[]backend.DecoderEvent,
		[]backend.TileSpan,
		[]backend.TileJob,
		[]backend.TileBatch,
	) (backend.DecoderFrameWorkResidualStreamPlan, error) = backend.DecoderFrameWorkResidualRTPPayloadsStreamPlan
	var _ func(
		backend.DecoderFrameWorkResidualStreamPlan,
		*backend.DecoderStream,
		backend.DecoderFrameWorkResidualEventRuntime,
		backend.DecoderFrameWorkResidualStreamScratch,
		*backend.DecoderFrameWorkBatchResidualRunner,
	) (backend.DecoderFrameWorkResidualStreamRunner, backend.DecoderFrameWorkSideData, error) = backend.BindDecoderFrameWorkResidualStreamPlanRunner
	var _ func(
		*backend.DecoderFrameWorkResidualStreamRunner,
		*backend.DecoderFrameWorkResidualStreamResult,
		[]byte,
		backend.DecoderFrameWorkPostFilterFunc,
	) error = (*backend.DecoderFrameWorkResidualStreamRunner).RunRTPPayloadInto
	var _ func(
		*backend.DecoderFrameWorkResidualStreamRunner,
		*backend.DecoderFrameWorkResidualStreamResult,
		[]byte,
		backend.DecoderFrameWorkPostFilterFunc,
	) error = (*backend.DecoderFrameWorkResidualStreamRunner).RunRTPPayloadAfterLossInto
}

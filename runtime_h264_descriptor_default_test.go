//go:build !goav_goh264

package goav

import (
	"context"
	"errors"
	"testing"

	goh264adapter "github.com/thesyncim/goav/adapters/goh264"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
)

func TestRuntimeBuilderH264DescriptorOnlyDecodeUnavailable(t *testing.T) {
	streams := []av.Stream{{
		ID:   "video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:        av.CodecH264,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     640,
			Height:    360,
		},
	}}
	demuxer := &remuxTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, remuxTestDemuxerFactory{demuxer: demuxer}),
	)
	codecs := withTestCodecs(goh264adapter.Register)

	_, err := newTestBuilder(t, formats, codecs).
		Input(format.Input{Name: "input.ogg"}).
		Decode(testSelectVideo()).
		Sink(&runtimeTestSink{name: "frames"}).
		Build(context.Background())
	if !errors.Is(err, codec.ErrUnavailable) {
		t.Fatalf("err = %v, want codec.ErrUnavailable", err)
	}
	if !demuxer.closed {
		t.Fatal("demux source should be closed after unavailable decoder")
	}
}

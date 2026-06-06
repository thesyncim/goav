package format

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
)

type testProber struct {
	result ProbeResult
	err    error
}

func (p testProber) Probe(context.Context, ProbeRequest) (ProbeResult, error) {
	return p.result, p.err
}

type testDemuxerFactory struct{}

func (testDemuxerFactory) NewDemuxer(context.Context, ProbeResult) (Demuxer, error) {
	return nil, nil
}

type testMuxerFactory struct{}

func (testMuxerFactory) NewMuxer(context.Context, av.FormatID) (Muxer, error) {
	return nil, nil
}

func TestRegistryProbeChoosesHighestScore(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterProber(testProber{result: ProbeResult{Format: av.FormatOgg, Score: 50}})
	registry.RegisterProber(testProber{result: ProbeResult{Format: av.FormatIVF, Score: 90}})

	result, err := registry.Probe(context.Background(), ProbeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatIVF {
		t.Fatalf("format = %s, want %s", result.Format, av.FormatIVF)
	}
}

func TestRegistryFactories(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterDemuxer(av.FormatOgg, testDemuxerFactory{})
	registry.RegisterMuxer(av.FormatOgg, testMuxerFactory{})

	if _, err := registry.DemuxerFactory(av.FormatOgg); err != nil {
		t.Fatalf("demuxer: %v", err)
	}
	if _, err := registry.MuxerFactory(av.FormatOgg); err != nil {
		t.Fatalf("muxer: %v", err)
	}
	if _, err := registry.DemuxerFactory(av.FormatIVF); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

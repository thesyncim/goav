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

func TestRegistryDescriptorsAreCloned(t *testing.T) {
	registry := NewRegistry()
	desc := Descriptor{
		Format:     av.FormatOgg,
		Media:      []av.MediaType{av.MediaAudio, av.MediaVideo},
		Codecs:     []av.CodecID{av.CodecOpus, av.CodecVP9},
		MinStreams: 1,
		MaxStreams: 2,
		Realtime:   true,
		Metadata:   av.Metadata{"profile": "web"},
	}
	registry.RegisterMuxerDescriptor(desc, testMuxerFactory{})

	desc.Media[0] = av.MediaData
	desc.Codecs[0] = av.CodecPCM
	desc.Metadata["profile"] = "mutated"

	got, err := registry.MuxerDescriptor(av.FormatOgg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Format != av.FormatOgg ||
		got.Media[0] != av.MediaAudio ||
		got.Codecs[0] != av.CodecOpus ||
		got.MinStreams != 1 ||
		got.MaxStreams != 2 ||
		!got.Realtime ||
		got.Metadata["profile"] != "web" {
		t.Fatalf("descriptor = %+v, want cloned original", got)
	}
	list := registry.MuxerDescriptors()
	if len(list) != 1 ||
		list[0].Format != av.FormatOgg ||
		list[0].Media[0] != av.MediaAudio ||
		list[0].Codecs[0] != av.CodecOpus ||
		list[0].Metadata["profile"] != "web" {
		t.Fatalf("muxer descriptors = %+v, want cloned descriptor", list)
	}
	list[0].Media[0] = av.MediaData
	list[0].Codecs[0] = av.CodecPCM
	list[0].Metadata["profile"] = "changed"

	got.Media[0] = av.MediaData
	got.Codecs[0] = av.CodecPCM
	got.Metadata["profile"] = "changed"
	again, err := registry.MuxerDescriptor(av.FormatOgg)
	if err != nil {
		t.Fatal(err)
	}
	if again.Media[0] != av.MediaAudio ||
		again.Codecs[0] != av.CodecOpus ||
		again.Metadata["profile"] != "web" {
		t.Fatalf("descriptor = %+v, want registry-owned copy", again)
	}
}

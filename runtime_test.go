package goav

import (
	"context"
	"errors"
	"testing"

	gopusadapter "github.com/thesyncim/goav/adapters/gopus"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
)

func TestNewRuntimeDefaults(t *testing.T) {
	runtime := New()
	if runtime.Codecs() == nil || runtime.Formats() == nil || runtime.Pipelines() == nil {
		t.Fatalf("runtime defaults incomplete: %+v", runtime)
	}
	if _, err := runtime.Probe(context.Background(), ProbeRequest{}); !errors.Is(err, format.ErrNotFound) {
		t.Fatalf("probe err = %v, want format.ErrNotFound", err)
	}
	result, err := runtime.Probe(context.Background(), ProbeRequest{Name: "audio.opus"})
	if err != nil {
		t.Fatalf("probe opus: %v", err)
	}
	if result.Format != av.FormatOgg {
		t.Fatalf("format = %s, want ogg", result.Format)
	}
}

func TestRuntimeWithCodecAdapter(t *testing.T) {
	runtime := New(WithCodecAdapter(gopusadapter.Register))

	if _, err := runtime.Codecs().DecoderFactory(av.CodecOpus); err != nil {
		t.Fatalf("decoder factory: %v", err)
	}
}

func TestRuntimeBuilderEmptyTask(t *testing.T) {
	task, err := New().New().Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBuilderRefusesUnimplementedGraph(t *testing.T) {
	_, err := New().New().
		Input(Input{Name: "input"}).
		Decode(SelectAudio()).
		Output(Output{Name: "output"}).
		Build(context.Background())
	if !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want ErrUnsupportedBuild", err)
	}
}

func TestRuntimeWithCustomCodecRegistry(t *testing.T) {
	registry := codec.NewRegistry(codec.WithDescriptor(codec.Descriptor{
		ID:    av.CodecAV1,
		Modes: []codec.Mode{codec.ModeDecode},
	}))
	runtime := New(WithCodecRegistry(registry))

	found, err := runtime.Codecs().Find(av.CodecAV1, codec.ModeDecode)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 {
		t.Fatalf("found = %d, want 1", len(found))
	}
}

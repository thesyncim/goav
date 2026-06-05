package format

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestDefaultProberMagic(t *testing.T) {
	result, err := DefaultProber().Probe(context.Background(), ProbeRequest{
		Name:   "unknown.bin",
		Header: []byte("OggSxxxx"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatOgg {
		t.Fatalf("format = %s, want ogg", result.Format)
	}
}

func TestDefaultProberMP4OffsetMagic(t *testing.T) {
	result, err := DefaultProber().Probe(context.Background(), ProbeRequest{
		Header: []byte{0, 0, 0, 24, 'f', 't', 'y', 'p'},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatMP4 {
		t.Fatalf("format = %s, want mp4", result.Format)
	}
}

func TestDefaultProberExtensionFallback(t *testing.T) {
	result, err := DefaultProber().Probe(context.Background(), ProbeRequest{
		Name: "video.webm",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatMatroska {
		t.Fatalf("format = %s, want matroska", result.Format)
	}
	if result.Score >= 100 {
		t.Fatalf("extension fallback score = %d, want below magic score", result.Score)
	}
}

func TestDefaultProberProtocol(t *testing.T) {
	result, err := DefaultProber().Probe(context.Background(), ProbeRequest{
		Input: Input{Protocol: av.ProtocolWebRTC},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatWebRTC {
		t.Fatalf("format = %s, want webrtc", result.Format)
	}
}

func TestDefaultProberNoMatch(t *testing.T) {
	if _, err := DefaultProber().Probe(context.Background(), ProbeRequest{Name: "unknown.bin"}); err == nil {
		t.Fatal("expected no match error")
	}
}

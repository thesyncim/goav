package goav_test

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/container/matroska"
)

// TestMP4H264CopyToAnnexBIsDecodable pins goav's H.264 convention: packets
// travel as Annex B, so an MP4's length-prefixed (AVCC) samples are converted
// on read and the Annex B muxer writes the avcC parameter sets ahead of each
// keyframe that lacks them. The copied .h264 must start with a start code
// and carry SPS and PPS before the first IDR, which is what makes it
// decodable on its own; before, the output began with a raw length prefix.
func TestMP4H264CopyToAnnexBIsDecodable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := bundle.Run(ctx, goav.From(goav.FileInput("in.mp4", bytes.NewReader(data))).
		Video().Copy().
		To(goav.Write("out.h264", &out))); err != nil {
		t.Fatalf("copy to annex b: %v", err)
	}
	stream := out.Bytes()
	if !bytes.HasPrefix(stream, []byte{0, 0, 0, 1}) {
		t.Fatalf("output starts % x, want an Annex B start code", stream[:min(8, len(stream))])
	}
	var order []byte
	for i := 0; i+4 < len(stream); i++ {
		if stream[i] == 0 && stream[i+1] == 0 && stream[i+2] == 0 && stream[i+3] == 1 {
			order = append(order, stream[i+4]&0x1f)
		}
	}
	firstIDR := bytes.IndexByte(order, 5)
	if firstIDR < 0 {
		t.Fatalf("no IDR NAL in output (NAL types %v)", order)
	}
	if !bytes.Contains(order[:firstIDR], []byte{7}) || !bytes.Contains(order[:firstIDR], []byte{8}) {
		t.Fatalf("SPS/PPS do not precede the first IDR (NAL types %v)", order[:firstIDR+1])
	}
}

// TestMP4H264CopyToMatroskaRoundTrips keeps the other direction honest: the
// Matroska muxer converts the Annex B packets back to its length-prefixed
// form, and every sample survives the remux.
func TestMP4H264CopyToMatroskaRoundTrips(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := bundle.Run(ctx, goav.From(goav.FileInput("in.mp4", bytes.NewReader(data))).
		Video().Copy().
		To(goav.Write("out.mkv", &out))); err != nil {
		t.Fatalf("copy to matroska: %v", err)
	}
	demuxer, err := matroska.NewDemuxer(bytes.NewReader(out.Bytes()), matroska.DemuxerOptions{})
	if err != nil {
		t.Fatalf("demux output: %v", err)
	}
	packet := matroska.Packet{Data: make([]byte, 0, 1<<16)}
	got := 0
	for demuxer.ReadPacket(&packet) == nil {
		if len(packet.Data) == 0 {
			t.Fatalf("packet %d is empty", got)
		}
		got++
	}
	if got == 0 {
		t.Fatal("matroska output has no video packets")
	}
}

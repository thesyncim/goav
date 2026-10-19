package mp4

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// Parameter sets VideoToolbox wrote for a 64x48 HEVC Main stream.
var (
	testVPS = unhex("40010c01ffff016000000300b0000003000003001e0bc090")
	testSPS = unhex("420101016000000300b0000003000003001ea0142041c18c4e202fb916452ffcb9fc4fe880")
	testPPS = unhex("4401c072f05324")
)

func unhex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func annexBUnits(nals ...[]byte) []byte {
	var out []byte
	for _, nal := range nals {
		out = append(append(out, 0, 0, 0, 1), nal...)
	}
	return out
}

func TestParseHEVCSPS(t *testing.T) {
	sps, err := parseHEVCSPS(testSPS)
	if err != nil {
		t.Fatal(err)
	}
	ptl := unhex("0160000000b00000000000" + "1e")
	if !bytes.Equal(sps.profileTierLevel[:], ptl) || sps.chromaFormat != 1 || sps.lumaDepth != 8 || sps.chromaDepth != 8 || sps.temporalLayers != 1 || sps.temporalIDNested != 1 {
		t.Fatalf("SPS %+v", sps)
	}
	for n := range len(testSPS) - 1 {
		if _, err := parseHEVCSPS(testSPS[:n]); err == nil && n < 16 {
			t.Errorf("an SPS cut to %d bytes parsed", n)
		}
	}
}

// TestMuxHEVCDerivesItsHVCC: a stream that declares no hvcC gets one built
// from its first keyframe's parameter sets, which then leave the samples.
func TestMuxHEVCDerivesItsHVCC(t *testing.T) {
	idr, trail := []byte{0x26, 0x01, 0xaf}, []byte{0x02, 0x01, 0xd0}
	streams := []av.Stream{{ID: "h", Type: av.MediaVideo, TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{ID: av.CodecHEVC, Type: av.MediaVideo, Width: 64, Height: 48}}}
	packets := []muxPacket{
		{stream: "h", key: true, payload: annexBUnits(testVPS, testSPS, testPPS, idr)},
		{stream: "h", pts: 40 * time.Millisecond, dts: 40 * time.Millisecond, payload: annexBUnits(trail)},
	}
	out := &sliceWriteSeeker{}
	writeMP4(t, out, streams, packets, false)
	read, got := readMP4(t, out.data)
	config := read[0].Codec.ExtraData.Bytes
	if want, _ := hvcCFromAnnexB(annexBUnits(testVPS, testSPS, testPPS)); !bytes.Equal(config, want) {
		t.Fatalf("hvcC % x, want % x", config, want)
	}
	if config[1] != 0x01 || config[12] != 0x1e || config[16] != 0xfd || config[17] != 0xf8 || config[21] != 0x0f || config[22] != 3 {
		t.Fatalf("hvcC header % x", config[:23])
	}
	if len(got) != 2 || !bytes.Equal(got[0].payload, annexBUnits(idr)) || !bytes.Equal(got[1].payload, annexBUnits(trail)) {
		t.Fatalf("samples %v", got)
	}
}

func FuzzHVCCFromAnnexB(f *testing.F) {
	f.Add(annexBUnits(testVPS, testSPS, testPPS))
	f.Fuzz(func(t *testing.T, data []byte) {
		if record, err := hvcCFromAnnexB(data); err == nil && hvc1Config(record) == nil {
			t.Fatalf("built a record hvc1Config cannot read: % x", record)
		}
		hvc1Config(data)
	})
}

package mp4

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/hevc"
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

// TestMuxHEVCAsHVC1: the hvcC, the stream's (rewritten to 4-byte lengths and
// complete arrays) or one built from the first keyframe's parameter sets,
// rides in an hvc1 entry; the samples drop their parameter sets; the
// demuxer hands back Annex B.
func TestMuxHEVCAsHVC1(t *testing.T) {
	complete, _ := hevc.ConfigFromAnnexB(annexBUnits(testVPS, testSPS, testPPS))
	short := bytes.Clone(complete)
	short[21] = short[21]&^3 | 1 // 2-byte lengths
	short[23] &^= 0x80           // the VPS array incomplete
	for name, extra := range map[string][]byte{"derived": nil, "declared": short} {
		t.Run(name, func(t *testing.T) { muxHEVC(t, extra, complete) })
	}
}

func muxHEVC(t *testing.T, extra, want []byte) {
	idr, trail := []byte{0x26, 0x01, 0xaf}, []byte{0x02, 0x01, 0xd0}
	streams := []av.Stream{{ID: "h", Type: av.MediaVideo, TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{ID: av.CodecHEVC, Type: av.MediaVideo, Width: 64, Height: 48, ExtraData: av.Buffer{Bytes: extra}}}}
	packets := []muxPacket{
		{stream: "h", key: true, payload: annexBUnits(testVPS, testSPS, testPPS, idr)},
		{stream: "h", pts: 40 * time.Millisecond, dts: 40 * time.Millisecond, payload: annexBUnits(trail)},
	}
	out := &sliceWriteSeeker{}
	writeMP4(t, out, streams, packets, false)
	if !bytes.Contains(out.data, []byte("hvc1")) || !bytes.Contains(out.data, []byte("hvcC")) {
		t.Fatal("the file lacks its hvc1 entry or hvcC")
	}
	read, got := readMP4(t, out.data)
	if read[0].Codec.ID != av.CodecHEVC || !bytes.Equal(read[0].Codec.ExtraData.Bytes, want) {
		t.Fatalf("stream %s, hvcC % x, want % x", read[0].Codec.ID, read[0].Codec.ExtraData.Bytes, want)
	}
	if len(got) != 2 || !bytes.Equal(got[0].payload, annexBUnits(idr)) || !bytes.Equal(got[1].payload, annexBUnits(trail)) {
		t.Fatalf("samples %v", got)
	}
}

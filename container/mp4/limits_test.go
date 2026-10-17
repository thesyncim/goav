package mp4

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"runtime"
	"testing"
)

// fragmentedMoov builds a moov with one avc1 track (id 1) whose samples all
// arrive in fragments, plus the trex defaults a fragment run falls back to.
func fragmentedMoov() []byte {
	tkhd := fullBox("tkhd", 0, 0, cat(u32b(0), u32b(0), u32b(1)))
	mdhd := fullBox("mdhd", 0, 0, cat(u32b(0), u32b(0), u32b(1000)))
	hdlr := fullBox("hdlr", 0, 0, cat(u32b(0), []byte("vide")))
	stsd := fullBox("stsd", 0, 0, cat(u32b(1), avc1Entry(16, 16, nil)))
	stbl := container("stbl", stsd)
	trak := container("trak", tkhd, container("mdia", mdhd, hdlr, container("minf", stbl)))
	trex := fullBox("trex", 0, 0, cat(u32b(1), u32b(1), u32b(1), u32b(1), u32b(0)))
	return container("moov", trak, container("mvex", trex))
}

// allocatedBytes reports the bytes fn allocates.
func allocatedBytes(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestDemuxerRefusesFieldlessRunAmplification pins the sample budget: four
// field-less trun boxes each declaring 1M samples fit in a few hundred
// bytes, and used to allocate over a gigabyte of sample records. The file
// cannot hold that many samples, so the demuxer refuses it cheaply.
func TestDemuxerRefusesFieldlessRunAmplification(t *testing.T) {
	traf := [][]byte{fullBox("tfhd", 0, 0, u32b(1))}
	for range 4 {
		traf = append(traf, fullBox("trun", 0, 0, u32b(1<<20)))
	}
	file := cat(fragmentedMoov(), container("moof", container("traf", traf...)))

	var err error
	allocated := allocatedBytes(func() {
		_, err = NewDemuxer(bytes.NewReader(file), int64(len(file)))
	})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("NewDemuxer(%d-byte amplifying file) err = %v, want ErrInvalidData", len(file), err)
	}
	if allocated > 1<<20 {
		t.Fatalf("NewDemuxer allocated %d bytes for a %d-byte file, want under 1 MiB", allocated, len(file))
	}
}

// TestDemuxerRefusesSampleOffsetOverflow pins the overflow-safe bounds check:
// a base data offset near MaxInt64 made offset+size wrap negative, pass the
// file-size check, and allocate the declared 4 GiB sample on ReadInto.
func TestDemuxerRefusesSampleOffsetOverflow(t *testing.T) {
	base := make([]byte, 8)
	binary.BigEndian.PutUint64(base, math.MaxInt64-16)
	tfhd := fullBox("tfhd", 0, tfhdBaseDataOffset|tfhdDefaultSampleSize, cat(u32b(1), base, u32b(0xFFFFFFF0)))
	file := cat(fragmentedMoov(), container("moof", container("traf", tfhd, fullBox("trun", 0, 0, u32b(1)))))

	demuxer, err := NewDemuxer(bytes.NewReader(file), int64(len(file)))
	if err != nil {
		// Refusing at open is also correct; the pin is that nothing allocates.
		return
	}
	var sample Sample
	allocated := allocatedBytes(func() {
		err = demuxer.ReadInto(nil, &sample)
	})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("ReadInto err = %v, want ErrInvalidData", err)
	}
	if allocated > 1<<20 {
		t.Fatalf("ReadInto allocated %d bytes, want under 1 MiB", allocated)
	}
}

func TestSampleBudgetClaims(t *testing.T) {
	budget := newSampleBudget(1000)
	if !budget.claim(10, 50) {
		t.Fatal("claim(10 samples x 50 bytes) refused in a 1000-byte file")
	}
	if budget.claim(10, 101) {
		t.Fatal("claim accepted 1010 payload bytes in a 1000-byte file")
	}
	if !budget.claim(990, 0) {
		t.Fatal("claim refused the remaining 990 table-sized samples")
	}
	if budget.claim(1, 0) {
		t.Fatal("claim accepted more samples than the file has bytes")
	}
}

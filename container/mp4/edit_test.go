package mp4

import (
	"bytes"
	"os"
	"testing"
	"time"
)

// elstBox builds an edit list; each entry is {segment duration, media time}.
func elstBox(entries ...[2]int32) []byte {
	payload := u32b(uint32(len(entries)))
	for _, e := range entries {
		payload = cat(payload, u32b(uint32(e[0])), u32b(uint32(e[1])), u16b(1), u16b(0))
	}
	return container("edts", fullBox("elst", 0, 0, payload))
}

// editedTrak is syntheticTrak (timescale 1000, every sample a sync sample)
// with an edit list and a constant composition offset, the shape encoders
// write for B-frame video.
func editedTrak(id uint32, handler string, entry []byte, count, sampleSize int, deltaTicks uint32, chunkOffset uint32, cttsOffset uint32, edts []byte) []byte {
	sizes := []byte{}
	for range count {
		sizes = append(sizes, u32b(uint32(sampleSize))...)
	}
	children := [][]byte{
		fullBox("stsd", 0, 0, cat(u32b(1), entry)),
		fullBox("stts", 0, 0, cat(u32b(1), u32b(uint32(count)), u32b(deltaTicks))),
		fullBox("stsc", 0, 0, cat(u32b(1), u32b(1), u32b(uint32(count)), u32b(1))),
		fullBox("stsz", 0, 0, cat(u32b(0), u32b(uint32(count)), sizes)),
		fullBox("stco", 0, 0, cat(u32b(1), u32b(chunkOffset))),
	}
	if cttsOffset != 0 {
		children = append(children, fullBox("ctts", 0, 0, cat(u32b(1), u32b(uint32(count)), u32b(cttsOffset))))
	}
	stbl := container("stbl", children...)
	mdhd := fullBox("mdhd", 0, 0, cat(u32b(0), u32b(0), u32b(1000), u32b(uint32(count)*deltaTicks), u16b(0), u16b(0)))
	hdlr := fullBox("hdlr", 0, 0, cat(u32b(0), []byte(handler), make([]byte, 12), []byte("x\x00")))
	tkhd := fullBox("tkhd", 0, 1, cat(u32b(0), u32b(0), u32b(id)))
	parts := [][]byte{tkhd}
	if edts != nil {
		parts = append(parts, edts)
	}
	parts = append(parts, container("mdia", mdhd, hdlr, container("minf", stbl)))
	return container("trak", parts...)
}

// movieHeader is an mvhd with the given movie timescale.
func movieHeader(timescale uint32) []byte {
	return fullBox("mvhd", 0, 0, cat(u32b(0), u32b(0), u32b(timescale), make([]byte, 84)))
}

func firstTimes(t *testing.T, data []byte) map[uint32][2]int64 {
	t.Helper()
	d, err := NewDemuxer(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	out := map[uint32][2]int64{}
	for _, tr := range d.tracks {
		out[tr.id] = [2]int64{tr.samples[0].cts, tr.samples[0].dts}
	}
	return out
}

// TestEditListsAlignTracks pins the edit lists encoders write: B-frame video
// whose edit starts presentation at its composition offset, and AAC-style
// audio whose edit skips priming. Each track lands on its edited timeline,
// then all tracks lift together so the earliest presentation time is zero:
// the priming frame presents at 0 and the first video frame with the first
// real audio sample, 50 ms later; video decode time stays before it.
func TestEditListsAlignTracks(t *testing.T) {
	ftyp := box("ftyp", cat([]byte("isom"), u32b(0x200), []byte("isom"))...)
	const payload = 20 + 8
	mdat := box("mdat", make([]byte, 40)...)
	video := editedTrak(1, "vide", avc1Entry(64, 48, nil), 4, 5, 100, payload, 200, elstBox([2]int32{400, 200}))
	audio := editedTrak(2, "soun", opusEntry(), 4, 5, 100, payload+20, 0, elstBox([2]int32{350, 50}))
	got := firstTimes(t, cat(ftyp, mdat, container("moov", movieHeader(1000), video, audio)))
	if got[1] != [2]int64{50, -150} {
		t.Fatalf("video first (cts, dts) = %v, want (50, -150): presentation from the edit, lifted by the audio priming", got[1])
	}
	if got[2] != [2]int64{0, 0} {
		t.Fatalf("audio first (cts, dts) = %v, want the priming frame at (0, 0)", got[2])
	}
}

// TestEditListLeadingEmptyEditDelaysTrack pins an empty edit (media time -1):
// its duration, in the movie timescale, delays the track's presentation.
func TestEditListLeadingEmptyEditDelaysTrack(t *testing.T) {
	ftyp := box("ftyp", cat([]byte("isom"), u32b(0x200), []byte("isom"))...)
	const payload = 20 + 8
	mdat := box("mdat", make([]byte, 40)...)
	video := editedTrak(1, "vide", avc1Entry(64, 48, nil), 4, 5, 100, payload, 0, nil)
	// Movie timescale 600: 180 movie ticks are 300 ms, 300 media ticks.
	audio := editedTrak(2, "soun", opusEntry(), 4, 5, 100, payload+20, 0, elstBox([2]int32{180, -1}, [2]int32{240, 0}))
	got := firstTimes(t, cat(ftyp, mdat, container("moov", movieHeader(600), video, audio)))
	if got[1][0] != 0 || got[2][0] != 300 {
		t.Fatalf("first presentation video=%d audio=%d, want 0 and 300 (delayed by the empty edit)", got[1][0], got[2][0])
	}
}

// TestEditListMalformedIsIgnored pins that a list with an invalid media time
// leaves the track on its media timeline instead of shifting it.
func TestEditListMalformedIsIgnored(t *testing.T) {
	if edit := parseElst(cat(u32b(0), u32b(1), u32b(100), u32b(uint32(0xFFFFFFF0)), u16b(1), u16b(0))); edit != (trackEdit{}) {
		t.Fatalf("parseElst(media time -16) = %+v, want no edit", edit)
	}
	if edit := parseElst(cat(u32b(0), u32b(2), u32b(100))); edit != (trackEdit{}) {
		t.Fatalf("parseElst(truncated) = %+v, want no edit", edit)
	}
}

// TestEditListRealFileAlignsAACPriming pins the ffmpeg-written fixture: its
// audio edit skips 1024 AAC priming samples, so the first real audio sample
// presents with the first video frame (within one tick of the coarser video
// timescale), and nothing presents before zero. Before edit lists, audio
// content trailed the video by the 23 ms of priming.
func TestEditListRealFileAlignsAACPriming(t *testing.T) {
	data, err := os.ReadFile("testdata/h264_aac.mp4")
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDemuxer(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var videoNS, audioNS int64
	for _, tr := range d.tracks {
		for i := range tr.samples {
			if tr.samples[i].cts < 0 {
				t.Fatalf("track %d sample %d presents at %d, before zero", tr.id, i, tr.samples[i].cts)
			}
		}
		switch tr.media {
		case "video":
			videoNS = timescaleNS(tr.samples[0].cts, tr.timescale)
		case "audio":
			if tr.edit.mediaTime != 1024 {
				t.Fatalf("audio edit media time = %d, want the 1024 priming samples", tr.edit.mediaTime)
			}
			audioNS = timescaleNS(tr.samples[1].cts, tr.timescale)
		}
	}
	const videoTickNS = int64(time.Second) / 12288
	if diff := videoNS - audioNS; diff < -videoTickNS || diff > videoTickNS {
		t.Fatalf("first video at %d ns, first real audio at %d ns: want them together", videoNS, audioNS)
	}
}

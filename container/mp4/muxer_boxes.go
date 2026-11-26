package mp4

import (
	"math"
	"time"

	"github.com/thesyncim/goav/av"
)

func writeFtyp(w *boxWriter, tracks []*muxTrack, fragmented bool) {
	w.start("ftyp")
	brands := []string{"isom", "iso2", "mp41"}
	if fragmented {
		w.str("iso6")
		w.u32(0)
		brands = []string{"iso6", "isom", "mp41"}
	} else {
		w.str("isom")
		w.u32(0x200)
	}
	for _, b := range brands {
		w.str(b)
	}
	for _, t := range tracks {
		switch t.codec {
		case av.CodecH264:
			w.str("avc1")
		case av.CodecAV1:
			w.str("av01")
		}
	}
	w.end()
}

// writeMoov writes the movie: mvhd, a trak per track, and for a fragmented
// file the mvex that declares its tracks' fragments.
func (m *FormatMuxer) writeMoov(w *boxWriter, fragmented bool) {
	origin := m.originNS()
	var movieDuration int64
	edits := make([]trackEditPlan, len(m.tracks))
	for i, t := range m.tracks {
		edits[i] = t.editPlan(origin, fragmented)
		movieDuration = max(movieDuration, edits[i].total)
	}
	w.start("moov")
	w.startFull("mvhd", 1, 0)
	w.u64(0) // creation time
	w.u64(0) // modification time
	w.u32(movieTimescale)
	w.u64(uint64(movieDuration))
	w.u32(0x00010000) // rate
	w.u16(0x0100)     // volume
	w.zeros(10)
	w.matrix()
	w.zeros(24)
	w.u32(uint32(len(m.tracks) + 1)) // next track id
	w.end()
	for i, t := range m.tracks {
		t.writeTrak(w, edits[i], fragmented)
	}
	if fragmented {
		w.start("mvex")
		for _, t := range m.tracks {
			w.startFull("trex", 0, 0)
			w.u32(t.id)
			w.u32(1) // sample description index
			w.u32(0)
			w.u32(0)
			w.u32(0)
			w.end()
		}
		w.end()
	}
	w.end()
}

// originNS is the movie's time zero: the earliest presentation of any
// track, past its pre-roll.
func (m *FormatMuxer) originNS() int64 {
	origin := int64(math.MaxInt64)
	for _, t := range m.tracks {
		if t.started {
			origin = min(origin, ticksNS(t.startPTS, t.timescale)+t.prerollNS)
		}
	}
	if origin == math.MaxInt64 {
		return 0
	}
	return origin
}

// trackEditPlan places a track on the movie timeline: an empty edit of
// delay movie ticks, then its media from mediaTime ticks for duration movie
// ticks (0 while fragments are still to come).
type trackEditPlan struct {
	delay, mediaTime, duration, total int64
}

func (t *muxTrack) editPlan(originNS int64, fragmented bool) trackEditPlan {
	if !t.started {
		return trackEditPlan{}
	}
	presentNS := ticksNS(t.startPTS, t.timescale) + t.prerollNS
	plan := trackEditPlan{
		delay:     nsTicks(presentNS-originNS, movieTimescale),
		mediaTime: t.startPTS + nsTicks(t.prerollNS, t.timescale) - t.startDTS,
	}
	if !fragmented {
		plan.duration = max(nsTicks(ticksNS(t.endPTS, t.timescale)-presentNS, movieTimescale), 0)
		plan.total = plan.delay + plan.duration
	}
	return plan
}

func (t *muxTrack) writeTrak(w *boxWriter, edit trackEditPlan, fragmented bool) {
	var mediaDuration int64
	if !fragmented {
		for _, s := range t.samples {
			mediaDuration += s.duration
		}
	}
	w.start("trak")
	w.startFull("tkhd", 1, 0x000003) // enabled, in movie
	w.u64(0)
	w.u64(0)
	w.u32(t.id)
	w.u32(0)
	w.u64(uint64(edit.total))
	w.zeros(8)
	w.u16(0) // layer
	w.u16(0) // alternate group
	if t.media == av.MediaAudio {
		w.u16(0x0100)
	} else {
		w.u16(0)
	}
	w.u16(0)
	w.matrix()
	w.u32(uint32(t.width) << 16)
	w.u32(uint32(t.height) << 16)
	w.end()
	if edit.delay != 0 || edit.mediaTime != 0 {
		w.start("edts")
		entries := uint32(1)
		if edit.delay > 0 {
			entries = 2
		}
		w.startFull("elst", 1, 0)
		w.u32(entries)
		if edit.delay > 0 {
			w.u64(uint64(edit.delay))
			w.u64(math.MaxUint64) // media_time -1: empty
			w.u32(0x00010000)
		}
		w.u64(uint64(edit.duration))
		w.u64(uint64(edit.mediaTime))
		w.u32(0x00010000)
		w.end()
		w.end()
	}
	w.start("mdia")
	w.startFull("mdhd", 1, 0)
	w.u64(0)
	w.u64(0)
	w.u32(t.timescale)
	w.u64(uint64(mediaDuration))
	w.u16(0x55c4) // language: und
	w.u16(0)
	w.end()
	w.startFull("hdlr", 0, 0)
	w.u32(0)
	if t.media == av.MediaAudio {
		w.str("soun")
		w.zeros(12)
		w.str("SoundHandler\x00")
	} else {
		w.str("vide")
		w.zeros(12)
		w.str("VideoHandler\x00")
	}
	w.end()
	w.start("minf")
	if t.media == av.MediaAudio {
		w.startFull("smhd", 0, 0)
		w.u32(0)
		w.end()
	} else {
		w.startFull("vmhd", 0, 1)
		w.zeros(8)
		w.end()
	}
	w.start("dinf")
	w.startFull("dref", 0, 0)
	w.u32(1)
	w.startFull("url ", 0, 1) // the media is in this file
	w.end()
	w.end()
	w.end()
	t.writeStbl(w, fragmented)
	w.end() // minf
	w.end() // mdia
	w.end() // trak
}

func (t *muxTrack) writeStbl(w *boxWriter, fragmented bool) {
	samples := t.samples
	if fragmented {
		samples = nil
	}
	w.start("stbl")
	w.startFull("stsd", 0, 0)
	w.u32(1)
	t.writeSampleEntry(w)
	w.end()
	// stts: decode-time deltas, run-length coded.
	w.startFull("stts", 0, 0)
	runs := runLength(len(samples), func(i int) int64 { return samples[i].duration })
	w.u32(uint32(len(runs)))
	for _, r := range runs {
		w.u32(r.count)
		w.u32(uint32(r.value))
	}
	w.end()
	if offsets := runLength(len(samples), func(i int) int64 { return samples[i].cts }); len(offsets) > 1 || len(offsets) == 1 && offsets[0].value != 0 {
		version := uint8(0)
		for _, r := range offsets {
			if r.value < 0 {
				version = 1
			}
		}
		w.startFull("ctts", version, 0)
		w.u32(uint32(len(offsets)))
		for _, r := range offsets {
			w.u32(r.count)
			w.u32(uint32(int32(r.value)))
		}
		w.end()
	}
	if t.media == av.MediaVideo {
		var syncs []uint32
		for i, s := range samples {
			if s.sync {
				syncs = append(syncs, uint32(i+1))
			}
		}
		if len(syncs) != len(samples) {
			w.startFull("stss", 0, 0)
			w.u32(uint32(len(syncs)))
			for _, n := range syncs {
				w.u32(n)
			}
			w.end()
		}
	}
	chunks := t.chunks
	if fragmented {
		chunks = nil
	}
	w.startFull("stsc", 0, 0)
	perChunk := runLength(len(chunks), func(i int) int64 { return int64(chunks[i].count) })
	w.u32(uint32(len(perChunk)))
	first := uint32(1)
	for _, r := range perChunk {
		w.u32(first)
		w.u32(uint32(r.value))
		w.u32(1)
		first += r.count
	}
	w.end()
	w.startFull("stsz", 0, 0)
	uniform := len(samples) > 0
	for _, s := range samples {
		uniform = uniform && s.size == samples[0].size
	}
	if uniform {
		w.u32(samples[0].size)
		w.u32(uint32(len(samples)))
	} else {
		w.u32(0)
		w.u32(uint32(len(samples)))
		for _, s := range samples {
			w.u32(s.size)
		}
	}
	w.end()
	wide := len(chunks) != 0 && chunks[len(chunks)-1].offset > math.MaxUint32
	if wide {
		w.startFull("co64", 0, 0)
	} else {
		w.startFull("stco", 0, 0)
	}
	w.u32(uint32(len(chunks)))
	for _, c := range chunks {
		if wide {
			w.u64(uint64(c.offset))
		} else {
			w.u32(uint32(c.offset))
		}
	}
	w.end()
	w.end()
}

type run struct {
	count uint32
	value int64
}

func runLength(n int, value func(int) int64) []run {
	var runs []run
	for i := range n {
		v := value(i)
		if len(runs) != 0 && runs[len(runs)-1].value == v {
			runs[len(runs)-1].count++
			continue
		}
		runs = append(runs, run{count: 1, value: v})
	}
	return runs
}

func ticksNS(ticks int64, timescale uint32) int64 {
	return int64(float64(ticks) * float64(time.Second) / float64(timescale))
}

func nsTicks(ns int64, timescale uint32) int64 {
	return int64(math.Round(float64(ns) * float64(timescale) / float64(time.Second)))
}

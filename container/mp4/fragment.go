package mp4

import "io"

// trexDefaults holds a track's movie-extends defaults, applied to trun samples
// that omit a per-sample duration, size, or flags.
type trexDefaults struct {
	sampleDuration uint32
	sampleSize     uint32
	sampleFlags    uint32
}

// tfhd (track fragment header) flag bits.
const (
	tfhdBaseDataOffset     = 0x000001
	tfhdSampleDescIndex    = 0x000002
	tfhdDefaultSampleDur   = 0x000008
	tfhdDefaultSampleSize  = 0x000010
	tfhdDefaultSampleFlags = 0x000020
)

// trun (track fragment run) flag bits.
const (
	trunDataOffset       = 0x000001
	trunFirstSampleFlags = 0x000004
	trunSampleDuration   = 0x000100
	trunSampleSize       = 0x000200
	trunSampleFlags      = 0x000400
	trunSampleCompOffset = 0x000800
)

// sampleNonSync is the sample_flags bit set on non-sync (non-keyframe) samples;
// when it is clear the sample is a sync point.
const sampleNonSync = 0x00010000

// parseFragments walks the top-level moof boxes and appends every track run's
// samples to the matching track, so fragmented (fMP4/CMAF) files demux through
// the same flat sample index as progressive files.
func parseFragments(r io.ReaderAt, size int64, byID map[uint32]*track) error {
	return walkBoxes(r, 0, size, func(h boxHeader) error {
		if h.Type != "moof" {
			return nil
		}
		return parseMoof(r, h, byID)
	})
}

func parseMoof(r io.ReaderAt, moof boxHeader, byID map[uint32]*track) error {
	return walkBoxes(r, moof.PayloadOffset, moof.end(), func(h boxHeader) error {
		if h.Type != "traf" {
			return nil
		}
		return parseTraf(r, h, moof.Offset, byID)
	})
}

func parseTraf(r io.ReaderAt, traf boxHeader, moofOffset int64, byID map[uint32]*track) error {
	var (
		tr           *track
		baseOffset   = moofOffset // default-base-is-moof and the common case
		baseDTS      int64
		defDur       uint32
		defSize      uint32
		defFlags     uint32
		haveDefDur   bool
		haveDefSize  bool
		haveDefFlags bool
		runs         []boxHeader
	)

	if err := walkBoxes(r, traf.PayloadOffset, traf.end(), func(h boxHeader) error {
		switch h.Type {
		case "tfhd":
			pl, err := readPayload(r, h)
			if err != nil {
				return err
			}
			p := newParser(pl)
			_, flags := p.fullBox()
			tr = byID[p.u32()]
			if flags&tfhdBaseDataOffset != 0 {
				baseOffset = int64(p.u64())
			}
			if flags&tfhdSampleDescIndex != 0 {
				p.u32()
			}
			if flags&tfhdDefaultSampleDur != 0 {
				defDur, haveDefDur = p.u32(), true
			}
			if flags&tfhdDefaultSampleSize != 0 {
				defSize, haveDefSize = p.u32(), true
			}
			if flags&tfhdDefaultSampleFlags != 0 {
				defFlags, haveDefFlags = p.u32(), true
			}
		case "tfdt":
			pl, err := readPayload(r, h)
			if err != nil {
				return err
			}
			p := newParser(pl)
			if version, _ := p.fullBox(); version == 1 {
				baseDTS = int64(p.u64())
			} else {
				baseDTS = int64(p.u32())
			}
		case "trun":
			runs = append(runs, h)
		}
		return nil
	}); err != nil {
		return err
	}
	if tr == nil {
		return nil // a track fragment for a codec we do not expose
	}

	dur := tr.defaultSampleDuration
	if haveDefDur {
		dur = defDur
	}
	size := tr.defaultSampleSize
	if haveDefSize {
		size = defSize
	}
	flags := tr.defaultSampleFlags
	if haveDefFlags {
		flags = defFlags
	}

	dts := baseDTS
	for _, run := range runs {
		next, err := parseTrun(r, run, tr, baseOffset, dts, dur, size, flags)
		if err != nil {
			return err
		}
		dts = next
	}
	return nil
}

// parseTrun appends one run's samples to tr and returns the decode time that
// the next run continues from.
func parseTrun(r io.ReaderAt, run boxHeader, tr *track, baseOffset, dts int64, defDur, defSize, defFlags uint32) (int64, error) {
	pl, err := readPayload(r, run)
	if err != nil {
		return dts, err
	}
	p := newParser(pl)
	version, flags := p.fullBox()
	count := p.u32()
	if count > maxSamples {
		return dts, ErrInvalidData
	}
	dataOffset := int64(0)
	if flags&trunDataOffset != 0 {
		dataOffset = int64(p.i32())
	}
	firstFlags := defFlags
	if flags&trunFirstSampleFlags != 0 {
		firstFlags = p.u32()
	}
	offset := baseOffset + dataOffset
	for i := uint32(0); i < count; i++ {
		d := defDur
		if flags&trunSampleDuration != 0 {
			d = p.u32()
		}
		s := defSize
		if flags&trunSampleSize != 0 {
			s = p.u32()
		}
		sflags := defFlags
		if flags&trunSampleFlags != 0 {
			sflags = p.u32()
		}
		if i == 0 {
			sflags = firstFlags
		}
		var comp int32
		if flags&trunSampleCompOffset != 0 {
			if version == 1 {
				comp = p.i32()
			} else {
				comp = int32(p.u32())
			}
		}
		tr.samples = append(tr.samples, sample{
			offset:   offset,
			size:     int(s),
			dts:      dts,
			cts:      dts + int64(comp),
			keyframe: sflags&sampleNonSync == 0,
		})
		offset += int64(s)
		dts += int64(d)
	}
	if p.err() != nil {
		return dts, ErrInvalidData
	}
	return dts, nil
}

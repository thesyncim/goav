package mp4

import (
	"io"

	"github.com/thesyncim/goav/av"
)

// track is one parsed, demuxable MP4 track: its codec parameters plus the flat
// sample list timed in the media timescale.
type track struct {
	id        uint32
	media     av.MediaType
	codec     av.CodecID
	timescale uint32
	params    av.CodecParameters
	samples   []sample
	// fragments holds the movie-extends (trex) defaults applied to trun samples
	// that omit a per-sample duration, size, or flags.
	defaultSampleDuration uint32
	defaultSampleSize     uint32
	defaultSampleFlags    uint32
}

// parseMovie locates the moov box and parses every supported track into a flat
// sample index. Tracks with no codec goav can decode, or with unusable sample
// tables, are skipped rather than failing the whole file.
func parseMovie(r io.ReaderAt, size int64) ([]*track, error) {
	var moov boxHeader
	found := false
	if err := walkBoxes(r, 0, size, func(h boxHeader) error {
		if h.Type == "moov" {
			moov = h
			found = true
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNoMovie
	}

	var tracks []*track
	trex := map[uint32]trexDefaults{}
	if err := walkBoxes(r, moov.PayloadOffset, moov.end(), func(h boxHeader) error {
		switch h.Type {
		case "trak":
			tr, err := parseTrak(r, h)
			if err != nil {
				return err
			}
			if tr != nil {
				tracks = append(tracks, tr)
			}
		case "mvex":
			return walkBoxes(r, h.PayloadOffset, h.end(), func(m boxHeader) error {
				if m.Type == "trex" {
					if pl, err := readPayload(r, m); err == nil {
						id, d := parseTrex(pl)
						trex[id] = d
					}
				}
				return nil
			})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(tracks) == 0 {
		return nil, ErrNoTracks
	}
	for _, tr := range tracks {
		if d, ok := trex[tr.id]; ok {
			tr.defaultSampleDuration = d.sampleDuration
			tr.defaultSampleSize = d.sampleSize
			tr.defaultSampleFlags = d.sampleFlags
		}
	}
	return tracks, nil
}

// parseTrex reads a movie-extends track defaults box: the per-track default
// sample duration, size, and flags applied to fragment runs that omit them.
func parseTrex(payload []byte) (uint32, trexDefaults) {
	p := newParser(payload)
	p.fullBox()
	trackID := p.u32()
	p.u32() // default_sample_description_index
	return trackID, trexDefaults{
		sampleDuration: p.u32(),
		sampleSize:     p.u32(),
		sampleFlags:    p.u32(),
	}
}

func parseTrak(r io.ReaderAt, trak boxHeader) (*track, error) {
	tr := &track{}
	var stblBox boxHeader
	haveStbl := false
	if err := walkBoxes(r, trak.PayloadOffset, trak.end(), func(h boxHeader) error {
		switch h.Type {
		case "tkhd":
			if pl, err := readPayload(r, h); err == nil {
				tr.id = parseTkhdTrackID(pl)
			}
		case "mdia":
			return walkBoxes(r, h.PayloadOffset, h.end(), func(m boxHeader) error {
				switch m.Type {
				case "mdhd":
					if pl, err := readPayload(r, m); err == nil {
						tr.timescale = parseMdhdTimescale(pl)
					}
				case "hdlr":
					if pl, err := readPayload(r, m); err == nil {
						tr.media = parseHdlrMedia(pl)
					}
				case "minf":
					return walkBoxes(r, m.PayloadOffset, m.end(), func(n boxHeader) error {
						if n.Type == "stbl" {
							stblBox = n
							haveStbl = true
						}
						return nil
					})
				}
				return nil
			})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if !haveStbl || tr.timescale == 0 {
		return nil, nil
	}

	tables, codecID, media, params, err := parseStbl(r, stblBox)
	if err != nil {
		return nil, err
	}
	if codecID == av.CodecUnknown {
		return nil, nil
	}
	if tr.media == av.MediaUnknown {
		tr.media = media
	}
	tr.codec = codecID
	params.ClockRate = tr.timescale
	tr.params = params
	samples, err := tables.build()
	if err != nil {
		return nil, err
	}
	// Keep tracks with empty sample tables: a fragmented movie carries its
	// samples in moof/trun, which parseFragments appends after the moov walk.
	tr.samples = samples
	return tr, nil
}

func parseStbl(r io.ReaderAt, stbl boxHeader) (*sampleTables, av.CodecID, av.MediaType, av.CodecParameters, error) {
	tables := &sampleTables{}
	var codecID av.CodecID
	var media av.MediaType
	var params av.CodecParameters
	err := walkBoxes(r, stbl.PayloadOffset, stbl.end(), func(h boxHeader) error {
		switch h.Type {
		case "stsd":
			c, m, p, err := parseStsd(r, h)
			if err != nil {
				return err
			}
			codecID, media, params = c, m, p
			return nil
		case "stts":
			pl, err := readPayload(r, h)
			if err != nil {
				return err
			}
			tables.stts = parseStts(pl)
		case "ctts":
			pl, err := readPayload(r, h)
			if err != nil {
				return err
			}
			tables.ctts = parseCtts(pl)
		case "stsc":
			pl, err := readPayload(r, h)
			if err != nil {
				return err
			}
			tables.stsc = parseStsc(pl)
		case "stsz":
			pl, err := readPayload(r, h)
			if err != nil {
				return err
			}
			tables.sizes = parseStsz(pl)
		case "stz2":
			pl, err := readPayload(r, h)
			if err != nil {
				return err
			}
			tables.sizes = parseStz2(pl)
		case "stco":
			pl, err := readPayload(r, h)
			if err != nil {
				return err
			}
			tables.chunkOffsets = parseStco(pl)
		case "co64":
			pl, err := readPayload(r, h)
			if err != nil {
				return err
			}
			tables.chunkOffsets = parseCo64(pl)
		case "stss":
			pl, err := readPayload(r, h)
			if err != nil {
				return err
			}
			tables.sync = parseStss(pl)
			tables.syncPresent = true
		}
		return nil
	})
	return tables, codecID, media, params, err
}

func parseStsd(r io.ReaderAt, h boxHeader) (av.CodecID, av.MediaType, av.CodecParameters, error) {
	if h.PayloadSize < 8 {
		return av.CodecUnknown, av.MediaUnknown, av.CodecParameters{}, ErrInvalidData
	}
	var codecID av.CodecID
	var media av.MediaType
	var params av.CodecParameters
	// Skip the FullBox header and entry_count; the entries are child boxes.
	err := walkBoxes(r, h.PayloadOffset+8, h.end(), func(e boxHeader) error {
		if codecID != av.CodecUnknown {
			return nil
		}
		id, m := sampleEntryCodec(e.Type)
		if id == av.CodecUnknown {
			return nil
		}
		pl, err := readPayload(r, e)
		if err != nil {
			return nil
		}
		params = av.CodecParameters{ID: id, Type: m}
		if m == av.MediaVideo {
			w, ht, extra := parseVisualSampleEntry(pl)
			params.Width = w
			params.Height = ht
			if len(extra) != 0 {
				params.ExtraData = av.Buffer{Bytes: extra, Ownership: av.BufferImmutable}
			}
		} else {
			ch, sr, extra := parseAudioSampleEntry(pl)
			params.Channels = ch
			params.SampleRate = sr
			if len(extra) != 0 {
				params.ExtraData = av.Buffer{Bytes: extra, Ownership: av.BufferImmutable}
			}
		}
		codecID, media = id, m
		return nil
	})
	return codecID, media, params, err
}

func parseTkhdTrackID(payload []byte) uint32 {
	p := newParser(payload)
	version, _ := p.fullBox()
	if version == 1 {
		p.skip(16) // creation + modification (64-bit)
	} else {
		p.skip(8) // creation + modification (32-bit)
	}
	return p.u32()
}

func parseMdhdTimescale(payload []byte) uint32 {
	p := newParser(payload)
	version, _ := p.fullBox()
	if version == 1 {
		p.skip(16)
	} else {
		p.skip(8)
	}
	return p.u32()
}

func parseHdlrMedia(payload []byte) av.MediaType {
	p := newParser(payload)
	p.fullBox()
	p.skip(4) // pre_defined
	handler := p.take(4)
	if p.err() != nil {
		return av.MediaUnknown
	}
	switch string(handler) {
	case "vide":
		return av.MediaVideo
	case "soun":
		return av.MediaAudio
	default:
		return av.MediaUnknown
	}
}

func parseStts(payload []byte) []sttsEntry {
	p := newParser(payload)
	p.fullBox()
	count := p.u32()
	if !fits(p, count, 8) {
		return nil
	}
	out := make([]sttsEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		out = append(out, sttsEntry{count: p.u32(), delta: p.u32()})
	}
	if p.err() != nil {
		return nil
	}
	return out
}

func parseCtts(payload []byte) []cttsEntry {
	p := newParser(payload)
	p.fullBox()
	count := p.u32()
	if !fits(p, count, 8) {
		return nil
	}
	out := make([]cttsEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		out = append(out, cttsEntry{count: p.u32(), offset: p.i32()})
	}
	if p.err() != nil {
		return nil
	}
	return out
}

func parseStsc(payload []byte) []stscEntry {
	p := newParser(payload)
	p.fullBox()
	count := p.u32()
	if !fits(p, count, 12) {
		return nil
	}
	out := make([]stscEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		first := p.u32()
		perChunk := p.u32()
		p.u32() // sample_description_index
		out = append(out, stscEntry{firstChunk: first, samplesPerChunk: perChunk})
	}
	if p.err() != nil {
		return nil
	}
	return out
}

func parseStsz(payload []byte) []uint32 {
	p := newParser(payload)
	p.fullBox()
	uniform := p.u32()
	count := p.u32()
	if count > maxSamples {
		return nil
	}
	out := make([]uint32, 0, count)
	if uniform != 0 {
		for i := uint32(0); i < count; i++ {
			out = append(out, uniform)
		}
		return out
	}
	if !fits(p, count, 4) {
		return nil
	}
	for i := uint32(0); i < count; i++ {
		out = append(out, p.u32())
	}
	if p.err() != nil {
		return nil
	}
	return out
}

func parseStz2(payload []byte) []uint32 {
	p := newParser(payload)
	p.fullBox()
	p.skip(3) // reserved
	fieldSize := int(p.u8())
	count := p.u32()
	if count > maxSamples {
		return nil
	}
	out := make([]uint32, 0, count)
	switch fieldSize {
	case 16:
		for i := uint32(0); i < count; i++ {
			out = append(out, uint32(p.u16()))
		}
	case 8:
		for i := uint32(0); i < count; i++ {
			out = append(out, uint32(p.u8()))
		}
	case 4:
		for i := uint32(0); i < count; i += 2 {
			b := p.u8()
			out = append(out, uint32(b>>4))
			if i+1 < count {
				out = append(out, uint32(b&0x0f))
			}
		}
	default:
		return nil
	}
	if p.err() != nil {
		return nil
	}
	return out
}

func parseStco(payload []byte) []int64 {
	p := newParser(payload)
	p.fullBox()
	count := p.u32()
	if !fits(p, count, 4) {
		return nil
	}
	out := make([]int64, 0, count)
	for i := uint32(0); i < count; i++ {
		out = append(out, int64(p.u32()))
	}
	if p.err() != nil {
		return nil
	}
	return out
}

func parseCo64(payload []byte) []int64 {
	p := newParser(payload)
	p.fullBox()
	count := p.u32()
	if !fits(p, count, 8) {
		return nil
	}
	out := make([]int64, 0, count)
	for i := uint32(0); i < count; i++ {
		out = append(out, int64(p.u64()))
	}
	if p.err() != nil {
		return nil
	}
	return out
}

func parseStss(payload []byte) []uint32 {
	p := newParser(payload)
	p.fullBox()
	count := p.u32()
	if !fits(p, count, 4) {
		return nil
	}
	out := make([]uint32, 0, count)
	for i := uint32(0); i < count; i++ {
		out = append(out, p.u32())
	}
	if p.err() != nil {
		return nil
	}
	return out
}

// fits reports whether count entries of entrySize bytes can come from what
// remains in the parser, refusing declared counts that exceed the payload (a
// crafted huge count) or the per-track sample cap.
func fits(p *parser, count uint32, entrySize int) bool {
	if count > maxSamples {
		return false
	}
	return int64(count)*int64(entrySize) <= int64(p.remaining())
}

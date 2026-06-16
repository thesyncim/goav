package mp4

// sample is one demuxable unit located at Offset in the file, sized Size bytes,
// timed in the track's media timescale. DTS is decode time; CTS is composition
// (presentation) time = DTS + the ctts offset.
type sample struct {
	offset   int64
	size     int
	dts      int64
	cts      int64
	keyframe bool
}

type sttsEntry struct {
	count uint32
	delta uint32
}

type cttsEntry struct {
	count  uint32
	offset int32
}

type stscEntry struct {
	firstChunk      uint32
	samplesPerChunk uint32
}

// sampleTables holds the raw stbl tables before they are assembled into a flat
// sample list. sizes is already expanded to one entry per sample; sync holds
// 1-based sync-sample numbers, and a nil sync means every sample is a sync
// point (the common case for audio and all-intra video).
type sampleTables struct {
	stts         []sttsEntry
	ctts         []cttsEntry
	stsc         []stscEntry
	sizes        []uint32
	chunkOffsets []int64
	sync         []uint32
	syncPresent  bool
}

// maxSamples caps the per-track sample count so a crafted table cannot allocate
// unbounded memory. It comfortably covers multi-hour high-frame-rate media.
const maxSamples = 8 << 20

// build assembles the raw tables into a flat sample list in decode order. It
// returns nil for an empty table and ErrInvalidData for tables that cannot be
// reconciled (sample/chunk mismatch, overflow).
func (t *sampleTables) build() ([]sample, error) {
	n := len(t.sizes)
	if n == 0 {
		return nil, nil
	}
	if n > maxSamples {
		return nil, ErrInvalidData
	}
	samples := make([]sample, n)
	for i := range samples {
		samples[i].size = int(t.sizes[i])
		samples[i].keyframe = !t.syncPresent
	}

	dts := int64(0)
	idx := 0
	for _, e := range t.stts {
		for c := uint32(0); c < e.count && idx < n; c++ {
			samples[idx].dts = dts
			dts += int64(e.delta)
			idx++
		}
	}
	for i := range samples {
		samples[i].cts = samples[i].dts
	}
	idx = 0
	for _, e := range t.ctts {
		for c := uint32(0); c < e.count && idx < n; c++ {
			samples[idx].cts = samples[idx].dts + int64(e.offset)
			idx++
		}
	}
	for _, num := range t.sync {
		if num >= 1 && int(num) <= n {
			samples[num-1].keyframe = true
		}
	}
	if err := t.assignOffsets(samples); err != nil {
		return nil, err
	}
	return samples, nil
}

// assignOffsets walks the sample-to-chunk and chunk-offset tables to locate
// every sample in the file. Samples within a chunk are contiguous, so each
// offset is the chunk offset plus the running size of earlier samples in the
// chunk.
func (t *sampleTables) assignOffsets(samples []sample) error {
	if len(t.stsc) == 0 || len(t.chunkOffsets) == 0 {
		return ErrInvalidData
	}
	sampleIdx := 0
	entry := 0
	for chunk := 1; chunk <= len(t.chunkOffsets) && sampleIdx < len(samples); chunk++ {
		for entry+1 < len(t.stsc) && uint32(chunk) >= t.stsc[entry+1].firstChunk {
			entry++
		}
		perChunk := t.stsc[entry].samplesPerChunk
		offset := t.chunkOffsets[chunk-1]
		for k := uint32(0); k < perChunk && sampleIdx < len(samples); k++ {
			samples[sampleIdx].offset = offset
			offset += int64(samples[sampleIdx].size)
			sampleIdx++
		}
	}
	if sampleIdx != len(samples) {
		return ErrInvalidData
	}
	return nil
}

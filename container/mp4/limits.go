package mp4

// maxFileSamples caps the samples one file may declare across all of its
// tracks and fragments.
const maxFileSamples = 16 << 20

// sampleBudget bounds the sample tables one file may declare, so a few hundred
// crafted bytes cannot force a multi-gigabyte allocation. Every sample costs at
// least one byte of the file (payload or a table entry), so a count larger
// than the file is malformed, and so is a count whose uniform sample size
// cannot fit in the file. The budget is file-wide: many small tables or runs
// cannot add up past it either.
type sampleBudget struct {
	fileSize int64
	left     int64
}

func newSampleBudget(fileSize int64) *sampleBudget {
	left := int64(maxFileSamples)
	if fileSize >= 0 && fileSize < left {
		left = fileSize
	}
	return &sampleBudget{fileSize: fileSize, left: left}
}

// claim reserves count samples of bytesEach payload bytes each (0 when sizes
// come from a per-sample table, which fits already bounds by its payload). It
// reports false, reserving nothing, when the file cannot hold them.
func (b *sampleBudget) claim(count uint32, bytesEach uint32) bool {
	n := int64(count)
	if n > b.left || n > maxSamples {
		return false
	}
	if bytesEach > 0 && n*int64(bytesEach) > b.fileSize {
		return false
	}
	b.left -= n
	return true
}

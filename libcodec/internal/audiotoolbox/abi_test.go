package audiotoolbox

import (
	"testing"
	"unsafe"
)

// TestABIMatchesCoreAudio pins the mirrored structures to the sizes and
// offsets the C compiler gives CoreAudioTypes.h.
func TestABIMatchesCoreAudio(t *testing.T) {
	var d streamDescription
	var l bufferList
	var p packetDesc
	for _, c := range []struct {
		name      string
		got, want uintptr
	}{
		{"AudioStreamBasicDescription", unsafe.Sizeof(d), 40},
		{"AudioStreamBasicDescription.mFormatID", unsafe.Offsetof(d.FormatID), 8},
		{"AudioStreamBasicDescription.mReserved", unsafe.Offsetof(d.Reserved), 36},
		{"AudioBufferList", unsafe.Sizeof(l), 24},
		{"AudioBufferList.mBuffers[0].mNumberChannels", unsafe.Offsetof(l.NumberChannels), 8},
		{"AudioBufferList.mBuffers[0].mDataByteSize", unsafe.Offsetof(l.DataByteSize), 12},
		{"AudioBufferList.mBuffers[0].mData", unsafe.Offsetof(l.Data), 16},
		{"AudioStreamPacketDescription", unsafe.Sizeof(p), 16},
		{"AudioStreamPacketDescription.mDataByteSize", unsafe.Offsetof(p.DataByteSize), 12},
	} {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}

//go:build arm64

package goav

import "encoding/binary"

func mixS16ArchKernel() (mixS16Kernel, bool) {
	return mixS16Kernel{name: "arm64/sqadd2", fn: mixS16ARM64Kernel}, true
}

func mixS16ARM64Kernel(dst []byte, inputs [][]byte, n int) {
	if len(inputs) != 2 || n < 16 {
		mixS16ScalarKernel(dst, inputs, n)
		return
	}
	a := inputs[0]
	b := inputs[1]
	vec := n &^ 15
	mixS16Add2ARM64(dst[:vec], a[:vec], b[:vec], vec)
	for off := vec; off < n; off += 2 {
		sum := int32(int16(binary.LittleEndian.Uint16(a[off:]))) +
			int32(int16(binary.LittleEndian.Uint16(b[off:])))
		if sum > 32767 {
			sum = 32767
		} else if sum < -32768 {
			sum = -32768
		}
		binary.LittleEndian.PutUint16(dst[off:], uint16(int16(sum)))
	}
}

//go:noescape
func mixS16Add2ARM64(dst []byte, a []byte, b []byte, n int)

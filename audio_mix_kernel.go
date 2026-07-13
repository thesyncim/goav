package goav

import "encoding/binary"

type mixS16Kernel struct {
	name string
	fn   func(dst []byte, inputs [][]byte, n int)
}

var selectedMixS16Kernel = detectMixS16Kernel()

func detectMixS16Kernel() mixS16Kernel {
	if kernel, ok := mixS16ArchKernel(); ok {
		return kernel
	}
	return mixS16Kernel{name: "scalar", fn: mixS16ScalarKernel}
}

func mixS16KernelName() string {
	return selectedMixS16Kernel.name
}

func mixS16Into(dst []byte, inputs [][]byte, n int) {
	kernel := selectedMixS16Kernel
	if kernel.fn == nil {
		kernel = mixS16Kernel{name: "scalar", fn: mixS16ScalarKernel}
	}
	kernel.fn(dst, inputs, n)
}

func mixS16ScalarKernel(dst []byte, inputs [][]byte, n int) {
	for off := 0; off < n; off += 2 {
		var sum int32
		for i := range inputs {
			sum += int32(int16(binary.LittleEndian.Uint16(inputs[i][off:])))
		}
		if sum > 32767 {
			sum = 32767
		} else if sum < -32768 {
			sum = -32768
		}
		binary.LittleEndian.PutUint16(dst[off:], uint16(int16(sum)))
	}
}

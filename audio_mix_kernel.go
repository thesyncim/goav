package goav

import "encoding/binary"

// mixS16Kernel is the S16 mix routine selected once at init: the Go SIMD
// kernel on GOEXPERIMENT=simd builds with hardware vectors, the scalar loop
// everywhere else. Every kernel is bit-exact with mixS16ScalarKernel.
type mixS16Kernel struct {
	name string
	fn   func(dst []byte, inputs [][]byte, n int)
}

var selectedMixS16Kernel = detectMixS16Kernel()

func detectMixS16Kernel() mixS16Kernel {
	if kernel, ok := mixS16VectorKernel(); ok {
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
	if n <= 0 {
		return
	}
	switch len(inputs) {
	case 0:
		clear(dst[:n])
		return
	case 1:
		copy(dst[:n], inputs[0][:n])
		return
	case 2:
		mixS16Scalar2(dst, inputs[0], inputs[1], n)
		return
	case 3:
		mixS16Scalar3(dst, inputs[0], inputs[1], inputs[2], n)
		return
	}
	d := dst[:n]
	off := 0
	for ; off+16 <= n; off += 16 {
		var s0, s1, s2, s3, s4, s5, s6, s7 int32
		for _, in := range inputs {
			in16 := in[off : off+16]
			v0 := binary.LittleEndian.Uint64(in16)
			v1 := binary.LittleEndian.Uint64(in16[8:])
			s0 += int32(int16(v0))
			s1 += int32(int16(v0 >> 16))
			s2 += int32(int16(v0 >> 32))
			s3 += int32(int16(v0 >> 48))
			s4 += int32(int16(v1))
			s5 += int32(int16(v1 >> 16))
			s6 += int32(int16(v1 >> 32))
			s7 += int32(int16(v1 >> 48))
		}
		d16 := d[off : off+16]
		binary.LittleEndian.PutUint64(d16, packS16x4(s0, s1, s2, s3))
		binary.LittleEndian.PutUint64(d16[8:], packS16x4(s4, s5, s6, s7))
	}
	if off < n {
		mixS16ScalarRange(dst, inputs, off, n)
	}
}

func mixS16Scalar2(dst, a, b []byte, n int) {
	d, a, b := dst[:n], a[:n], b[:n]
	off := 0
	for ; off+16 <= n; off += 16 {
		a16, b16, d16 := a[off:off+16], b[off:off+16], d[off:off+16]
		va0 := binary.LittleEndian.Uint64(a16)
		vb0 := binary.LittleEndian.Uint64(b16)
		va1 := binary.LittleEndian.Uint64(a16[8:])
		vb1 := binary.LittleEndian.Uint64(b16[8:])
		binary.LittleEndian.PutUint64(d16, packS16x4(
			int32(int16(va0))+int32(int16(vb0)),
			int32(int16(va0>>16))+int32(int16(vb0>>16)),
			int32(int16(va0>>32))+int32(int16(vb0>>32)),
			int32(int16(va0>>48))+int32(int16(vb0>>48)),
		))
		binary.LittleEndian.PutUint64(d16[8:], packS16x4(
			int32(int16(va1))+int32(int16(vb1)),
			int32(int16(va1>>16))+int32(int16(vb1>>16)),
			int32(int16(va1>>32))+int32(int16(vb1>>32)),
			int32(int16(va1>>48))+int32(int16(vb1>>48)),
		))
	}
	for ; off < n; off += 2 {
		s := int32(int16(binary.LittleEndian.Uint16(a[off:]))) + int32(int16(binary.LittleEndian.Uint16(b[off:])))
		binary.LittleEndian.PutUint16(d[off:], uint16(clampS16(s)))
	}
}

func mixS16Scalar3(dst, a, b, c []byte, n int) {
	d, a, b, c := dst[:n], a[:n], b[:n], c[:n]
	off := 0
	for ; off+8 <= n; off += 8 {
		va := binary.LittleEndian.Uint64(a[off:])
		vb := binary.LittleEndian.Uint64(b[off:])
		vc := binary.LittleEndian.Uint64(c[off:])
		binary.LittleEndian.PutUint64(d[off:], packS16x4(
			int32(int16(va))+int32(int16(vb))+int32(int16(vc)),
			int32(int16(va>>16))+int32(int16(vb>>16))+int32(int16(vc>>16)),
			int32(int16(va>>32))+int32(int16(vb>>32))+int32(int16(vc>>32)),
			int32(int16(va>>48))+int32(int16(vb>>48))+int32(int16(vc>>48)),
		))
	}
	for ; off < n; off += 2 {
		s := int32(int16(binary.LittleEndian.Uint16(a[off:]))) +
			int32(int16(binary.LittleEndian.Uint16(b[off:]))) +
			int32(int16(binary.LittleEndian.Uint16(c[off:])))
		binary.LittleEndian.PutUint16(d[off:], uint16(clampS16(s)))
	}
}

func packS16x4(s0, s1, s2, s3 int32) uint64 {
	if (uint32(s0+32768)|uint32(s1+32768)|uint32(s2+32768)|uint32(s3+32768)) > 65535 {
		s0 = clampS16(s0)
		s1 = clampS16(s1)
		s2 = clampS16(s2)
		s3 = clampS16(s3)
	}
	return uint64(uint16(s0)) |
		uint64(uint16(s1))<<16 |
		uint64(uint16(s2))<<32 |
		uint64(uint16(s3))<<48
}

func clampS16(s int32) int32 {
	if s > 32767 {
		return 32767
	}
	if s < -32768 {
		return -32768
	}
	return s
}

// mixS16ScalarRange mixes the samples in byte range [from, n): the reference
// semantics every vector kernel matches, and the tail loop they share.
func mixS16ScalarRange(dst []byte, inputs [][]byte, from int, n int) {
	for off := from; off < n; off += 2 {
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

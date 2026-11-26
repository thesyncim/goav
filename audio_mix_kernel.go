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
	const (
		maskLo15 = 0x7fff7fff7fff7fff
		maskHi1  = 0x8000800080008000
	)
	d := dst[:n]
	off := 0
	for ; off+32 <= n; off += 32 {
		in0 := inputs[0][off : off+32]
		s0 := binary.LittleEndian.Uint64(in0)
		s1 := binary.LittleEndian.Uint64(in0[8:])
		s2 := binary.LittleEndian.Uint64(in0[16:])
		s3 := binary.LittleEndian.Uint64(in0[24:])
		var ovf uint64
		for _, in := range inputs[1:] {
			in32 := in[off : off+32]
			v0 := binary.LittleEndian.Uint64(in32)
			v1 := binary.LittleEndian.Uint64(in32[8:])
			v2 := binary.LittleEndian.Uint64(in32[16:])
			v3 := binary.LittleEndian.Uint64(in32[24:])

			n0 := ((s0 & maskLo15) + (v0 & maskLo15)) ^ ((s0 ^ v0) & maskHi1)
			n1 := ((s1 & maskLo15) + (v1 & maskLo15)) ^ ((s1 ^ v1) & maskHi1)
			n2 := ((s2 & maskLo15) + (v2 & maskLo15)) ^ ((s2 ^ v2) & maskHi1)
			n3 := ((s3 & maskLo15) + (v3 & maskLo15)) ^ ((s3 ^ v3) & maskHi1)

			ovf |= (^(s0 ^ v0) & (s0 ^ n0)) |
				(^(s1 ^ v1) & (s1 ^ n1)) |
				(^(s2 ^ v2) & (s2 ^ n2)) |
				(^(s3 ^ v3) & (s3 ^ n3))
			s0, s1, s2, s3 = n0, n1, n2, n3
		}
		if ovf&maskHi1 != 0 {
			mixS16ScalarExact32(d[off:off+32], inputs, off)
			continue
		}
		d32 := d[off : off+32]
		binary.LittleEndian.PutUint64(d32, s0)
		binary.LittleEndian.PutUint64(d32[8:], s1)
		binary.LittleEndian.PutUint64(d32[16:], s2)
		binary.LittleEndian.PutUint64(d32[24:], s3)
	}
	if off < n {
		mixS16ScalarRange(dst, inputs, off, n)
	}
}

func mixS16ScalarExact32(d32 []byte, inputs [][]byte, off int) {
	var a0, a1, a2, a3, a4, a5, a6, a7 int32
	var b0, b1, b2, b3, b4, b5, b6, b7 int32
	for _, in := range inputs {
		in32 := in[off : off+32]
		v0 := binary.LittleEndian.Uint64(in32)
		v1 := binary.LittleEndian.Uint64(in32[8:])
		v2 := binary.LittleEndian.Uint64(in32[16:])
		v3 := binary.LittleEndian.Uint64(in32[24:])
		a0 += int32(int16(v0))
		a1 += int32(int16(v0 >> 16))
		a2 += int32(int16(v0 >> 32))
		a3 += int32(int16(v0 >> 48))
		a4 += int32(int16(v1))
		a5 += int32(int16(v1 >> 16))
		a6 += int32(int16(v1 >> 32))
		a7 += int32(int16(v1 >> 48))
		b0 += int32(int16(v2))
		b1 += int32(int16(v2 >> 16))
		b2 += int32(int16(v2 >> 32))
		b3 += int32(int16(v2 >> 48))
		b4 += int32(int16(v3))
		b5 += int32(int16(v3 >> 16))
		b6 += int32(int16(v3 >> 32))
		b7 += int32(int16(v3 >> 48))
	}
	binary.LittleEndian.PutUint64(d32, packS16x4(a0, a1, a2, a3))
	binary.LittleEndian.PutUint64(d32[8:], packS16x4(a4, a5, a6, a7))
	binary.LittleEndian.PutUint64(d32[16:], packS16x4(b0, b1, b2, b3))
	binary.LittleEndian.PutUint64(d32[24:], packS16x4(b4, b5, b6, b7))
}

func mixS16Scalar2(dst, a, b []byte, n int) {
	const (
		maskLo15 = 0x7fff7fff7fff7fff
		maskHi1  = 0x8000800080008000
	)
	d, a, b := dst[:n], a[:n], b[:n]
	off := 0
	for ; off+32 <= n; off += 32 {
		a32, b32, d32 := a[off:off+32], b[off:off+32], d[off:off+32]
		va0, vb0 := binary.LittleEndian.Uint64(a32), binary.LittleEndian.Uint64(b32)
		va1, vb1 := binary.LittleEndian.Uint64(a32[8:]), binary.LittleEndian.Uint64(b32[8:])
		va2, vb2 := binary.LittleEndian.Uint64(a32[16:]), binary.LittleEndian.Uint64(b32[16:])
		va3, vb3 := binary.LittleEndian.Uint64(a32[24:]), binary.LittleEndian.Uint64(b32[24:])

		s0 := ((va0 & maskLo15) + (vb0 & maskLo15)) ^ ((va0 ^ vb0) & maskHi1)
		s1 := ((va1 & maskLo15) + (vb1 & maskLo15)) ^ ((va1 ^ vb1) & maskHi1)
		s2 := ((va2 & maskLo15) + (vb2 & maskLo15)) ^ ((va2 ^ vb2) & maskHi1)
		s3 := ((va3 & maskLo15) + (vb3 & maskLo15)) ^ ((va3 ^ vb3) & maskHi1)

		ovf := (^(va0 ^ vb0) & (va0 ^ s0)) |
			(^(va1 ^ vb1) & (va1 ^ s1)) |
			(^(va2 ^ vb2) & (va2 ^ s2)) |
			(^(va3 ^ vb3) & (va3 ^ s3))
		if ovf&maskHi1 != 0 {
			s0 = clampS16Add2U64(va0, vb0)
			s1 = clampS16Add2U64(va1, vb1)
			s2 = clampS16Add2U64(va2, vb2)
			s3 = clampS16Add2U64(va3, vb3)
		}
		binary.LittleEndian.PutUint64(d32, s0)
		binary.LittleEndian.PutUint64(d32[8:], s1)
		binary.LittleEndian.PutUint64(d32[16:], s2)
		binary.LittleEndian.PutUint64(d32[24:], s3)
	}
	for ; off+8 <= n; off += 8 {
		va := binary.LittleEndian.Uint64(a[off:])
		vb := binary.LittleEndian.Uint64(b[off:])
		s := ((va & maskLo15) + (vb & maskLo15)) ^ ((va ^ vb) & maskHi1)
		if (^(va ^ vb) & (va ^ s) & maskHi1) != 0 {
			s = clampS16Add2U64(va, vb)
		}
		binary.LittleEndian.PutUint64(d[off:], s)
	}
	for ; off < n; off += 2 {
		s := int32(int16(binary.LittleEndian.Uint16(a[off:]))) + int32(int16(binary.LittleEndian.Uint16(b[off:])))
		binary.LittleEndian.PutUint16(d[off:], uint16(clampS16(s)))
	}
}

func clampS16Add2U64(va, vb uint64) uint64 {
	return packS16x4(
		int32(int16(va))+int32(int16(vb)),
		int32(int16(va>>16))+int32(int16(vb>>16)),
		int32(int16(va>>32))+int32(int16(vb>>32)),
		int32(int16(va>>48))+int32(int16(vb>>48)),
	)
}

func mixS16Scalar3(dst, a, b, c []byte, n int) {
	const (
		maskLo15 = 0x7fff7fff7fff7fff
		maskHi1  = 0x8000800080008000
	)
	d, a, b, c := dst[:n], a[:n], b[:n], c[:n]
	off := 0
	for ; off+16 <= n; off += 16 {
		a16, b16, c16, d16 := a[off:off+16], b[off:off+16], c[off:off+16], d[off:off+16]
		va0, vb0, vc0 := binary.LittleEndian.Uint64(a16), binary.LittleEndian.Uint64(b16), binary.LittleEndian.Uint64(c16)
		va1, vb1, vc1 := binary.LittleEndian.Uint64(a16[8:]), binary.LittleEndian.Uint64(b16[8:]), binary.LittleEndian.Uint64(c16[8:])

		ab0 := ((va0 & maskLo15) + (vb0 & maskLo15)) ^ ((va0 ^ vb0) & maskHi1)
		s0 := ((ab0 & maskLo15) + (vc0 & maskLo15)) ^ ((ab0 ^ vc0) & maskHi1)
		ab1 := ((va1 & maskLo15) + (vb1 & maskLo15)) ^ ((va1 ^ vb1) & maskHi1)
		s1 := ((ab1 & maskLo15) + (vc1 & maskLo15)) ^ ((ab1 ^ vc1) & maskHi1)

		ovf := (^(va0 ^ vb0) & (va0 ^ ab0)) | (^(ab0 ^ vc0) & (ab0 ^ s0)) |
			(^(va1 ^ vb1) & (va1 ^ ab1)) | (^(ab1 ^ vc1) & (ab1 ^ s1))
		if ovf&maskHi1 != 0 {
			s0 = clampS16Add3U64(va0, vb0, vc0)
			s1 = clampS16Add3U64(va1, vb1, vc1)
		}
		binary.LittleEndian.PutUint64(d16, s0)
		binary.LittleEndian.PutUint64(d16[8:], s1)
	}
	for ; off+8 <= n; off += 8 {
		va := binary.LittleEndian.Uint64(a[off:])
		vb := binary.LittleEndian.Uint64(b[off:])
		vc := binary.LittleEndian.Uint64(c[off:])
		ab := ((va & maskLo15) + (vb & maskLo15)) ^ ((va ^ vb) & maskHi1)
		s := ((ab & maskLo15) + (vc & maskLo15)) ^ ((ab ^ vc) & maskHi1)
		if ((^(va ^ vb) & (va ^ ab)) | (^(ab ^ vc) & (ab ^ s)))&maskHi1 != 0 {
			s = clampS16Add3U64(va, vb, vc)
		}
		binary.LittleEndian.PutUint64(d[off:], s)
	}
	for ; off < n; off += 2 {
		s := int32(int16(binary.LittleEndian.Uint16(a[off:]))) +
			int32(int16(binary.LittleEndian.Uint16(b[off:]))) +
			int32(int16(binary.LittleEndian.Uint16(c[off:])))
		binary.LittleEndian.PutUint16(d[off:], uint16(clampS16(s)))
	}
}

func clampS16Add3U64(va, vb, vc uint64) uint64 {
	return packS16x4(
		int32(int16(va))+int32(int16(vb))+int32(int16(vc)),
		int32(int16(va>>16))+int32(int16(vb>>16))+int32(int16(vc>>16)),
		int32(int16(va>>32))+int32(int16(vb>>32))+int32(int16(vc>>32)),
		int32(int16(va>>48))+int32(int16(vb>>48))+int32(int16(vc>>48)),
	)
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

//go:build arm64

#include "textflag.h"

// mixS16Add2ARM64 performs signed saturating addition for two S16 streams.
// n is a byte count and must be a multiple of 16. The raw WORD below is:
//
//	SQADD V2.8H, V0.8H, V1.8H
//
// Go 1.26's ARM64 assembler does not expose a SQADD mnemonic, so the encoding
// is pinned by TestMixSIMDMatchesScalar.
TEXT ·mixS16Add2ARM64(SB), NOSPLIT|NOFRAME, $0-80
	MOVD dst_base+0(FP), R0
	MOVD a_base+24(FP), R1
	MOVD b_base+48(FP), R2
	MOVD n+72(FP), R3

	CMP $16, R3
	BLT done

loop:
	VLD1.P 16(R1), [V0.B16]
	VLD1.P 16(R2), [V1.B16]
	WORD $0x4e610c02
	VST1.P [V2.B16], 16(R0)
	SUB $16, R3
	CMP $16, R3
	BGE loop

done:
	RET

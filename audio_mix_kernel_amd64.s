//go:build amd64

#include "textflag.h"

// mixS16Add2AMD64 performs signed saturating addition for two S16 streams.
// n is a byte count and must be a multiple of 16.
TEXT ·mixS16Add2AMD64(SB), NOSPLIT|NOFRAME, $0-80
	MOVQ dst_base+0(FP), DI
	MOVQ a_base+24(FP), SI
	MOVQ b_base+48(FP), DX
	MOVQ n+72(FP), CX

	CMPQ CX, $16
	JL done

loop:
	MOVOU (SI), X0
	MOVOU (DX), X1
	PADDSW X1, X0
	MOVOU X0, (DI)
	ADDQ $16, SI
	ADDQ $16, DX
	ADDQ $16, DI
	SUBQ $16, CX
	CMPQ CX, $16
	JGE loop

done:
	RET

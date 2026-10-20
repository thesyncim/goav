#include "textflag.h"

TEXT ·nativeCaptureAddress(SB), NOSPLIT, $0-8
 LEAQ ·nativeCapture(SB), AX
 MOVQ AX, ret+0(FP)
 RET

// System V C ABI: self=DI, selector=SI, output=DX, sample=CX, connection=R8.
TEXT ·nativeCapture(SB), NOSPLIT|NOFRAME, $0
 PUSHQ R12
 PUSHQ R13
 PUSHQ R14
 MOVQ 8(DI), R12
 TESTQ R12, R12
 JZ done
 MOVQ CX, R13
 MOVQ R13, DI
 MOVQ 8(R12), AX
 CALL AX
 MOVQ R13, R14
 XCHGQ R14, 0(R12)
 TESTQ R14, R14
 JZ notify
 MOVQ R14, DI
 MOVQ 16(R12), AX
 CALL AX
 JMP done
notify:
 MOVQ 32(R12), DI
 MOVQ 24(R12), AX
 CALL AX
done:
 POPQ R14
 POPQ R13
 POPQ R12
 RET

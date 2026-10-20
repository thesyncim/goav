#include "textflag.h"

TEXT ·nativeCaptureAddress(SB), NOSPLIT, $0-8
 MOVD $·nativeCapture(SB), R0
 MOVD R0, ret+0(FP)
 RET

// C ABI: self=x0, selector=x1, output=x2, sample=x3, connection=x4.
// No Go calls, TLS access, Go pointers, or Go stack conventions.
TEXT ·nativeCapture(SB), NOSPLIT|NOFRAME, $0
 SUB $48, RSP
 STP (R19, R20), 0(RSP)
 STP (R21, R22), 16(RSP)
 MOVD R30, 32(RSP)
 MOVD 8(R0), R19
 CBZ R19, done
 MOVD R3, R20
 MOVD R20, R0
 MOVD 8(R19), R9
 CALL (R9)
exchange:
 LDAXR (R19), R21
 STLXR R20, (R19), R22
 CBNZ R22, exchange
 CBZ R21, notify
 MOVD R21, R0
 MOVD 16(R19), R9
 CALL (R9)
 B done
notify:
 MOVD 32(R19), R0
 MOVD 24(R19), R9
 CALL (R9)
done:
 MOVD 32(RSP), R30
 LDP 16(RSP), (R21, R22)
 LDP 0(RSP), (R19, R20)
 ADD $48, RSP
 RET

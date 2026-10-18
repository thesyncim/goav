package matroska

// opusPacketDurationNS reports the decoded length of an Opus packet from its
// TOC byte (RFC 6716 §3.1): the frame duration its configuration names times
// its frame count. It reports false for an empty or malformed packet.
func opusPacketDurationNS(packet []byte) (int64, bool) {
	if len(packet) == 0 {
		return 0, false
	}
	toc := packet[0]
	config := int(toc >> 3)
	var frameNS int64
	switch {
	case config < 12: // SILK: 10, 20, 40, 60 ms
		frameNS = [...]int64{10e6, 20e6, 40e6, 60e6}[config%4]
	case config < 16: // Hybrid: 10, 20 ms
		frameNS = [...]int64{10e6, 20e6}[config%2]
	default: // CELT: 2.5, 5, 10, 20 ms
		frameNS = [...]int64{2_500_000, 5e6, 10e6, 20e6}[config%4]
	}
	frames := 1
	switch toc & 3 {
	case 1, 2:
		frames = 2
	case 3:
		if len(packet) < 2 {
			return 0, false
		}
		frames = int(packet[1] & 0x3f)
	}
	if frames == 0 {
		return 0, false
	}
	return frameNS * int64(frames), true
}

// opusDiscardPaddingNS is the trailing time a shortened Opus packet carries
// past its duration: what the reader discards so decoded audio ends where the
// encoder's input did.
func opusDiscardPaddingNS(packet []byte, durationNS int64) int64 {
	natural, ok := opusPacketDurationNS(packet)
	if !ok || durationNS <= 0 || durationNS >= natural {
		return 0
	}
	return natural - durationNS
}

package hls

import "testing"

func entry(header int, child string, payload ...byte) []byte {
	b := make([]byte, header)
	size := 8 + len(payload)
	b = append(b, byte(size>>24), byte(size>>16), byte(size>>8), byte(size))
	b = append(b, child...)
	return append(b, payload...)
}

func TestCodecStrings(t *testing.T) {
	for want, got := range map[string]string{
		"avc1.64001f":   codecString("avc1", entry(78, "avcC", 1, 0x64, 0x00, 0x1f, 0xff)),
		"av01.0.08M.08": codecString("av01", entry(78, "av1C", 0x81, 0x08, 0x0c, 0x00)),
		"av01.2.13H.10": codecString("av01", entry(78, "av1C", 0x81, 0x4d, 0xc0, 0x00)),
		"vp09.02.31.10": codecString("vp09", entry(78, "vpcC", 1, 0, 0, 0, 2, 31, 0xa0)),
		"mp4a.40.5": codecString("mp4a", entry(28, "esds", 0, 0, 0, 0,
			0x03, 0x19, 0, 0, 0, // ES descriptor: id, flags
			0x04, 0x11, 0x40, 0x15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // decoder config
			0x05, 0x02, 0x2b, 0x10)), // AudioSpecificConfig: object type 5
		"Opus": codecString("Opus", nil),
	} {
		if got != want {
			t.Errorf("codec string = %q, want %q", got, want)
		}
	}
}

package matroska

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/thesyncim/goav/container/ebml"
)

func zlibTestPayload(label string) []byte {
	prefix := []byte("zlib content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*16)
	for range 16 {
		payload = append(payload, prefix...)
	}
	return payload
}

func lzoContentEncodings(t testing.TB) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoLZO1X, nil))
		}),
	)
}

func lzoTestPayload(label string) []byte {
	prefix := []byte("lzo1x content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*16)
	for range 16 {
		payload = append(payload, prefix...)
	}
	return payload
}

func chainedContentPayload(label string) []byte {
	prefix := []byte("ordered content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*16)
	for range 16 {
		payload = append(payload, prefix...)
	}
	return payload
}

func withContentEncodingHeader(header, data []byte) []byte {
	out := make([]byte, 0, len(header)+len(data))
	out = append(out, header...)
	out = append(out, data...)
	return out
}

func chainedHeaderZlibContentEncodings(header []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
		},
	}
}

func chainedHeaderLZOContentEncodings(header []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoLZO1X},
		},
	}
}

func chainedHeaderBzlibBlockContentEncodings(header []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoBzlib},
		},
	}
}

func chainedHeaderBzlibContentEncodings(t testing.TB, header []byte) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			if err := w.WriteUInt(idContentEncodingOrd, 0); err != nil {
				return err
			}
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoHeaderStripping, header))
		}),
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			if err := w.WriteUInt(idContentEncodingOrd, 1); err != nil {
				return err
			}
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoBzlib, nil))
		}),
	)
}

func bzlibContentEncodings(t testing.TB) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoBzlib, nil))
		}),
	)
}

func bzlibTestPayload(label string) []byte {
	prefix := []byte("bzip2 content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*8)
	for range 8 {
		payload = append(payload, prefix...)
	}
	return payload
}

func bzlibCompressedPayload(t testing.TB, label string) []byte {
	t.Helper()
	fixtures := map[string]string{
		"simple block": "425a6839314159265359c57d93a80000339980400010103eafcc302000902984d34069881554d3d4068f51934dc662e1a0e45854762c207236102c2074207a205435171518161d0c0b8f87c282824486e1a0918141b091228207a2a3f177245385090c57d93a80",
		"laced first":  "425a6839314159265359e931c9840000339980400010103fa5dc302000902984d340698815553f54dea6937a8d344f481e0d848fa286048d05c6048b0dc6050b8d0585c48dc205850c8c0919143c10287438091610351fc703210351d0c8f45dc914e14243a4c72610",
		"laced second": "425a6839314159265359c10c71800000339980400010103ea5cc3020009029a6462626205553351a34c9ea34a0f440a0e45c48a8644881617123c18141020587a24586438181a1b8d0ec5c68545448ec6db8b0702e30282064687e2ee48a70a1218218e300",
	}
	value, ok := fixtures[label]
	if !ok {
		t.Fatalf("missing bzlib fixture %q", label)
	}
	out, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func aesCTRContentEncryptionKeyID() []byte {
	return []byte("aes-ctr-key")
}

func aesCTRContentEncryptionKeys(keyID []byte) []ContentEncryptionKey {
	return []ContentEncryptionKey{{
		KeyID: append([]byte(nil), keyID...),
		Key: []byte{
			0x10, 0x11, 0x12, 0x13,
			0x14, 0x15, 0x16, 0x17,
			0x18, 0x19, 0x1a, 0x1b,
			0x1c, 0x1d, 0x1e, 0x1f,
		},
	}}
}

func aesCTRContentEncryptionInitialIV() []byte {
	return []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
}

func aesCTRContentEncryptionTrackEncodings(keyID []byte) []ContentEncoding {
	return []ContentEncoding{{
		Type:          ContentEncodingTypeEncryption,
		EncryptionSet: true,
		Encryption: ContentEncryption{
			Algorithm:      ContentEncAlgoAES,
			KeyID:          append([]byte(nil), keyID...),
			AESSettingsSet: true,
			AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCTR},
		},
	}}
}

func chainedHeaderCompressionAESCTRContentEncodings(header []byte, algorithm uint64, keyID []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: algorithm},
		},
		{
			Order:         2,
			Type:          ContentEncodingTypeEncryption,
			EncryptionSet: true,
			Encryption: ContentEncryption{
				Algorithm:      ContentEncAlgoAES,
				KeyID:          append([]byte(nil), keyID...),
				AESSettingsSet: true,
				AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCTR},
			},
		},
	}
}

func aesCTRContentEncryptionContentEncodings(t testing.TB, keyID []byte) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			if err := w.WriteUInt(idContentEncodingType, ContentEncodingTypeEncryption); err != nil {
				return err
			}
			return w.WriteElement(idContentEncryption, contentEncryptionPayload(t, ContentEncAlgoAES, keyID, ContentEncAESCipherModeCTR))
		}),
	)
}

func encryptedTestPayload(label string) []byte {
	prefix := []byte("aes ctr content encryption test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*12)
	for range 12 {
		payload = append(payload, prefix...)
	}
	return payload
}

func partitionedAESCTRContentEncryptionFrame(signal byte, iv []byte, partitions []uint32, payload []byte) []byte {
	frame := append([]byte{signal}, iv...)
	frame = append(frame, byte(len(partitions)))
	var offset [4]byte
	for i := range partitions {
		binary.BigEndian.PutUint32(offset[:], partitions[i])
		frame = append(frame, offset[:]...)
	}
	return append(frame, payload...)
}

package matroska

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"io"
)

const (
	contentEncryptionSignalEncrypted   byte = 0x01
	contentEncryptionSignalPartitioned byte = 0x02
	contentEncryptionSignalExtension   byte = 0x80
	contentEncryptionIVSize                 = 8
)

func cloneContentEncryptionKeys(keys []ContentEncryptionKey) []ContentEncryptionKey {
	if len(keys) == 0 {
		return nil
	}
	out := make([]ContentEncryptionKey, len(keys))
	for i := range keys {
		if keys[i].KeyID != nil {
			out[i].KeyID = append([]byte(nil), keys[i].KeyID...)
		}
		if keys[i].Key != nil {
			out[i].Key = append([]byte(nil), keys[i].Key...)
		}
	}
	return out
}

func validateContentEncryptionOptions(keys []ContentEncryptionKey, initialIV []byte) error {
	if len(initialIV) != 0 && len(initialIV) != contentEncryptionIVSize {
		return ErrInvalidData
	}
	for i := range keys {
		if !validAESKeySize(len(keys[i].Key)) {
			return ErrInvalidData
		}
		for j := i + 1; j < len(keys); j++ {
			if bytes.Equal(keys[i].KeyID, keys[j].KeyID) {
				return ErrInvalidData
			}
		}
	}
	return nil
}

func validAESKeySize(size int) bool {
	return size == 16 || size == 24 || size == 32
}

func contentEncryptionKey(keys []ContentEncryptionKey, keyID []byte) ([]byte, error) {
	for i := range keys {
		if bytes.Equal(keys[i].KeyID, keyID) {
			if !validAESKeySize(len(keys[i].Key)) {
				return nil, ErrInvalidData
			}
			return keys[i].Key, nil
		}
	}
	return nil, ErrUnsupportedContentEncoding
}

func (m *Muxer) encryptBlockPayload(encoding blockContentEncodingInfo, payload []byte, partitions []uint32) ([]byte, error) {
	if err := validateContentEncryptionPartitions(partitions, len(payload)); err != nil {
		return nil, err
	}
	key, err := contentEncryptionKey(m.options.ContentEncryptionKeys, encoding.encryption.KeyID)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalidData
	}
	if err := m.ensureContentEncryptionIV(); err != nil {
		return nil, err
	}

	var counter [aes.BlockSize]byte
	copy(counter[:contentEncryptionIVSize], m.contentEncryptionIV[:])
	stream := cipher.NewCTR(block, counter[:])

	m.encryptionPayload.Reset()
	headerSize := 1 + contentEncryptionIVSize
	if len(partitions) != 0 {
		headerSize += 1 + 4*len(partitions)
	}
	m.encryptionPayload.Grow(headerSize + len(payload))
	signal := contentEncryptionSignalEncrypted
	if len(partitions) != 0 {
		signal |= contentEncryptionSignalPartitioned
	}
	if err := m.encryptionPayload.WriteByte(signal); err != nil {
		return nil, err
	}
	if _, err := m.encryptionPayload.Write(m.contentEncryptionIV[:]); err != nil {
		return nil, err
	}
	if len(partitions) != 0 {
		if err := m.encryptionPayload.WriteByte(byte(len(partitions))); err != nil {
			return nil, err
		}
		var offsetScratch [4]byte
		for i := range partitions {
			binary.BigEndian.PutUint32(offsetScratch[:], partitions[i])
			if _, err := m.encryptionPayload.Write(offsetScratch[:]); err != nil {
				return nil, err
			}
		}
	}
	payloadOffset := m.encryptionPayload.Len()
	if _, err := m.encryptionPayload.Write(payload); err != nil {
		return nil, err
	}
	out := m.encryptionPayload.Bytes()
	if len(partitions) == 0 {
		stream.XORKeyStream(out[payloadOffset:], out[payloadOffset:])
	} else {
		xorContentEncryptionPartitions(stream, out[payloadOffset:], partitions)
	}
	incrementContentEncryptionIV(&m.contentEncryptionIV)
	return out, nil
}

func (m *Muxer) ensureContentEncryptionIV() error {
	if m.contentEncryptionIVSet {
		return nil
	}
	if len(m.options.ContentEncryptionInitialIV) == contentEncryptionIVSize {
		copy(m.contentEncryptionIV[:], m.options.ContentEncryptionInitialIV)
		m.contentEncryptionIVSet = true
		return nil
	}
	if _, err := io.ReadFull(rand.Reader, m.contentEncryptionIV[:]); err != nil {
		return err
	}
	m.contentEncryptionIVSet = true
	return nil
}

func incrementContentEncryptionIV(iv *[contentEncryptionIVSize]byte) {
	for i := len(iv) - 1; i >= 0; i-- {
		iv[i]++
		if iv[i] != 0 {
			return
		}
	}
}

func validateContentEncryptionPartitions(partitions []uint32, payloadSize int) error {
	if len(partitions) == 0 {
		return nil
	}
	if len(partitions) > 255 || payloadSize < 0 {
		return ErrInvalidData
	}
	previous := uint32(0)
	for i := range partitions {
		offset := partitions[i]
		if uint64(offset) > uint64(payloadSize) {
			return ErrInvalidData
		}
		if i > 0 && offset <= previous {
			return ErrInvalidData
		}
		previous = offset
	}
	return nil
}

func (d *Demuxer) decryptBlockPayload(encoding blockContentEncodingInfo, frame []byte) ([]byte, error) {
	if len(frame) == 0 {
		return nil, ErrInvalidData
	}
	signal := frame[0]
	if signal&contentEncryptionSignalExtension != 0 {
		return nil, ErrUnsupportedContentEncoding
	}
	payload := frame[1:]
	if signal&contentEncryptionSignalPartitioned != 0 && signal&contentEncryptionSignalEncrypted == 0 {
		return nil, ErrInvalidData
	}
	if signal&contentEncryptionSignalEncrypted == 0 {
		return payload, nil
	}
	if len(payload) < contentEncryptionIVSize {
		return nil, ErrInvalidData
	}
	iv := payload[:contentEncryptionIVSize]
	payload = payload[contentEncryptionIVSize:]
	partitions := d.contentPartitions[:0]
	if signal&contentEncryptionSignalPartitioned != 0 {
		if len(payload) == 0 {
			return nil, ErrInvalidData
		}
		count := int(payload[0])
		payload = payload[1:]
		if len(payload) < 4*count {
			return nil, ErrInvalidData
		}
		if cap(partitions) < count {
			partitions = make([]uint32, 0, count)
		}
		partitions = partitions[:count]
		for i := range partitions {
			partitions[i] = binary.BigEndian.Uint32(payload[:4])
			payload = payload[4:]
		}
		if err := validateContentEncryptionPartitions(partitions, len(payload)); err != nil {
			return nil, err
		}
		d.contentPartitions = partitions
	}
	key, err := contentEncryptionKey(d.options.ContentEncryptionKeys, encoding.encryption.KeyID)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalidData
	}
	var counter [aes.BlockSize]byte
	copy(counter[:contentEncryptionIVSize], iv)
	stream := cipher.NewCTR(block, counter[:])
	if len(partitions) == 0 {
		stream.XORKeyStream(payload, payload)
	} else {
		xorContentEncryptionPartitions(stream, payload, partitions)
	}
	return payload, nil
}

func xorContentEncryptionPartitions(stream cipher.Stream, payload []byte, partitions []uint32) {
	encrypted := false
	start := 0
	for i := range partitions {
		end := int(partitions[i])
		if encrypted {
			stream.XORKeyStream(payload[start:end], payload[start:end])
		}
		encrypted = !encrypted
		start = end
	}
	if encrypted {
		stream.XORKeyStream(payload[start:], payload[start:])
	}
}

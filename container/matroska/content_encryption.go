package matroska

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
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

func (m *Muxer) encryptBlockPayload(encoding blockContentEncodingInfo, payload []byte) ([]byte, error) {
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
	m.encryptionPayload.Grow(1 + contentEncryptionIVSize + len(payload))
	if err := m.encryptionPayload.WriteByte(contentEncryptionSignalEncrypted); err != nil {
		return nil, err
	}
	if _, err := m.encryptionPayload.Write(m.contentEncryptionIV[:]); err != nil {
		return nil, err
	}
	payloadOffset := m.encryptionPayload.Len()
	if _, err := m.encryptionPayload.Write(payload); err != nil {
		return nil, err
	}
	out := m.encryptionPayload.Bytes()
	stream.XORKeyStream(out[payloadOffset:], out[payloadOffset:])
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

func (d *Demuxer) decryptBlockPayload(encoding blockContentEncodingInfo, frame []byte) ([]byte, error) {
	if len(frame) == 0 {
		return nil, ErrInvalidData
	}
	signal := frame[0]
	if signal&contentEncryptionSignalExtension != 0 || signal&contentEncryptionSignalPartitioned != 0 {
		return nil, ErrUnsupportedContentEncoding
	}
	payload := frame[1:]
	if signal&contentEncryptionSignalEncrypted == 0 {
		return payload, nil
	}
	if len(payload) < contentEncryptionIVSize {
		return nil, ErrInvalidData
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
	copy(counter[:contentEncryptionIVSize], payload[:contentEncryptionIVSize])
	payload = payload[contentEncryptionIVSize:]
	stream := cipher.NewCTR(block, counter[:])
	stream.XORKeyStream(payload, payload)
	return payload, nil
}

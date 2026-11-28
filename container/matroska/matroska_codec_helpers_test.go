package matroska

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func equalInt64s(left []int64, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func equalBlockAdditions(left []BlockAddition, right []BlockAddition) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].ID != right[i].ID || !bytes.Equal(left[i].Data, right[i].Data) {
			return false
		}
	}
	return true
}

func equalBlockAdditionMapping(left BlockAdditionMapping, right BlockAdditionMapping) bool {
	return left.IDValue == right.IDValue &&
		left.Name == right.Name &&
		left.Type == right.Type &&
		bytes.Equal(left.ExtraData, right.ExtraData)
}

func equalTrackTranslate(left TrackTranslate, right TrackTranslate) bool {
	return left.Codec == right.Codec &&
		bytes.Equal(left.TrackID, right.TrackID) &&
		reflect.DeepEqual(left.EditionUIDs, right.EditionUIDs)
}

func equalCuePoint(left CuePoint, right CuePoint) bool {
	if left.TrackID != right.TrackID ||
		left.TimeNS != right.TimeNS ||
		left.ClusterPosition != right.ClusterPosition ||
		left.RelativePosition != right.RelativePosition ||
		left.RelativePositionSet != right.RelativePositionSet ||
		left.DurationNS != right.DurationNS ||
		left.DurationSet != right.DurationSet ||
		left.BlockNumber != right.BlockNumber ||
		left.BlockNumberSet != right.BlockNumberSet ||
		left.CodecStatePosition != right.CodecStatePosition ||
		left.CodecStateSet != right.CodecStateSet ||
		!equalCueReferences(left.References, right.References) ||
		len(left.Positions) != len(right.Positions) {
		return false
	}
	for i := range left.Positions {
		if !equalCueTrackPosition(left.Positions[i], right.Positions[i]) {
			return false
		}
	}
	return true
}

func equalCueTrackPosition(left CueTrackPosition, right CueTrackPosition) bool {
	return left.TrackID == right.TrackID &&
		left.ClusterPosition == right.ClusterPosition &&
		left.RelativePosition == right.RelativePosition &&
		left.RelativePositionSet == right.RelativePositionSet &&
		left.DurationNS == right.DurationNS &&
		left.DurationSet == right.DurationSet &&
		left.BlockNumber == right.BlockNumber &&
		left.BlockNumberSet == right.BlockNumberSet &&
		left.CodecStatePosition == right.CodecStatePosition &&
		left.CodecStateSet == right.CodecStateSet &&
		equalCueReferences(left.References, right.References)
}

func equalCueReferences(left []CueReference, right []CueReference) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func requirePacketTrackIndex(t *testing.T, demuxer *Demuxer, trackID uint32, times []int64) {
	t.Helper()
	entries, ok := demuxer.packetIndexEntriesForTrack(trackID)
	if !ok {
		t.Fatalf("packet track index for track %d was not built", trackID)
	}
	if len(entries) != len(times) {
		t.Fatalf("packet track index for track %d has %d entries, want %d", trackID, len(entries), len(times))
	}
	for i, entryIndex := range entries {
		if entryIndex < 0 || entryIndex >= len(demuxer.packetIndex) {
			t.Fatalf("packet track index %d for track %d points outside packet index: %d", i, trackID, entryIndex)
		}
		entry := demuxer.packetIndex[entryIndex]
		if entry.TrackID != trackID || entry.TimeNS != times[i] {
			t.Fatalf("packet track index %d for track %d = %+v, want time %d", i, trackID, entry, times[i])
		}
	}
}

func expectedOpusHead(channels int, sampleRate int) []byte {
	if channels == 0 {
		channels = 2
	}
	if sampleRate == 0 {
		sampleRate = 48000
	}
	header := make([]byte, 19)
	copy(header, "OpusHead")
	header[8] = 1
	header[9] = byte(channels)
	binary.LittleEndian.PutUint32(header[12:16], uint32(sampleRate))
	return header
}

func expectedOpusHeadWithPreSkip(channels int, sampleRate int, preSkip int) []byte {
	header := expectedOpusHead(channels, sampleRate)
	binary.LittleEndian.PutUint16(header[10:12], uint16(preSkip))
	return header
}

func expectedMSACMWaveFormat(codec Codec, channels int, sampleRate int) []byte {
	tag := waveFormatMuLaw
	if codec == CodecPCMA {
		tag = waveFormatALaw
	}
	return msACMWaveFormatBytes(tag, channels, sampleRate, uint32(sampleRate*channels), channels, 8, nil)
}

func msACMWaveFormatBytes(tag uint16, channels int, sampleRate int, avgBytesPerSec uint32, blockAlign int, bitsPerSample int, extra []byte) []byte {
	private := make([]byte, waveFormatExBaseSize+len(extra))
	binary.LittleEndian.PutUint16(private[0:2], tag)
	binary.LittleEndian.PutUint16(private[2:4], uint16(channels))
	binary.LittleEndian.PutUint32(private[4:8], uint32(sampleRate))
	binary.LittleEndian.PutUint32(private[8:12], avgBytesPerSec)
	binary.LittleEndian.PutUint16(private[12:14], uint16(blockAlign))
	binary.LittleEndian.PutUint16(private[14:16], uint16(bitsPerSample))
	binary.LittleEndian.PutUint16(private[16:18], uint16(len(extra)))
	copy(private[waveFormatExBaseSize:], extra)
	return private
}

func validFLACCodecPrivate() []byte {
	private := make([]byte, 4+4+34)
	copy(private, "fLaC")
	private[4] = 0x80
	private[7] = 34
	streamInfo := private[8:]
	binary.BigEndian.PutUint16(streamInfo[0:2], 4096)
	binary.BigEndian.PutUint16(streamInfo[2:4], 4096)
	writeFLACStreamInfoAudio(streamInfo, 48000, 2, 16)
	return private
}

func flacCodecPrivateWithHeaderByte(offset int, value byte) []byte {
	private := validFLACCodecPrivate()
	private[offset] = value
	return private
}

func flacCodecPrivateWithStreamInfoBytes(offset int, values ...byte) []byte {
	private := validFLACCodecPrivate()
	copy(private[8+offset:], values)
	return private
}

func writeFLACStreamInfoAudio(streamInfo []byte, sampleRate int, channels int, bitsPerSample int) {
	channelsMinusOne := channels - 1
	bitsMinusOne := bitsPerSample - 1
	streamInfo[10] = byte(sampleRate >> 12)
	streamInfo[11] = byte(sampleRate >> 4)
	streamInfo[12] = byte((sampleRate&0x0f)<<4 | (channelsMinusOne&0x07)<<1 | (bitsMinusOne>>4)&0x01)
	streamInfo[13] = byte(bitsMinusOne << 4)
}

func makeAACAudioSpecificConfig(sampleRate int, channels int) []byte {
	index := aacSamplingFrequencyIndex(sampleRate)
	if index < 0 || channels < 1 || channels > 7 {
		panic("invalid AAC test config")
	}
	channelConfig := channels
	if channels == 8 {
		channelConfig = 7
	}
	value := uint16(2<<11 | index<<7 | channelConfig<<3)
	return []byte{byte(value >> 8), byte(value)}
}

func aacSamplingFrequencyIndex(sampleRate int) int {
	for i, rate := range aacSamplingFrequencies {
		if rate == sampleRate {
			return i
		}
	}
	return -1
}

func validVorbisCodecPrivate() []byte {
	identification := make([]byte, 30)
	identification[0] = 1
	copy(identification[1:], "vorbis")
	identification[11] = 2
	binary.LittleEndian.PutUint32(identification[12:16], 48000)
	identification[28] = 0xb6
	identification[29] = 1
	comment := []byte{3, 'v', 'o', 'r', 'b', 'i', 's'}
	setup := []byte{5, 'v', 'o', 'r', 'b', 'i', 's', 0}
	return vorbisCodecPrivateWithHeaders(identification, comment, setup)
}

func vorbisCodecPrivateWithIdentificationByte(offset int, value byte) []byte {
	private := validVorbisCodecPrivate()
	idStart := 3
	private[idStart+offset] = value
	return private
}

func vorbisCodecPrivateWithIdentificationBytes(offset int, values ...byte) []byte {
	private := validVorbisCodecPrivate()
	idStart := 3
	copy(private[idStart+offset:], values)
	return private
}

func vorbisCodecPrivateWithHeaders(headers ...[]byte) []byte {
	if len(headers) == 0 {
		return nil
	}
	private := []byte{byte(len(headers) - 1)}
	for i := 0; i < len(headers)-1; i++ {
		size := len(headers[i])
		for size >= 255 {
			private = append(private, 255)
			size -= 255
		}
		private = append(private, byte(size))
	}
	for i := range headers {
		private = append(private, headers[i]...)
	}
	return private
}

func av1CodecConfig() []byte {
	private := []byte{0x81, 0x05, 0x10, 0x00}
	return append(private, av1SequenceHeaderOBU()...)
}

func av1CodecConfigWithByte(index int, value byte) []byte {
	private := av1CodecConfig()
	private[index] = value
	return private
}

func av1CodecConfigWithPrefixOBU(obu []byte) []byte {
	private := []byte{0x81, 0x05, 0x10, 0x00}
	private = append(private, obu...)
	private = append(private, av1SequenceHeaderOBU()...)
	return private
}

func av1SequenceHeaderOBU() []byte {
	return []byte{0x0a, 0x06, 0x19, 0x5d, 0xc3, 0xc3, 0xda, 0x44}
}

func av1TemporalDelimiterOBU() []byte {
	return []byte{0x12, 0x00}
}

func h264AVCDecoderConfig() []byte {
	sps := h264SPS()
	pps := h264PPS()
	private := make([]byte, 0, 11+len(sps)+len(pps))
	private = append(private, 0x01, sps[1], sps[2], sps[3], 0xff, 0xe1)
	private = binary.BigEndian.AppendUint16(private, uint16(len(sps)))
	private = append(private, sps...)
	private = append(private, 0x01)
	private = binary.BigEndian.AppendUint16(private, uint16(len(pps)))
	private = append(private, pps...)
	return private
}

func h264SPS() []byte {
	return []byte{0x67, 0x42, 0x00, 0x0a, 0xf8, 0x41, 0xa2}
}

func h264PPS() []byte {
	return []byte{0x68, 0xce, 0x0f, 0x2c, 0x80}
}

func h264AVCDecoderConfigWithLengthSize(lengthSize int) []byte {
	private := h264AVCDecoderConfig()
	private[4] = 0xfc | byte(lengthSize-1)
	return private
}

func h264AVCDecoderConfigWithByte(index int, value byte) []byte {
	private := h264AVCDecoderConfig()
	private[index] = value
	return private
}

func h264AnnexBAccessUnit() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x99, 0x00, 0x00, 0x00, 0x01, 0x41, 0x9a}
}

func h264AnnexBParameterAccessUnit() []byte {
	var data []byte
	data = append(data, h264StartCode[:]...)
	data = append(data, h264SPS()...)
	data = append(data, h264StartCode[:]...)
	data = append(data, h264PPS()...)
	data = append(data, h264StartCode[:]...)
	data = append(data, 0x65, 0x88, 0x99)
	return data
}

func h264AnnexBInterFrame() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x41, 0xab, 0xcd}
}

func h264AVCSampleWithLengthSize2() []byte {
	return []byte{0x00, 0x03, 0x65, 0x88, 0x99, 0x00, 0x02, 0x41, 0x9a}
}

func h264AVCSampleWithLengthSize1() []byte {
	return []byte{0x03, 0x65, 0x88, 0x99, 0x02, 0x41, 0x9a}
}

func h264AVCSampleWithParameterSetsLengthSize4() []byte {
	var data []byte
	for _, nalu := range [][]byte{h264SPS(), h264PPS(), []byte{0x65, 0x88, 0x99}} {
		data = binary.BigEndian.AppendUint32(data, uint32(len(nalu)))
		data = append(data, nalu...)
	}
	return data
}

func h264AVCInterFrameWithLengthSize2() []byte {
	return []byte{0x00, 0x03, 0x41, 0xab, 0xcd}
}

func h265HEVCDecoderConfig() []byte {
	vps := h265VPS()
	sps := h265SPS()
	pps := h265PPS()
	private := []byte{
		0x01,
		0x01,
		0x60, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x78,
		0xf0, 0x00,
		0xfc,
		0xfd,
		0xf8,
		0xf8,
		0x00, 0x00,
		0x0f,
		0x03,
	}
	private = appendHEVCConfigArray(private, h265NALUVPS, vps)
	private = appendHEVCConfigArray(private, h265NALUSPS, sps)
	private = appendHEVCConfigArray(private, h265NALUPPS, pps)
	return private
}

func appendHEVCConfigArray(private []byte, naluType int, nalu []byte) []byte {
	private = append(private, 0x80|byte(naluType))
	private = binary.BigEndian.AppendUint16(private, 1)
	private = binary.BigEndian.AppendUint16(private, uint16(len(nalu)))
	return append(private, nalu...)
}

func h265HEVCDecoderConfigWithLengthSize(lengthSize int) []byte {
	private := h265HEVCDecoderConfig()
	private[21] = (private[21] & 0xfc) | byte(lengthSize-1)
	return private
}

func h265HEVCDecoderConfigWithByte(index int, value byte) []byte {
	private := h265HEVCDecoderConfig()
	private[index] = value
	return private
}

func h265VPS() []byte {
	return []byte{0x40, 0x01, 0x0c, 0x01, 0xff}
}

func h265SPS() []byte {
	return []byte{
		0x42, 0x01,
		0x01,
		0x01,
		0x60, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x78,
	}
}

func h265PPS() []byte {
	return []byte{0x44, 0x01, 0xc0}
}

func h265AnnexBAccessUnit() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x26, 0x01, 0x88, 0x99, 0x00, 0x00, 0x00, 0x01, 0x02, 0x01, 0xab}
}

func h265AnnexBParameterAccessUnit() []byte {
	var data []byte
	for _, nalu := range [][]byte{h265VPS(), h265SPS(), h265PPS(), []byte{0x26, 0x01, 0x88, 0x99}} {
		data = append(data, h264StartCode[:]...)
		data = append(data, nalu...)
	}
	return data
}

func h265AnnexBInterFrame() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x02, 0x01, 0xcd}
}

func h265LengthPrefixedSample2() []byte {
	return []byte{0x00, 0x04, 0x26, 0x01, 0x88, 0x99, 0x00, 0x03, 0x02, 0x01, 0xab}
}

func h265LengthPrefixedInterFrame2() []byte {
	return []byte{0x00, 0x03, 0x02, 0x01, 0xcd}
}

func h265LengthPrefixedParameterSample4() []byte {
	var data []byte
	for _, nalu := range [][]byte{h265VPS(), h265SPS(), h265PPS(), []byte{0x26, 0x01, 0x88, 0x99}} {
		data = binary.BigEndian.AppendUint32(data, uint32(len(nalu)))
		data = append(data, nalu...)
	}
	return data
}

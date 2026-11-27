package matroska

import (
	"bytes"
	"compress/zlib"
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

func scaledReferenceBlockTicks(timeNS int64, scaleNS int64, track Track) int64 {
	ticks, _ := checkedScaledReferenceBlockTicks(timeNS, scaleNS, track)
	return ticks
}

func checkedScaledReferenceBlockTicks(timeNS int64, scaleNS int64, track Track) (int64, error) {
	unit, err := trackTickUnitNS(scaleNS, track)
	if err != nil {
		return 0, err
	}
	value := math.Round(float64(timeNS) / unit)
	if math.IsNaN(value) || math.IsInf(value, 0) || value < float64(math.MinInt64) || value > float64(math.MaxInt64) {
		return 0, ErrInvalidData
	}
	return int64(value), nil
}

func scaleTrackTicksNS(ticks int64, scaleNS int64, track Track) (int64, error) {
	unit, err := trackTickUnitNS(scaleNS, track)
	if err != nil {
		return 0, err
	}
	value := math.Round(float64(ticks) * unit)
	if math.IsNaN(value) || math.IsInf(value, 0) || value < float64(math.MinInt64) || value > float64(math.MaxInt64) {
		return 0, ErrInvalidData
	}
	return int64(value), nil
}

func trackBlockTimestampNS(clusterTimecode int64, blockTimecode int64, scaleNS int64, track Track) (int64, error) {
	if scaleNS <= 0 {
		return 0, ErrInvalidData
	}
	scale := trackTimestampScale(track)
	if scale <= 0 || math.IsNaN(scale) || math.IsInf(scale, 0) {
		return 0, ErrInvalidData
	}
	value := math.Round((float64(clusterTimecode) + float64(blockTimecode)*scale) * float64(scaleNS))
	if math.IsNaN(value) || math.IsInf(value, 0) || value < float64(math.MinInt64) || value > float64(math.MaxInt64) {
		return 0, ErrInvalidData
	}
	return addInt64(int64(value), track.TrackOffsetNS)
}

func addInt64(left int64, right int64) (int64, error) {
	if (right > 0 && left > math.MaxInt64-right) || (right < 0 && left < math.MinInt64-right) {
		return 0, ErrInvalidData
	}
	return left + right, nil
}

func subInt64(left int64, right int64) (int64, error) {
	if (right > 0 && left < math.MinInt64+right) || (right < 0 && left > math.MaxInt64+right) {
		return 0, ErrInvalidData
	}
	return left - right, nil
}

func lacingFlag(mode LacingMode, frames [][]byte, sizes []int) (byte, error) {
	if len(frames) < 2 || len(frames) > 256 {
		return 0, ErrInvalidData
	}
	switch mode {
	case LacingAuto:
		if equalFrameSizes(frames, sizes) {
			return simpleBlockLacingFixed, nil
		}
		return simpleBlockLacingEBML, nil
	case LacingXiph:
		return simpleBlockLacingXiph, nil
	case LacingEBML:
		return simpleBlockLacingEBML, nil
	case LacingFixed:
		if !equalFrameSizes(frames, sizes) {
			return 0, ErrInvalidData
		}
		return simpleBlockLacingFixed, nil
	default:
		return 0, ErrUnsupportedLacing
	}
}

func equalFrameSizes(frames [][]byte, sizes []int) bool {
	if len(frames) == 0 {
		return true
	}
	size := framePayloadSize(frames, sizes, 0)
	for i := 1; i < len(frames); i++ {
		if framePayloadSize(frames, sizes, i) != size {
			return false
		}
	}
	return true
}

func framePayloadSize(frames [][]byte, sizes []int, index int) int {
	if len(sizes) != 0 {
		return sizes[index]
	}
	return len(frames[index])
}

func (m *Muxer) lacedPacketEndTime(packet LacedPacket) (int64, error) {
	end := packet.TimeNS
	durationNS := packet.FrameDurationNS
	if durationNS <= 0 {
		durationNS = m.defaultDurationNS(packet.TrackID)
	}
	if durationNS <= 0 {
		return end, nil
	}
	if int64(len(packet.Frames)) > math.MaxInt64/durationNS {
		return 0, ErrInvalidData
	}
	totalDuration := int64(len(packet.Frames)) * durationNS
	if packet.TimeNS > math.MaxInt64-totalDuration {
		return 0, ErrInvalidData
	}
	return packet.TimeNS + totalDuration, nil
}

func lacedPacketNeedsBlockGroup(packet LacedPacket) bool {
	return packet.FrameDurationNS > 0 ||
		len(packet.ReferenceBlockTimeNS) != 0 ||
		packet.ReferencePriority != 0 ||
		packet.DiscardPaddingNS != 0 ||
		len(packet.CodecState) != 0 ||
		len(packet.BlockAdditions) != 0
}

func blockPayloadFrameSizes(frames [][]byte, track Track, sizes []int) error {
	if len(sizes) != len(frames) {
		return ErrInvalidData
	}
	for i := range frames {
		size, err := blockPayloadSize(track, frames[i])
		if err != nil {
			return err
		}
		sizes[i] = size
	}
	return nil
}

func blockPayloadsNeedSizing(track Track) bool {
	if codecUsesLengthPrefixedSamples(track) {
		return true
	}
	if len(track.ContentEncodings) == 0 {
		return false
	}
	encoding, err := blockContentEncoding(track)
	return err != nil || encoding.headerSet || encoding.compression != blockContentTransformNone || encoding.encryptionSet
}

func blockPayloadNeedsBuffering(track Track) bool {
	if len(track.ContentEncodings) == 0 {
		return false
	}
	encoding, err := blockContentEncoding(track)
	return err != nil || encoding.compression != blockContentTransformNone || encoding.encryptionSet
}

func blockPayloadSize(track Track, data []byte) (int, error) {
	if len(track.ContentEncodings) != 0 {
		encoding, err := blockContentEncoding(track)
		if err != nil {
			return 0, err
		}
		if encoding.headerSet {
			if !bytes.HasPrefix(data, encoding.headerSettings) {
				return 0, ErrInvalidData
			}
			data = data[len(encoding.headerSettings):]
		}
		switch encoding.compression {
		case blockContentTransformNone:
			if encoding.encryptionSet {
				return 0, ErrInvalidData
			}
		case blockContentTransformZlib:
			return 0, ErrInvalidData
		case blockContentTransformBzlib:
			return 0, ErrBzip2ContentEncodingWrite
		default:
			return 0, ErrUnsupportedContentEncoding
		}
	}
	if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
		size, _, err := h264MuxedPayloadSize(track, data)
		return size, err
	}
	if track.Codec == CodecH265 && len(track.CodecPrivate) != 0 {
		size, _, err := h265MuxedPayloadSize(track, data)
		return size, err
	}
	return len(data), nil
}

func (m *Muxer) muxedBlockPayload(track Track, data []byte, partitions []uint32) ([]byte, error) {
	if len(track.ContentEncodings) == 0 {
		if len(partitions) != 0 {
			return nil, ErrInvalidData
		}
		return m.codecMuxedBlockPayload(track, data)
	}
	encoding, err := blockContentEncoding(track)
	if err != nil {
		return nil, err
	}
	if len(partitions) != 0 && !encoding.encryptionSet {
		return nil, ErrInvalidData
	}
	if encoding.headerSet {
		if !bytes.HasPrefix(data, encoding.headerSettings) {
			return nil, ErrInvalidData
		}
		data = data[len(encoding.headerSettings):]
	}
	payload, err := m.codecMuxedBlockPayload(track, data)
	if err != nil {
		return nil, err
	}
	switch encoding.compression {
	case blockContentTransformNone:
	case blockContentTransformZlib:
		payload, err = m.zlibCompressBlockPayload(payload)
	case blockContentTransformBzlib:
		return nil, ErrBzip2ContentEncodingWrite
	default:
		return nil, ErrUnsupportedContentEncoding
	}
	if err != nil {
		return nil, err
	}
	if encoding.encryptionSet {
		return m.encryptBlockPayload(encoding, payload, partitions)
	}
	return payload, nil
}

func (m *Muxer) codecMuxedBlockPayload(track Track, data []byte) ([]byte, error) {
	if !codecUsesLengthPrefixedSamples(track) {
		return data, nil
	}
	m.codecPayload.Reset()
	switch track.Codec {
	case CodecH264:
		if err := h264WriteMuxedPayload(&m.codecPayload, track, data, &m.blockScratch); err != nil {
			return nil, err
		}
	case CodecH265:
		if err := h265WriteMuxedPayload(&m.codecPayload, track, data, &m.blockScratch); err != nil {
			return nil, err
		}
	default:
		return nil, ErrInvalidData
	}
	return m.codecPayload.Bytes(), nil
}

func codecUsesLengthPrefixedSamples(track Track) bool {
	return len(track.CodecPrivate) != 0 && (track.Codec == CodecH264 || track.Codec == CodecH265)
}

func (m *Muxer) zlibCompressBlockPayload(data []byte) ([]byte, error) {
	m.blockPayload.Reset()
	if m.zlibWriter == nil {
		m.zlibWriter = zlib.NewWriter(&m.blockPayload)
	} else {
		m.zlibWriter.Reset(&m.blockPayload)
	}
	if _, err := m.zlibWriter.Write(data); err != nil {
		_ = m.zlibWriter.Close()
		return nil, err
	}
	if err := m.zlibWriter.Close(); err != nil {
		return nil, err
	}
	return m.blockPayload.Bytes(), nil
}

func (m *Muxer) prepareLacedBlockPayload(frames [][]byte, track Track, sizes []int) ([]byte, error) {
	if len(sizes) != len(frames) {
		return nil, ErrInvalidData
	}
	if _, err := blockContentEncoding(track); err != nil {
		return nil, err
	}
	m.lacePayload = m.lacePayload[:0]
	for i := range frames {
		payload, err := m.muxedBlockPayload(track, frames[i], nil)
		if err != nil {
			return nil, err
		}
		sizes[i] = len(payload)
		m.lacePayload = append(m.lacePayload, payload...)
	}
	return m.lacePayload, nil
}

func (m *Muxer) lacedBlockPayloadSize(packet LacedPacket, lacing byte, muxedFrameSizes []int) (uint64, error) {
	trackWidth, err := ebml.UnsignedVINTWidth(uint64(packet.TrackID))
	if err != nil {
		return 0, err
	}
	size := uint64(trackWidth + 3 + 1)
	laceSize, err := laceSizePayloadWidth(packet.Frames, muxedFrameSizes, lacing)
	if err != nil {
		return 0, err
	}
	size, err = checkedAddUint64(size, laceSize)
	if err != nil {
		return 0, err
	}
	for i := range packet.Frames {
		size, err = checkedAddUint64(size, uint64(framePayloadSize(packet.Frames, muxedFrameSizes, i)))
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

func laceSizePayloadWidth(frames [][]byte, sizes []int, lacing byte) (uint64, error) {
	switch lacing {
	case simpleBlockLacingXiph:
		var size uint64
		for i := 0; i < len(frames)-1; i++ {
			size += uint64(framePayloadSize(frames, sizes, i)/255 + 1)
		}
		return size, nil
	case simpleBlockLacingFixed:
		if !equalFrameSizes(frames, sizes) {
			return 0, ErrInvalidData
		}
		return 0, nil
	case simpleBlockLacingEBML:
		width, err := ebml.UnsignedVINTWidth(uint64(framePayloadSize(frames, sizes, 0)))
		if err != nil {
			return 0, err
		}
		size := uint64(width)
		previous := framePayloadSize(frames, sizes, 0)
		for i := 1; i < len(frames)-1; i++ {
			current := framePayloadSize(frames, sizes, i)
			delta := current - previous
			width, err := signedLaceSizeVINTWidth(int64(delta))
			if err != nil {
				return 0, err
			}
			size += uint64(width)
			previous = current
		}
		return size, nil
	default:
		return 0, ErrUnsupportedLacing
	}
}

func signedLaceSizeVINTWidth(value int64) (int, error) {
	for width := 1; width <= ebml.MaxSizeWidth; width++ {
		bias := int64((uint64(1) << uint(7*width-1)) - 1)
		if value > math.MaxInt64-bias {
			continue
		}
		encoded := value + bias
		if encoded < 0 {
			continue
		}
		if uint64(encoded) <= (uint64(1)<<uint(7*width))-2 {
			return width, nil
		}
	}
	return 0, ErrInvalidData
}

func encodeSignedLaceSizeVINT(dst []byte, value int64) (int, error) {
	width, err := signedLaceSizeVINTWidth(value)
	if err != nil {
		return 0, err
	}
	bias := int64((uint64(1) << uint(7*width-1)) - 1)
	return ebml.EncodeUnsignedVINTWidth(dst, uint64(value+bias), width)
}

func checkedAddUint64(left uint64, right uint64) (uint64, error) {
	if left > ^uint64(0)-right {
		return 0, ErrInvalidData
	}
	return left + right, nil
}

func elementEncodedSize(id ebml.ID, payloadSize uint64) (uint64, error) {
	idWidth, err := ebml.IDWidth(id)
	if err != nil {
		return 0, err
	}
	sizeWidth, err := ebml.SizeVINTWidth(payloadSize)
	if err != nil {
		return 0, err
	}
	return uint64(idWidth + sizeWidth), nil
}

func uintElementEncodedSize(id ebml.ID, value uint64) (uint64, error) {
	payloadSize := uint64(uintPayloadWidthLocal(value))
	headerSize, err := elementEncodedSize(id, payloadSize)
	if err != nil {
		return 0, err
	}
	return headerSize + payloadSize, nil
}

func intElementEncodedSize(id ebml.ID, value int64) (uint64, error) {
	payloadSize := uint64(intPayloadWidthLocal(value))
	headerSize, err := elementEncodedSize(id, payloadSize)
	if err != nil {
		return 0, err
	}
	return headerSize + payloadSize, nil
}

func binaryElementEncodedSize(id ebml.ID, value []byte) (uint64, error) {
	payloadSize := uint64(len(value))
	headerSize, err := elementEncodedSize(id, payloadSize)
	if err != nil {
		return 0, err
	}
	return headerSize + payloadSize, nil
}

func blockAdditionsElementEncodedSize(additions []BlockAddition) (uint64, error) {
	var payloadSize uint64
	for i := range additions {
		size, err := blockMoreElementEncodedSize(additions[i])
		if err != nil {
			return 0, err
		}
		payloadSize, err = checkedAddUint64(payloadSize, size)
		if err != nil {
			return 0, err
		}
	}
	headerSize, err := elementEncodedSize(idBlockAdditions, payloadSize)
	if err != nil {
		return 0, err
	}
	return headerSize + payloadSize, nil
}

func blockMoreElementEncodedSize(addition BlockAddition) (uint64, error) {
	payloadSize, err := binaryElementEncodedSize(idBlockAdditional, addition.Data)
	if err != nil {
		return 0, err
	}
	id := blockAdditionID(addition.ID)
	if id != 1 {
		idSize, err := uintElementEncodedSize(idBlockAddID, id)
		if err != nil {
			return 0, err
		}
		payloadSize, err = checkedAddUint64(payloadSize, idSize)
		if err != nil {
			return 0, err
		}
	}
	headerSize, err := elementEncodedSize(idBlockMore, payloadSize)
	if err != nil {
		return 0, err
	}
	return headerSize + payloadSize, nil
}

func uintPayloadWidthLocal(value uint64) int {
	switch {
	case value <= 0xff:
		return 1
	case value <= 0xffff:
		return 2
	case value <= 0xffffff:
		return 3
	case value <= 0xffffffff:
		return 4
	case value <= 0xffffffffff:
		return 5
	case value <= 0xffffffffffff:
		return 6
	case value <= 0xffffffffffffff:
		return 7
	default:
		return 8
	}
}

func intPayloadWidthLocal(value int64) int {
	for width := 1; width < 8; width++ {
		shift := uint(width * 8)
		min := int64(-1) << (shift - 1)
		max := int64(1)<<(shift-1) - 1
		if value >= min && value <= max {
			return width
		}
	}
	return 8
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) != 0 {
		n, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func buildElementBytes(id ebml.ID, payload []byte) ([]byte, error) {
	var out bytes.Buffer
	w := ebml.NewWriter(&out)
	if err := w.WriteElement(id, payload); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func buildElementBytesWithExtraSizeByte(id ebml.ID, payload []byte) ([]byte, error) {
	idWidth, err := ebml.IDWidth(id)
	if err != nil {
		return nil, err
	}
	sizeWidth, err := ebml.SizeVINTWidth(uint64(len(payload)))
	if err != nil {
		return nil, err
	}
	if sizeWidth == ebml.MaxSizeWidth {
		return nil, ErrSeekHeadTooSmall
	}
	out := make([]byte, idWidth+sizeWidth+1+len(payload))
	if _, err := ebml.EncodeID(out, id); err != nil {
		return nil, err
	}
	if _, err := ebml.EncodeSizeVINTWidth(out[idWidth:], uint64(len(payload)), sizeWidth+1); err != nil {
		return nil, err
	}
	copy(out[idWidth+sizeWidth+1:], payload)
	return out, nil
}

func writeVoidTotal(w *ebml.Writer, total int) error {
	if total == 0 {
		return nil
	}
	if total < 2 {
		return ErrInvalidData
	}
	_, payloadSize, err := voidLayout(total)
	if err != nil {
		return err
	}
	if err := w.WriteHeader(idVoid, uint64(payloadSize)); err != nil {
		return err
	}
	var zero [64]byte
	for payloadSize > 0 {
		n := min(payloadSize, len(zero))
		if _, err := w.Write(zero[:n]); err != nil {
			return err
		}
		payloadSize -= n
	}
	return nil
}

func writeVoidTotalRaw(writer io.Writer, total int) error {
	if total == 0 {
		return nil
	}
	if total < 2 {
		return ErrInvalidData
	}
	width, payloadSize, err := voidLayout(total)
	if err != nil {
		return err
	}
	var header [1 + ebml.MaxSizeWidth]byte
	header[0] = byte(idVoid)
	if _, err := ebml.EncodeSizeVINTWidth(header[1:], uint64(payloadSize), width); err != nil {
		return err
	}
	if err := writeFull(writer, header[:1+width]); err != nil {
		return err
	}
	var zero [64]byte
	for payloadSize > 0 {
		n := min(payloadSize, len(zero))
		if err := writeFull(writer, zero[:n]); err != nil {
			return err
		}
		payloadSize -= n
	}
	return nil
}

func voidLayout(total int) (int, int, error) {
	for width := 1; width <= ebml.MaxSizeWidth; width++ {
		payloadSize := total - 1 - width
		if payloadSize < 0 {
			continue
		}
		if uint64(payloadSize) <= maxKnownSize(width) {
			return width, payloadSize, nil
		}
	}
	return 0, 0, ErrInvalidData
}

func maxKnownSize(width int) uint64 {
	return (uint64(1) << uint(7*width)) - 2
}

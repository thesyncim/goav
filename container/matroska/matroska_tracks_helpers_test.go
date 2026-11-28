package matroska

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/thesyncim/goav/container/ebml"
)

func writeTracksWithTrackNumber(writer *ebml.Writer, trackNumber uint64) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, trackNumber); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithFlagValue(writer *ebml.Writer, flagID ebml.ID, value uint64) error {
	return writeTracksWithTrackMetadata(writer, trackUIntElement{flagID, value})
}

type trackUIntElement struct {
	id    ebml.ID
	value uint64
}

func writeTracksWithTrackMetadata(writer *ebml.Writer, elements ...trackUIntElement) error {
	return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
		for i := range elements {
			if err := ew.WriteUInt(elements[i].id, elements[i].value); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeTracksWithTrackExtra(writer *ebml.Writer, writeExtra func(*ebml.Writer) error) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if writeExtra != nil {
		if err := writeExtra(ew); err != nil {
			return err
		}
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

type trackEntryFixture struct {
	Number           uint64
	UID              uint64
	Type             uint64
	CodecID          string
	NumberSet        bool
	UIDSet           bool
	TypeSet          bool
	CodecIDSet       bool
	MediaSet         bool
	DuplicateNumber  bool
	DuplicateUID     bool
	DuplicateType    bool
	DuplicateCodecID bool
	DuplicateMedia   bool
}

func defaultTrackEntryFixture() trackEntryFixture {
	return trackEntryFixture{
		Number:     1,
		UID:        1,
		Type:       matroskaTrackVideo,
		CodecID:    codecIDVP8,
		NumberSet:  true,
		UIDSet:     true,
		TypeSet:    true,
		CodecIDSet: true,
		MediaSet:   true,
	}
}

func writeTracksWithTrackEntryFixtures(writer *ebml.Writer, entries ...trackEntryFixture) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	for i := range entries {
		var entry bytes.Buffer
		ew := ebml.NewWriter(&entry)
		if err := writeTrackEntryFixture(ew, entries[i]); err != nil {
			return err
		}
		if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
			return err
		}
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTrackEntryFixture(w *ebml.Writer, entry trackEntryFixture) error {
	if entry.NumberSet {
		if err := w.WriteUInt(idTrackNumber, entry.Number); err != nil {
			return err
		}
		if entry.DuplicateNumber {
			if err := w.WriteUInt(idTrackNumber, entry.Number); err != nil {
				return err
			}
		}
	}
	if entry.UIDSet {
		if err := w.WriteUInt(idTrackUID, entry.UID); err != nil {
			return err
		}
		if entry.DuplicateUID {
			if err := w.WriteUInt(idTrackUID, entry.UID); err != nil {
				return err
			}
		}
	}
	if entry.TypeSet {
		if err := w.WriteUInt(idTrackType, entry.Type); err != nil {
			return err
		}
		if entry.DuplicateType {
			if err := w.WriteUInt(idTrackType, entry.Type); err != nil {
				return err
			}
		}
	}
	if entry.CodecIDSet {
		if err := w.WriteString(idCodecID, entry.CodecID); err != nil {
			return err
		}
		if entry.DuplicateCodecID {
			if err := w.WriteString(idCodecID, entry.CodecID); err != nil {
				return err
			}
		}
	}
	if entry.MediaSet {
		if err := writeTrackEntryFixtureMedia(w, entry.Type); err != nil {
			return err
		}
		if entry.DuplicateMedia {
			if err := writeTrackEntryFixtureMedia(w, entry.Type); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeTrackEntryFixtureMedia(w *ebml.Writer, trackType uint64) error {
	switch trackType {
	case matroskaTrackAudio:
		return writeAudio(w, AudioConfig{SampleRate: 48000, Channels: 2})
	case matroskaTrackVideo:
		return writeVideo(w, VideoConfig{Width: 16, Height: 16})
	default:
		return nil
	}
}

func trackTranslatePayload(t testing.TB, writeID bool, writeCodec bool) []byte {
	t.Helper()
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if writeID {
		if err := writeBinary(tw, idTrackTranslateTrack, []byte{0x01}); err != nil {
			t.Fatal(err)
		}
	}
	if writeCodec {
		if err := tw.WriteUInt(idTrackTranslateCodec, 1); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func contentEncodingsPayload(t testing.TB, encodings ...[]byte) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	for i := range encodings {
		if err := writer.WriteElement(idContentEncoding, encodings[i]); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func contentEncodingPayload(t testing.TB, write func(*ebml.Writer) error) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if write != nil {
		if err := write(writer); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func contentCompressionPayload(t testing.TB, algorithm uint64, settings []byte) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idContentCompAlgo, algorithm); err != nil {
		t.Fatal(err)
	}
	if settings != nil {
		if err := writeBinary(writer, idContentCompSettings, settings); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

const contentAESCipherUnset = ^uint64(0)

func contentEncryptionPayload(t testing.TB, algorithm uint64, keyID []byte, cipherMode uint64) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idContentEncAlgo, algorithm); err != nil {
		t.Fatal(err)
	}
	if keyID != nil {
		if err := writeBinary(writer, idContentEncKeyID, keyID); err != nil {
			t.Fatal(err)
		}
	}
	if cipherMode != contentAESCipherUnset {
		var aesPayload bytes.Buffer
		aesWriter := ebml.NewWriter(&aesPayload)
		if err := aesWriter.WriteUInt(idContentEncAESCipher, cipherMode); err != nil {
			t.Fatal(err)
		}
		if err := writer.WriteElement(idContentEncAES, aesPayload.Bytes()); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := writer.WriteElement(idContentEncAES, nil); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func blockAdditionMappingPayload(t testing.TB, id uint64, writeID bool, typ uint64, extraData []byte) []byte {
	t.Helper()
	var payload bytes.Buffer
	mw := ebml.NewWriter(&payload)
	if writeID {
		if err := mw.WriteUInt(idBlockAddIDValue, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := mw.WriteUInt(idBlockAddIDType, typ); err != nil {
		t.Fatal(err)
	}
	if extraData != nil {
		if err := writeBinary(mw, idBlockAddIDExtraData, extraData); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func writeTracksWithVideoDimensions(writer *ebml.Writer, width uint64, height uint64) error {
	return writeTracksWithVideoElements(writer,
		videoUIntElement{idPixelWidth, width},
		videoUIntElement{idPixelHeight, height},
	)
}

type videoUIntElement struct {
	id    ebml.ID
	value uint64
}

func writeTracksWithVideoElements(writer *ebml.Writer, elements ...videoUIntElement) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	var video bytes.Buffer
	vw := ebml.NewWriter(&video)
	for i := range elements {
		if err := vw.WriteUInt(elements[i].id, elements[i].value); err != nil {
			return err
		}
	}
	if err := ew.WriteElement(idVideo, video.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

type colourUIntElement struct {
	id    ebml.ID
	value uint64
}

type colourFloatElement struct {
	id    ebml.ID
	value float64
}

func writeTracksWithVideoColourElements(writer *ebml.Writer, elements ...colourUIntElement) error {
	return writeTracksWithVideoColourMetadata(writer, elements, nil)
}

func writeTracksWithVideoMasteringFloatElements(writer *ebml.Writer, elements ...colourFloatElement) error {
	return writeTracksWithVideoColourMetadata(writer, nil, elements)
}

func writeTracksWithVideoColourMetadata(writer *ebml.Writer, uints []colourUIntElement, floats []colourFloatElement) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	var video bytes.Buffer
	vw := ebml.NewWriter(&video)
	if err := vw.WriteUInt(idPixelWidth, 16); err != nil {
		return err
	}
	if err := vw.WriteUInt(idPixelHeight, 16); err != nil {
		return err
	}
	var colour bytes.Buffer
	cw := ebml.NewWriter(&colour)
	for i := range uints {
		if err := cw.WriteUInt(uints[i].id, uints[i].value); err != nil {
			return err
		}
	}
	if len(floats) != 0 {
		var mastering bytes.Buffer
		mw := ebml.NewWriter(&mastering)
		for i := range floats {
			if err := mw.WriteFloat64(floats[i].id, floats[i].value); err != nil {
				return err
			}
		}
		if err := cw.WriteElement(idMasteringMetadata, mastering.Bytes()); err != nil {
			return err
		}
	}
	if err := vw.WriteElement(idColour, colour.Bytes()); err != nil {
		return err
	}
	if err := ew.WriteElement(idVideo, video.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

type projectionUIntElement struct {
	id    ebml.ID
	value uint64
}

type projectionFloatElement struct {
	id    ebml.ID
	value float64
}

func writeTracksWithVideoProjectionMetadata(writer *ebml.Writer, uints []projectionUIntElement, floats []projectionFloatElement, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	var video bytes.Buffer
	vw := ebml.NewWriter(&video)
	if err := vw.WriteUInt(idPixelWidth, 16); err != nil {
		return err
	}
	if err := vw.WriteUInt(idPixelHeight, 16); err != nil {
		return err
	}
	var projection bytes.Buffer
	pw := ebml.NewWriter(&projection)
	for i := range uints {
		if err := pw.WriteUInt(uints[i].id, uints[i].value); err != nil {
			return err
		}
	}
	for i := range floats {
		if err := pw.WriteFloat64(floats[i].id, floats[i].value); err != nil {
			return err
		}
	}
	if len(private) != 0 {
		if err := writeBinary(pw, idProjectionPrivate, private); err != nil {
			return err
		}
	}
	if err := vw.WriteElement(idProjection, projection.Bytes()); err != nil {
		return err
	}
	if err := ew.WriteElement(idVideo, video.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithAudioMetadata(writer *ebml.Writer, sampleRate float64, channels uint64, bitDepth uint64) error {
	return writeTracksWithAudioMetadataValues(writer, sampleRate, 0, false, channels, bitDepth)
}

func writeTracksWithAudioOutputMetadata(writer *ebml.Writer, sampleRate float64, outputSampleRate float64, channels uint64, bitDepth uint64) error {
	return writeTracksWithAudioMetadataValues(writer, sampleRate, outputSampleRate, true, channels, bitDepth)
}

func writeTracksWithAudioMetadataValues(writer *ebml.Writer, sampleRate float64, outputSampleRate float64, writeOutputSampleRate bool, channels uint64, bitDepth uint64) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "A_OPUS"); err != nil {
		return err
	}
	var audio bytes.Buffer
	aw := ebml.NewWriter(&audio)
	if err := aw.WriteFloat64(idSamplingFreq, sampleRate); err != nil {
		return err
	}
	if writeOutputSampleRate {
		if err := aw.WriteFloat64(idOutputFreq, outputSampleRate); err != nil {
			return err
		}
	}
	if err := aw.WriteUInt(idChannels, channels); err != nil {
		return err
	}
	if err := aw.WriteUInt(idBitDepth, bitDepth); err != nil {
		return err
	}
	if err := ew.WriteElement(idAudio, audio.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithAudioElements(writer *ebml.Writer, writeAudioElements func(*ebml.Writer) error) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDOpus); err != nil {
		return err
	}
	var audio bytes.Buffer
	aw := ebml.NewWriter(&audio)
	if writeAudioElements != nil {
		if err := writeAudioElements(aw); err != nil {
			return err
		}
	}
	if err := ew.WriteElement(idAudio, audio.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithOpusPrivate(writer *ebml.Writer, private []byte) error {
	return writeTracksWithOpusPrivateAndTiming(writer, private, 0, 0)
}

func writeTracksWithMSACMPrivate(writer *ebml.Writer, private []byte, audioConfig AudioConfig) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDMS); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeAudio(ew, audioConfig); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithOpusPrivateAndTiming(writer *ebml.Writer, private []byte, codecDelayNS uint64, seekPreRollNS uint64) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "A_OPUS"); err != nil {
		return err
	}
	if codecDelayNS != 0 {
		if err := ew.WriteUInt(idCodecDelay, codecDelayNS); err != nil {
			return err
		}
	}
	if seekPreRollNS != 0 {
		if err := ew.WriteUInt(idSeekPreRoll, seekPreRollNS); err != nil {
			return err
		}
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeAudio(ew, AudioConfig{SampleRate: 48000, Channels: 2}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithAV1Private(writer *ebml.Writer, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDAV1); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithH264Private(writer *ebml.Writer, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDH264); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithH265Private(writer *ebml.Writer, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDH265); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeCuesWithTrackNumber(writer *ebml.Writer, trackNumber uint64) error {
	var cues bytes.Buffer
	cw := ebml.NewWriter(&cues)
	var point bytes.Buffer
	pw := ebml.NewWriter(&point)
	if err := pw.WriteUInt(idCueTime, 0); err != nil {
		return err
	}
	var positions bytes.Buffer
	tw := ebml.NewWriter(&positions)
	if err := tw.WriteUInt(idCueTrack, trackNumber); err != nil {
		return err
	}
	if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
		return err
	}
	if err := pw.WriteElement(idCueTrackPositions, positions.Bytes()); err != nil {
		return err
	}
	if err := cw.WriteElement(idCuePoint, point.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idCues, cues.Bytes())
}

func writeSimpleBlockWithTrackNumber(writer *ebml.Writer, trackNumber uint64, frame []byte) error {
	payload, err := blockPayloadWithTrackNumber(trackNumber, simpleBlockKeyframe, frame)
	if err != nil {
		return err
	}
	return writer.WriteElement(idSimpleBlock, payload)
}

func writeBlockWithTrackNumber(writer *ebml.Writer, trackNumber uint64, frame []byte) error {
	payload, err := blockPayloadWithTrackNumber(trackNumber, 0, frame)
	if err != nil {
		return err
	}
	return writer.WriteElement(idBlock, payload)
}

func blockPayloadWithTrackNumber(trackNumber uint64, flags byte, frame []byte) ([]byte, error) {
	var payload bytes.Buffer
	var scratch [ebml.MaxSizeWidth]byte
	n, err := ebml.EncodeUnsignedVINT(scratch[:], trackNumber)
	if err != nil {
		return nil, err
	}
	payload.Write(scratch[:n])
	var blockHeader [3]byte
	blockHeader[2] = flags
	payload.Write(blockHeader[:])
	payload.Write(frame)
	return payload.Bytes(), nil
}

func writeLacedSimpleBlock(writer *ebml.Writer, trackID uint32, lacing byte, frames [][]byte) error {
	return writeLacedBlockElement(writer, idSimpleBlock, trackID, simpleBlockKeyframe|lacing, frames)
}

func writeLacedBlockElement(writer *ebml.Writer, id ebml.ID, trackID uint32, flags byte, frames [][]byte) error {
	var payload bytes.Buffer
	var scratch [ebml.MaxSizeWidth]byte
	n, err := ebml.EncodeUnsignedVINT(scratch[:], uint64(trackID))
	if err != nil {
		return err
	}
	payload.Write(scratch[:n])
	var blockHeader [3]byte
	binary.BigEndian.PutUint16(blockHeader[:2], 0)
	blockHeader[2] = flags
	payload.Write(blockHeader[:])
	if len(frames) < 2 || len(frames) > 256 {
		return ErrInvalidData
	}
	payload.WriteByte(byte(len(frames) - 1))
	lacing := flags & simpleBlockLacingMask
	switch lacing {
	case simpleBlockLacingXiph:
		for i := 0; i < len(frames)-1; i++ {
			size := len(frames[i])
			for size >= 255 {
				payload.WriteByte(255)
				size -= 255
			}
			payload.WriteByte(byte(size))
		}
	case simpleBlockLacingFixed:
		size := len(frames[0])
		for i := range frames {
			if len(frames[i]) != size {
				return ErrInvalidData
			}
		}
	case simpleBlockLacingEBML:
		n, err := ebml.EncodeUnsignedVINT(scratch[:], uint64(len(frames[0])))
		if err != nil {
			return err
		}
		payload.Write(scratch[:n])
	default:
		return ErrUnsupportedLacing
	}
	if lacing == simpleBlockLacingEBML {
		previous := len(frames[0])
		for i := 1; i < len(frames)-1; i++ {
			delta := len(frames[i]) - previous
			n, err := encodeSignedLaceVINT(scratch[:], int64(delta))
			if err != nil {
				return err
			}
			payload.Write(scratch[:n])
			previous = len(frames[i])
		}
	}
	for i := range frames {
		payload.Write(frames[i])
	}
	return writer.WriteElement(id, payload.Bytes())
}

func encodeSignedLaceVINT(dst []byte, value int64) (int, error) {
	for width := 1; width <= ebml.MaxSizeWidth; width++ {
		bias := int64((uint64(1) << uint(7*width-1)) - 1)
		encoded := value + bias
		if encoded < 0 {
			continue
		}
		if uint64(encoded) <= (uint64(1)<<uint(7*width))-2 {
			return ebml.EncodeUnsignedVINTWidth(dst, uint64(encoded), width)
		}
	}
	return 0, ErrInvalidData
}

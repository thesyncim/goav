package matroska

import (
	"bytes"
	"testing"

	"github.com/thesyncim/goav/container/ebml"
)

func makeLacedMatroskaData(tb testing.TB, lacing byte, frames [][]byte, defaultDurationNS int64) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: defaultDurationNS,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	if err := writeLacedSimpleBlock(muxer.ebml, trackID, lacing, frames); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeSpecLacedMatroskaData(tb testing.TB, lacing byte, frames [][]byte, defaultDurationNS int64) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: defaultDurationNS,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	if err := writeSpecLacedSimpleBlock(muxer.ebml, trackID, lacing, frames); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func writeSpecLacedSimpleBlock(writer *ebml.Writer, trackID uint32, lacing byte, frames [][]byte) error {
	const (
		specSimpleBlockKeyframe = 0x80
		specLacingMask          = 0x06
		specLacingXiph          = 0x02
		specLacingFixed         = 0x04
		specLacingEBML          = 0x06
	)
	var payload bytes.Buffer
	var scratch [ebml.MaxSizeWidth]byte
	n, err := ebml.EncodeUnsignedVINT(scratch[:], uint64(trackID))
	if err != nil {
		return err
	}
	payload.Write(scratch[:n])
	var blockHeader [3]byte
	blockHeader[2] = specSimpleBlockKeyframe | lacing
	payload.Write(blockHeader[:])
	if len(frames) < 2 || len(frames) > 256 {
		return ErrInvalidData
	}
	payload.WriteByte(byte(len(frames) - 1))
	switch lacing & specLacingMask {
	case specLacingXiph:
		for i := 0; i < len(frames)-1; i++ {
			size := len(frames[i])
			for size >= 255 {
				payload.WriteByte(255)
				size -= 255
			}
			payload.WriteByte(byte(size))
		}
	case specLacingFixed:
		size := len(frames[0])
		for i := range frames {
			if len(frames[i]) != size {
				return ErrInvalidData
			}
		}
	case specLacingEBML:
		n, err := ebml.EncodeUnsignedVINT(scratch[:], uint64(len(frames[0])))
		if err != nil {
			return err
		}
		payload.Write(scratch[:n])
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
	default:
		return ErrUnsupportedLacing
	}
	for i := range frames {
		payload.Write(frames[i])
	}
	return writer.WriteElement(idSimpleBlock, payload.Bytes())
}

func makeLacedBlockGroupMatroskaData(tb testing.TB, lacing byte, frames [][]byte) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{TimecodeScaleNS: 1_000_000})
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:               TrackVideo,
		Codec:              CodecVP8,
		MaxBlockAdditionID: 2,
		Video:              VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	var group bytes.Buffer
	writer := ebml.NewWriter(&group)
	if err := writeLacedBlockElement(writer, idBlock, trackID, simpleBlockInvisible|lacing, frames); err != nil {
		tb.Fatal(err)
	}
	if err := writeBlockAdditions(writer, []BlockAddition{{ID: 2, Data: []byte{0xaa, 0xbb}}}); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idBlockDuration, 60); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idReferencePriority, 2); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteInt(idReferenceBlk, -20); err != nil {
		tb.Fatal(err)
	}
	if err := writeBinary(writer, idCodecState, []byte{0xcc, 0xdd}); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteInt(idDiscardPad, -3_000_000); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteElement(idBlockGroup, group.Bytes()); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeTrackNumberMatroskaData(tb testing.TB, trackNumber uint64) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := writeTracksWithTrackNumber(muxer.ebml, trackNumber); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeCueTrackNumberMatroskaData(tb testing.TB, cueTrackNumber uint64) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := muxer.writeTracks(); err != nil {
		tb.Fatal(err)
	}
	if err := writeCuesWithTrackNumber(muxer.ebml, cueTrackNumber); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeCueMetadataMatroskaData(tb testing.TB, writeCues func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := muxer.writeTracks(); err != nil {
		tb.Fatal(err)
	}
	if err := writeCues(muxer.ebml); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	if err := writeSimpleBlockWithTrackNumber(muxer.ebml, 1, []byte{1}); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeBlockTrackNumberMatroskaData(tb testing.TB, trackNumber uint64) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	if err := writeSimpleBlockWithTrackNumber(muxer.ebml, trackNumber, []byte{1}); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeBlockAdditionsMatroskaData(tb testing.TB, writeAdditions func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	var group bytes.Buffer
	gw := ebml.NewWriter(&group)
	if err := writeBlockWithTrackNumber(gw, uint64(trackID), []byte{1}); err != nil {
		tb.Fatal(err)
	}
	if err := writeAdditions(gw); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteElement(idBlockGroup, group.Bytes()); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeBlockGroupMetadataMatroskaData(tb testing.TB, writeGroup func(*ebml.Writer, uint32) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:               TrackVideo,
		Codec:              CodecVP8,
		MaxBlockAdditionID: 4,
		Video:              VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	var group bytes.Buffer
	gw := ebml.NewWriter(&group)
	if err := writeGroup(gw, trackID); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteElement(idBlockGroup, group.Bytes()); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeContentEncodedBlockMatroskaData(tb testing.TB, encodings []byte, frame []byte) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := writeTracksWithTrackExtra(muxer.ebml, func(ew *ebml.Writer) error {
		return ew.WriteElement(idContentEncodings, encodings)
	}); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	if err := writeSimpleBlockWithTrackNumber(muxer.ebml, 1, frame); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeContentEncodedLacedMatroskaData(tb testing.TB, encodings []byte, frames [][]byte) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := writeTracksWithTrackExtra(muxer.ebml, func(ew *ebml.Writer) error {
		if err := ew.WriteUInt(idDefaultDur, 20_000_000); err != nil {
			return err
		}
		return ew.WriteElement(idContentEncodings, encodings)
	}); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	if err := writeLacedSimpleBlock(muxer.ebml, 1, simpleBlockLacingXiph, frames); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func writeBlockMoreFixture(w *ebml.Writer, id uint64, data []byte, writeID bool) error {
	var payload bytes.Buffer
	pw := ebml.NewWriter(&payload)
	if writeID {
		if err := pw.WriteUInt(idBlockAddID, id); err != nil {
			return err
		}
	}
	if data != nil {
		if err := writeBinary(pw, idBlockAdditional, data); err != nil {
			return err
		}
	}
	return w.WriteElement(idBlockMore, payload.Bytes())
}

func makeTrackMetadataMatroskaData(tb testing.TB, writeTracks func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := writeTracks(muxer.ebml); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeInfoMetadataMatroskaData(tb testing.TB, writeInfo func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeEBMLHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	muxer.segmentData = muxer.ebml.Offset()
	if err := writeInfo(muxer.ebml); err != nil {
		tb.Fatal(err)
	}
	if err := writeTracksWithVideoDimensions(muxer.ebml, 16, 16); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteElement(idCluster, nil); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeTopLevelMetadataMatroskaData(tb testing.TB, writeMetadata func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := writeTracksWithVideoDimensions(muxer.ebml, 16, 16); err != nil {
		tb.Fatal(err)
	}
	if writeMetadata != nil {
		if err := writeMetadata(muxer.ebml); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.ebml.WriteElement(idCluster, nil); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func writeAttachmentsElement(w *ebml.Writer, attachments ...Attachment) error {
	var payload bytes.Buffer
	aw := ebml.NewWriter(&payload)
	for i := range attachments {
		if err := writeAttachedFile(aw, attachments[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idAttachments, payload.Bytes())
}

func writeAttachmentsWithAttachedFilePayload(w *ebml.Writer, writeFile func(*ebml.Writer) error) error {
	var attachments bytes.Buffer
	aw := ebml.NewWriter(&attachments)
	var file bytes.Buffer
	if err := writeFile(ebml.NewWriter(&file)); err != nil {
		return err
	}
	if err := aw.WriteElement(idAttachedFile, file.Bytes()); err != nil {
		return err
	}
	return w.WriteElement(idAttachments, attachments.Bytes())
}

func writeChaptersElement(w *ebml.Writer, editions ...ChapterEdition) error {
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	for i := range editions {
		if err := writeEditionEntry(cw, editions[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idChapters, payload.Bytes())
}

func writeChaptersWithEditionPayload(w *ebml.Writer, writeEdition func(*ebml.Writer) error) error {
	var chapters bytes.Buffer
	cw := ebml.NewWriter(&chapters)
	var edition bytes.Buffer
	if err := writeEdition(ebml.NewWriter(&edition)); err != nil {
		return err
	}
	if err := cw.WriteElement(idEditionEntry, edition.Bytes()); err != nil {
		return err
	}
	return w.WriteElement(idChapters, chapters.Bytes())
}

func metadataValidationChapter(uid uint64, startNS int64) Chapter {
	return Chapter{
		UID:        uid,
		StartNS:    startNS,
		Enabled:    true,
		EnabledSet: true,
	}
}

func duplicateChapterTimeStartPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteUInt(idChapterUID, 2); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteUInt(idChapterTimeStart, 0); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteUInt(idChapterTimeStart, 1); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func duplicateChapterDisplayStringChapterPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteUInt(idChapterUID, 2); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteUInt(idChapterTimeStart, 0); err != nil {
		tb.Fatal(err)
	}
	var display bytes.Buffer
	dw := ebml.NewWriter(&display)
	if err := dw.WriteString(idChapString, "Intro"); err != nil {
		tb.Fatal(err)
	}
	if err := dw.WriteString(idChapString, "Again"); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteElement(idChapterDisplay, display.Bytes()); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func writeTagsWithTagPayload(w *ebml.Writer, writeTagPayload func(*ebml.Writer) error) error {
	var tags bytes.Buffer
	tw := ebml.NewWriter(&tags)
	var tag bytes.Buffer
	if err := writeTagPayload(ebml.NewWriter(&tag)); err != nil {
		return err
	}
	if err := tw.WriteElement(idTag, tag.Bytes()); err != nil {
		return err
	}
	return w.WriteElement(idTags, tags.Bytes())
}

func writeTagsElement(w *ebml.Writer, tags ...Tag) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	for i := range tags {
		if err := writeTag(tw, tags[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idTags, payload.Bytes())
}

func validTagTargetsPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteUInt(idTargetTypeValue, 50); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTargetType, "MOVIE"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func duplicateTagTargetTypePayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteUInt(idTargetTypeValue, 50); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTargetType, "MOVIE"); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTargetType, "EPISODE"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func validSimpleTagPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteString(idTagName, "TITLE"); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTagString, "Scene"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func duplicateSimpleTagNamePayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteString(idTagName, "TITLE"); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTagName, "ALBUM"); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTagString, "Scene"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func writeInfoWithElements(writer *ebml.Writer, writeExtra func(*ebml.Writer) error) error {
	var info bytes.Buffer
	iw := ebml.NewWriter(&info)
	if writeExtra != nil {
		if err := writeExtra(iw); err != nil {
			return err
		}
	}
	if err := iw.WriteUInt(idTimestampScale, uint64(defaultTimecodeScaleNS)); err != nil {
		return err
	}
	if err := iw.WriteString(idMuxingApp, defaultMuxingApp); err != nil {
		return err
	}
	if err := iw.WriteString(idWritingApp, defaultWritingApp); err != nil {
		return err
	}
	return writer.WriteElement(idInfo, info.Bytes())
}

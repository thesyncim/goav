package matroska

import (
	"bytes"
	"testing"

	"github.com/thesyncim/goav/container/ebml"
)

func writeInfoWithCRC32(writer *ebml.Writer, mutate func([]byte)) error {
	var body bytes.Buffer
	bw := ebml.NewWriter(&body)
	if err := bw.WriteString(idTitle, "crc protected"); err != nil {
		return err
	}
	if err := bw.WriteUInt(idTimestampScale, uint64(defaultTimecodeScaleNS)); err != nil {
		return err
	}
	if err := bw.WriteString(idMuxingApp, "crc-mux"); err != nil {
		return err
	}
	if err := bw.WriteString(idWritingApp, "crc-write"); err != nil {
		return err
	}
	return writeMasterWithCRC32(writer, idInfo, body.Bytes(), mutate)
}

func writeMasterWithCRC32(writer *ebml.Writer, id ebml.ID, payload []byte, mutate func([]byte)) error {
	protected := append([]byte(nil), payload...)
	var info bytes.Buffer
	iw := ebml.NewWriter(&info)
	if err := iw.WriteCRC32(protected); err != nil {
		return err
	}
	if mutate != nil {
		mutate(protected)
	}
	if _, err := iw.Write(protected); err != nil {
		return err
	}
	return writer.WriteElement(id, info.Bytes())
}

func makeMetadataCRCMatroskaData(tb testing.TB, checkedID ebml.ID, mutate func([]byte)) []byte {
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
	if checkedID == idSeekHead {
		if err := writeMasterWithCRC32(muxer.ebml, idSeekHead, seekHeadPayload(tb), mutate); err != nil {
			tb.Fatal(err)
		}
	}
	if err := writeInfoWithElements(muxer.ebml, nil); err != nil {
		tb.Fatal(err)
	}
	if checkedID == idTracks {
		payload := elementPayload(tb, idTracks, func(w *ebml.Writer) error {
			return writeTracksWithVideoDimensions(w, 16, 16)
		})
		if err := writeMasterWithCRC32(muxer.ebml, idTracks, payload, mutate); err != nil {
			tb.Fatal(err)
		}
	} else if err := writeTracksWithVideoDimensions(muxer.ebml, 16, 16); err != nil {
		tb.Fatal(err)
	}
	if checkedID == idCues {
		payload := elementPayload(tb, idCues, func(w *ebml.Writer) error {
			return writeCuesWithTrackNumber(w, 1)
		})
		if err := writeMasterWithCRC32(muxer.ebml, idCues, payload, mutate); err != nil {
			tb.Fatal(err)
		}
	}
	if checkedID == idAttachments {
		var payload bytes.Buffer
		if err := writeAttachedFile(ebml.NewWriter(&payload), Attachment{
			UID:      1,
			Filename: "note.txt",
			MIMEType: "text/plain",
			Data:     []byte("hello"),
		}); err != nil {
			tb.Fatal(err)
		}
		if err := writeMasterWithCRC32(muxer.ebml, idAttachments, payload.Bytes(), mutate); err != nil {
			tb.Fatal(err)
		}
	}
	if checkedID == idChapters {
		if err := writeMasterWithCRC32(muxer.ebml, idChapters, crcChaptersPayload(tb), mutate); err != nil {
			tb.Fatal(err)
		}
	}
	if checkedID == idTags {
		if err := writeMasterWithCRC32(muxer.ebml, idTags, crcTagsPayload(tb), mutate); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.ebml.WriteElement(idCluster, nil); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeNestedCRCMatroskaData(tb testing.TB, target string, mutate func([]byte)) []byte {
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
	if target == "seek" {
		if err := muxer.ebml.WriteElement(idSeekHead, checkedElement(tb, idSeek, seekEntryPayload(tb, idInfo, 0), mutate)); err != nil {
			tb.Fatal(err)
		}
	}
	if err := writeInfoWithElements(muxer.ebml, nil); err != nil {
		tb.Fatal(err)
	}
	switch target {
	case "track", "video", "audio", "colour", "mastering", "projection":
		if err := writeNestedCRCTracks(muxer.ebml, target, mutate); err != nil {
			tb.Fatal(err)
		}
	default:
		if err := writeTracksWithVideoDimensions(muxer.ebml, 16, 16); err != nil {
			tb.Fatal(err)
		}
	}
	switch target {
	case "cuepoint":
		if err := muxer.ebml.WriteElement(idCues, checkedElement(tb, idCuePoint, cuePointPayload(tb, false, nil), mutate)); err != nil {
			tb.Fatal(err)
		}
	case "cuepositions":
		if err := muxer.ebml.WriteElement(idCues, cuePointElement(tb, true, mutate)); err != nil {
			tb.Fatal(err)
		}
	}
	if target == "attachedfile" {
		if err := muxer.ebml.WriteElement(idAttachments, checkedElement(tb, idAttachedFile, attachedFilePayload(tb), mutate)); err != nil {
			tb.Fatal(err)
		}
	}
	switch target {
	case "edition", "chapter", "chaptertrack", "chapterdisplay":
		if err := muxer.ebml.WriteElement(idChapters, nestedCRCChaptersPayload(tb, target, mutate)); err != nil {
			tb.Fatal(err)
		}
	case "tag", "targets", "simpletag", "childsimpletag":
		if err := muxer.ebml.WriteElement(idTags, nestedCRCTagsPayload(tb, target, mutate)); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.ebml.WriteElement(idCluster, nil); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func crcChaptersPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	if err := writeEditionEntry(ebml.NewWriter(&payload), ChapterEdition{
		UID: 1,
		Chapters: []Chapter{{
			UID:        2,
			StartNS:    0,
			Enabled:    true,
			EnabledSet: true,
			TrackUIDs:  []uint64{1},
			Displays: []ChapterDisplay{{
				String:   "Intro",
				Language: "eng",
			}},
		}},
	}); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func nestedCRCChaptersPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	editionPayload := crcEditionPayload(tb, target, mutate)
	var err error
	if target == "edition" {
		_, err = writer.Write(checkedElement(tb, idEditionEntry, editionPayload, mutate))
	} else {
		err = writer.WriteElement(idEditionEntry, editionPayload)
	}
	if err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcEditionPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idEditionUID, 1); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idEditionFlagHidden, 0); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idEditionFlagDefault, 0); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idEditionFlagOrdered, 0); err != nil {
		tb.Fatal(err)
	}
	chapterPayload := crcChapterPayload(tb, target, mutate)
	var err error
	if target == "chapter" {
		_, err = writer.Write(checkedElement(tb, idChapterAtom, chapterPayload, mutate))
	} else {
		err = writer.WriteElement(idChapterAtom, chapterPayload)
	}
	if err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcChapterPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idChapterUID, 2); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idChapterTimeStart, 0); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idChapterFlagHidden, 0); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idChapterFlagEnabled, 1); err != nil {
		tb.Fatal(err)
	}
	trackPayload := crcChapterTrackPayload(tb)
	if target == "chaptertrack" {
		if _, err := writer.Write(checkedElement(tb, idChapterTrack, trackPayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	} else if err := writer.WriteElement(idChapterTrack, trackPayload); err != nil {
		tb.Fatal(err)
	}
	displayPayload := crcChapterDisplayPayload(tb)
	if target == "chapterdisplay" {
		if _, err := writer.Write(checkedElement(tb, idChapterDisplay, displayPayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	} else if err := writer.WriteElement(idChapterDisplay, displayPayload); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcChapterTrackPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idChapterTrackUID, 1); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcChapterDisplayPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteString(idChapString, "Intro"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idChapLanguage, "eng"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcTagsPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	if err := writeTag(ebml.NewWriter(&payload), Tag{
		Target: TagTarget{
			TypeValue: 50,
			Type:      "MOVIE",
			TrackUIDs: []uint64{1},
		},
		Simple: []SimpleTag{{
			Name:       "TITLE",
			Language:   "und",
			Default:    true,
			DefaultSet: true,
			String:     "CRC",
			StringSet:  true,
		}},
	}); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func nestedCRCTagsPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	tagPayload := crcTagPayload(tb, target, mutate)
	var err error
	if target == "tag" {
		_, err = writer.Write(checkedElement(tb, idTag, tagPayload, mutate))
	} else {
		err = writer.WriteElement(idTag, tagPayload)
	}
	if err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcTagPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	targetsPayload := crcTargetsPayload(tb)
	if target == "targets" {
		if _, err := writer.Write(checkedElement(tb, idTargets, targetsPayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	} else if err := writer.WriteElement(idTargets, targetsPayload); err != nil {
		tb.Fatal(err)
	}
	simplePayload := crcSimpleTagPayload(tb, target, mutate)
	if target == "simpletag" {
		if _, err := writer.Write(checkedElement(tb, idSimpleTag, simplePayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	} else if err := writer.WriteElement(idSimpleTag, simplePayload); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcTargetsPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idTargetTypeValue, 50); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idTargetType, "MOVIE"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idTagTrackUID, 1); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcSimpleTagPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteString(idTagName, "TITLE"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idTagLanguage, "und"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idTagDefault, 1); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idTagString, "CRC"); err != nil {
		tb.Fatal(err)
	}
	if target == "childsimpletag" {
		childPayload := crcSimpleTagLeafPayload(tb)
		if _, err := writer.Write(checkedElement(tb, idSimpleTag, childPayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	}
	return payload.Bytes()
}

func crcSimpleTagLeafPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteString(idTagName, "SORT_WITH"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idTagLanguage, "und"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idTagDefault, 1); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idTagString, "crc"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func writeNestedCRCTracks(writer *ebml.Writer, target string, mutate func([]byte)) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	trackType := uint64(matroskaTrackVideo)
	codecID := codecIDVP8
	if target == "audio" {
		trackType = matroskaTrackAudio
		codecID = codecIDOpus
	}
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, trackType); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecID); err != nil {
		return err
	}
	switch target {
	case "audio":
		if _, err := ew.Write(checkedElement(nil, idAudio, audioPayload(), mutate)); err != nil {
			return err
		}
	case "video", "colour", "mastering", "projection":
		if _, err := ew.Write(videoElementForNestedCRC(target, mutate)); err != nil {
			return err
		}
	default:
		if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
			return err
		}
	}
	if target == "track" {
		if _, err := tw.Write(checkedElement(nil, idTrackEntry, entry.Bytes(), mutate)); err != nil {
			return err
		}
	} else if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func videoElementForNestedCRC(target string, mutate func([]byte)) []byte {
	var video bytes.Buffer
	vw := ebml.NewWriter(&video)
	if err := vw.WriteUInt(idPixelWidth, 16); err != nil {
		panic(err)
	}
	if err := vw.WriteUInt(idPixelHeight, 16); err != nil {
		panic(err)
	}
	switch target {
	case "colour", "mastering":
		if _, err := vw.Write(colourElementForNestedCRC(target, mutate)); err != nil {
			panic(err)
		}
	case "projection":
		if _, err := vw.Write(checkedElement(nil, idProjection, projectionPayload(), mutate)); err != nil {
			panic(err)
		}
	}
	if target == "video" {
		return checkedElement(nil, idVideo, video.Bytes(), mutate)
	}
	return elementBytes(idVideo, video.Bytes())
}

func colourElementForNestedCRC(target string, mutate func([]byte)) []byte {
	var colour bytes.Buffer
	cw := ebml.NewWriter(&colour)
	if err := cw.WriteUInt(idMatrixCoefficients, 1); err != nil {
		panic(err)
	}
	if target == "mastering" {
		if _, err := cw.Write(checkedElement(nil, idMasteringMetadata, masteringPayload(), mutate)); err != nil {
			panic(err)
		}
	}
	if target == "colour" {
		return checkedElement(nil, idColour, colour.Bytes(), mutate)
	}
	return elementBytes(idColour, colour.Bytes())
}

func checkedElement(tb testing.TB, id ebml.ID, payload []byte, mutate func([]byte)) []byte {
	if tb != nil {
		tb.Helper()
	}
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeMasterWithCRC32(writer, id, payload, mutate); err != nil {
		if tb != nil {
			tb.Fatal(err)
		}
		panic(err)
	}
	return buffer.Bytes()
}

func elementBytes(id ebml.ID, payload []byte) []byte {
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writer.WriteElement(id, payload); err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

func seekEntryPayload(tb testing.TB, id ebml.ID, position uint64) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	var idPayload [ebml.MaxIDWidth]byte
	n, err := ebml.EncodeID(idPayload[:], id)
	if err != nil {
		tb.Fatal(err)
	}
	if err := writeBinary(writer, idSeekID, idPayload[:n]); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idSeekPosition, position); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func cuePointElement(tb testing.TB, checkedPositions bool, mutate func([]byte)) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	payload := cuePointPayload(tb, checkedPositions, mutate)
	if err := writer.WriteElement(idCuePoint, payload); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func cuePointPayload(tb testing.TB, checkedPositions bool, mutate func([]byte)) []byte {
	tb.Helper()
	var point bytes.Buffer
	writer := ebml.NewWriter(&point)
	if err := writer.WriteUInt(idCueTime, 0); err != nil {
		tb.Fatal(err)
	}
	positionsPayload := cueTrackPositionsPayload(tb)
	if checkedPositions {
		if _, err := writer.Write(checkedElement(tb, idCueTrackPositions, positionsPayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	} else if err := writer.WriteElement(idCueTrackPositions, positionsPayload); err != nil {
		tb.Fatal(err)
	}
	return point.Bytes()
}

func cueTrackPositionsPayload(tb testing.TB) []byte {
	tb.Helper()
	var positions bytes.Buffer
	writer := ebml.NewWriter(&positions)
	if err := writer.WriteUInt(idCueTrack, 1); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idCueClusterPosition, 0); err != nil {
		tb.Fatal(err)
	}
	return positions.Bytes()
}

func writeCuePointWithPositionsPayload(w *ebml.Writer, positionsPayload []byte) error {
	var point bytes.Buffer
	pw := ebml.NewWriter(&point)
	if err := pw.WriteUInt(idCueTime, 0); err != nil {
		return err
	}
	if err := pw.WriteElement(idCueTrackPositions, positionsPayload); err != nil {
		return err
	}
	return w.WriteElement(idCuePoint, point.Bytes())
}

func writeCuePointWithReferencePayload(w *ebml.Writer, referencePayload []byte) error {
	var positions bytes.Buffer
	tw := ebml.NewWriter(&positions)
	if err := tw.WriteUInt(idCueTrack, 1); err != nil {
		return err
	}
	if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
		return err
	}
	if err := tw.WriteElement(idCueReference, referencePayload); err != nil {
		return err
	}
	return writeCuePointWithPositionsPayload(w, positions.Bytes())
}

func audioPayload() []byte {
	var audio bytes.Buffer
	writer := ebml.NewWriter(&audio)
	if err := writer.WriteFloat64(idSamplingFreq, 48000); err != nil {
		panic(err)
	}
	if err := writer.WriteUInt(idChannels, 2); err != nil {
		panic(err)
	}
	return audio.Bytes()
}

func masteringPayload() []byte {
	var metadata bytes.Buffer
	writer := ebml.NewWriter(&metadata)
	if err := writer.WriteFloat64(idLuminanceMax, 1000); err != nil {
		panic(err)
	}
	if err := writer.WriteFloat64(idLuminanceMin, 0.01); err != nil {
		panic(err)
	}
	return metadata.Bytes()
}

func projectionPayload() []byte {
	var projection bytes.Buffer
	writer := ebml.NewWriter(&projection)
	if err := writer.WriteUInt(idProjectionType, 0); err != nil {
		panic(err)
	}
	return projection.Bytes()
}

func attachedFilePayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteString(idFileName, "note.txt"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idFileMediaType, "text/plain"); err != nil {
		tb.Fatal(err)
	}
	if err := writeBinary(writer, idFileData, []byte("hello")); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idFileUID, 1); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func seekHeadPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writeSeekEntry(writer, idInfo, 0); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func elementPayload(tb testing.TB, id ebml.ID, writeElement func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeElement(writer); err != nil {
		tb.Fatal(err)
	}
	reader := ebml.NewReader(bytes.NewReader(buffer.Bytes()), ebml.ReaderOptions{})
	header, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if header.ID != id || header.Size.Unknown {
		tb.Fatalf("element header = %+v, want id 0x%x", header, uint64(id))
	}
	payload, err := readBinaryPayload(reader, header.Size.Value)
	if err != nil {
		tb.Fatal(err)
	}
	if reader.Offset() != int64(len(buffer.Bytes())) {
		tb.Fatalf("payload length left reader at %d, want %d", reader.Offset(), len(buffer.Bytes()))
	}
	return payload
}

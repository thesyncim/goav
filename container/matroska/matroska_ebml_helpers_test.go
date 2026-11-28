package matroska

import (
	"bytes"
	"testing"

	"github.com/thesyncim/goav/container/ebml"
)

type ebmlHeaderFixture struct {
	EBMLVersion                 uint64
	EBMLReadVersion             uint64
	EBMLMaxIDLength             uint64
	EBMLMaxSizeLength           uint64
	DocType                     string
	DocTypeVersion              uint64
	DocTypeReadVersion          uint64
	EBMLVersionSet              bool
	EBMLReadVersionSet          bool
	EBMLMaxIDLengthSet          bool
	EBMLMaxSizeLengthSet        bool
	DocTypeSet                  bool
	DocTypeVersionSet           bool
	DocTypeReadVersionSet       bool
	DuplicateEBMLVersion        bool
	DuplicateEBMLReadVersion    bool
	DuplicateEBMLMaxIDLength    bool
	DuplicateEBMLMaxSizeLength  bool
	DuplicateDocType            bool
	DuplicateDocTypeVersion     bool
	DuplicateDocTypeReadVersion bool
}

func defaultEBMLHeaderFixture() ebmlHeaderFixture {
	return ebmlHeaderFixture{
		EBMLVersion:           1,
		EBMLReadVersion:       1,
		EBMLMaxIDLength:       ebml.MaxIDWidth,
		EBMLMaxSizeLength:     ebml.MaxSizeWidth,
		DocType:               "matroska",
		DocTypeVersion:        defaultDocTypeVersion,
		DocTypeReadVersion:    defaultDocTypeReadVersion,
		EBMLVersionSet:        true,
		EBMLReadVersionSet:    true,
		EBMLMaxIDLengthSet:    true,
		EBMLMaxSizeLengthSet:  true,
		DocTypeSet:            true,
		DocTypeVersionSet:     true,
		DocTypeReadVersionSet: true,
	}
}

func makeEBMLHeaderMatroskaData(tb testing.TB, header ebmlHeaderFixture) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeEBMLHeaderFixture(writer, header); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	if err := writeInfoWithElements(writer, nil); err != nil {
		tb.Fatal(err)
	}
	if err := writeTracksWithVideoDimensions(writer, 16, 16); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteElement(idCluster, nil); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func writeEBMLHeaderFixture(writer *ebml.Writer, header ebmlHeaderFixture) error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if header.EBMLVersionSet {
		if err := w.WriteUInt(idEBMLVersion, header.EBMLVersion); err != nil {
			return err
		}
		if header.DuplicateEBMLVersion {
			if err := w.WriteUInt(idEBMLVersion, header.EBMLVersion); err != nil {
				return err
			}
		}
	}
	if header.EBMLReadVersionSet {
		if err := w.WriteUInt(idEBMLReadVersion, header.EBMLReadVersion); err != nil {
			return err
		}
		if header.DuplicateEBMLReadVersion {
			if err := w.WriteUInt(idEBMLReadVersion, header.EBMLReadVersion); err != nil {
				return err
			}
		}
	}
	if header.EBMLMaxIDLengthSet {
		if err := w.WriteUInt(idEBMLMaxIDLength, header.EBMLMaxIDLength); err != nil {
			return err
		}
		if header.DuplicateEBMLMaxIDLength {
			if err := w.WriteUInt(idEBMLMaxIDLength, header.EBMLMaxIDLength); err != nil {
				return err
			}
		}
	}
	if header.EBMLMaxSizeLengthSet {
		if err := w.WriteUInt(idEBMLMaxSizeLength, header.EBMLMaxSizeLength); err != nil {
			return err
		}
		if header.DuplicateEBMLMaxSizeLength {
			if err := w.WriteUInt(idEBMLMaxSizeLength, header.EBMLMaxSizeLength); err != nil {
				return err
			}
		}
	}
	if header.DocTypeSet {
		if err := w.WriteString(idDocType, header.DocType); err != nil {
			return err
		}
		if header.DuplicateDocType {
			if err := w.WriteString(idDocType, header.DocType); err != nil {
				return err
			}
		}
	}
	if header.DocTypeVersionSet {
		if err := w.WriteUInt(idDocTypeVersion, header.DocTypeVersion); err != nil {
			return err
		}
		if header.DuplicateDocTypeVersion {
			if err := w.WriteUInt(idDocTypeVersion, header.DocTypeVersion); err != nil {
				return err
			}
		}
	}
	if header.DocTypeReadVersionSet {
		if err := w.WriteUInt(idDocTypeReadVersion, header.DocTypeReadVersion); err != nil {
			return err
		}
		if header.DuplicateDocTypeReadVersion {
			if err := w.WriteUInt(idDocTypeReadVersion, header.DocTypeReadVersion); err != nil {
				return err
			}
		}
	}
	return writer.WriteElement(idEBML, payload.Bytes())
}

func makeSeekHeadMetadataMatroskaData(tb testing.TB, writeSeekHead func(*ebml.Writer) error) []byte {
	tb.Helper()
	return makeTopLevelMetadataMatroskaData(tb, func(writer *ebml.Writer) error {
		var payload bytes.Buffer
		w := ebml.NewWriter(&payload)
		if err := writeSeekHead(w); err != nil {
			return err
		}
		return writer.WriteElement(idSeekHead, payload.Bytes())
	})
}

func makeDeferredRequiredMetadataMatroskaData(tb testing.TB, withSeekHead bool) []byte {
	tb.Helper()
	infoElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeInfoWithElements(w, nil)
	})
	tracksElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeTracksWithVideoDimensions(w, 16, 16)
	})
	clusterElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		var payload bytes.Buffer
		cw := ebml.NewWriter(&payload)
		if err := cw.WriteUInt(idTimestamp, 0); err != nil {
			return err
		}
		if err := writeSimpleBlockWithTrackNumber(cw, 1, []byte{0x77}); err != nil {
			return err
		}
		return w.WriteElement(idCluster, payload.Bytes())
	})
	var seekHeadElement []byte
	if withSeekHead {
		seekHeadElement = seekHeadElementForDeferredMetadata(tb, len(clusterElement), len(infoElement))
	}

	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeEBMLHeaderFixture(writer, defaultEBMLHeaderFixture()); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	for _, element := range [][]byte{seekHeadElement, clusterElement, infoElement, tracksElement} {
		if len(element) == 0 {
			continue
		}
		if _, err := writer.Write(element); err != nil {
			tb.Fatal(err)
		}
	}
	return buffer.Bytes()
}

func makeDeferredOptionalMetadataMatroskaData(tb testing.TB, requiredMetadataBeforeCluster bool, attachments []Attachment, chapters []ChapterEdition, tags []Tag) []byte {
	tb.Helper()
	infoElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeInfoWithElements(w, nil)
	})
	tracksElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeTracksWithVideoDimensions(w, 16, 16)
	})
	attachmentsElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeAttachmentsElement(w, attachments...)
	})
	chaptersElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeChaptersElement(w, chapters...)
	})
	tagsElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeTagsElement(w, tags...)
	})
	clusterElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		var payload bytes.Buffer
		cw := ebml.NewWriter(&payload)
		if err := cw.WriteUInt(idTimestamp, 0); err != nil {
			return err
		}
		if err := writeSimpleBlockWithTrackNumber(cw, 1, []byte{0x77}); err != nil {
			return err
		}
		return w.WriteElement(idCluster, payload.Bytes())
	})
	lateElements := []struct {
		id   ebml.ID
		data []byte
	}{
		{id: idAttachments, data: attachmentsElement},
		{id: idChapters, data: chaptersElement},
		{id: idTags, data: tagsElement},
	}
	positionPrefix := len(clusterElement)
	if !requiredMetadataBeforeCluster {
		lateElements = append([]struct {
			id   ebml.ID
			data []byte
		}{
			{id: idInfo, data: infoElement},
			{id: idTracks, data: tracksElement},
		}, lateElements...)
	} else {
		positionPrefix += len(infoElement) + len(tracksElement)
	}
	var seekHeadElement []byte
	seekHeadConverged := false
	for range 8 {
		positions := make([]uint64, len(lateElements))
		position := uint64(len(seekHeadElement) + positionPrefix)
		for i := range lateElements {
			positions[i] = position
			position += uint64(len(lateElements[i].data))
		}
		next := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
			var payload bytes.Buffer
			sw := ebml.NewWriter(&payload)
			for i := range lateElements {
				if err := writeSeekEntry(sw, lateElements[i].id, positions[i]); err != nil {
					return err
				}
			}
			return w.WriteElement(idSeekHead, payload.Bytes())
		})
		if len(next) == len(seekHeadElement) {
			seekHeadElement = next
			seekHeadConverged = true
			break
		}
		seekHeadElement = next
	}
	if !seekHeadConverged {
		tb.Fatal("SeekHead element size did not converge")
	}

	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeEBMLHeaderFixture(writer, defaultEBMLHeaderFixture()); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	elements := [][]byte{seekHeadElement}
	if requiredMetadataBeforeCluster {
		elements = append(elements, infoElement, tracksElement, clusterElement)
	} else {
		elements = append(elements, clusterElement, infoElement, tracksElement)
	}
	elements = append(elements, attachmentsElement, chaptersElement, tagsElement)
	for i := range elements {
		if _, err := writer.Write(elements[i]); err != nil {
			tb.Fatal(err)
		}
	}
	return buffer.Bytes()
}

func segmentChildElementBytes(tb testing.TB, writeElement func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeElement(writer); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func seekHeadElementForDeferredMetadata(tb testing.TB, clusterElementSize int, infoElementSize int) []byte {
	tb.Helper()
	var seekHead []byte
	for range 8 {
		infoPosition := uint64(len(seekHead) + clusterElementSize)
		tracksPosition := infoPosition + uint64(infoElementSize)
		next := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
			var payload bytes.Buffer
			sw := ebml.NewWriter(&payload)
			if err := writeSeekEntry(sw, idInfo, infoPosition); err != nil {
				return err
			}
			if err := writeSeekEntry(sw, idTracks, tracksPosition); err != nil {
				return err
			}
			return w.WriteElement(idSeekHead, payload.Bytes())
		})
		if len(next) == len(seekHead) {
			return next
		}
		seekHead = next
	}
	tb.Fatal("SeekHead element size did not converge")
	return nil
}

func writeSeekIDElement(w *ebml.Writer, id ebml.ID) error {
	var idPayload [ebml.MaxIDWidth]byte
	n, err := ebml.EncodeID(idPayload[:], id)
	if err != nil {
		return err
	}
	return writeBinary(w, idSeekID, idPayload[:n])
}

func writeMatroskaSegmentPrefix(tb testing.TB, muxer *Muxer) {
	tb.Helper()
	if err := muxer.writeEBMLHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	muxer.segmentData = muxer.ebml.Offset()
	if err := muxer.writeInfo(); err != nil {
		tb.Fatal(err)
	}
}

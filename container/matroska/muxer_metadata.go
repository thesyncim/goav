package matroska

import (
	"bytes"

	"github.com/thesyncim/goav/container/ebml"
)

func (m *Muxer) writeTracks() error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for i := range m.tracks {
		if err := writeTrackEntry(w, m.tracks[i], &m.scratch); err != nil {
			return err
		}
	}
	if err := writeUnknownElements(w, m.options.UnknownTracksElements); err != nil {
		return err
	}
	return m.writeMasterElement(idTracks, payload.Bytes())
}

func (m *Muxer) writeAttachments() error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for i := range m.options.Attachments {
		if err := writeAttachedFile(w, m.options.Attachments[i]); err != nil {
			return err
		}
	}
	if err := writeUnknownElements(w, m.options.UnknownAttachmentsElements); err != nil {
		return err
	}
	return m.writeMasterElement(idAttachments, payload.Bytes())
}

func (m *Muxer) writeChapters() error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	options := chapterWriteOptions{webm: m.options.DocType == "webm"}
	for i := range m.options.Chapters {
		if err := writeEditionEntryWithOptions(w, m.options.Chapters[i], options); err != nil {
			return err
		}
	}
	if err := writeUnknownElements(w, m.options.UnknownChaptersElements); err != nil {
		return err
	}
	return m.writeMasterElement(idChapters, payload.Bytes())
}

type chapterWriteOptions struct {
	webm bool
}

func writeEditionEntry(w *ebml.Writer, edition ChapterEdition) error {
	return writeEditionEntryWithOptions(w, edition, chapterWriteOptions{})
}

func writeEditionEntryWithOptions(w *ebml.Writer, edition ChapterEdition, options chapterWriteOptions) error {
	var payload bytes.Buffer
	ew := ebml.NewWriter(&payload)
	if options.webm {
		if edition.Hidden || edition.Default || edition.Ordered || len(edition.UnknownElements) != 0 {
			return ErrInvalidData
		}
	} else if edition.UID != 0 {
		if err := ew.WriteUInt(idEditionUID, edition.UID); err != nil {
			return err
		}
	}
	if !options.webm {
		if err := writeBoolElement(ew, idEditionFlagHidden, edition.Hidden); err != nil {
			return err
		}
		if err := writeBoolElement(ew, idEditionFlagDefault, edition.Default); err != nil {
			return err
		}
		if err := writeBoolElement(ew, idEditionFlagOrdered, edition.Ordered); err != nil {
			return err
		}
	}
	for i := range edition.Chapters {
		if err := writeChapterAtomWithOptions(ew, edition.Chapters[i], options); err != nil {
			return err
		}
	}
	if !options.webm {
		if err := writeUnknownElements(ew, edition.UnknownElements); err != nil {
			return err
		}
	}
	return w.WriteElement(idEditionEntry, payload.Bytes())
}

func writeChapterAtomWithOptions(w *ebml.Writer, chapter Chapter, options chapterWriteOptions) error {
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	if options.webm && (chapter.Hidden || len(chapter.TrackUIDs) != 0 || len(chapter.UnknownElements) != 0) {
		return ErrInvalidData
	}
	if err := cw.WriteUInt(idChapterUID, chapter.UID); err != nil {
		return err
	}
	if chapter.StringUID != "" {
		if err := cw.WriteString(idChapterStringUID, chapter.StringUID); err != nil {
			return err
		}
	}
	if err := cw.WriteUInt(idChapterTimeStart, uint64(chapter.StartNS)); err != nil {
		return err
	}
	if chapter.EndSet {
		if err := cw.WriteUInt(idChapterTimeEnd, uint64(chapter.EndNS)); err != nil {
			return err
		}
	}
	if !options.webm {
		if err := writeBoolElement(cw, idChapterFlagHidden, chapter.Hidden); err != nil {
			return err
		}
		if err := writeBoolElement(cw, idChapterFlagEnabled, chapter.Enabled); err != nil {
			return err
		}
		if len(chapter.TrackUIDs) != 0 {
			if err := writeChapterTrack(cw, chapter.TrackUIDs); err != nil {
				return err
			}
		}
	}
	for i := range chapter.Displays {
		if err := writeChapterDisplayWithOptions(cw, chapter.Displays[i], options); err != nil {
			return err
		}
	}
	for i := range chapter.Children {
		if err := writeChapterAtomWithOptions(cw, chapter.Children[i], options); err != nil {
			return err
		}
	}
	if !options.webm {
		if err := writeUnknownElements(cw, chapter.UnknownElements); err != nil {
			return err
		}
	}
	return w.WriteElement(idChapterAtom, payload.Bytes())
}

func writeChapterTrack(w *ebml.Writer, trackUIDs []uint64) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	for _, uid := range trackUIDs {
		if err := tw.WriteUInt(idChapterTrackUID, uid); err != nil {
			return err
		}
	}
	return w.WriteElement(idChapterTrack, payload.Bytes())
}

func writeChapterDisplayWithOptions(w *ebml.Writer, display ChapterDisplay, options chapterWriteOptions) error {
	var payload bytes.Buffer
	dw := ebml.NewWriter(&payload)
	if options.webm && len(display.UnknownElements) != 0 {
		return ErrInvalidData
	}
	if err := dw.WriteString(idChapString, display.String); err != nil {
		return err
	}
	language := display.Language
	if language == "" && display.LanguageBCP47 == "" {
		language = "eng"
	}
	if language != "" {
		if err := dw.WriteString(idChapLanguage, language); err != nil {
			return err
		}
	}
	if display.LanguageBCP47 != "" {
		if err := dw.WriteString(idChapLanguageBCP47, display.LanguageBCP47); err != nil {
			return err
		}
	}
	if display.Country != "" {
		if err := dw.WriteString(idChapCountry, display.Country); err != nil {
			return err
		}
	}
	if !options.webm {
		if err := writeUnknownElements(dw, display.UnknownElements); err != nil {
			return err
		}
	}
	return w.WriteElement(idChapterDisplay, payload.Bytes())
}

func (m *Muxer) writeTags() error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for i := range m.options.Tags {
		if err := writeTag(w, m.options.Tags[i]); err != nil {
			return err
		}
	}
	if err := writeUnknownElements(w, m.options.UnknownTagsElements); err != nil {
		return err
	}
	return m.writeMasterElement(idTags, payload.Bytes())
}

func (m *Muxer) writeUnknownSegmentElements() error {
	return writeUnknownElements(m.ebml, m.options.UnknownSegmentElements)
}

func (m *Muxer) writeMasterElement(id ebml.ID, payload []byte) error {
	if !m.options.WriteCRC32 {
		return m.ebml.WriteElement(id, payload)
	}
	return writeElementWithCRC32(m.ebml, id, payload)
}

func writeElementWithCRC32(w *ebml.Writer, id ebml.ID, payload []byte) error {
	protected, err := payloadWithCRC32(payload)
	if err != nil {
		return err
	}
	return w.WriteElement(id, protected)
}

func payloadWithCRC32(payload []byte) ([]byte, error) {
	var protected bytes.Buffer
	w := ebml.NewWriter(&protected)
	if err := w.WriteCRC32(payload); err != nil {
		return nil, err
	}
	if _, err := w.Write(payload); err != nil {
		return nil, err
	}
	return protected.Bytes(), nil
}

func writeUnknownElements(w *ebml.Writer, elements []UnknownElement) error {
	for i := range elements {
		if _, err := w.Write(elements[i].Raw); err != nil {
			return err
		}
	}
	return nil
}

func writeTag(w *ebml.Writer, tag Tag) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if err := writeTagTargets(tw, tag.Target); err != nil {
		return err
	}
	for i := range tag.Simple {
		if err := writeSimpleTag(tw, tag.Simple[i]); err != nil {
			return err
		}
	}
	if err := writeUnknownElements(tw, tag.UnknownElements); err != nil {
		return err
	}
	return w.WriteElement(idTag, payload.Bytes())
}

func writeTagTargets(w *ebml.Writer, target TagTarget) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if target.TypeValue != 0 {
		if err := tw.WriteUInt(idTargetTypeValue, target.TypeValue); err != nil {
			return err
		}
	}
	if target.Type != "" {
		if err := tw.WriteString(idTargetType, target.Type); err != nil {
			return err
		}
	}
	for _, uid := range target.TrackUIDs {
		if err := tw.WriteUInt(idTagTrackUID, uid); err != nil {
			return err
		}
	}
	for _, uid := range target.EditionUIDs {
		if err := tw.WriteUInt(idTagEditionUID, uid); err != nil {
			return err
		}
	}
	for _, uid := range target.ChapterUIDs {
		if err := tw.WriteUInt(idTagChapterUID, uid); err != nil {
			return err
		}
	}
	for _, uid := range target.AttachmentUIDs {
		if err := tw.WriteUInt(idTagAttachmentUID, uid); err != nil {
			return err
		}
	}
	if err := writeUnknownElements(tw, target.UnknownElements); err != nil {
		return err
	}
	return w.WriteElement(idTargets, payload.Bytes())
}

func writeSimpleTag(w *ebml.Writer, tag SimpleTag) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if err := tw.WriteString(idTagName, tag.Name); err != nil {
		return err
	}
	if tag.Language != "" {
		if err := tw.WriteString(idTagLanguage, tag.Language); err != nil {
			return err
		}
	}
	if tag.LanguageBCP47 != "" {
		if err := tw.WriteString(idTagLanguageBCP47, tag.LanguageBCP47); err != nil {
			return err
		}
	}
	if err := writeBoolElement(tw, idTagDefault, tag.Default); err != nil {
		return err
	}
	if tag.StringSet {
		if err := tw.WriteString(idTagString, tag.String); err != nil {
			return err
		}
	}
	if tag.Binary != nil {
		if err := writeBinary(tw, idTagBinary, tag.Binary); err != nil {
			return err
		}
	}
	for i := range tag.Children {
		if err := writeSimpleTag(tw, tag.Children[i]); err != nil {
			return err
		}
	}
	if err := writeUnknownElements(tw, tag.UnknownElements); err != nil {
		return err
	}
	return w.WriteElement(idSimpleTag, payload.Bytes())
}

func writeBoolElement(w *ebml.Writer, id ebml.ID, value bool) error {
	if value {
		return w.WriteUInt(id, 1)
	}
	return w.WriteUInt(id, 0)
}

func writeAttachedFile(w *ebml.Writer, attachment Attachment) error {
	var payload bytes.Buffer
	aw := ebml.NewWriter(&payload)
	if attachment.Description != "" {
		if err := aw.WriteString(idFileDescription, attachment.Description); err != nil {
			return err
		}
	}
	if err := aw.WriteString(idFileName, attachment.Filename); err != nil {
		return err
	}
	if err := aw.WriteString(idFileMediaType, attachment.MIMEType); err != nil {
		return err
	}
	if err := writeBinary(aw, idFileData, attachment.Data); err != nil {
		return err
	}
	if err := aw.WriteUInt(idFileUID, attachment.UID); err != nil {
		return err
	}
	if err := writeUnknownElements(aw, attachment.UnknownElements); err != nil {
		return err
	}
	return w.WriteElement(idAttachedFile, payload.Bytes())
}


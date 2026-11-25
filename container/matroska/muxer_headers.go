package matroska

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"slices"
	"sort"

	"github.com/thesyncim/goav/container/ebml"
)

func (m *Muxer) writeEBMLHeader() error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteUInt(idEBMLVersion, 1); err != nil {
		return err
	}
	if err := w.WriteUInt(idEBMLReadVersion, 1); err != nil {
		return err
	}
	if err := w.WriteUInt(idEBMLMaxIDLength, ebml.MaxIDWidth); err != nil {
		return err
	}
	if err := w.WriteUInt(idEBMLMaxSizeLength, ebml.MaxSizeWidth); err != nil {
		return err
	}
	if err := w.WriteString(idDocType, m.options.DocType); err != nil {
		return err
	}
	if err := w.WriteUInt(idDocTypeVersion, m.options.DocTypeVersion); err != nil {
		return err
	}
	if err := w.WriteUInt(idDocTypeReadVersion, m.options.DocTypeReadVersion); err != nil {
		return err
	}
	return m.ebml.WriteElement(idEBML, payload.Bytes())
}

func (m *Muxer) writeInfo() error {
	if !m.options.Streaming && m.seekable {
		return m.writeSeekableInfo()
	}
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := m.writeInfoFields(w, false); err != nil {
		return err
	}
	return m.writeMasterElement(idInfo, payload.Bytes())
}

func (m *Muxer) writeSeekableInfo() error {
	patch, err := m.ebml.StartSizedElement(idInfo, 4)
	if err != nil {
		return err
	}
	if err := m.writeInfoFields(m.ebml, true); err != nil {
		return err
	}
	return m.ebml.FinishSizedElement(patch)
}

func (m *Muxer) writeInfoFields(w *ebml.Writer, includeDuration bool) error {
	info := m.options.Info
	if err := writeOptionalBinary(w, idSegmentUUID, info.SegmentUUID); err != nil {
		return err
	}
	if info.SegmentFilename != "" {
		if err := w.WriteString(idSegmentFilename, info.SegmentFilename); err != nil {
			return err
		}
	}
	if err := writeOptionalBinary(w, idPrevUUID, info.PrevUUID); err != nil {
		return err
	}
	if info.PrevFilename != "" {
		if err := w.WriteString(idPrevFilename, info.PrevFilename); err != nil {
			return err
		}
	}
	if err := writeOptionalBinary(w, idNextUUID, info.NextUUID); err != nil {
		return err
	}
	if info.NextFilename != "" {
		if err := w.WriteString(idNextFilename, info.NextFilename); err != nil {
			return err
		}
	}
	if err := w.WriteUInt(idTimestampScale, uint64(m.options.TimecodeScaleNS)); err != nil {
		return err
	}
	if includeDuration {
		if err := w.WriteHeader(idDuration, 8); err != nil {
			return err
		}
		m.durationOffset = w.Offset()
		clear(m.blockScratch[:8])
		if _, err := w.Write(m.blockScratch[:8]); err != nil {
			return err
		}
		m.durationPatch = true
	} else if info.DurationSet {
		if err := w.WriteFloat64(idDuration, float64(info.DurationNS)/float64(m.options.TimecodeScaleNS)); err != nil {
			return err
		}
	}
	if info.DateUTCSet {
		if err := writeDate(w, idDateUTC, info.DateUTC); err != nil {
			return err
		}
	}
	if info.Title != "" {
		if err := w.WriteString(idTitle, info.Title); err != nil {
			return err
		}
	}
	if err := w.WriteString(idMuxingApp, m.options.MuxingApp); err != nil {
		return err
	}
	if err := w.WriteString(idWritingApp, m.options.WritingApp); err != nil {
		return err
	}
	return writeUnknownElements(w, info.UnknownElements)
}

func writeOptionalBinary(w *ebml.Writer, id ebml.ID, value []byte) error {
	if len(value) == 0 {
		return nil
	}
	return writeBinary(w, id, value)
}

func validateSegmentInfo(info SegmentInfo) error {
	if err := validateSegmentUUID(info.SegmentUUID); err != nil {
		return err
	}
	if err := validateSegmentUUID(info.PrevUUID); err != nil {
		return err
	}
	if err := validateSegmentUUID(info.NextUUID); err != nil {
		return err
	}
	if len(info.SegmentUUID) != 0 {
		if bytes.Equal(info.SegmentUUID, info.PrevUUID) || bytes.Equal(info.SegmentUUID, info.NextUUID) {
			return ErrInvalidData
		}
	}
	if info.DateUTCSet {
		if _, err := ebmlDateNanos(info.DateUTC); err != nil {
			return err
		}
	}
	if info.DurationSet && info.DurationNS < 0 {
		return ErrInvalidData
	}
	return validateUnknownElementsFor(info.UnknownElements, isKnownInfoElement)
}

func normalizeSegmentInfo(info SegmentInfo) (SegmentInfo, error) {
	out := cloneSegmentInfo(info)
	unknownElements, err := normalizeUnknownElementsFor(out.UnknownElements, isKnownInfoElement)
	if err != nil {
		return SegmentInfo{}, err
	}
	out.UnknownElements = unknownElements
	if err := validateSegmentInfo(out); err != nil {
		return SegmentInfo{}, err
	}
	return out, nil
}

func validateSegmentUUID(value []byte) error {
	if len(value) == 0 {
		return nil
	}
	if len(value) != 16 {
		return ErrInvalidData
	}
	for i := range value {
		if value[i] != 0 {
			return nil
		}
	}
	return ErrInvalidData
}

func normalizeAttachments(attachments []Attachment) ([]Attachment, error) {
	if len(attachments) == 0 {
		return nil, nil
	}
	out := cloneAttachments(attachments)
	used := make(map[uint64]struct{}, len(out))
	for i := range out {
		if out[i].UID == 0 {
			continue
		}
		if _, ok := used[out[i].UID]; ok {
			return nil, ErrInvalidData
		}
		used[out[i].UID] = struct{}{}
	}
	var next uint64 = 1
	for i := range out {
		if out[i].UID == 0 {
			for {
				if _, ok := used[next]; !ok {
					break
				}
				next++
			}
			out[i].UID = next
			used[next] = struct{}{}
		}
		unknownElements, err := normalizeUnknownElementsFor(out[i].UnknownElements, isKnownAttachedFileElement)
		if err != nil {
			return nil, err
		}
		out[i].UnknownElements = unknownElements
		if err := validateAttachment(out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func validateAttachment(attachment Attachment) error {
	if attachment.UID == 0 || attachment.Filename == "" || attachment.MIMEType == "" || attachment.Data == nil {
		return ErrInvalidData
	}
	return validateUnknownElementsFor(attachment.UnknownElements, isKnownAttachedFileElement)
}

func normalizeChapters(editions []ChapterEdition) ([]ChapterEdition, error) {
	if len(editions) == 0 {
		return nil, nil
	}
	out := cloneChapters(editions)
	var nextUID uint64 = 1
	usedEditions := make(map[uint64]struct{}, len(out))
	usedChapters := make(map[uint64]struct{})
	for i := range out {
		if out[i].UID != 0 {
			if _, ok := usedEditions[out[i].UID]; ok {
				return nil, ErrInvalidData
			}
			usedEditions[out[i].UID] = struct{}{}
		}
		if len(out[i].Chapters) == 0 {
			return nil, ErrInvalidData
		}
		unknownElements, err := normalizeUnknownElementsFor(out[i].UnknownElements, isKnownEditionEntryElement)
		if err != nil {
			return nil, err
		}
		out[i].UnknownElements = unknownElements
	}
	for i := range out {
		if out[i].UID == 0 {
			for {
				if _, ok := usedEditions[nextUID]; !ok {
					break
				}
				nextUID++
			}
			out[i].UID = nextUID
			usedEditions[nextUID] = struct{}{}
		}
		if err := normalizeChapterList(out[i].Chapters, &nextUID, usedChapters); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func normalizeChapterList(chapters []Chapter, nextUID *uint64, used map[uint64]struct{}) error {
	for i := range chapters {
		if chapters[i].UID == 0 {
			for {
				if _, ok := used[*nextUID]; !ok {
					break
				}
				(*nextUID)++
			}
			chapters[i].UID = *nextUID
			used[*nextUID] = struct{}{}
		} else {
			if _, ok := used[chapters[i].UID]; ok {
				return ErrInvalidData
			}
			used[chapters[i].UID] = struct{}{}
		}
		if !chapters[i].EnabledSet {
			chapters[i].Enabled = true
			chapters[i].EnabledSet = true
		}
		unknownElements, err := normalizeUnknownElementsFor(chapters[i].UnknownElements, isKnownChapterAtomElement)
		if err != nil {
			return err
		}
		chapters[i].UnknownElements = unknownElements
		for j := range chapters[i].Displays {
			unknownElements, err := normalizeUnknownElementsFor(chapters[i].Displays[j].UnknownElements, isKnownChapterDisplayElement)
			if err != nil {
				return err
			}
			chapters[i].Displays[j].UnknownElements = unknownElements
		}
		if err := validateChapter(chapters[i]); err != nil {
			return err
		}
		for j := range chapters[i].Displays {
			if chapters[i].Displays[j].Language == "" {
				chapters[i].Displays[j].Language = "eng"
			}
		}
		if err := normalizeChapterList(chapters[i].Children, nextUID, used); err != nil {
			return err
		}
	}
	return nil
}

func validateChapter(chapter Chapter) error {
	if chapter.UID == 0 || chapter.StartNS < 0 || (chapter.EndSet && chapter.EndNS < chapter.StartNS) {
		return ErrInvalidData
	}
	if slices.Contains(chapter.TrackUIDs, 0) {
		return ErrInvalidData
	}
	for i := range chapter.Displays {
		if chapter.Displays[i].String == "" {
			return ErrInvalidData
		}
		if err := validateUnknownElementsFor(chapter.Displays[i].UnknownElements, isKnownChapterDisplayElement); err != nil {
			return err
		}
	}
	return validateUnknownElementsFor(chapter.UnknownElements, isKnownChapterAtomElement)
}

func normalizeTags(tags []Tag) ([]Tag, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	out := cloneTags(tags)
	for i := range out {
		if len(out[i].Simple) == 0 {
			return nil, ErrInvalidData
		}
		if out[i].Target.TypeValue == 0 {
			out[i].Target.TypeValue = 50
		}
		unknownElements, err := normalizeUnknownElementsFor(out[i].UnknownElements, isKnownTagElement)
		if err != nil {
			return nil, err
		}
		out[i].UnknownElements = unknownElements
		unknownElements, err = normalizeUnknownElementsFor(out[i].Target.UnknownElements, isKnownTagTargetsElement)
		if err != nil {
			return nil, err
		}
		out[i].Target.UnknownElements = unknownElements
		if err := validateTagTarget(out[i].Target); err != nil {
			return nil, err
		}
		if err := normalizeSimpleTags(out[i].Simple); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func normalizeUnknownElements(elements []UnknownElement) ([]UnknownElement, error) {
	return normalizeUnknownElementsFor(elements, isKnownSegmentElement)
}

func normalizeUnknownElementsFor(elements []UnknownElement, isKnown func(ebml.ID) bool) ([]UnknownElement, error) {
	if len(elements) == 0 {
		return nil, nil
	}
	out := cloneUnknownElements(elements)
	for i := range out {
		id, err := validateUnknownElement(out[i], isKnown)
		if err != nil {
			return nil, err
		}
		out[i].ID = uint64(id)
	}
	return out, nil
}

func validateUnknownElementsFor(elements []UnknownElement, isKnown func(ebml.ID) bool) error {
	for i := range elements {
		if _, err := validateUnknownElement(elements[i], isKnown); err != nil {
			return err
		}
	}
	return nil
}

func validateUnknownElement(element UnknownElement, isKnown func(ebml.ID) bool) (ebml.ID, error) {
	if len(element.Raw) == 0 {
		return 0, ErrInvalidData
	}
	reader := ebml.NewReader(bytes.NewReader(element.Raw), ebml.ReaderOptions{})
	header, err := reader.ReadHeader()
	if err != nil {
		return 0, err
	}
	if header.Size.Unknown {
		return 0, ErrInvalidData
	}
	if element.ID != 0 && element.ID != uint64(header.ID) {
		return 0, ErrInvalidData
	}
	if isKnown != nil && isKnown(header.ID) {
		return 0, ErrInvalidData
	}
	if header.DataOffset+int64(header.Size.Value) != int64(len(element.Raw)) {
		return 0, ErrInvalidData
	}
	return header.ID, nil
}

func isKnownSegmentElement(id ebml.ID) bool {
	switch id {
	case idSeekHead, idInfo, idTracks, idCluster, idCues, idAttachments, idChapters, idTags, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownTracksElement(id ebml.ID) bool {
	switch id {
	case idTrackEntry, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownAttachmentsElement(id ebml.ID) bool {
	switch id {
	case idAttachedFile, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownChaptersElement(id ebml.ID) bool {
	switch id {
	case idEditionEntry, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownTagsElement(id ebml.ID) bool {
	switch id {
	case idTag, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownClusterElement(id ebml.ID) bool {
	switch id {
	case idTimestamp, idSimpleBlock, idBlockGroup, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownInfoElement(id ebml.ID) bool {
	switch id {
	case idSegmentUUID, idSegmentFilename, idPrevUUID, idPrevFilename, idNextUUID, idNextFilename,
		idTimestampScale, idDuration, idDateUTC, idTitle, idMuxingApp, idWritingApp, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownTrackEntryElement(id ebml.ID) bool {
	switch id {
	case idTrackNumber, idTrackUID, idTrackType, idFlagEnabled, idFlagDefault, idFlagForced,
		idFlagHearingImpaired, idFlagVisualImpaired, idFlagTextDescriptions, idFlagOriginal,
		idFlagCommentary, idFlagLacing, idName, idLanguage, idLanguageBCP, idCodecID,
		idCodecName, idCodecPrivate, idCodecDecodeAll, idCodecDelay, idSeekPreRoll,
		idMinCache, idMaxCache, idTrackTimestampScale, idTrackOffset, idTrackOverlay,
		idTrackTranslate, idContentEncodings, idVideo, idAudio, idDefaultDur,
		idDefaultDecodedDur, idMaxBlockAdditionID, idBlockAdditionMapping, idVoid,
		idCRC32:
		return true
	default:
		return false
	}
}

func isKnownAttachedFileElement(id ebml.ID) bool {
	switch id {
	case idFileDescription, idFileName, idFileMediaType, idFileData, idFileUID, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownEditionEntryElement(id ebml.ID) bool {
	switch id {
	case idEditionUID, idEditionFlagHidden, idEditionFlagDefault, idEditionFlagOrdered, idChapterAtom, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownChapterAtomElement(id ebml.ID) bool {
	switch id {
	case idChapterUID, idChapterStringUID, idChapterTimeStart, idChapterTimeEnd, idChapterFlagHidden,
		idChapterFlagEnabled, idChapterTrack, idChapterDisplay, idChapterAtom, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownChapterDisplayElement(id ebml.ID) bool {
	switch id {
	case idChapString, idChapLanguage, idChapLanguageBCP47, idChapCountry, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownTagElement(id ebml.ID) bool {
	switch id {
	case idTargets, idSimpleTag, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownTagTargetsElement(id ebml.ID) bool {
	switch id {
	case idTargetTypeValue, idTargetType, idTagTrackUID, idTagEditionUID, idTagChapterUID,
		idTagAttachmentUID, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func isKnownSimpleTagElement(id ebml.ID) bool {
	switch id {
	case idTagName, idTagLanguage, idTagLanguageBCP47, idTagDefault, idTagString, idTagBinary,
		idSimpleTag, idVoid, idCRC32:
		return true
	default:
		return false
	}
}

func validateTagTarget(target TagTarget) error {
	if target.TypeValue > uint64(^uint32(0)) {
		return ErrInvalidData
	}
	return validateUnknownElementsFor(target.UnknownElements, isKnownTagTargetsElement)
}

func normalizeSimpleTags(tags []SimpleTag) error {
	for i := range tags {
		if tags[i].Name == "" || (tags[i].StringSet && tags[i].Binary != nil) {
			return ErrInvalidData
		}
		unknownElements, err := normalizeUnknownElementsFor(tags[i].UnknownElements, isKnownSimpleTagElement)
		if err != nil {
			return err
		}
		tags[i].UnknownElements = unknownElements
		if tags[i].Language == "" {
			tags[i].Language = "und"
		}
		if !tags[i].DefaultSet {
			tags[i].Default = true
			tags[i].DefaultSet = true
		}
		if err := normalizeSimpleTags(tags[i].Children); err != nil {
			return err
		}
	}
	return nil
}

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

func (m *Muxer) startCluster(timecode int64) error {
	if m.clusterSized {
		if err := m.ebml.FinishSizedElement(m.clusterPatch); err != nil {
			return err
		}
		m.clusterSized = false
	}
	clusterOffset := m.ebml.Offset()
	if m.segmentData != 0 && clusterOffset >= m.segmentData {
		m.clusterPosition = uint64(clusterOffset - m.segmentData)
	}
	if !m.options.Streaming && m.seekable {
		patch, err := m.ebml.StartSizedElement(idCluster, ebml.MaxSizeWidth)
		if err != nil {
			return err
		}
		m.clusterPatch = patch
		m.clusterSized = true
	} else {
		if err := m.ebml.WriteUnknownHeader(idCluster, ebml.MaxSizeWidth); err != nil {
			return err
		}
	}
	if err := m.ebml.WriteUInt(idTimestamp, uint64(timecode)); err != nil {
		return err
	}
	m.clusterOpen = true
	m.clusterTimecode = timecode
	m.clusterBlock = 0
	return nil
}

func (m *Muxer) writeSeekHeadPlaceholder() error {
	const reserve = 512
	m.seekHeadOffset = m.ebml.Offset()
	m.seekHeadReserve = reserve
	return writeVoidTotal(m.ebml, reserve)
}

func (m *Muxer) writeSimpleBlock(packet Packet, blockTimecode int16, track Track) error {
	if packet.DurationNS > 0 ||
		len(packet.ReferenceBlockTimeNS) != 0 ||
		packet.ReferencePriority != 0 ||
		packet.DiscardPaddingNS != 0 ||
		len(packet.CodecState) != 0 ||
		len(packet.BlockAdditions) != 0 {
		return m.writeBlockGroup(packet, blockTimecode, track)
	}
	return m.writeBlock(idSimpleBlock, packet, blockTimecode, simpleBlockFlags(packet), track)
}

func (m *Muxer) addCue(packet Packet, track Track, timecode int64, relativePosition uint64, blockNumber uint64) {
	if !m.shouldCuePacket(packet, track) {
		return
	}
	position := CueTrackPosition{
		TrackID:             packet.TrackID,
		ClusterPosition:     m.clusterPosition,
		RelativePosition:    relativePosition,
		RelativePositionSet: true,
		BlockNumber:         blockNumber,
		BlockNumberSet:      blockNumber != 0,
	}
	if packet.DurationNS > 0 {
		position.DurationNS = packet.DurationNS
		position.DurationSet = true
	}
	if len(packet.ReferenceBlockTimeNS) != 0 {
		position.References = m.cueReferences(packet)
	}
	m.addCuePosition(timecode*m.options.TimecodeScaleNS, position)
}

func (m *Muxer) addCuePosition(timeNS int64, position CueTrackPosition) {
	for i := range m.cues {
		if m.cues[i].TimeNS != timeNS {
			continue
		}
		if cueHasTrackPosition(m.cues[i], position.TrackID) {
			continue
		}
		if len(m.cues[i].Positions) == 0 && m.cues[i].TrackID != 0 {
			m.cues[i].Positions = append(m.cues[i].Positions, cueTrackPositionFromLegacy(m.cues[i]))
		}
		m.cues[i].Positions = append(m.cues[i].Positions, position)
		return
	}
	cue := CuePoint{
		TimeNS:    timeNS,
		Positions: []CueTrackPosition{position},
	}
	applyCuePosition(&cue, position)
	m.cues = append(m.cues, cue)
}

func cueHasTrackPosition(cue CuePoint, trackID uint32) bool {
	if len(cue.Positions) == 0 {
		return cue.TrackID == trackID
	}
	for i := range cue.Positions {
		if cue.Positions[i].TrackID == trackID {
			return true
		}
	}
	return false
}

func (m *Muxer) cueReferences(packet Packet) []CueReference {
	var references []CueReference
	for i := range packet.ReferenceBlockTimeNS {
		timeNS, ok := cueReferenceTimeNS(packet.TimeNS, packet.ReferenceBlockTimeNS[i])
		if !ok {
			continue
		}
		position, ok := m.cuePositionForReference(packet.TrackID, timeNS)
		if !ok {
			continue
		}
		references = append(references, CueReference{
			TimeNS:             timeNS,
			ClusterPosition:    position.ClusterPosition,
			BlockNumber:        position.BlockNumber,
			BlockNumberSet:     position.BlockNumberSet,
			CodecStatePosition: position.CodecStatePosition,
			CodecStateSet:      position.CodecStateSet,
		})
	}
	return references
}

func cueReferenceTimeNS(packetTimeNS int64, offsetNS int64) (int64, bool) {
	if offsetNS > 0 && packetTimeNS > math.MaxInt64-offsetNS {
		return 0, false
	}
	if offsetNS < 0 {
		if offsetNS == math.MinInt64 || packetTimeNS < -offsetNS {
			return 0, false
		}
	}
	return packetTimeNS + offsetNS, true
}

func (m *Muxer) cuePositionForReference(trackID uint32, timeNS int64) (CueTrackPosition, bool) {
	for _, v := range slices.Backward(m.cues) {
		if v.TimeNS != timeNS {
			continue
		}
		if position, ok := cuePositionForTrack(v, trackID); ok {
			return position, true
		}
	}
	return CueTrackPosition{}, false
}

func (m *Muxer) shouldCuePacket(packet Packet, track Track) bool {
	if !m.collectsCues() {
		return false
	}
	switch m.options.CuePolicy {
	case CuePolicyNone:
		return false
	case CuePolicyAllPackets:
		return true
	case CuePolicyKeyframes:
		return packet.Keyframe
	default:
		return packet.Keyframe || track.Type == TrackAudio
	}
}

func (m *Muxer) collectsCues() bool {
	return !m.options.Streaming && m.seekable && m.options.CuePolicy != CuePolicyNone
}

func (m *Muxer) hasCuesToWrite() bool {
	return m.collectsCues() && len(m.cues) != 0
}

func (m *Muxer) writeCues() error {
	m.cuesPosition = m.relativeSegmentPosition()
	m.sortCues()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for i := range m.cues {
		if err := writeCuePoint(w, m.cues[i], m.options.TimecodeScaleNS); err != nil {
			return err
		}
	}
	return m.writeMasterElement(idCues, payload.Bytes())
}

func (m *Muxer) sortCues() {
	sort.SliceStable(m.cues, func(i, j int) bool {
		return m.cues[i].TimeNS < m.cues[j].TimeNS
	})
}

func writeCuePoint(w *ebml.Writer, cue CuePoint, scaleNS int64) error {
	if scaleNS <= 0 || cue.TimeNS < 0 {
		return ErrInvalidData
	}
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	if err := cw.WriteUInt(idCueTime, uint64(cue.TimeNS/scaleNS)); err != nil {
		return err
	}
	if len(cue.Positions) == 0 {
		if cue.TrackID == 0 {
			return ErrInvalidData
		}
		position := cueTrackPositionFromLegacy(cue)
		if err := writeCueTrackPosition(cw, position, scaleNS); err != nil {
			return err
		}
		return w.WriteElement(idCuePoint, payload.Bytes())
	}
	for i := range cue.Positions {
		if err := writeCueTrackPosition(cw, cue.Positions[i], scaleNS); err != nil {
			return err
		}
	}
	return w.WriteElement(idCuePoint, payload.Bytes())
}

func cueTrackPositionFromLegacy(cue CuePoint) CueTrackPosition {
	return CueTrackPosition{
		TrackID:             cue.TrackID,
		ClusterPosition:     cue.ClusterPosition,
		RelativePosition:    cue.RelativePosition,
		RelativePositionSet: cue.RelativePositionSet,
		DurationNS:          cue.DurationNS,
		DurationSet:         cue.DurationSet,
		BlockNumber:         cue.BlockNumber,
		BlockNumberSet:      cue.BlockNumberSet,
		CodecStatePosition:  cue.CodecStatePosition,
		CodecStateSet:       cue.CodecStateSet,
		References:          cue.References,
	}
}

func writeCueTrackPosition(w *ebml.Writer, position CueTrackPosition, scaleNS int64) error {
	if position.TrackID == 0 || position.DurationNS < 0 || (position.BlockNumberSet && position.BlockNumber == 0) {
		return ErrInvalidData
	}
	var positions bytes.Buffer
	pw := ebml.NewWriter(&positions)
	if err := pw.WriteUInt(idCueTrack, uint64(position.TrackID)); err != nil {
		return err
	}
	if err := pw.WriteUInt(idCueClusterPosition, position.ClusterPosition); err != nil {
		return err
	}
	if position.RelativePositionSet {
		if err := pw.WriteUInt(idCueRelativePos, position.RelativePosition); err != nil {
			return err
		}
	}
	if position.DurationSet {
		durationTicks, err := scaledSegmentDurationTicks(position.DurationNS, scaleNS)
		if err != nil {
			return err
		}
		if err := pw.WriteUInt(idCueDuration, durationTicks); err != nil {
			return err
		}
	}
	if position.BlockNumberSet {
		if err := pw.WriteUInt(idCueBlockNumber, position.BlockNumber); err != nil {
			return err
		}
	}
	if position.CodecStateSet {
		if err := pw.WriteUInt(idCueCodecState, position.CodecStatePosition); err != nil {
			return err
		}
	}
	for i := range position.References {
		if err := writeCueReference(pw, position.References[i], scaleNS); err != nil {
			return err
		}
	}
	return w.WriteElement(idCueTrackPositions, positions.Bytes())
}

func writeCueReference(w *ebml.Writer, reference CueReference, scaleNS int64) error {
	if reference.TimeNS < 0 || (reference.BlockNumberSet && reference.BlockNumber == 0) {
		return ErrInvalidData
	}
	var payload bytes.Buffer
	rw := ebml.NewWriter(&payload)
	if err := rw.WriteUInt(idCueRefTime, uint64(reference.TimeNS/scaleNS)); err != nil {
		return err
	}
	if err := rw.WriteUInt(idCueRefCluster, reference.ClusterPosition); err != nil {
		return err
	}
	if reference.BlockNumberSet {
		if err := rw.WriteUInt(idCueRefNumber, reference.BlockNumber); err != nil {
			return err
		}
	}
	if reference.CodecStateSet {
		if err := rw.WriteUInt(idCueRefCodecState, reference.CodecStatePosition); err != nil {
			return err
		}
	}
	return w.WriteElement(idCueReference, payload.Bytes())
}

func (m *Muxer) updateMaxTime(packet Packet) {
	end := packet.TimeNS
	if packet.DurationNS > 0 {
		end += packet.DurationNS
	}
	if end > m.maxTimeNS {
		m.maxTimeNS = end
	}
}

func (m *Muxer) patchDuration() error {
	seeker, ok := m.writer.(io.Seeker)
	if !ok {
		return nil
	}
	current := m.ebml.Offset()
	if _, err := seeker.Seek(m.durationOffset, io.SeekStart); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(m.blockScratch[:8], math.Float64bits(float64(m.maxTimeNS)/float64(m.options.TimecodeScaleNS)))
	if err := writeFull(m.writer, m.blockScratch[:8]); err != nil {
		return err
	}
	if _, err := seeker.Seek(current, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (m *Muxer) patchSeekHead() error {
	seeker, ok := m.writer.(io.Seeker)
	if !ok {
		return nil
	}
	payload, err := m.buildSeekHeadPayload()
	if err != nil {
		return err
	}
	element, err := buildElementBytes(idSeekHead, payload)
	if err != nil {
		return err
	}
	if m.seekHeadReserve-len(element) == 1 {
		element, err = buildElementBytesWithExtraSizeByte(idSeekHead, payload)
		if err != nil {
			return err
		}
	}
	if len(element) > m.seekHeadReserve {
		return ErrSeekHeadTooSmall
	}
	current := m.ebml.Offset()
	if _, err := seeker.Seek(m.seekHeadOffset, io.SeekStart); err != nil {
		return err
	}
	if err := writeFull(m.writer, element); err != nil {
		return err
	}
	if err := writeVoidTotalRaw(m.writer, m.seekHeadReserve-len(element)); err != nil {
		return err
	}
	if _, err := seeker.Seek(current, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (m *Muxer) buildSeekHeadPayload() ([]byte, error) {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	for _, entry := range []struct {
		id       ebml.ID
		position uint64
	}{
		{id: idInfo, position: m.infoPosition},
		{id: idTracks, position: m.tracksPosition},
		{id: idAttachments, position: m.attachPosition},
		{id: idChapters, position: m.chapterPosition},
		{id: idTags, position: m.tagsPosition},
		{id: idCues, position: m.cuesPosition},
	} {
		if entry.position == 0 {
			continue
		}
		if err := writeSeekEntry(w, entry.id, entry.position); err != nil {
			return nil, err
		}
	}
	if !m.options.WriteCRC32 {
		return payload.Bytes(), nil
	}
	return payloadWithCRC32(payload.Bytes())
}

func writeSeekEntry(w *ebml.Writer, id ebml.ID, position uint64) error {
	var payload bytes.Buffer
	sw := ebml.NewWriter(&payload)
	var idPayload [ebml.MaxIDWidth]byte
	n, err := ebml.EncodeID(idPayload[:], id)
	if err != nil {
		return err
	}
	if err := writeBinary(sw, idSeekID, idPayload[:n]); err != nil {
		return err
	}
	if err := sw.WriteUInt(idSeekPosition, position); err != nil {
		return err
	}
	return w.WriteElement(idSeek, payload.Bytes())
}

func (m *Muxer) relativeSegmentPosition() uint64 {
	if m.segmentData == 0 {
		return 0
	}
	offset := m.ebml.Offset()
	if offset < m.segmentData {
		return 0
	}
	return uint64(offset - m.segmentData)
}

func (m *Muxer) relativeClusterPosition() uint64 {
	if !m.clusterOpen {
		return 0
	}
	offset := m.ebml.Offset()
	clusterData := m.clusterDataOffset()
	if offset < clusterData {
		return 0
	}
	return uint64(offset - clusterData)
}

func (m *Muxer) clusterDataOffset() int64 {
	if m.clusterSized {
		return m.clusterPatch.Start
	}
	return int64(m.clusterPosition) + m.segmentData + int64(ebml.MaxIDWidth+ebml.MaxSizeWidth)
}

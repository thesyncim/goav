package matroska

func (d *Demuxer) Tracks() []Track {
	if d == nil || len(d.tracks) == 0 {
		return nil
	}
	tracks := make([]Track, len(d.tracks))
	for i := range d.tracks {
		tracks[i] = cloneTrack(d.tracks[i])
	}
	return tracks
}

func cloneTrack(track Track) Track {
	if len(track.CodecPrivate) != 0 {
		track.CodecPrivate = append([]byte(nil), track.CodecPrivate...)
	}
	if len(track.Video.Projection.Private) != 0 {
		track.Video.Projection.Private = append([]byte(nil), track.Video.Projection.Private...)
	}
	track.BlockAdditionMappings = cloneBlockAdditionMappings(track.BlockAdditionMappings)
	if len(track.TrackOverlays) != 0 {
		track.TrackOverlays = append([]uint64(nil), track.TrackOverlays...)
	}
	track.TrackTranslates = cloneTrackTranslates(track.TrackTranslates)
	track.ContentEncodings = cloneContentEncodings(track.ContentEncodings)
	track.UnknownElements = cloneUnknownElements(track.UnknownElements)
	return track
}

func cloneBlockAdditionMappings(mappings []BlockAdditionMapping) []BlockAdditionMapping {
	if len(mappings) == 0 {
		return nil
	}
	out := make([]BlockAdditionMapping, len(mappings))
	for i := range mappings {
		out[i] = mappings[i]
		if mappings[i].ExtraData != nil {
			out[i].ExtraData = append([]byte(nil), mappings[i].ExtraData...)
		}
	}
	return out
}

func cloneTrackTranslates(translates []TrackTranslate) []TrackTranslate {
	if len(translates) == 0 {
		return nil
	}
	out := make([]TrackTranslate, len(translates))
	for i := range translates {
		out[i] = translates[i]
		if translates[i].TrackID != nil {
			out[i].TrackID = append([]byte(nil), translates[i].TrackID...)
		}
		if len(translates[i].EditionUIDs) != 0 {
			out[i].EditionUIDs = append([]uint64(nil), translates[i].EditionUIDs...)
		}
	}
	return out
}

func cloneContentEncodings(encodings []ContentEncoding) []ContentEncoding {
	if len(encodings) == 0 {
		return nil
	}
	out := make([]ContentEncoding, len(encodings))
	for i := range encodings {
		out[i] = encodings[i]
		if encodings[i].Compression.Settings != nil {
			out[i].Compression.Settings = append([]byte(nil), encodings[i].Compression.Settings...)
		}
		if encodings[i].Encryption.KeyID != nil {
			out[i].Encryption.KeyID = append([]byte(nil), encodings[i].Encryption.KeyID...)
		}
		if encodings[i].Encryption.Signature != nil {
			out[i].Encryption.Signature = append([]byte(nil), encodings[i].Encryption.Signature...)
		}
		if encodings[i].Encryption.SignatureKeyID != nil {
			out[i].Encryption.SignatureKeyID = append([]byte(nil), encodings[i].Encryption.SignatureKeyID...)
		}
	}
	return out
}

func cloneSegmentInfo(info SegmentInfo) SegmentInfo {
	if len(info.SegmentUUID) != 0 {
		info.SegmentUUID = append([]byte(nil), info.SegmentUUID...)
	}
	if len(info.PrevUUID) != 0 {
		info.PrevUUID = append([]byte(nil), info.PrevUUID...)
	}
	if len(info.NextUUID) != 0 {
		info.NextUUID = append([]byte(nil), info.NextUUID...)
	}
	info.UnknownElements = cloneUnknownElements(info.UnknownElements)
	return info
}

func cloneAttachments(attachments []Attachment) []Attachment {
	if len(attachments) == 0 {
		return nil
	}
	out := make([]Attachment, len(attachments))
	for i := range attachments {
		out[i] = attachments[i]
		if attachments[i].Data != nil {
			out[i].Data = append([]byte(nil), attachments[i].Data...)
		}
		out[i].UnknownElements = cloneUnknownElements(attachments[i].UnknownElements)
	}
	return out
}

func cloneChapters(editions []ChapterEdition) []ChapterEdition {
	if len(editions) == 0 {
		return nil
	}
	out := make([]ChapterEdition, len(editions))
	for i := range editions {
		out[i] = editions[i]
		out[i].Chapters = cloneChapterList(editions[i].Chapters)
		out[i].UnknownElements = cloneUnknownElements(editions[i].UnknownElements)
	}
	return out
}

func cloneChapterList(chapters []Chapter) []Chapter {
	if len(chapters) == 0 {
		return nil
	}
	out := make([]Chapter, len(chapters))
	for i := range chapters {
		out[i] = chapters[i]
		if len(chapters[i].TrackUIDs) != 0 {
			out[i].TrackUIDs = append([]uint64(nil), chapters[i].TrackUIDs...)
		}
		out[i].Displays = cloneChapterDisplays(chapters[i].Displays)
		out[i].Children = cloneChapterList(chapters[i].Children)
		out[i].UnknownElements = cloneUnknownElements(chapters[i].UnknownElements)
	}
	return out
}

func cloneChapterDisplays(displays []ChapterDisplay) []ChapterDisplay {
	if len(displays) == 0 {
		return nil
	}
	out := make([]ChapterDisplay, len(displays))
	for i := range displays {
		out[i] = displays[i]
		out[i].UnknownElements = cloneUnknownElements(displays[i].UnknownElements)
	}
	return out
}

func cloneTags(tags []Tag) []Tag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]Tag, len(tags))
	for i := range tags {
		out[i] = tags[i]
		out[i].Target = cloneTagTarget(tags[i].Target)
		out[i].Simple = cloneSimpleTags(tags[i].Simple)
		out[i].UnknownElements = cloneUnknownElements(tags[i].UnknownElements)
	}
	return out
}

func cloneUnknownElements(elements []UnknownElement) []UnknownElement {
	if len(elements) == 0 {
		return nil
	}
	out := make([]UnknownElement, len(elements))
	for i := range elements {
		out[i] = elements[i]
		if elements[i].Raw != nil {
			out[i].Raw = append([]byte(nil), elements[i].Raw...)
		}
	}
	return out
}

func cloneCues(cues []CuePoint) []CuePoint {
	if len(cues) == 0 {
		return nil
	}
	out := make([]CuePoint, len(cues))
	for i := range cues {
		out[i] = cues[i]
		out[i].References = cloneCueReferences(cues[i].References)
		out[i].Positions = cloneCueTrackPositions(cues[i].Positions)
	}
	return out
}

func cloneCueTrackPositions(positions []CueTrackPosition) []CueTrackPosition {
	if len(positions) == 0 {
		return nil
	}
	out := make([]CueTrackPosition, len(positions))
	for i := range positions {
		out[i] = positions[i]
		out[i].References = cloneCueReferences(positions[i].References)
	}
	return out
}

func cloneCueReferences(references []CueReference) []CueReference {
	if len(references) == 0 {
		return nil
	}
	out := make([]CueReference, len(references))
	copy(out, references)
	return out
}

func cloneTagTarget(target TagTarget) TagTarget {
	if len(target.TrackUIDs) != 0 {
		target.TrackUIDs = append([]uint64(nil), target.TrackUIDs...)
	}
	if len(target.EditionUIDs) != 0 {
		target.EditionUIDs = append([]uint64(nil), target.EditionUIDs...)
	}
	if len(target.ChapterUIDs) != 0 {
		target.ChapterUIDs = append([]uint64(nil), target.ChapterUIDs...)
	}
	if len(target.AttachmentUIDs) != 0 {
		target.AttachmentUIDs = append([]uint64(nil), target.AttachmentUIDs...)
	}
	target.UnknownElements = cloneUnknownElements(target.UnknownElements)
	return target
}

func cloneSimpleTags(tags []SimpleTag) []SimpleTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]SimpleTag, len(tags))
	for i := range tags {
		out[i] = tags[i]
		if tags[i].Binary != nil {
			out[i].Binary = append([]byte(nil), tags[i].Binary...)
		}
		out[i].Children = cloneSimpleTags(tags[i].Children)
		out[i].UnknownElements = cloneUnknownElements(tags[i].UnknownElements)
	}
	return out
}

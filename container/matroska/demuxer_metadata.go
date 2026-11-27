package matroska

import (
	"io"

	"github.com/thesyncim/goav/container/ebml"
)

func (d *Demuxer) parseAttachments(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	master, err := d.checkedMasterReader(d.reader, header.Size.Value)
	if err != nil {
		return err
	}
	usedUIDs := make(map[uint64]struct{}, len(d.attachments))
	for i := range d.attachments {
		if d.attachments[i].UID == 0 {
			return ErrInvalidData
		}
		if _, ok := usedUIDs[d.attachments[i].UID]; ok {
			return ErrInvalidData
		}
		usedUIDs[d.attachments[i].UID] = struct{}{}
	}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idAttachedFile:
			attachment, err := d.parseAttachedFile(master.Reader(), child)
			if err != nil {
				return err
			}
			if _, ok := usedUIDs[attachment.UID]; ok {
				return ErrInvalidData
			}
			usedUIDs[attachment.UID] = struct{}{}
			d.attachments = append(d.attachments, attachment)
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownAttachmentsElement, &d.unknownAttachments); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}

func (d *Demuxer) parseAttachedFile(parent io.Reader, header ebml.Header) (Attachment, error) {
	if header.Size.Unknown {
		return Attachment{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return Attachment{}, err
	}
	var attachment Attachment
	var descriptionSeen bool
	var filenameSeen bool
	var mediaTypeSeen bool
	var dataSeen bool
	var uidSeen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Attachment{}, err
		}
		switch child.ID {
		case idFileDescription:
			if err := markElementSeen(&descriptionSeen); err != nil {
				return Attachment{}, err
			}
			attachment.Description, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		case idFileName:
			if err := markElementSeen(&filenameSeen); err != nil {
				return Attachment{}, err
			}
			attachment.Filename, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		case idFileMediaType:
			if err := markElementSeen(&mediaTypeSeen); err != nil {
				return Attachment{}, err
			}
			attachment.MIMEType, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		case idFileData:
			if err := markElementSeen(&dataSeen); err != nil {
				return Attachment{}, err
			}
			attachment.Data, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		case idFileUID:
			if err := markElementSeen(&uidSeen); err != nil {
				return Attachment{}, err
			}
			attachment.UID, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Attachment{}, err
			}
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownAttachedFileElement, &attachment.UnknownElements); err != nil {
				return Attachment{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return Attachment{}, err
	}
	if !dataSeen {
		return Attachment{}, ErrInvalidData
	}
	if err := validateAttachment(attachment); err != nil {
		return Attachment{}, ErrInvalidData
	}
	return attachment, nil
}


func (d *Demuxer) parseTags(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	master, err := d.checkedMasterReader(d.reader, header.Size.Value)
	if err != nil {
		return err
	}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idTag:
			tag, err := d.parseTag(master.Reader(), child)
			if err != nil {
				return err
			}
			d.tags = append(d.tags, tag)
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownTagsElement, &d.unknownTags); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}

func (d *Demuxer) parseTag(parent io.Reader, header ebml.Header) (Tag, error) {
	if header.Size.Unknown {
		return Tag{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return Tag{}, err
	}
	var tag Tag
	targetSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Tag{}, err
		}
		switch child.ID {
		case idTargets:
			if err := markElementSeen(&targetSeen); err != nil {
				return Tag{}, err
			}
			tag.Target, err = d.parseTagTargets(master.Reader(), child)
			if err != nil {
				return Tag{}, err
			}
		case idSimpleTag:
			simple, err := d.parseSimpleTag(master.Reader(), child)
			if err != nil {
				return Tag{}, err
			}
			tag.Simple = append(tag.Simple, simple)
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownTagElement, &tag.UnknownElements); err != nil {
				return Tag{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return Tag{}, err
	}
	if !targetSeen {
		tag.Target.TypeValue = 50
	}
	if len(tag.Simple) == 0 || validateTagTarget(tag.Target) != nil ||
		validateUnknownElementsFor(tag.UnknownElements, isKnownTagElement) != nil {
		return Tag{}, ErrInvalidData
	}
	return tag, nil
}

func (d *Demuxer) parseTagTargets(parent io.Reader, header ebml.Header) (TagTarget, error) {
	if header.Size.Unknown {
		return TagTarget{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return TagTarget{}, err
	}
	target := TagTarget{TypeValue: 50}
	var typeValueSeen bool
	var typeSeen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return TagTarget{}, err
		}
		switch child.ID {
		case idTargetTypeValue:
			if err := markElementSeen(&typeValueSeen); err != nil {
				return TagTarget{}, err
			}
			target.TypeValue, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return TagTarget{}, err
			}
		case idTargetType:
			if err := markElementSeen(&typeSeen); err != nil {
				return TagTarget{}, err
			}
			target.Type, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return TagTarget{}, err
			}
		case idTagTrackUID:
			target.TrackUIDs, err = appendTagUID(master.Reader(), child, target.TrackUIDs)
			if err != nil {
				return TagTarget{}, err
			}
		case idTagEditionUID:
			target.EditionUIDs, err = appendTagUID(master.Reader(), child, target.EditionUIDs)
			if err != nil {
				return TagTarget{}, err
			}
		case idTagChapterUID:
			target.ChapterUIDs, err = appendTagUID(master.Reader(), child, target.ChapterUIDs)
			if err != nil {
				return TagTarget{}, err
			}
		case idTagAttachmentUID:
			target.AttachmentUIDs, err = appendTagUID(master.Reader(), child, target.AttachmentUIDs)
			if err != nil {
				return TagTarget{}, err
			}
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownTagTargetsElement, &target.UnknownElements); err != nil {
				return TagTarget{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return TagTarget{}, err
	}
	if err := validateTagTarget(target); err != nil {
		return TagTarget{}, err
	}
	return target, nil
}

func appendTagUID(reader io.Reader, child ebml.Header, values []uint64) ([]uint64, error) {
	value, err := readUIntPayload(reader, child.Size.Value)
	if err != nil {
		return nil, err
	}
	return append(values, value), nil
}

func (d *Demuxer) parseSimpleTag(parent io.Reader, header ebml.Header) (SimpleTag, error) {
	if header.Size.Unknown {
		return SimpleTag{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return SimpleTag{}, err
	}
	tag := SimpleTag{Language: "und", Default: true}
	var nameSeen bool
	var languageSeen bool
	var languageBCP47Seen bool
	var defaultSeen bool
	var stringSeen bool
	var binarySeen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return SimpleTag{}, err
		}
		switch child.ID {
		case idTagName:
			if err := markElementSeen(&nameSeen); err != nil {
				return SimpleTag{}, err
			}
			tag.Name, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
		case idTagLanguage:
			if err := markElementSeen(&languageSeen); err != nil {
				return SimpleTag{}, err
			}
			tag.Language, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
		case idTagLanguageBCP47:
			if err := markElementSeen(&languageBCP47Seen); err != nil {
				return SimpleTag{}, err
			}
			tag.LanguageBCP47, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
		case idTagDefault:
			if err := markElementSeen(&defaultSeen); err != nil {
				return SimpleTag{}, err
			}
			tag.Default, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
			tag.DefaultSet = true
		case idTagString:
			if err := markElementSeen(&stringSeen); err != nil {
				return SimpleTag{}, err
			}
			tag.String, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
			tag.StringSet = true
		case idTagBinary:
			if err := markElementSeen(&binarySeen); err != nil {
				return SimpleTag{}, err
			}
			tag.Binary, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return SimpleTag{}, err
			}
		case idSimpleTag:
			childTag, err := d.parseSimpleTag(master.Reader(), child)
			if err != nil {
				return SimpleTag{}, err
			}
			tag.Children = append(tag.Children, childTag)
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownSimpleTagElement, &tag.UnknownElements); err != nil {
				return SimpleTag{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return SimpleTag{}, err
	}
	if err := normalizeSimpleTags([]SimpleTag{tag}); err != nil {
		return SimpleTag{}, err
	}
	return tag, nil
}


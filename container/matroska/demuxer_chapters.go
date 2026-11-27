package matroska

import (
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

func (d *Demuxer) parseChapters(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	master, err := d.checkedMasterReader(d.reader, header.Size.Value)
	if err != nil {
		return err
	}
	usedEditions := make(map[uint64]struct{}, len(d.chapters))
	usedChapters := make(map[uint64]struct{})
	for i := range d.chapters {
		if d.chapters[i].UID != 0 {
			if _, ok := usedEditions[d.chapters[i].UID]; ok {
				return ErrInvalidData
			}
			usedEditions[d.chapters[i].UID] = struct{}{}
		}
		if err := collectChapterUIDs(d.chapters[i].Chapters, usedChapters); err != nil {
			return err
		}
	}
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idEditionEntry:
			edition, err := d.parseEditionEntry(master.Reader(), child)
			if err != nil {
				return err
			}
			if edition.UID != 0 {
				if _, ok := usedEditions[edition.UID]; ok {
					return ErrInvalidData
				}
				usedEditions[edition.UID] = struct{}{}
			}
			if err := collectChapterUIDs(edition.Chapters, usedChapters); err != nil {
				return err
			}
			d.chapters = append(d.chapters, edition)
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownChaptersElement, &d.unknownChapters); err != nil {
				return err
			}
		}
	}
	return master.Validate()
}

func collectChapterUIDs(chapters []Chapter, used map[uint64]struct{}) error {
	for i := range chapters {
		if chapters[i].UID == 0 {
			return ErrInvalidData
		}
		if _, ok := used[chapters[i].UID]; ok {
			return ErrInvalidData
		}
		used[chapters[i].UID] = struct{}{}
		if err := collectChapterUIDs(chapters[i].Children, used); err != nil {
			return err
		}
	}
	return nil
}

func (d *Demuxer) parseEditionEntry(parent io.Reader, header ebml.Header) (ChapterEdition, error) {
	if header.Size.Unknown {
		return ChapterEdition{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ChapterEdition{}, err
	}
	var edition ChapterEdition
	var uidSeen bool
	var hiddenSeen bool
	var defaultSeen bool
	var orderedSeen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ChapterEdition{}, err
		}
		switch child.ID {
		case idEditionUID:
			if err := markElementSeen(&uidSeen); err != nil {
				return ChapterEdition{}, err
			}
			edition.UID, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterEdition{}, err
			}
		case idEditionFlagHidden:
			if err := markElementSeen(&hiddenSeen); err != nil {
				return ChapterEdition{}, err
			}
			edition.Hidden, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterEdition{}, err
			}
		case idEditionFlagDefault:
			if err := markElementSeen(&defaultSeen); err != nil {
				return ChapterEdition{}, err
			}
			edition.Default, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterEdition{}, err
			}
		case idEditionFlagOrdered:
			if err := markElementSeen(&orderedSeen); err != nil {
				return ChapterEdition{}, err
			}
			edition.Ordered, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterEdition{}, err
			}
		case idChapterAtom:
			chapter, err := d.parseChapterAtom(master.Reader(), child)
			if err != nil {
				return ChapterEdition{}, err
			}
			edition.Chapters = append(edition.Chapters, chapter)
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownEditionEntryElement, &edition.UnknownElements); err != nil {
				return ChapterEdition{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ChapterEdition{}, err
	}
	if len(edition.Chapters) == 0 {
		return ChapterEdition{}, ErrInvalidData
	}
	if err := validateUnknownElementsFor(edition.UnknownElements, isKnownEditionEntryElement); err != nil {
		return ChapterEdition{}, ErrInvalidData
	}
	return edition, nil
}

func (d *Demuxer) parseChapterAtom(parent io.Reader, header ebml.Header) (Chapter, error) {
	if header.Size.Unknown {
		return Chapter{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return Chapter{}, err
	}
	chapter := Chapter{Enabled: true}
	startSeen := false
	var uidSeen bool
	var stringUIDSeen bool
	var endSeen bool
	var hiddenSeen bool
	var enabledSeen bool
	var trackSeen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Chapter{}, err
		}
		switch child.ID {
		case idChapterUID:
			if err := markElementSeen(&uidSeen); err != nil {
				return Chapter{}, err
			}
			chapter.UID, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
		case idChapterStringUID:
			if err := markElementSeen(&stringUIDSeen); err != nil {
				return Chapter{}, err
			}
			chapter.StringUID, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
		case idChapterTimeStart:
			if err := markElementSeen(&startSeen); err != nil {
				return Chapter{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
			if value > uint64(math.MaxInt64) {
				return Chapter{}, ErrInvalidData
			}
			chapter.StartNS = int64(value)
		case idChapterTimeEnd:
			if err := markElementSeen(&endSeen); err != nil {
				return Chapter{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
			if value > uint64(math.MaxInt64) {
				return Chapter{}, ErrInvalidData
			}
			chapter.EndNS = int64(value)
			chapter.EndSet = true
		case idChapterFlagHidden:
			if err := markElementSeen(&hiddenSeen); err != nil {
				return Chapter{}, err
			}
			chapter.Hidden, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
		case idChapterFlagEnabled:
			if err := markElementSeen(&enabledSeen); err != nil {
				return Chapter{}, err
			}
			chapter.Enabled, err = readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Chapter{}, err
			}
			chapter.EnabledSet = true
		case idChapterTrack:
			if err := markElementSeen(&trackSeen); err != nil {
				return Chapter{}, err
			}
			chapter.TrackUIDs, err = d.parseChapterTrack(master.Reader(), child)
			if err != nil {
				return Chapter{}, err
			}
		case idChapterDisplay:
			display, err := d.parseChapterDisplay(master.Reader(), child)
			if err != nil {
				return Chapter{}, err
			}
			chapter.Displays = append(chapter.Displays, display)
		case idChapterAtom:
			childChapter, err := d.parseChapterAtom(master.Reader(), child)
			if err != nil {
				return Chapter{}, err
			}
			chapter.Children = append(chapter.Children, childChapter)
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownChapterAtomElement, &chapter.UnknownElements); err != nil {
				return Chapter{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return Chapter{}, err
	}
	if !startSeen || validateChapter(chapter) != nil {
		return Chapter{}, ErrInvalidData
	}
	return chapter, nil
}

func (d *Demuxer) parseChapterTrack(parent io.Reader, header ebml.Header) ([]uint64, error) {
	if header.Size.Unknown {
		return nil, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return nil, err
	}
	var trackUIDs []uint64
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return nil, err
		}
		switch child.ID {
		case idChapterTrackUID:
			uid, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return nil, err
			}
			if uid == 0 {
				return nil, ErrInvalidData
			}
			trackUIDs = append(trackUIDs, uid)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return nil, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return nil, err
	}
	if len(trackUIDs) == 0 {
		return nil, ErrInvalidData
	}
	return trackUIDs, nil
}

func (d *Demuxer) parseChapterDisplay(parent io.Reader, header ebml.Header) (ChapterDisplay, error) {
	if header.Size.Unknown {
		return ChapterDisplay{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ChapterDisplay{}, err
	}
	display := ChapterDisplay{Language: "eng"}
	var stringSeen bool
	var languageBCP47Seen bool
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ChapterDisplay{}, err
		}
		switch child.ID {
		case idChapString:
			if err := markElementSeen(&stringSeen); err != nil {
				return ChapterDisplay{}, err
			}
			display.String, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterDisplay{}, err
			}
		case idChapLanguage:
			display.Language, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterDisplay{}, err
			}
		case idChapLanguageBCP47:
			if err := markElementSeen(&languageBCP47Seen); err != nil {
				return ChapterDisplay{}, err
			}
			display.LanguageBCP47, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterDisplay{}, err
			}
		case idChapCountry:
			display.Country, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ChapterDisplay{}, err
			}
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownChapterDisplayElement, &display.UnknownElements); err != nil {
				return ChapterDisplay{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ChapterDisplay{}, err
	}
	if display.String == "" {
		return ChapterDisplay{}, ErrInvalidData
	}
	if err := validateUnknownElementsFor(display.UnknownElements, isKnownChapterDisplayElement); err != nil {
		return ChapterDisplay{}, ErrInvalidData
	}
	return display, nil
}

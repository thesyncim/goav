package matroska

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/container/ebml"
)

func TestMuxerDemuxerPreservesAttachments(t *testing.T) {
	ws := &memoryWriteSeeker{}
	opts := MuxerOptions{
		Attachments: []Attachment{
			{
				Filename:    "cover.png",
				MIMEType:    "image/png",
				Description: "cover art",
				Data:        []byte{0x89, 0x50, 0x4e, 0x47},
			},
			{
				UID:      42,
				Filename: "font.otf",
				MIMEType: "font/otf",
				Data:     []byte{1, 2, 3, 4},
			},
		},
	}
	muxer, err := NewMuxer(ws, opts)
	if err != nil {
		t.Fatal(err)
	}
	opts.Attachments[0].Data[0] = 0xff
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	positions := collectTopLevelPositions(t, ws.bytes)
	if _, ok := positions[idAttachments]; !ok {
		t.Fatalf("missing attachments in top-level positions %+v", positions)
	}
	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertSeekEntry(t, demuxer.SeekEntries(), idAttachments, positions[idAttachments])
	want := []Attachment{
		{
			UID:         1,
			Filename:    "cover.png",
			MIMEType:    "image/png",
			Description: "cover art",
			Data:        []byte{0x89, 0x50, 0x4e, 0x47},
		},
		{
			UID:      42,
			Filename: "font.otf",
			MIMEType: "font/otf",
			Data:     []byte{1, 2, 3, 4},
		},
	}
	got := demuxer.Attachments()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("attachments = %+v, want %+v", got, want)
	}
	got[0].Data[0] = 0xee
	fresh := demuxer.Attachments()
	if !bytes.Equal(fresh[0].Data, want[0].Data) {
		t.Fatalf("attachment data alias was not protected: %x", fresh[0].Data)
	}
}

func TestMuxerDemuxerPreservesChaptersAndTags(t *testing.T) {
	ws := &memoryWriteSeeker{}
	opts := MuxerOptions{
		Chapters: []ChapterEdition{{
			UID:     77,
			Default: true,
			Chapters: []Chapter{{
				UID:       100,
				StringUID: "intro",
				StartNS:   1_000_000_000,
				EndNS:     2_000_000_000,
				EndSet:    true,
				TrackUIDs: []uint64{1},
				Displays: []ChapterDisplay{{
					String:        "Intro",
					Language:      "eng",
					LanguageBCP47: "en-US",
					Country:       "us",
				}},
				Children: []Chapter{{
					UID:       101,
					StringUID: "intro-a",
					StartNS:   1_200_000_000,
					EndNS:     1_500_000_000,
					EndSet:    true,
					Displays:  []ChapterDisplay{{String: "Beat A"}},
				}},
			}},
		}},
		Tags: []Tag{{
			Target: TagTarget{
				TypeValue:   50,
				Type:        "MOVIE",
				TrackUIDs:   []uint64{1},
				EditionUIDs: []uint64{77},
				ChapterUIDs: []uint64{100},
			},
			Simple: []SimpleTag{{
				Name:          "TITLE",
				Language:      "eng",
				LanguageBCP47: "en-US",
				Default:       true,
				DefaultSet:    true,
				String:        "Camera Roll",
				StringSet:     true,
				Children: []SimpleTag{{
					Name:    "SORT_WITH",
					Binary:  []byte{1, 2, 3},
					Default: false,
				}},
			}},
		}},
	}
	muxer, err := NewMuxer(ws, opts)
	if err != nil {
		t.Fatal(err)
	}
	wantChapters, err := normalizeChapters(opts.Chapters)
	if err != nil {
		t.Fatal(err)
	}
	wantTags, err := normalizeTags(opts.Tags)
	if err != nil {
		t.Fatal(err)
	}
	opts.Tags[0].Simple[0].Children[0].Binary[0] = 0xff
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	positions := collectTopLevelPositions(t, ws.bytes)
	for _, id := range []ebml.ID{idChapters, idTags} {
		if _, ok := positions[id]; !ok {
			t.Fatalf("missing top-level element 0x%x in positions %+v", uint64(id), positions)
		}
	}
	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertSeekEntry(t, demuxer.SeekEntries(), idChapters, positions[idChapters])
	assertSeekEntry(t, demuxer.SeekEntries(), idTags, positions[idTags])
	if got := demuxer.Chapters(); !reflect.DeepEqual(got, wantChapters) {
		t.Fatalf("chapters = %+v, want %+v", got, wantChapters)
	}
	gotTags := demuxer.Tags()
	if !reflect.DeepEqual(gotTags, wantTags) {
		t.Fatalf("tags = %+v, want %+v", gotTags, wantTags)
	}
	gotTags[0].Simple[0].Children[0].Binary[0] = 0xee
	fresh := demuxer.Tags()
	if !bytes.Equal(fresh[0].Simple[0].Children[0].Binary, []byte{1, 2, 3}) {
		t.Fatalf("tag binary alias was not protected: %x", fresh[0].Simple[0].Children[0].Binary)
	}
}

func TestDemuxerRejectsDuplicateMetadataUIDs(t *testing.T) {
	tests := []struct {
		name          string
		writeMetadata func(*ebml.Writer) error
	}{
		{
			name: "attachment uid in one attachments master",
			writeMetadata: func(w *ebml.Writer) error {
				return writeAttachmentsElement(w,
					Attachment{UID: 7, Filename: "a.txt", MIMEType: "text/plain", Data: []byte("a")},
					Attachment{UID: 7, Filename: "b.txt", MIMEType: "text/plain", Data: []byte("b")},
				)
			},
		},
		{
			name: "attachment uid across attachments masters",
			writeMetadata: func(w *ebml.Writer) error {
				if err := writeAttachmentsElement(w,
					Attachment{UID: 8, Filename: "a.txt", MIMEType: "text/plain", Data: []byte("a")},
				); err != nil {
					return err
				}
				return writeAttachmentsElement(w,
					Attachment{UID: 8, Filename: "b.txt", MIMEType: "text/plain", Data: []byte("b")},
				)
			},
		},
		{
			name: "edition uid in one chapters master",
			writeMetadata: func(w *ebml.Writer) error {
				return writeChaptersElement(w,
					ChapterEdition{UID: 9, Chapters: []Chapter{metadataValidationChapter(1, 0)}},
					ChapterEdition{UID: 9, Chapters: []Chapter{metadataValidationChapter(2, 1)}},
				)
			},
		},
		{
			name: "edition uid across chapters masters",
			writeMetadata: func(w *ebml.Writer) error {
				if err := writeChaptersElement(w,
					ChapterEdition{UID: 10, Chapters: []Chapter{metadataValidationChapter(1, 0)}},
				); err != nil {
					return err
				}
				return writeChaptersElement(w,
					ChapterEdition{UID: 10, Chapters: []Chapter{metadataValidationChapter(2, 1)}},
				)
			},
		},
		{
			name: "chapter uid siblings",
			writeMetadata: func(w *ebml.Writer) error {
				return writeChaptersElement(w,
					ChapterEdition{UID: 11, Chapters: []Chapter{
						metadataValidationChapter(3, 0),
						metadataValidationChapter(3, 1),
					}},
				)
			},
		},
		{
			name: "chapter uid nested",
			writeMetadata: func(w *ebml.Writer) error {
				chapter := metadataValidationChapter(4, 0)
				chapter.Children = []Chapter{metadataValidationChapter(4, 1)}
				return writeChaptersElement(w,
					ChapterEdition{UID: 12, Chapters: []Chapter{chapter}},
				)
			},
		},
		{
			name: "chapter uid across editions",
			writeMetadata: func(w *ebml.Writer) error {
				return writeChaptersElement(w,
					ChapterEdition{UID: 13, Chapters: []Chapter{metadataValidationChapter(5, 0)}},
					ChapterEdition{UID: 14, Chapters: []Chapter{metadataValidationChapter(5, 1)}},
				)
			},
		},
		{
			name: "chapter uid across chapters masters",
			writeMetadata: func(w *ebml.Writer) error {
				if err := writeChaptersElement(w,
					ChapterEdition{UID: 15, Chapters: []Chapter{metadataValidationChapter(6, 0)}},
				); err != nil {
					return err
				}
				return writeChaptersElement(w,
					ChapterEdition{UID: 16, Chapters: []Chapter{metadataValidationChapter(6, 1)}},
				)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeTopLevelMetadataMatroskaData(t, tt.writeMetadata)
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidAttachments(t *testing.T) {
	tests := []struct {
		name        string
		attachments []Attachment
	}{
		{
			name: "missing filename",
			attachments: []Attachment{{
				MIMEType: "text/plain",
				Data:     []byte{1},
			}},
		},
		{
			name: "missing media type",
			attachments: []Attachment{{
				Filename: "note.txt",
				Data:     []byte{1},
			}},
		},
		{
			name: "missing data",
			attachments: []Attachment{{
				Filename: "note.txt",
				MIMEType: "text/plain",
			}},
		},
		{
			name: "duplicate uid",
			attachments: []Attachment{
				{UID: 7, Filename: "one.txt", MIMEType: "text/plain", Data: []byte{1}},
				{UID: 7, Filename: "two.txt", MIMEType: "text/plain", Data: []byte{2}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewMuxer(discardWriter{}, MuxerOptions{Attachments: tt.attachments}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidChaptersAndTags(t *testing.T) {
	t.Run("edition without chapters", func(t *testing.T) {
		if _, err := NewMuxer(discardWriter{}, MuxerOptions{Chapters: []ChapterEdition{{UID: 1}}}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("negative chapter start", func(t *testing.T) {
		opts := MuxerOptions{Chapters: []ChapterEdition{{Chapters: []Chapter{{StartNS: -1}}}}}
		if _, err := NewMuxer(discardWriter{}, opts); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("chapter end before start", func(t *testing.T) {
		opts := MuxerOptions{Chapters: []ChapterEdition{{Chapters: []Chapter{{StartNS: 10, EndNS: 9, EndSet: true}}}}}
		if _, err := NewMuxer(discardWriter{}, opts); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("chapter display without string", func(t *testing.T) {
		opts := MuxerOptions{Chapters: []ChapterEdition{{Chapters: []Chapter{{StartNS: 0, Displays: []ChapterDisplay{{Language: "eng"}}}}}}}
		if _, err := NewMuxer(discardWriter{}, opts); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("duplicate chapter uid", func(t *testing.T) {
		opts := MuxerOptions{Chapters: []ChapterEdition{{Chapters: []Chapter{{UID: 7, StartNS: 0}, {UID: 7, StartNS: 1}}}}}
		if _, err := NewMuxer(discardWriter{}, opts); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("tag without simple tag", func(t *testing.T) {
		if _, err := NewMuxer(discardWriter{}, MuxerOptions{Tags: []Tag{{}}}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("simple tag without name", func(t *testing.T) {
		opts := MuxerOptions{Tags: []Tag{{Simple: []SimpleTag{{String: "x", StringSet: true}}}}}
		if _, err := NewMuxer(discardWriter{}, opts); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("simple tag string and binary", func(t *testing.T) {
		opts := MuxerOptions{Tags: []Tag{{Simple: []SimpleTag{{Name: "TITLE", String: "x", StringSet: true, Binary: []byte{1}}}}}}
		if _, err := NewMuxer(discardWriter{}, opts); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
}

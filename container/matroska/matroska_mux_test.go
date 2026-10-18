package matroska

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/ebml"
	"github.com/thesyncim/goav/format"
)

func TestMuxerDemuxerPreservesSegmentInfoMetadata(t *testing.T) {
	created := time.Date(2026, 6, 7, 12, 34, 56, 789, time.FixedZone("test", 3600))
	wantInfo := SegmentInfo{
		SegmentUUID:     []byte{0x10, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
		SegmentFilename: "camera-a.mkv",
		PrevUUID:        []byte{0x20, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
		PrevFilename:    "camera-prev.mkv",
		NextUUID:        []byte{0x30, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
		NextFilename:    "camera-next.mkv",
		DurationNS:      123_000_000,
		DurationSet:     true,
		Title:           "camera main",
		DateUTC:         created.UTC(),
		DateUTCSet:      true,
		MuxingApp:       "goav-test-mux",
		WritingApp:      "goav-test-write",
	}

	var buffer bytes.Buffer
	opts := MuxerOptions{
		MuxingApp:  wantInfo.MuxingApp,
		WritingApp: wantInfo.WritingApp,
		Info: SegmentInfo{
			SegmentUUID:     append([]byte(nil), wantInfo.SegmentUUID...),
			SegmentFilename: wantInfo.SegmentFilename,
			PrevUUID:        append([]byte(nil), wantInfo.PrevUUID...),
			PrevFilename:    wantInfo.PrevFilename,
			NextUUID:        append([]byte(nil), wantInfo.NextUUID...),
			NextFilename:    wantInfo.NextFilename,
			DurationNS:      wantInfo.DurationNS,
			DurationSet:     wantInfo.DurationSet,
			Title:           wantInfo.Title,
			DateUTC:         created,
			DateUTCSet:      true,
		},
	}
	muxer, err := NewMuxer(&buffer, opts)
	if err != nil {
		t.Fatal(err)
	}
	opts.Info.SegmentUUID[0] = 0xff
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

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gotInfo := demuxer.Info()
	if !reflect.DeepEqual(gotInfo, wantInfo) {
		t.Fatalf("info = %+v, want %+v", gotInfo, wantInfo)
	}
	gotInfo.SegmentUUID[0] = 0xee
	fresh := demuxer.Info()
	if !bytes.Equal(fresh.SegmentUUID, wantInfo.SegmentUUID) {
		t.Fatalf("segment uuid alias was not protected: %x", fresh.SegmentUUID)
	}
}

func TestMuxerDemuxerPreservesUnknownSegmentElements(t *testing.T) {
	unknownID := ebml.ID(0x4fff)
	raw := unknownElementBytes(t, unknownID, []byte{0xaa, 0xbb, 0xcc})
	ws := &memoryWriteSeeker{}
	opts := MuxerOptions{
		UnknownSegmentElements: []UnknownElement{{Raw: append([]byte(nil), raw...)}},
	}
	muxer, err := NewMuxer(ws, opts)
	if err != nil {
		t.Fatal(err)
	}
	opts.UnknownSegmentElements[0].Raw[0] = 0
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(ws.bytes, raw) {
		t.Fatalf("muxed data does not contain raw unknown element %x", raw)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	elements := demuxer.UnknownSegmentElements()
	if len(elements) != 1 || elements[0].ID != uint64(unknownID) || !bytes.Equal(elements[0].Raw, raw) {
		t.Fatalf("unknown elements = %+v, want id=0x%x raw=%x", elements, uint64(unknownID), raw)
	}
	elements[0].Raw[0] = 0
	fresh := demuxer.UnknownSegmentElements()
	if !bytes.Equal(fresh[0].Raw, raw) {
		t.Fatalf("unknown element alias was not protected: %x", fresh[0].Raw)
	}
}

func TestMuxerDemuxerRemuxesNestedUnknownElements(t *testing.T) {
	infoUnknown := unknownElementBytes(t, 0x4ff1, []byte{0x01})
	trackUnknown := unknownElementBytes(t, 0x4ff2, []byte{0x02, 0x12})
	attachmentUnknown := unknownElementBytes(t, 0x4ff3, []byte{0x03, 0x13, 0x23})
	editionUnknown := unknownElementBytes(t, 0x4ff4, []byte{0x04})
	chapterUnknown := unknownElementBytes(t, 0x4ff5, []byte{0x05})
	displayUnknown := unknownElementBytes(t, 0x4ff6, []byte{0x06})
	tagUnknown := unknownElementBytes(t, 0x4ff7, []byte{0x07})
	targetUnknown := unknownElementBytes(t, 0x4ff8, []byte{0x08})
	simpleUnknown := unknownElementBytes(t, 0x4ff9, []byte{0x09})
	childSimpleUnknown := unknownElementBytes(t, 0x4ffa, []byte{0x0a})
	allUnknowns := [][]byte{
		infoUnknown,
		trackUnknown,
		attachmentUnknown,
		editionUnknown,
		chapterUnknown,
		displayUnknown,
		tagUnknown,
		targetUnknown,
		simpleUnknown,
		childSimpleUnknown,
	}

	var source bytes.Buffer
	opts := MuxerOptions{
		Info: SegmentInfo{
			Title:           "nested unknowns",
			UnknownElements: []UnknownElement{{Raw: append([]byte(nil), infoUnknown...)}},
		},
		Attachments: []Attachment{{
			Filename:        "note.txt",
			MIMEType:        "text/plain",
			Data:            []byte("attachment"),
			UnknownElements: []UnknownElement{{Raw: append([]byte(nil), attachmentUnknown...)}},
		}},
		Chapters: []ChapterEdition{{
			UnknownElements: []UnknownElement{{Raw: append([]byte(nil), editionUnknown...)}},
			Chapters: []Chapter{{
				StartNS:         0,
				UnknownElements: []UnknownElement{{Raw: append([]byte(nil), chapterUnknown...)}},
				Displays: []ChapterDisplay{{
					String:          "Intro",
					UnknownElements: []UnknownElement{{Raw: append([]byte(nil), displayUnknown...)}},
				}},
			}},
		}},
		Tags: []Tag{{
			UnknownElements: []UnknownElement{{Raw: append([]byte(nil), tagUnknown...)}},
			Target: TagTarget{
				UnknownElements: []UnknownElement{{Raw: append([]byte(nil), targetUnknown...)}},
			},
			Simple: []SimpleTag{{
				Name:            "TITLE",
				String:          "hello",
				StringSet:       true,
				UnknownElements: []UnknownElement{{Raw: append([]byte(nil), simpleUnknown...)}},
				Children: []SimpleTag{{
					Name:            "CHILD",
					String:          "world",
					StringSet:       true,
					UnknownElements: []UnknownElement{{Raw: append([]byte(nil), childSimpleUnknown...)}},
				}},
			}},
		}},
	}
	muxer, err := NewMuxer(&source, opts)
	if err != nil {
		t.Fatal(err)
	}
	opts.Info.UnknownElements[0].Raw[0] = 0
	opts.Attachments[0].UnknownElements[0].Raw[0] = 0
	opts.Chapters[0].UnknownElements[0].Raw[0] = 0
	opts.Chapters[0].Chapters[0].UnknownElements[0].Raw[0] = 0
	opts.Chapters[0].Chapters[0].Displays[0].UnknownElements[0].Raw[0] = 0
	opts.Tags[0].UnknownElements[0].Raw[0] = 0
	opts.Tags[0].Target.UnknownElements[0].Raw[0] = 0
	opts.Tags[0].Simple[0].UnknownElements[0].Raw[0] = 0
	opts.Tags[0].Simple[0].Children[0].UnknownElements[0].Raw[0] = 0

	track := Track{
		Type:            TrackVideo,
		Codec:           CodecVP8,
		Video:           VideoConfig{Width: 16, Height: 16},
		UnknownElements: []UnknownElement{{Raw: append([]byte(nil), trackUnknown...)}},
	}
	trackID, err := muxer.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	track.UnknownElements[0].Raw[0] = 0
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, raw := range allUnknowns {
		if !bytes.Contains(source.Bytes(), raw) {
			t.Fatalf("muxed data does not contain raw unknown element %x", raw)
		}
	}

	demuxer, err := NewDemuxer(bytes.NewReader(source.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertUnknownElement(t, "info", demuxer.Info().UnknownElements, 0x4ff1, infoUnknown)
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	assertUnknownElement(t, "track", tracks[0].UnknownElements, 0x4ff2, trackUnknown)
	attachments := demuxer.Attachments()
	if len(attachments) != 1 {
		t.Fatalf("attachments = %d, want 1", len(attachments))
	}
	assertUnknownElement(t, "attachment", attachments[0].UnknownElements, 0x4ff3, attachmentUnknown)
	chapters := demuxer.Chapters()
	if len(chapters) != 1 || len(chapters[0].Chapters) != 1 || len(chapters[0].Chapters[0].Displays) != 1 {
		t.Fatalf("chapters = %+v, want one edition/chapter/display", chapters)
	}
	assertUnknownElement(t, "edition", chapters[0].UnknownElements, 0x4ff4, editionUnknown)
	assertUnknownElement(t, "chapter", chapters[0].Chapters[0].UnknownElements, 0x4ff5, chapterUnknown)
	assertUnknownElement(t, "display", chapters[0].Chapters[0].Displays[0].UnknownElements, 0x4ff6, displayUnknown)
	tags := demuxer.Tags()
	if len(tags) != 1 || len(tags[0].Simple) != 1 || len(tags[0].Simple[0].Children) != 1 {
		t.Fatalf("tags = %+v, want one tag/simple/child", tags)
	}
	assertUnknownElement(t, "tag", tags[0].UnknownElements, 0x4ff7, tagUnknown)
	assertUnknownElement(t, "target", tags[0].Target.UnknownElements, 0x4ff8, targetUnknown)
	assertUnknownElement(t, "simple tag", tags[0].Simple[0].UnknownElements, 0x4ff9, simpleUnknown)
	assertUnknownElement(t, "child simple tag", tags[0].Simple[0].Children[0].UnknownElements, 0x4ffa, childSimpleUnknown)

	info := demuxer.Info()
	info.UnknownElements[0].Raw[0] = 0
	assertUnknownElement(t, "fresh info", demuxer.Info().UnknownElements, 0x4ff1, infoUnknown)
	tracks[0].UnknownElements[0].Raw[0] = 0
	assertUnknownElement(t, "fresh track", demuxer.Tracks()[0].UnknownElements, 0x4ff2, trackUnknown)
	attachments[0].UnknownElements[0].Raw[0] = 0
	assertUnknownElement(t, "fresh attachment", demuxer.Attachments()[0].UnknownElements, 0x4ff3, attachmentUnknown)
	chapters[0].Chapters[0].Displays[0].UnknownElements[0].Raw[0] = 0
	assertUnknownElement(t, "fresh display", demuxer.Chapters()[0].Chapters[0].Displays[0].UnknownElements, 0x4ff6, displayUnknown)
	tags[0].Simple[0].Children[0].UnknownElements[0].Raw[0] = 0
	assertUnknownElement(t, "fresh child simple tag", demuxer.Tags()[0].Simple[0].Children[0].UnknownElements, 0x4ffa, childSimpleUnknown)

	var remuxed bytes.Buffer
	remuxer, err := NewMuxer(&remuxed, MuxerOptions{
		Info:        demuxer.Info(),
		Attachments: demuxer.Attachments(),
		Chapters:    demuxer.Chapters(),
		Tags:        demuxer.Tags(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, track := range demuxer.Tracks() {
		if _, err := remuxer.AddTrack(track); err != nil {
			t.Fatal(err)
		}
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if err := remuxer.WritePacket(got); err != nil {
		t.Fatal(err)
	}
	if err := remuxer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, raw := range allUnknowns {
		if !bytes.Contains(remuxed.Bytes(), raw) {
			t.Fatalf("remuxed data does not contain raw unknown element %x", raw)
		}
	}
}

func TestMuxerDemuxerRemuxesUnknownMasterElements(t *testing.T) {
	tracksUnknown := unknownElementBytes(t, 0x4feb, []byte{0x11})
	attachmentsUnknown := unknownElementBytes(t, 0x4fec, []byte{0x12, 0x22})
	chaptersUnknown := unknownElementBytes(t, 0x4fed, []byte{0x13, 0x23, 0x33})
	tagsUnknown := unknownElementBytes(t, 0x4fee, []byte{0x14})
	allUnknowns := [][]byte{tracksUnknown, attachmentsUnknown, chaptersUnknown, tagsUnknown}

	ws := &memoryWriteSeeker{}
	opts := MuxerOptions{
		UnknownTracksElements:      []UnknownElement{{Raw: append([]byte(nil), tracksUnknown...)}},
		UnknownAttachmentsElements: []UnknownElement{{Raw: append([]byte(nil), attachmentsUnknown...)}},
		UnknownChaptersElements:    []UnknownElement{{Raw: append([]byte(nil), chaptersUnknown...)}},
		UnknownTagsElements:        []UnknownElement{{Raw: append([]byte(nil), tagsUnknown...)}},
	}
	muxer, err := NewMuxer(ws, opts)
	if err != nil {
		t.Fatal(err)
	}
	opts.UnknownTracksElements[0].Raw[0] = 0
	opts.UnknownAttachmentsElements[0].Raw[0] = 0
	opts.UnknownChaptersElements[0].Raw[0] = 0
	opts.UnknownTagsElements[0].Raw[0] = 0
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, raw := range allUnknowns {
		if !bytes.Contains(ws.bytes, raw) {
			t.Fatalf("muxed data does not contain raw unknown master child %x", raw)
		}
	}
	positions := collectTopLevelPositions(t, ws.bytes)
	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertSeekEntry(t, demuxer.SeekEntries(), idAttachments, positions[idAttachments])
	assertSeekEntry(t, demuxer.SeekEntries(), idChapters, positions[idChapters])
	assertSeekEntry(t, demuxer.SeekEntries(), idTags, positions[idTags])
	assertUnknownElement(t, "tracks master", demuxer.UnknownTracksElements(), 0x4feb, tracksUnknown)
	assertUnknownElement(t, "attachments master", demuxer.UnknownAttachmentsElements(), 0x4fec, attachmentsUnknown)
	assertUnknownElement(t, "chapters master", demuxer.UnknownChaptersElements(), 0x4fed, chaptersUnknown)
	assertUnknownElement(t, "tags master", demuxer.UnknownTagsElements(), 0x4fee, tagsUnknown)

	elements := demuxer.UnknownTracksElements()
	elements[0].Raw[0] = 0
	assertUnknownElement(t, "fresh tracks master", demuxer.UnknownTracksElements(), 0x4feb, tracksUnknown)
	elements = demuxer.UnknownAttachmentsElements()
	elements[0].Raw[0] = 0
	assertUnknownElement(t, "fresh attachments master", demuxer.UnknownAttachmentsElements(), 0x4fec, attachmentsUnknown)
	elements = demuxer.UnknownChaptersElements()
	elements[0].Raw[0] = 0
	assertUnknownElement(t, "fresh chapters master", demuxer.UnknownChaptersElements(), 0x4fed, chaptersUnknown)
	elements = demuxer.UnknownTagsElements()
	elements[0].Raw[0] = 0
	assertUnknownElement(t, "fresh tags master", demuxer.UnknownTagsElements(), 0x4fee, tagsUnknown)

	var remuxed bytes.Buffer
	remuxer, err := NewMuxer(&remuxed, MuxerOptions{
		UnknownTracksElements:      demuxer.UnknownTracksElements(),
		UnknownAttachmentsElements: demuxer.UnknownAttachmentsElements(),
		UnknownChaptersElements:    demuxer.UnknownChaptersElements(),
		UnknownTagsElements:        demuxer.UnknownTagsElements(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, track := range demuxer.Tracks() {
		if _, err := remuxer.AddTrack(track); err != nil {
			t.Fatal(err)
		}
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if err := remuxer.WritePacket(got); err != nil {
		t.Fatal(err)
	}
	if err := remuxer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, raw := range allUnknowns {
		if !bytes.Contains(remuxed.Bytes(), raw) {
			t.Fatalf("remuxed data does not contain raw unknown master child %x", raw)
		}
	}
}

func TestMuxerDemuxerRemuxesUnknownClusterElements(t *testing.T) {
	clusterUnknown := unknownElementBytes(t, 0x4fe9, []byte{0x21, 0x31})
	var source bytes.Buffer
	muxer, err := NewMuxer(&source, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{
		TrackID:                trackID,
		TimeNS:                 0,
		Keyframe:               true,
		Data:                   []byte{1, 2, 3},
		UnknownClusterElements: []UnknownElement{{Raw: append([]byte(nil), clusterUnknown...)}},
	}
	if err := muxer.WritePacket(packet); err != nil {
		t.Fatal(err)
	}
	packet.UnknownClusterElements[0].Raw[0] = 0
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(source.Bytes(), clusterUnknown) {
		t.Fatalf("muxed data does not contain raw unknown Cluster child %x", clusterUnknown)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(source.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	assertUnknownElement(t, "packet cluster", got.UnknownClusterElements, 0x4fe9, clusterUnknown)
	got.UnknownClusterElements[0].Raw[0] = 0
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}

	demuxer, err = NewDemuxer(bytes.NewReader(source.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var remuxed bytes.Buffer
	remuxer, err := NewMuxer(&remuxed, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, track := range demuxer.Tracks() {
		if _, err := remuxer.AddTrack(track); err != nil {
			t.Fatal(err)
		}
	}
	got = Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if err := remuxer.WritePacket(got); err != nil {
		t.Fatal(err)
	}
	if err := remuxer.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(remuxed.Bytes(), clusterUnknown) {
		t.Fatalf("remuxed data does not contain raw unknown Cluster child %x", clusterUnknown)
	}
	redemuxer, err := NewDemuxer(bytes.NewReader(remuxed.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got = Packet{Data: make([]byte, 0, 8)}
	if err := redemuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	assertUnknownElement(t, "remuxed packet cluster", got.UnknownClusterElements, 0x4fe9, clusterUnknown)
}

func TestMuxerRejectsInvalidUnknownSegmentElements(t *testing.T) {
	unknownID := ebml.ID(0x4ffd)
	valid := unknownElementBytes(t, unknownID, []byte{1})
	known := unknownElementBytes(t, idInfo, []byte{})
	trailing := append(append([]byte(nil), valid...), 0)
	var unknownSize bytes.Buffer
	unknownSizeWriter := ebml.NewWriter(&unknownSize)
	if err := unknownSizeWriter.WriteUnknownHeader(unknownID, 1); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		element UnknownElement
	}{
		{name: "empty", element: UnknownElement{}},
		{name: "known segment id", element: UnknownElement{Raw: known}},
		{name: "mismatched id", element: UnknownElement{ID: uint64(unknownID) + 1, Raw: valid}},
		{name: "trailing bytes", element: UnknownElement{Raw: trailing}},
		{name: "unknown size", element: UnknownElement{Raw: unknownSize.Bytes()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewMuxer(discardWriter{}, MuxerOptions{UnknownSegmentElements: []UnknownElement{tt.element}}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidUnknownClusterElements(t *testing.T) {
	known := unknownElementBytes(t, idSimpleBlock, []byte{})
	var unknownSize bytes.Buffer
	unknownSizeWriter := ebml.NewWriter(&unknownSize)
	if err := unknownSizeWriter.WriteUnknownHeader(0x4fe6, 1); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		element UnknownElement
	}{
		{name: "known cluster id", element: UnknownElement{Raw: known}},
		{name: "unknown size", element: UnknownElement{Raw: unknownSize.Bytes()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 16, Height: 16},
			})
			if err != nil {
				t.Fatal(err)
			}
			err = muxer.WritePacket(Packet{
				TrackID:                trackID,
				TimeNS:                 0,
				Keyframe:               true,
				Data:                   []byte{1},
				UnknownClusterElements: []UnknownElement{tt.element},
			})
			if !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidUnknownMasterElements(t *testing.T) {
	tests := []struct {
		name string
		opts MuxerOptions
	}{
		{
			name: "known tracks child",
			opts: MuxerOptions{UnknownTracksElements: []UnknownElement{{
				Raw: unknownElementBytes(t, idTrackEntry, []byte{}),
			}}},
		},
		{
			name: "known attachments child",
			opts: MuxerOptions{UnknownAttachmentsElements: []UnknownElement{{
				Raw: unknownElementBytes(t, idAttachedFile, []byte{}),
			}}},
		},
		{
			name: "known chapters child",
			opts: MuxerOptions{UnknownChaptersElements: []UnknownElement{{
				Raw: unknownElementBytes(t, idEditionEntry, []byte{}),
			}}},
		},
		{
			name: "known tags child",
			opts: MuxerOptions{UnknownTagsElements: []UnknownElement{{
				Raw: unknownElementBytes(t, idTag, []byte{}),
			}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewMuxer(discardWriter{}, tt.opts); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidNestedUnknownElements(t *testing.T) {
	t.Run("known info id", func(t *testing.T) {
		raw := unknownElementBytes(t, idTitle, []byte("x"))
		_, err := NewMuxer(discardWriter{}, MuxerOptions{
			Info: SegmentInfo{UnknownElements: []UnknownElement{{Raw: raw}}},
		})
		if !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("known track id", func(t *testing.T) {
		raw := unknownElementBytes(t, idCodecName, []byte("x"))
		muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = muxer.AddTrack(Track{
			Type:            TrackVideo,
			Codec:           CodecVP8,
			Video:           VideoConfig{Width: 16, Height: 16},
			UnknownElements: []UnknownElement{{Raw: raw}},
		})
		if !errors.Is(err, ErrInvalidTrack) {
			t.Fatalf("err = %v, want ErrInvalidTrack", err)
		}
	})
	t.Run("known attachment id", func(t *testing.T) {
		raw := unknownElementBytes(t, idFileName, []byte("x"))
		_, err := NewMuxer(discardWriter{}, MuxerOptions{
			Attachments: []Attachment{{
				Filename:        "note.txt",
				MIMEType:        "text/plain",
				Data:            []byte{1},
				UnknownElements: []UnknownElement{{Raw: raw}},
			}},
		})
		if !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("known chapter id", func(t *testing.T) {
		raw := unknownElementBytes(t, idChapterUID, []byte{1})
		_, err := NewMuxer(discardWriter{}, MuxerOptions{
			Chapters: []ChapterEdition{{
				Chapters: []Chapter{{
					StartNS:         0,
					UnknownElements: []UnknownElement{{Raw: raw}},
				}},
			}},
		})
		if !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("known tag id", func(t *testing.T) {
		raw := unknownElementBytes(t, idTagName, []byte("x"))
		_, err := NewMuxer(discardWriter{}, MuxerOptions{
			Tags: []Tag{{
				Simple: []SimpleTag{{
					Name:            "TITLE",
					String:          "x",
					StringSet:       true,
					UnknownElements: []UnknownElement{{Raw: raw}},
				}},
			}},
		})
		if !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
}

func TestMuxerDemuxerRoundTrip(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{DocType: "webm"})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []Packet{
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{0x11, 0x22, 0x33}},
		{TrackID: audioID, TimeNS: 20_000_000, Keyframe: true, Data: []byte{0x44, 0x55}},
		{TrackID: videoID, TimeNS: 33_000_000, Data: []byte{0x66, 0x77, 0x88, 0x99}},
	}
	for i := range want {
		if err := muxer.WritePacket(want[i]); err != nil {
			t.Fatalf("write packet %d: %v", i, err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buffer.Bytes()[:4], []byte{0x1a, 0x45, 0xdf, 0xa3}) {
		t.Fatalf("missing ebml header magic")
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 2 {
		t.Fatalf("tracks = %d, want 2", len(tracks))
	}
	if tracks[0].Codec != CodecVP8 || tracks[0].Video.Width != 640 || tracks[1].Codec != CodecOpus || tracks[1].Audio.SampleRate != 48000 {
		t.Fatalf("tracks = %+v", tracks)
	}
	got := Packet{Data: make([]byte, 0, 16)}
	for i := range want {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatalf("read packet %d: %v", i, err)
		}
		if got.TrackID != want[i].TrackID || got.TimeNS != want[i].TimeNS || got.Keyframe != want[i].Keyframe ||
			!bytes.Equal(got.Data, want[i].Data) {
			t.Fatalf("packet %d = %+v data=%v, want %+v data=%v", i, got, got.Data, want[i], want[i].Data)
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestMuxerDemuxerPreservesAudioOutputSampleRate(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{
			SampleRate:       44100,
			OutputSampleRate: 48000,
			Channels:         2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0x01, 0x02},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if tracks[0].Audio.SampleRate != 44100 || tracks[0].Audio.OutputSampleRate != 48000 {
		t.Fatalf("audio = %+v", tracks[0].Audio)
	}
}

func TestMuxerDemuxerPreservesVideoDisplayMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantVideo := VideoConfig{
		Width:              1920,
		Height:             1080,
		FlagInterlaced:     1,
		FlagInterlacedSet:  true,
		FieldOrder:         1,
		FieldOrderSet:      true,
		PixelCropBottom:    2,
		PixelCropTop:       4,
		PixelCropLeft:      6,
		PixelCropRight:     8,
		DisplayWidth:       16,
		DisplayHeight:      9,
		DisplayUnit:        3,
		AspectRatioType:    1,
		AspectRatioTypeSet: true,
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: wantVideo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0x9d, 0x01, 0x2a},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if !reflect.DeepEqual(tracks[0].Video, wantVideo) {
		t.Fatalf("video = %+v, want %+v", tracks[0].Video, wantVideo)
	}
}

func TestMuxerDemuxerPreservesVideoModeMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantVideo := VideoConfig{
		Width:         640,
		Height:        360,
		StereoMode:    11,
		StereoModeSet: true,
		AlphaMode:     0,
		AlphaModeSet:  true,
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: wantVideo,
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

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if !reflect.DeepEqual(tracks[0].Video, wantVideo) {
		t.Fatalf("video = %+v, want %+v", tracks[0].Video, wantVideo)
	}
}

func TestMuxerDemuxerPreservesVideoColourMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantVideo := VideoConfig{
		Width:  640,
		Height: 360,
		Colour: VideoColourConfig{
			MatrixCoefficients:         9,
			MatrixCoefficientsSet:      true,
			BitsPerChannel:             0,
			BitsPerChannelSet:          true,
			ChromaSubsamplingHorz:      1,
			ChromaSubsamplingHorzSet:   true,
			ChromaSubsamplingVert:      1,
			ChromaSubsamplingVertSet:   true,
			CbSubsamplingHorz:          1,
			CbSubsamplingHorzSet:       true,
			CbSubsamplingVert:          1,
			CbSubsamplingVertSet:       true,
			ChromaSitingHorz:           0,
			ChromaSitingHorzSet:        true,
			ChromaSitingVert:           0,
			ChromaSitingVertSet:        true,
			Range:                      1,
			RangeSet:                   true,
			TransferCharacteristics:    16,
			TransferCharacteristicsSet: true,
			Primaries:                  9,
			PrimariesSet:               true,
			MaxCLL:                     1000,
			MaxCLLSet:                  true,
			MaxFALL:                    400,
			MaxFALLSet:                 true,
			MasteringMetadata: VideoMasteringMetadataConfig{
				PrimaryRChromaticityX:      0.708,
				PrimaryRChromaticityXSet:   true,
				PrimaryRChromaticityY:      0.292,
				PrimaryRChromaticityYSet:   true,
				PrimaryGChromaticityX:      0.17,
				PrimaryGChromaticityXSet:   true,
				PrimaryGChromaticityY:      0.797,
				PrimaryGChromaticityYSet:   true,
				PrimaryBChromaticityX:      0.131,
				PrimaryBChromaticityXSet:   true,
				PrimaryBChromaticityY:      0.046,
				PrimaryBChromaticityYSet:   true,
				WhitePointChromaticityX:    0.3127,
				WhitePointChromaticityXSet: true,
				WhitePointChromaticityY:    0.329,
				WhitePointChromaticityYSet: true,
				LuminanceMax:               1000,
				LuminanceMaxSet:            true,
				LuminanceMin:               0.005,
				LuminanceMinSet:            true,
			},
		},
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: wantVideo,
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

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if !reflect.DeepEqual(tracks[0].Video, wantVideo) {
		t.Fatalf("video = %+v, want %+v", tracks[0].Video, wantVideo)
	}
}

func TestMuxerDemuxerPreservesVideoProjectionMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantVideo := VideoConfig{
		Width:  640,
		Height: 360,
		Projection: VideoProjectionConfig{
			Set:       true,
			Type:      1,
			Private:   []byte{0, 0, 0, 0},
			PoseYaw:   45,
			PosePitch: -10.5,
			PoseRoll:  180,
		},
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: wantVideo,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantVideo.Projection.Private[0] = 0xff
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

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	wantVideo.Projection.Private[0] = 0
	if !reflect.DeepEqual(tracks[0].Video, wantVideo) {
		t.Fatalf("video = %+v, want %+v", tracks[0].Video, wantVideo)
	}
	tracks[0].Video.Projection.Private[0] = 0xee
	fresh := demuxer.Tracks()
	if !bytes.Equal(fresh[0].Video.Projection.Private, []byte{0, 0, 0, 0}) {
		t.Fatalf("projection private alias was not protected: %x", fresh[0].Video.Projection.Private)
	}
}

func TestMuxerDemuxerPreservesDefaultVideoProjectionMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantVideo := VideoConfig{
		Width:  640,
		Height: 360,
		Projection: VideoProjectionConfig{
			Set: true,
		},
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: wantVideo,
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

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if !reflect.DeepEqual(tracks[0].Video, wantVideo) {
		t.Fatalf("video = %+v, want %+v", tracks[0].Video, wantVideo)
	}
}

func TestMuxerDemuxerPreservesTrackUIDAndFlags(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		ID:                      7,
		UID:                     12345,
		Type:                    TrackVideo,
		Codec:                   CodecVP8,
		FlagEnabled:             false,
		FlagEnabledSet:          true,
		FlagDefault:             false,
		FlagDefaultSet:          true,
		FlagForced:              true,
		FlagForcedSet:           true,
		FlagHearingImpaired:     true,
		FlagHearingImpairedSet:  true,
		FlagVisualImpaired:      false,
		FlagVisualImpairedSet:   true,
		FlagTextDescriptions:    true,
		FlagTextDescriptionsSet: true,
		FlagOriginal:            false,
		FlagOriginalSet:         true,
		FlagCommentary:          true,
		FlagCommentarySet:       true,
		FlagLacing:              false,
		FlagLacingSet:           true,
		Video:                   VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	if trackID != 7 {
		t.Fatalf("trackID = %d, want 7", trackID)
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

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	track := tracks[0]
	if track.ID != 7 || track.UID != 12345 ||
		track.FlagEnabled || !track.FlagEnabledSet ||
		track.FlagDefault || !track.FlagDefaultSet ||
		!track.FlagForced || !track.FlagForcedSet ||
		!track.FlagHearingImpaired || !track.FlagHearingImpairedSet ||
		track.FlagVisualImpaired || !track.FlagVisualImpairedSet ||
		!track.FlagTextDescriptions || !track.FlagTextDescriptionsSet ||
		track.FlagOriginal || !track.FlagOriginalSet ||
		!track.FlagCommentary || !track.FlagCommentarySet ||
		track.FlagLacing || !track.FlagLacingSet {
		t.Fatalf("track = %+v", track)
	}
}

func TestMuxerDemuxerPreservesBlockAdditionMapping(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	track := Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		BlockAdditionMappings: []BlockAdditionMapping{{
			IDValue:   2,
			Name:      "alpha",
			Type:      1,
			ExtraData: []byte{1, 2, 3},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
	}
	trackID, err := muxer.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	track.BlockAdditionMappings[0].ExtraData[0] = 0xff
	if err := muxer.WritePacket(Packet{
		TrackID:        trackID,
		TimeNS:         0,
		Keyframe:       true,
		Data:           []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
		BlockAdditions: []BlockAddition{{ID: 2, Data: []byte{0xaa}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	wantMapping := BlockAdditionMapping{
		IDValue:   2,
		Name:      "alpha",
		Type:      1,
		ExtraData: []byte{1, 2, 3},
	}
	if tracks[0].MaxBlockAdditionID != 2 ||
		len(tracks[0].BlockAdditionMappings) != 1 ||
		!equalBlockAdditionMapping(tracks[0].BlockAdditionMappings[0], wantMapping) {
		t.Fatalf("track = %+v, want mapping %+v", tracks[0], wantMapping)
	}
	tracks[0].BlockAdditionMappings[0].ExtraData[0] = 0xee
	fresh := demuxer.Tracks()
	if !bytes.Equal(fresh[0].BlockAdditionMappings[0].ExtraData, []byte{1, 2, 3}) {
		t.Fatalf("mapping extra data alias was not protected: %x", fresh[0].BlockAdditionMappings[0].ExtraData)
	}
	packet := Packet{Data: make([]byte, 0, 16)}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if len(packet.BlockAdditions) != 1 || packet.BlockAdditions[0].ID != 2 ||
		!bytes.Equal(packet.BlockAdditions[0].Data, []byte{0xaa}) {
		t.Fatalf("packet additions = %+v", packet.BlockAdditions)
	}
}

func TestMuxerDemuxerPreservesTrackEntryMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	track := Track{
		UID:               42,
		Type:              TrackVideo,
		Codec:             CodecVP8,
		MinCache:          2,
		MinCacheSet:       true,
		MaxCache:          5,
		MaxCacheSet:       true,
		CodecDecodeAll:    false,
		CodecDecodeAllSet: true,
		TrackOverlays:     []uint64{101, 202},
		TrackTranslates: []TrackTranslate{{
			TrackID:     []byte{0x01, 0x02},
			Codec:       1,
			EditionUIDs: []uint64{7, 8},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
	}
	trackID, err := muxer.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	track.TrackOverlays[0] = 999
	track.TrackTranslates[0].TrackID[0] = 0xff
	track.TrackTranslates[0].EditionUIDs[0] = 99
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

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	got := tracks[0]
	wantTranslate := TrackTranslate{TrackID: []byte{0x01, 0x02}, Codec: 1, EditionUIDs: []uint64{7, 8}}
	if got.MinCache != 2 || !got.MinCacheSet ||
		got.MaxCache != 5 || !got.MaxCacheSet ||
		got.CodecDecodeAll || !got.CodecDecodeAllSet ||
		!reflect.DeepEqual(got.TrackOverlays, []uint64{101, 202}) ||
		len(got.TrackTranslates) != 1 ||
		!equalTrackTranslate(got.TrackTranslates[0], wantTranslate) {
		t.Fatalf("track = %+v, want translate %+v", got, wantTranslate)
	}
	tracks[0].TrackOverlays[0] = 303
	tracks[0].TrackTranslates[0].TrackID[0] = 0xee
	tracks[0].TrackTranslates[0].EditionUIDs[0] = 77
	fresh := demuxer.Tracks()
	if !reflect.DeepEqual(fresh[0].TrackOverlays, []uint64{101, 202}) ||
		!equalTrackTranslate(fresh[0].TrackTranslates[0], wantTranslate) {
		t.Fatalf("track metadata alias was not protected: %+v", fresh[0])
	}
}

func TestMuxerDemuxerPreservesDecodedFieldDuration(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:                          TrackVideo,
		Codec:                         CodecVP8,
		DefaultDurationNS:             20_000_000,
		DefaultDecodedFieldDurationNS: 10_000_000,
		Video:                         VideoConfig{Width: 640, Height: 360},
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

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if tracks[0].DefaultDurationNS != 20_000_000 || tracks[0].DefaultDecodedFieldDurationNS != 10_000_000 {
		t.Fatalf("track = %+v", tracks[0])
	}
}

func TestMuxerDemuxerPreservesLanguageBCP47(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:          TrackAudio,
		Codec:         CodecOpus,
		Language:      "por",
		LanguageBCP47: "pt-BR",
		Audio:         AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0xf8, 0xff, 0xfe},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if tracks[0].Language != "por" || tracks[0].LanguageBCP47 != "pt-BR" {
		t.Fatalf("track language legacy=%q bcp47=%q", tracks[0].Language, tracks[0].LanguageBCP47)
	}
}

func TestMuxerDefaultsTrackUIDAndFlags(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
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

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if tracks[0].UID != uint64(trackID) ||
		!tracks[0].FlagEnabled || !tracks[0].FlagEnabledSet ||
		!tracks[0].FlagDefault || !tracks[0].FlagDefaultSet ||
		tracks[0].FlagForced || !tracks[0].FlagForcedSet ||
		tracks[0].FlagHearingImpairedSet ||
		tracks[0].FlagVisualImpairedSet ||
		tracks[0].FlagTextDescriptionsSet ||
		tracks[0].FlagOriginalSet ||
		tracks[0].FlagCommentarySet ||
		!tracks[0].FlagLacing || !tracks[0].FlagLacingSet {
		t.Fatalf("track = %+v", tracks[0])
	}
}

func TestMuxerRejectsDuplicateTrackUID(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		UID:   42,
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		UID:   42,
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	}); !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
}

func TestMuxerRejectsLacedPacketWhenTrackDisablesLacing(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		FlagLacing:        false,
		FlagLacingSet:     true,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{{1}, {2}},
	}); !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
}

func TestMuxerDemuxerPreservesBlockGroupDuration(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{
		TrackID:    trackID,
		TimeNS:     40_000_000,
		DurationNS: 20_000_000,
		Keyframe:   false,
		Data:       []byte{0x11, 0x22, 0x33},
	}
	if err := muxer.WritePacket(packet); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != packet.TrackID || got.TimeNS != packet.TimeNS || got.DurationNS != packet.DurationNS ||
		got.Keyframe || len(got.ReferenceBlockTimeNS) != 1 || got.ReferenceBlockTimeNS[0] != 0 ||
		!bytes.Equal(got.Data, packet.Data) {
		t.Fatalf("packet = %+v data=%v, want %+v data=%v", got, got.Data, packet, packet.Data)
	}
}

func TestMuxerDemuxerPreservesBlockGroupReferences(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{
		TrackID:              trackID,
		TimeNS:               40_000_000,
		DurationNS:           20_000_000,
		ReferenceBlockTimeNS: []int64{-20_000_000, 40_000_000},
		Keyframe:             false,
		Data:                 []byte{0x11, 0x22, 0x33},
	}
	if err := muxer.WritePacket(packet); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{
		Data:                 make([]byte, 0, 8),
		ReferenceBlockTimeNS: make([]int64, 0, 2),
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != packet.TrackID || got.TimeNS != packet.TimeNS || got.DurationNS != packet.DurationNS ||
		got.Keyframe || !equalInt64s(got.ReferenceBlockTimeNS, packet.ReferenceBlockTimeNS) ||
		!bytes.Equal(got.Data, packet.Data) {
		t.Fatalf("packet = %+v data=%v, want %+v data=%v", got, got.Data, packet, packet.Data)
	}
}

func TestMuxerWritesReferenceOnlyBlockGroup(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{
		TrackID:              trackID,
		TimeNS:               40_000_000,
		ReferenceBlockTimeNS: []int64{-20_000_000},
		Data:                 []byte{0x11, 0x22, 0x33},
	}
	if err := muxer.WritePacket(packet); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{
		Data:                 make([]byte, 0, 8),
		ReferenceBlockTimeNS: make([]int64, 0, 1),
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != packet.TrackID || got.TimeNS != packet.TimeNS || got.DurationNS != 0 ||
		got.Keyframe || !equalInt64s(got.ReferenceBlockTimeNS, packet.ReferenceBlockTimeNS) ||
		!bytes.Equal(got.Data, packet.Data) {
		t.Fatalf("packet = %+v data=%v, want %+v data=%v", got, got.Data, packet, packet.Data)
	}
}

func TestMuxerDemuxerPreservesBlockGroupDiscardPadding(t *testing.T) {
	tests := []struct {
		name      string
		paddingNS int64
	}{
		{name: "end", paddingNS: 13_000_000},
		{name: "beginning", paddingNS: -7_000_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{TimecodeScaleNS: 1_000_000})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(Track{
				Type:  TrackAudio,
				Codec: CodecOpus,
				Audio: AudioConfig{SampleRate: 48000, Channels: 2},
			})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{
				TrackID:          trackID,
				TimeNS:           40_000_000,
				DiscardPaddingNS: tt.paddingNS,
				Keyframe:         true,
				Data:             []byte{0xf8, 0xff, 0xfe},
			}
			if err := muxer.WritePacket(packet); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if paddingNS, ok := readFirstDiscardPadding(t, buffer.Bytes()); !ok || paddingNS != tt.paddingNS {
				t.Fatalf("raw DiscardPadding = %d ok=%v, want %d", paddingNS, ok, tt.paddingNS)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			got := Packet{Data: make([]byte, 0, 8)}
			if err := demuxer.ReadPacket(&got); err != nil {
				t.Fatal(err)
			}
			if got.TrackID != packet.TrackID || got.TimeNS != packet.TimeNS ||
				got.DurationNS != 0 || got.DiscardPaddingNS != packet.DiscardPaddingNS ||
				!got.Keyframe || !bytes.Equal(got.Data, packet.Data) {
				t.Fatalf("packet = %+v data=%v, want %+v data=%v", got, got.Data, packet, packet.Data)
			}
		})
	}
}

func TestMuxerDemuxerPreservesBlockGroupExtras(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := Packet{
		TrackID:           trackID,
		TimeNS:            40_000_000,
		ReferencePriority: 3,
		CodecState:        []byte{0xaa, 0xbb, 0xcc},
		BlockAdditions: []BlockAddition{
			{Data: []byte{0x10, 0x11}},
			{ID: 2, Data: []byte{0x20, 0x21, 0x22}},
		},
		Keyframe: true,
		Data:     []byte{0x01, 0x02, 0x03},
	}
	second := Packet{
		TrackID:  trackID,
		TimeNS:   80_000_000,
		Keyframe: true,
		Data:     []byte{0x04, 0x05},
	}
	if err := muxer.WritePacket(first); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(second); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{
		Data:           make([]byte, 0, 8),
		CodecState:     make([]byte, 0, 4),
		BlockAdditions: make([]BlockAddition, 0, 2),
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	wantAdditions := []BlockAddition{
		{ID: 1, Data: []byte{0x10, 0x11}},
		{ID: 2, Data: []byte{0x20, 0x21, 0x22}},
	}
	if got.TrackID != first.TrackID || got.TimeNS != first.TimeNS ||
		got.ReferencePriority != first.ReferencePriority ||
		!bytes.Equal(got.CodecState, first.CodecState) ||
		!equalBlockAdditions(got.BlockAdditions, wantAdditions) ||
		!got.Keyframe || !bytes.Equal(got.Data, first.Data) {
		t.Fatalf("packet = %+v data=%v, want extras from %+v", got, got.Data, first)
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != second.TrackID || got.TimeNS != second.TimeNS ||
		got.ReferencePriority != 0 || len(got.CodecState) != 0 ||
		len(got.BlockAdditions) != 0 || !got.Keyframe ||
		!bytes.Equal(got.Data, second.Data) {
		t.Fatalf("second packet retained extras: %+v data=%v", got, got.Data)
	}
}

func TestNanosecondTimecodeScaleRoundTrip(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{TimecodeScaleNS: 1})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := Packet{
		TrackID:    trackID,
		TimeNS:     123_456_789,
		DurationNS: 2_500_001,
		Keyframe:   true,
		Data:       []byte{1, 2, 3},
	}
	if err := muxer.WritePacket(want); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != want.TimeNS || got.DurationNS != want.DurationNS {
		t.Fatalf("time=%d duration=%d, want time=%d duration=%d", got.TimeNS, got.DurationNS, want.TimeNS, want.DurationNS)
	}
}

func TestMuxerRejectsInvalidPacketDuration(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		packet Packet
	}{
		{
			name: "negative",
			packet: Packet{
				TrackID:    trackID,
				TimeNS:     0,
				DurationNS: -1,
				Keyframe:   true,
				Data:       []byte{1},
			},
		},
		{
			name: "overflow",
			packet: Packet{
				TrackID:    trackID,
				TimeNS:     math.MaxInt64,
				DurationNS: 1,
				Keyframe:   true,
				Data:       []byte{1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := muxer.WritePacket(tt.packet); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsKeyframeReferences(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = muxer.WritePacket(Packet{
		TrackID:              trackID,
		TimeNS:               0,
		ReferenceBlockTimeNS: []int64{-20_000_000},
		Keyframe:             true,
		Data:                 []byte{1},
	})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerRejectsUnscaledReferenceBlockTimes(t *testing.T) {
	for _, reference := range []int64{-500_000, 500_000} {
		t.Run(strconv.FormatInt(reference, 10), func(t *testing.T) {
			muxer, err := NewMuxer(discardWriter{}, MuxerOptions{TimecodeScaleNS: 1_000_000})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			err = muxer.WritePacket(Packet{
				TrackID:              trackID,
				TimeNS:               20_000_000,
				ReferenceBlockTimeNS: []int64{reference},
				Data:                 []byte{1},
			})
			if !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsUnscaledLacedFrameDuration(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{TimecodeScaleNS: 1_000_000})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = muxer.WriteLacedPacket(LacedPacket{
		TrackID:         trackID,
		TimeNS:          20_000_000,
		FrameDurationNS: 1_500_000,
		Lacing:          LacingXiph,
		Frames:          [][]byte{{1}, {2}},
	})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerRejectsDuplicateBlockAdditionIDs(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		additions []BlockAddition
	}{
		{
			name: "default and explicit one",
			additions: []BlockAddition{
				{Data: []byte{1}},
				{ID: 1, Data: []byte{2}},
			},
		},
		{
			name: "explicit duplicate",
			additions: []BlockAddition{
				{ID: 2, Data: []byte{1}},
				{ID: 2, Data: []byte{2}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := muxer.WritePacket(Packet{
				TrackID:        trackID,
				TimeNS:         0,
				Keyframe:       true,
				Data:           []byte{1},
				BlockAdditions: tt.additions,
			})
			if !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsLateBlockAdditionAboveTrackMax(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
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
	err = muxer.WritePacket(Packet{
		TrackID:        trackID,
		TimeNS:         20_000_000,
		Keyframe:       true,
		Data:           []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
		BlockAdditions: []BlockAddition{{Data: []byte{1}}},
	})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerRejectsInvalidBlockAdditionTrackMetadata(t *testing.T) {
	tests := []struct {
		name  string
		track Track
	}{
		{
			name: "mapping id below minimum",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				BlockAdditionMappings: []BlockAdditionMapping{{
					IDValue: 1,
					Type:    1,
				}},
				Video: VideoConfig{Width: 640, Height: 360},
			},
		},
		{
			name: "duplicate mapping id",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				BlockAdditionMappings: []BlockAdditionMapping{
					{IDValue: 2, Type: 1},
					{IDValue: 2, Type: 2},
				},
				Video: VideoConfig{Width: 640, Height: 360},
			},
		},
		{
			name: "max below mapping",
			track: Track{
				Type:               TrackVideo,
				Codec:              CodecVP8,
				MaxBlockAdditionID: 1,
				BlockAdditionMappings: []BlockAdditionMapping{{
					IDValue: 2,
					Type:    1,
				}},
				Video: VideoConfig{Width: 640, Height: 360},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := muxer.AddTrack(tt.track); !errors.Is(err, ErrInvalidTrack) {
				t.Fatalf("err = %v, want ErrInvalidTrack", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidTrackMetadata(t *testing.T) {
	tests := []struct {
		name  string
		track Track
	}{
		{
			name: "audio sample rate",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecOpus,
				Audio: AudioConfig{SampleRate: -1, Channels: 2},
			},
		},
		{
			name: "audio output sample rate",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecOpus,
				Audio: AudioConfig{SampleRate: 48000, OutputSampleRate: -1, Channels: 2},
			},
		},
		{
			name: "audio channels",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecOpus,
				Audio: AudioConfig{SampleRate: 48000, Channels: -1},
			},
		},
		{
			name: "audio bit depth",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecPCMU,
				Audio: AudioConfig{SampleRate: 8000, Channels: 1, BitDepth: -1},
			},
		},
		{
			name: "opus surround without private data",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecOpus,
				Audio: AudioConfig{SampleRate: 48000, Channels: 6},
			},
		},
		{
			name: "video width",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: -1, Height: 16},
			},
		},
		{
			name: "duplicate content encoding order",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				ContentEncodings: []ContentEncoding{
					{
						Order:          0,
						Type:           ContentEncodingTypeCompression,
						CompressionSet: true,
						Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
					},
					{
						Order:          0,
						Type:           ContentEncodingTypeCompression,
						CompressionSet: true,
						Compression:    ContentCompression{Algorithm: ContentCompAlgoHeaderStripping},
					},
				},
				Video: VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "content encoding scope",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				ContentEncodings: []ContentEncoding{{
					Scope:          8,
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
				}},
				Video: VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "content encoding compression missing",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				ContentEncodings: []ContentEncoding{{
					Type: ContentEncodingTypeCompression,
				}},
				Video: VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "content encoding encryption missing",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				ContentEncodings: []ContentEncoding{{
					Type: ContentEncodingTypeEncryption,
				}},
				Video: VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "content compression algorithm",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				ContentEncodings: []ContentEncoding{{
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoHeaderStripping + 1},
				}},
				Video: VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "content encryption aes cipher",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				ContentEncodings: []ContentEncoding{{
					Type:          ContentEncodingTypeEncryption,
					EncryptionSet: true,
					Encryption: ContentEncryption{
						Algorithm:      ContentEncAlgoAES,
						AESSettingsSet: true,
						AESSettings:    ContentEncAESSettings{CipherMode: 3},
					},
				}},
				Video: VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "video interlaced flag",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 16, Height: 16, FlagInterlaced: -1, FlagInterlacedSet: true},
			},
		},
		{
			name: "video field order",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 16, Height: 16, FieldOrder: -1, FieldOrderSet: true},
			},
		},
		{
			name: "video stereo mode",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 16, Height: 16, StereoMode: -1, StereoModeSet: true},
			},
		},
		{
			name: "video alpha mode",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 16, Height: 16, AlphaMode: -1, AlphaModeSet: true},
			},
		},
		{
			name: "video crop",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 16, Height: 16, PixelCropLeft: -1},
			},
		},
		{
			name: "video display width",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 16, Height: 16, DisplayWidth: -1},
			},
		},
		{
			name: "video display unit",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 16, Height: 16, DisplayUnit: -1},
			},
		},
		{
			name: "video aspect ratio type",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 16, Height: 16, AspectRatioType: -1, AspectRatioTypeSet: true},
			},
		},
		{
			name: "video colour",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{
					Width:  16,
					Height: 16,
					Colour: VideoColourConfig{MatrixCoefficients: -1, MatrixCoefficientsSet: true},
				},
			},
		},
		{
			name: "video colour chromaticity",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{
					Width:  16,
					Height: 16,
					Colour: VideoColourConfig{
						MasteringMetadata: VideoMasteringMetadataConfig{
							PrimaryRChromaticityX:    1.1,
							PrimaryRChromaticityXSet: true,
						},
					},
				},
			},
		},
		{
			name: "video colour luminance",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{
					Width:  16,
					Height: 16,
					Colour: VideoColourConfig{
						MasteringMetadata: VideoMasteringMetadataConfig{
							LuminanceMin:    -0.1,
							LuminanceMinSet: true,
						},
					},
				},
			},
		},
		{
			name: "video projection type",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{
					Width:      16,
					Height:     16,
					Projection: VideoProjectionConfig{Set: true, Type: -1},
				},
			},
		},
		{
			name: "video projection private on rectangular",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{
					Width:      16,
					Height:     16,
					Projection: VideoProjectionConfig{Set: true, Type: 0, Private: []byte{0}},
				},
			},
		},
		{
			name: "video projection private missing",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{
					Width:      16,
					Height:     16,
					Projection: VideoProjectionConfig{Set: true, Type: 1},
				},
			},
		},
		{
			name: "video projection pose",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{
					Width:      16,
					Height:     16,
					Projection: VideoProjectionConfig{Set: true, PosePitch: 90.1},
				},
			},
		},
		{
			name: "default duration",
			track: Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: -1,
				Video:             VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "default decoded field duration",
			track: Track{
				Type:                          TrackVideo,
				Codec:                         CodecVP8,
				DefaultDecodedFieldDurationNS: -1,
				Video:                         VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "codec delay",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecOpus,
				CodecDelayNS: -1,
				Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
			},
		},
		{
			name: "seek preroll",
			track: Track{
				Type:          TrackAudio,
				Codec:         CodecOpus,
				SeekPreRollNS: -1,
				Audio:         AudioConfig{SampleRate: 48000, Channels: 2},
			},
		},
		{
			name: "track overlay",
			track: Track{
				Type:          TrackVideo,
				Codec:         CodecVP8,
				TrackOverlays: []uint64{0},
				Video:         VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "track translate id",
			track: Track{
				Type:            TrackVideo,
				Codec:           CodecVP8,
				TrackTranslates: []TrackTranslate{{Codec: 1}},
				Video:           VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "timebase",
			track: Track{
				Type:        TrackAudio,
				Codec:       CodecOpus,
				TimebaseNum: -1,
				TimebaseDen: 48000,
				Audio:       AudioConfig{SampleRate: 48000, Channels: 2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := muxer.AddTrack(tt.track); !errors.Is(err, ErrInvalidTrack) {
				t.Fatalf("err = %v, want ErrInvalidTrack", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidSegmentInfoMetadata(t *testing.T) {
	tests := []struct {
		name string
		info SegmentInfo
	}{
		{
			name: "short segment uuid",
			info: SegmentInfo{SegmentUUID: []byte{1, 2, 3}},
		},
		{
			name: "zero segment uuid",
			info: SegmentInfo{SegmentUUID: make([]byte, 16)},
		},
		{
			name: "prev uuid equals segment uuid",
			info: SegmentInfo{
				SegmentUUID: []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				PrevUUID:    []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			},
		},
		{
			name: "date out of range",
			info: SegmentInfo{
				DateUTC:    time.Time{},
				DateUTCSet: true,
			},
		},
		{
			name: "negative duration",
			info: SegmentInfo{
				DurationNS:  -1,
				DurationSet: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewMuxer(discardWriter{}, MuxerOptions{Info: tt.info}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerSplitsClustersBeforeBlockTimecodeOverflow(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, packet := range []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 33_000_000_000, Keyframe: true, Data: []byte{2}},
	} {
		if err := muxer.WritePacket(packet); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if clusters := countClusters(t, buffer.Bytes()); clusters != 2 {
		t.Fatalf("clusters = %d, want 2", clusters)
	}
}

func TestMuxerDemuxerAppliesTrackTimestampScaleAndOffset(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{TimecodeScaleNS: 1_000_000})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:                   TrackVideo,
		Codec:                  CodecVP8,
		TrackTimestampScale:    0.5,
		TrackTimestampScaleSet: true,
		TrackOffsetNS:          5_000_000,
		TrackOffsetSet:         true,
		Video:                  VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, DurationNS: 10_000_000, Keyframe: true, Data: []byte{0}},
		{TrackID: trackID, TimeNS: 25_000_000, DurationNS: 10_000_000, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 35_000_000, DurationNS: 10_000_000, ReferenceBlockTimeNS: []int64{-10_000_000}, Data: []byte{2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatalf("write packet %d: %v", i, err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 ||
		tracks[0].TrackTimestampScale != 0.5 ||
		!tracks[0].TrackTimestampScaleSet ||
		tracks[0].TrackOffsetNS != 5_000_000 ||
		!tracks[0].TrackOffsetSet {
		t.Fatalf("tracks = %+v, want timestamp scale 0.5 and offset 5000000", tracks)
	}
	got := Packet{Data: make([]byte, 0, 4), ReferenceBlockTimeNS: make([]int64, 0, 1)}
	for i := range packets {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatalf("read packet %d: %v", i, err)
		}
		if got.TrackID != packets[i].TrackID ||
			got.TimeNS != packets[i].TimeNS ||
			got.DurationNS != packets[i].DurationNS ||
			got.Keyframe != packets[i].Keyframe ||
			!equalInt64s(got.ReferenceBlockTimeNS, packets[i].ReferenceBlockTimeNS) ||
			!bytes.Equal(got.Data, packets[i].Data) {
			t.Fatalf("packet %d = %+v data=%v, want %+v", i, got, got.Data, packets[i])
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestMuxerDemuxerAppliesTrackTimestampScaleToLacedBlockGroup(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{TimecodeScaleNS: 1_000_000})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:                   TrackAudio,
		Codec:                  CodecOpus,
		TrackTimestampScale:    0.5,
		TrackTimestampScaleSet: true,
		TrackOffsetNS:          5_000_000,
		TrackOffsetSet:         true,
		Audio:                  AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{{0xf8}, {0xff, 0xfe}}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:         trackID,
		TimeNS:          25_000_000,
		FrameDurationNS: 10_000_000,
		Keyframe:        true,
		Lacing:          LacingEBML,
		Frames:          frames,
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	for i := range frames {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		wantTimeNS := 25_000_000 + int64(i)*10_000_000
		if got.TrackID != trackID ||
			got.TimeNS != wantTimeNS ||
			got.DurationNS != 10_000_000 ||
			!got.Keyframe ||
			!bytes.Equal(got.Data, frames[i]) {
			t.Fatalf("frame %d = %+v data=%v, want time=%d data=%v", i, got, got.Data, wantTimeNS, frames[i])
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestMuxerWritesLacedSimpleBlocks(t *testing.T) {
	tests := []struct {
		name   string
		lacing LacingMode
		frames [][]byte
	}{
		{
			name:   "xiph",
			lacing: LacingXiph,
			frames: [][]byte{{1}, {2, 3}, {4, 5, 6}},
		},
		{
			name:   "fixed",
			lacing: LacingFixed,
			frames: [][]byte{{1, 2}, {3, 4}, {5, 6}},
		},
		{
			name:   "ebml",
			lacing: LacingEBML,
			frames: [][]byte{{1, 2, 3}, {4}, {5, 6}},
		},
		{
			name:   "auto",
			lacing: LacingAuto,
			frames: [][]byte{{1, 2, 3}, {4, 5}, {6, 7, 8, 9}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackAudio,
				Codec:             CodecOpus,
				DefaultDurationNS: 20_000_000,
				Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := muxer.WriteLacedPacket(LacedPacket{
				TrackID:     trackID,
				TimeNS:      40_000_000,
				Keyframe:    true,
				Invisible:   true,
				Discardable: true,
				Lacing:      tt.lacing,
				Frames:      tt.frames,
			}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, 16)}
			for i := range tt.frames {
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatalf("read frame %d: %v", i, err)
				}
				if packet.TrackID != trackID || packet.TimeNS != 40_000_000+int64(i)*20_000_000 ||
					packet.DurationNS != 20_000_000 || !packet.Keyframe || !packet.Invisible ||
					!packet.Discardable || !bytes.Equal(packet.Data, tt.frames[i]) {
					t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, tt.frames[i])
				}
			}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
				t.Fatalf("err = %v, want EOF", err)
			}
		})
	}
}

func TestMuxerWritesSpecLacingFlags(t *testing.T) {
	const (
		specLacingXiph  = 0x02
		specLacingFixed = 0x04
		specLacingEBML  = 0x06
		specLacingMask  = 0x06
	)
	tests := []struct {
		name     string
		lacing   LacingMode
		frames   [][]byte
		wantFlag byte
	}{
		{
			name:     "xiph",
			lacing:   LacingXiph,
			frames:   [][]byte{{1}, {2, 3}, {4, 5, 6}},
			wantFlag: specLacingXiph,
		},
		{
			name:     "fixed",
			lacing:   LacingFixed,
			frames:   [][]byte{{1, 2}, {3, 4}, {5, 6}},
			wantFlag: specLacingFixed,
		},
		{
			name:     "ebml",
			lacing:   LacingEBML,
			frames:   [][]byte{{1, 2, 3}, {4}, {5, 6}},
			wantFlag: specLacingEBML,
		},
		{
			name:     "auto fixed",
			lacing:   LacingAuto,
			frames:   [][]byte{{1, 2}, {3, 4}, {5, 6}},
			wantFlag: specLacingFixed,
		},
		{
			name:     "auto ebml",
			lacing:   LacingAuto,
			frames:   [][]byte{{1}, {2, 3}, {4, 5, 6}},
			wantFlag: specLacingEBML,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &memoryWriteSeeker{}
			muxer, err := NewMuxer(ws, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackAudio,
				Codec:             CodecOpus,
				DefaultDurationNS: 20_000_000,
				Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := muxer.WriteLacedPacket(LacedPacket{
				TrackID:  trackID,
				TimeNS:   40_000_000,
				Keyframe: true,
				Lacing:   tt.lacing,
				Frames:   tt.frames,
			}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			got := firstBlockFlags(t, ws.bytes, idSimpleBlock) & specLacingMask
			if got != tt.wantFlag {
				t.Fatalf("lacing flag = 0x%02x, want 0x%02x", got, tt.wantFlag)
			}
		})
	}
}

func TestMuxerWritesLacedBlockGroupsWithExtras(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{TimecodeScaleNS: 1_000_000})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{{0x10}, {0x20, 0x21}, {0x30}}
	packet := LacedPacket{
		TrackID:              trackID,
		TimeNS:               40_000_000,
		FrameDurationNS:      20_000_000,
		ReferenceBlockTimeNS: []int64{-20_000_000},
		ReferencePriority:    3,
		DiscardPaddingNS:     -2_000_000,
		CodecState:           []byte{0x99, 0x98},
		BlockAdditions:       []BlockAddition{{ID: 2, Data: []byte{0x88}}},
		Invisible:            true,
		Lacing:               LacingAuto,
		Frames:               frames,
	}
	if err := muxer.WriteLacedPacket(packet); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{
		Data:                 make([]byte, 0, 16),
		ReferenceBlockTimeNS: make([]int64, 0, 1),
		CodecState:           make([]byte, 0, 2),
		BlockAdditions:       make([]BlockAddition, 0, 1),
	}
	for i := range frames {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if got.TrackID != trackID || got.TimeNS != packet.TimeNS+int64(i)*packet.FrameDurationNS ||
			got.DurationNS != packet.FrameDurationNS || got.Keyframe || !got.Invisible ||
			got.ReferencePriority != packet.ReferencePriority ||
			got.DiscardPaddingNS != packet.DiscardPaddingNS ||
			!equalInt64s(got.ReferenceBlockTimeNS, packet.ReferenceBlockTimeNS) ||
			!bytes.Equal(got.CodecState, packet.CodecState) ||
			!equalBlockAdditions(got.BlockAdditions, packet.BlockAdditions) ||
			!bytes.Equal(got.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%v", i, got, got.Data)
		}
	}
}

func TestMuxerRejectsInvalidLacedPackets(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		packet LacedPacket
		want   error
	}{
		{
			name: "unknown track",
			packet: LacedPacket{
				TrackID: trackID + 1,
				Frames:  [][]byte{{1}, {2}},
			},
			want: ErrUnknownTrack,
		},
		{
			name: "single frame",
			packet: LacedPacket{
				TrackID: trackID,
				Frames:  [][]byte{{1}},
			},
			want: ErrInvalidData,
		},
		{
			name: "too many frames",
			packet: LacedPacket{
				TrackID: trackID,
				Frames:  makeLaceFrames(257, []byte{1}),
			},
			want: ErrInvalidData,
		},
		{
			name: "fixed unequal sizes",
			packet: LacedPacket{
				TrackID: trackID,
				Lacing:  LacingFixed,
				Frames:  [][]byte{{1}, {2, 3}},
			},
			want: ErrInvalidData,
		},
		{
			name: "unsupported lacing",
			packet: LacedPacket{
				TrackID: trackID,
				Lacing:  LacingMode(99),
				Frames:  [][]byte{{1}, {2}},
			},
			want: ErrUnsupportedLacing,
		},
		{
			name: "keyframe with references",
			packet: LacedPacket{
				TrackID:              trackID,
				Keyframe:             true,
				ReferenceBlockTimeNS: []int64{-20_000_000},
				Frames:               [][]byte{{1}, {2}},
			},
			want: ErrInvalidData,
		},
		{
			name: "discardable block group",
			packet: LacedPacket{
				TrackID:         trackID,
				FrameDurationNS: 20_000_000,
				Discardable:     true,
				Frames:          [][]byte{{1}, {2}},
			},
			want: ErrInvalidData,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := muxer.WriteLacedPacket(tt.packet); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestMuxerWritesMetadataCRC32(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{
		WriteCRC32: true,
		Attachments: []Attachment{{
			UID:      7,
			Filename: "cover.txt",
			MIMEType: "text/plain",
			Data:     []byte("cover"),
		}},
		Chapters: []ChapterEdition{{
			UID:      11,
			Chapters: []Chapter{metadataValidationChapter(13, 0)},
		}},
		Tags: []Tag{{
			Target: TagTarget{TrackUIDs: []uint64{1}},
			Simple: []SimpleTag{{
				Name:      "TITLE",
				String:    "CRC protected",
				StringSet: true,
			}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	for _, id := range []ebml.ID{idSeekHead, idTracks, idAttachments, idChapters, idTags, idCues} {
		assertTopLevelMasterStartsWithCRC32(t, ws.bytes, id)
	}
	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		t.Fatal(err)
	}
	if len(demuxer.Attachments()) != 1 || len(demuxer.Chapters()) != 1 || len(demuxer.Tags()) != 1 {
		t.Fatalf("metadata not preserved after crc-protected read")
	}
}

func TestStreamingMuxerWritesInfoCRC32(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{
		WriteCRC32: true,
		Streaming:  true,
		Info: SegmentInfo{
			Title: "stream crc",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	assertTopLevelMasterStartsWithCRC32(t, buffer.Bytes(), idInfo)
	assertTopLevelMasterStartsWithCRC32(t, buffer.Bytes(), idTracks)
	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := demuxer.Info(); got.Title != "stream crc" {
		t.Fatalf("info title = %q, want stream crc", got.Title)
	}
}

func TestFormatMuxerDemuxerRoundTrip(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Index:    0,
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:     av.CodecVP9,
			Type:   av.MediaVideo,
			Width:  320,
			Height: 180,
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		PTS:      av.Timestamp{Value: 1800, Base: stream.TimeBase},
		Keyframe: true,
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer := &FormatDemuxer{}
	if err := demuxer.Open(ctx, format.Input{Reader: bytes.NewReader(buffer.Bytes())}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	streams := demuxer.Streams()
	if len(streams) != 1 || streams[0].Codec.ID != av.CodecVP9 || streams[0].Codec.Width != 320 {
		t.Fatalf("streams = %+v", streams)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 8)}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady || result.Packet.StreamID != "1" || result.Packet.PTS.Value != 20_000_000 ||
		!bytes.Equal(result.Packet.Payload.Bytes, []byte{1, 2, 3}) {
		t.Fatalf("result = %+v packet=%+v", result, result.Packet)
	}
	if result.Packet.Type != av.MediaVideo {
		t.Fatalf("packet type = %q, want %q (producers must stamp the media kind)", result.Packet.Type, av.MediaVideo)
	}
}

func TestFormatMuxerRejectsNegativeDuration(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Index:    0,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: timeNS},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			SampleRate: 48000,
			Channels:   2,
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{1}},
		PTS:      av.Timestamp{Value: 0, Base: stream.TimeBase},
		Duration: av.Duration{Value: -1, Base: stream.TimeBase},
		Keyframe: true,
	}, nil)
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestWritePacketAllocs(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{TrackID: id, TimeNS: 0, Keyframe: true, Data: []byte{1, 2, 3, 4}}
	if err := muxer.WritePacket(packet); err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := muxer.WritePacket(packet); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func TestWriteLacedPacketAllocs(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := LacedPacket{
		TrackID:  id,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{{1, 2}, {3, 4}, {5, 6}},
	}
	if err := muxer.WriteLacedPacket(packet); err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := muxer.WriteLacedPacket(packet); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

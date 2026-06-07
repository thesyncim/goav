package matroska

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"encoding/hex"
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
	"github.com/woozymasta/lzo"
)

func TestRegisterProvidesFactoriesAndProber(t *testing.T) {
	registry := format.NewRegistry()
	Register(registry)

	result, err := registry.Probe(context.Background(), format.ProbeRequest{
		Name: "recording.mkv",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatMatroska {
		t.Fatalf("format = %s, want matroska", result.Format)
	}
	if _, err := registry.DemuxerFactory(av.FormatMatroska); err != nil {
		t.Fatalf("demuxer factory: %v", err)
	}
	if _, err := registry.MuxerFactory(av.FormatMatroska); err != nil {
		t.Fatalf("muxer factory: %v", err)
	}
}

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

func TestDemuxerReadsSegmentInfoDurationWithLateTimestampScale(t *testing.T) {
	data := makeInfoMetadataMatroskaData(t, func(writer *ebml.Writer) error {
		return writeInfoWithElements(writer, func(w *ebml.Writer) error {
			return w.WriteFloat64(idDuration, 12.5)
		})
	})
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	info := demuxer.Info()
	if !info.DurationSet || info.DurationNS != 12_500_000 {
		t.Fatalf("duration = %d set=%v, want 12500000 true", info.DurationNS, info.DurationSet)
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

func TestDemuxerRemuxesUnknownSegmentElements(t *testing.T) {
	unknownID := ebml.ID(0x4ffe)
	raw := unknownElementBytes(t, unknownID, []byte{0x10, 0x20, 0x30, 0x40})
	var source bytes.Buffer
	sourceMuxer, err := NewMuxer(&source, MuxerOptions{UnknownSegmentElements: []UnknownElement{{Raw: raw}}})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := sourceMuxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}}
	if err := sourceMuxer.WritePacket(packet); err != nil {
		t.Fatal(err)
	}
	if err := sourceMuxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(source.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var remuxed bytes.Buffer
	remuxer, err := NewMuxer(&remuxed, MuxerOptions{UnknownSegmentElements: demuxer.UnknownSegmentElements()})
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
	if !bytes.Contains(remuxed.Bytes(), raw) {
		t.Fatalf("remuxed data does not contain raw unknown element %x", raw)
	}
	redemuxer, err := NewDemuxer(bytes.NewReader(remuxed.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	elements := redemuxer.UnknownSegmentElements()
	if len(elements) != 1 || elements[0].ID != uint64(unknownID) || !bytes.Equal(elements[0].Raw, raw) {
		t.Fatalf("remuxed unknown elements = %+v, want id=0x%x raw=%x", elements, uint64(unknownID), raw)
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

func TestDemuxerRetriesLacedUnknownClusterElements(t *testing.T) {
	clusterUnknown := unknownElementBytes(t, 0x4fe8, []byte{0x41, 0x42, 0x43})
	var source bytes.Buffer
	muxer, err := NewMuxer(&source, MuxerOptions{})
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
		TrackID:                trackID,
		TimeNS:                 0,
		Keyframe:               true,
		Frames:                 [][]byte{{1, 2}, {3, 4}},
		UnknownClusterElements: []UnknownElement{{Raw: clusterUnknown}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(source.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 1)}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	got.Data = make([]byte, 0, 8)
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	assertUnknownElement(t, "first laced packet cluster", got.UnknownClusterElements, 0x4fe8, clusterUnknown)
	if !bytes.Equal(got.Data, []byte{1, 2}) {
		t.Fatalf("first laced data = %v", got.Data)
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.UnknownClusterElements) != 0 {
		t.Fatalf("second laced packet unknown cluster elements = %+v, want none", got.UnknownClusterElements)
	}
	if !bytes.Equal(got.Data, []byte{3, 4}) {
		t.Fatalf("second laced data = %v", got.Data)
	}
}

func TestDemuxerDropsUnknownClusterElementsForSkippedBlock(t *testing.T) {
	clusterUnknown := unknownElementBytes(t, 0x4fe7, []byte{0x51})
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
	if err := muxer.WritePacket(Packet{
		TrackID:                trackID,
		TimeNS:                 0,
		Keyframe:               true,
		Data:                   []byte{1, 2},
		UnknownClusterElements: []UnknownElement{{Raw: clusterUnknown}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 20_000_000, Keyframe: true, Data: []byte{3}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(source.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 1)}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	got.Data = make([]byte, 0, 8)
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != 20_000_000 || !bytes.Equal(got.Data, []byte{3}) {
		t.Fatalf("packet = %+v data=%v, want second packet", got, got.Data)
	}
	if len(got.UnknownClusterElements) != 0 {
		t.Fatalf("unknown cluster elements = %+v, want none after skipped block", got.UnknownClusterElements)
	}
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

func TestMuxerDemuxerSupportsWebRTCCodecs(t *testing.T) {
	tests := []struct {
		name  string
		track Track
		data  []byte
	}{
		{
			name: "opus",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecOpus,
				Audio: AudioConfig{SampleRate: 48000, Channels: 2},
			},
			data: []byte{0x01, 0x02},
		},
		{
			name: "av1",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecAV1,
				Video:        VideoConfig{Width: 640, Height: 360},
				CodecPrivate: av1CodecConfig(),
			},
			data: []byte{0x12, 0x00, 0x0a},
		},
		{
			name: "h264",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecH264,
				Video:        VideoConfig{Width: 640, Height: 360},
				CodecPrivate: h264AVCDecoderConfig(),
			},
			data: h264AnnexBAccessUnit(),
		},
		{
			name: "vp9",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP9,
				Video: VideoConfig{Width: 640, Height: 360},
			},
			data: []byte{0x83, 0x49, 0x83},
		},
		{
			name: "vp8",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 640, Height: 360},
			},
			data: []byte{0x9d, 0x01, 0x2a},
		},
	}

	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackIDs := make([]uint32, len(tests))
	for i := range tests {
		trackID, err := muxer.AddTrack(tests[i].track)
		if err != nil {
			t.Fatalf("%s add track: %v", tests[i].name, err)
		}
		trackIDs[i] = trackID
	}
	for i := range tests {
		if err := muxer.WritePacket(Packet{
			TrackID:  trackIDs[i],
			TimeNS:   int64(i) * 20_000_000,
			Keyframe: true,
			Data:     tests[i].data,
		}); err != nil {
			t.Fatalf("%s write packet: %v", tests[i].name, err)
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
	if len(tracks) != len(tests) {
		t.Fatalf("tracks = %d, want %d", len(tracks), len(tests))
	}
	for i := range tests {
		wantPrivate := tests[i].track.CodecPrivate
		if tests[i].track.Codec == CodecOpus && len(wantPrivate) == 0 {
			wantPrivate = expectedOpusHead(tests[i].track.Audio.Channels, tests[i].track.Audio.SampleRate)
		}
		if tracks[i].Codec != tests[i].track.Codec || tracks[i].Type != tests[i].track.Type ||
			!bytes.Equal(tracks[i].CodecPrivate, wantPrivate) {
			t.Fatalf("%s track = %+v, want %+v", tests[i].name, tracks[i], tests[i].track)
		}
	}
	got := Packet{Data: make([]byte, 0, 16)}
	for i := range tests {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatalf("%s read packet: %v", tests[i].name, err)
		}
		if got.TrackID != trackIDs[i] || got.TimeNS != int64(i)*20_000_000 ||
			!bytes.Equal(got.Data, tests[i].data) {
			t.Fatalf("%s packet = %+v data=%v", tests[i].name, got, got.Data)
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestMuxerDemuxerSupportsTextUTF8Subtitles(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackSubtitle,
		Codec:             CodecTextUTF8,
		Name:              "English captions",
		Language:          "eng",
		LanguageBCP47:     "en-US",
		FlagDefault:       false,
		FlagDefaultSet:    true,
		FlagForced:        false,
		FlagForcedSet:     true,
		DefaultDurationNS: 2_000_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantText := []byte("Hello from a UTF-8 subtitle.")
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     1_000_000_000,
		DurationNS: 2_000_000_000,
		Keyframe:   true,
		Data:       wantText,
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
	if tracks[0].Type != TrackSubtitle ||
		tracks[0].Codec != CodecTextUTF8 ||
		tracks[0].Name != "English captions" ||
		tracks[0].Language != "eng" ||
		tracks[0].LanguageBCP47 != "en-US" ||
		len(tracks[0].CodecPrivate) != 0 {
		t.Fatalf("track = %+v", tracks[0])
	}
	got := Packet{Data: make([]byte, 0, 64)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != trackID ||
		got.TimeNS != 1_000_000_000 ||
		got.DurationNS != 2_000_000_000 ||
		!got.Keyframe ||
		!bytes.Equal(got.Data, wantText) {
		t.Fatalf("packet = %+v data=%q", got, got.Data)
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestMuxerDemuxerPreservesMSACMG711CodecPrivate(t *testing.T) {
	tests := []struct {
		name  string
		codec Codec
		data  []byte
	}{
		{name: "pcmu", codec: CodecPCMU, data: []byte{0xff, 0x7f}},
		{name: "pcma", codec: CodecPCMA, data: []byte{0xd5, 0x2a}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(Track{
				Type:  TrackAudio,
				Codec: tt.codec,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := muxer.WritePacket(Packet{
				TrackID:  trackID,
				TimeNS:   0,
				Keyframe: true,
				Data:     tt.data,
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
			wantPrivate := expectedMSACMWaveFormat(tt.codec, 1, 8000)
			if tracks[0].Codec != tt.codec ||
				tracks[0].Audio.SampleRate != 8000 ||
				tracks[0].Audio.Channels != 1 ||
				tracks[0].Audio.BitDepth != 8 ||
				!bytes.Equal(tracks[0].CodecPrivate, wantPrivate) {
				t.Fatalf("track = %+v private=%x, want codec %v audio 8000/1/8 private=%x", tracks[0], tracks[0].CodecPrivate, tt.codec, wantPrivate)
			}
			got := Packet{Data: make([]byte, 0, 8)}
			if err := demuxer.ReadPacket(&got); err != nil {
				t.Fatal(err)
			}
			if got.TrackID != trackID || !bytes.Equal(got.Data, tt.data) {
				t.Fatalf("packet = %+v data=%x, want track %d data=%x", got, got.Data, trackID, tt.data)
			}
		})
	}
}

func TestMuxerDemuxerPreservesVorbisCodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	private := validVorbisCodecPrivate()
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackAudio,
		Codec:        CodecVorbis,
		Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
		CodecPrivate: private,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x11, 0x22, 0x33}
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     20_000_000,
		DurationNS: 20_000_000,
		Keyframe:   true,
		Data:       want,
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
	if tracks[0].Codec != CodecVorbis ||
		tracks[0].Type != TrackAudio ||
		tracks[0].Audio.SampleRate != 48000 ||
		tracks[0].Audio.Channels != 2 ||
		!bytes.Equal(tracks[0].CodecPrivate, private) {
		t.Fatalf("track = %+v private=%x", tracks[0], tracks[0].CodecPrivate)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != trackID ||
		got.TimeNS != 20_000_000 ||
		got.DurationNS != 20_000_000 ||
		!bytes.Equal(got.Data, want) {
		t.Fatalf("packet = %+v data=%x, want track %d data=%x", got, got.Data, trackID, want)
	}
}

func TestMuxerRejectsInvalidVorbisCodecPrivate(t *testing.T) {
	tests := []struct {
		name    string
		private []byte
	}{
		{name: "missing", private: nil},
		{name: "wrong packet count", private: []byte{1, 7, 7, 1, 'v', 'o', 'r', 'b', 'i', 's', 3, 'v', 'o', 'r', 'b', 'i', 's', 5, 'v', 'o', 'r', 'b', 'i', 's'}},
		{name: "truncated xiph lacing", private: []byte{2, 255}},
		{name: "empty identification", private: []byte{2, 0, 7, 3, 'v', 'o', 'r', 'b', 'i', 's', 5, 'v', 'o', 'r', 'b', 'i', 's'}},
		{name: "wrong header order", private: vorbisCodecPrivateWithHeaders([]byte{3, 'v', 'o', 'r', 'b', 'i', 's'}, []byte{1, 'v', 'o', 'r', 'b', 'i', 's'}, []byte{5, 'v', 'o', 'r', 'b', 'i', 's'})},
		{name: "zero channels", private: vorbisCodecPrivateWithIdentificationByte(11, 0)},
		{name: "zero sample rate", private: vorbisCodecPrivateWithIdentificationBytes(12, 0, 0, 0, 0)},
		{name: "missing framing bit", private: vorbisCodecPrivateWithIdentificationByte(29, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := muxer.AddTrack(Track{
				Type:         TrackAudio,
				Codec:        CodecVorbis,
				Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
				CodecPrivate: tt.private,
			}); !errors.Is(err, ErrInvalidTrack) {
				t.Fatalf("err = %v, want ErrInvalidTrack", err)
			}
		})
	}
}

func TestMuxerDemuxerPreservesFLACCodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	private := validFLACCodecPrivate()
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackAudio,
		Codec:        CodecFLAC,
		Audio:        AudioConfig{SampleRate: 48000, Channels: 2, BitDepth: 16},
		CodecPrivate: private,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0xff, 0xf8, 0x69, 0x18}
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     20_000_000,
		DurationNS: 20_000_000,
		Keyframe:   true,
		Data:       want,
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
	if tracks[0].Codec != CodecFLAC ||
		tracks[0].Type != TrackAudio ||
		tracks[0].Audio.SampleRate != 48000 ||
		tracks[0].Audio.Channels != 2 ||
		tracks[0].Audio.BitDepth != 16 ||
		!bytes.Equal(tracks[0].CodecPrivate, private) {
		t.Fatalf("track = %+v private=%x", tracks[0], tracks[0].CodecPrivate)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != trackID ||
		got.TimeNS != 20_000_000 ||
		got.DurationNS != 20_000_000 ||
		!bytes.Equal(got.Data, want) {
		t.Fatalf("packet = %+v data=%x, want track %d data=%x", got, got.Data, trackID, want)
	}
}

func TestMuxerRejectsInvalidFLACCodecPrivate(t *testing.T) {
	tests := []struct {
		name    string
		private []byte
	}{
		{name: "missing", private: nil},
		{name: "short", private: []byte("fLaC")},
		{name: "wrong marker", private: append([]byte("bad!"), validFLACCodecPrivate()[4:]...)},
		{name: "wrong metadata type", private: flacCodecPrivateWithHeaderByte(4, 1)},
		{name: "wrong streaminfo length", private: flacCodecPrivateWithHeaderByte(7, 33)},
		{name: "zero min block size", private: flacCodecPrivateWithStreamInfoBytes(0, 0, 0)},
		{name: "reversed block sizes", private: flacCodecPrivateWithStreamInfoBytes(0, 0x20, 0x00, 0x10, 0x00)},
		{name: "zero sample rate", private: flacCodecPrivateWithStreamInfoBytes(10, 0, 0, 0, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := muxer.AddTrack(Track{
				Type:         TrackAudio,
				Codec:        CodecFLAC,
				Audio:        AudioConfig{SampleRate: 48000, Channels: 2, BitDepth: 16},
				CodecPrivate: tt.private,
			}); !errors.Is(err, ErrInvalidTrack) {
				t.Fatalf("err = %v, want ErrInvalidTrack", err)
			}
		})
	}
}

func TestMuxerDemuxerPreservesAACCodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	private := makeAACAudioSpecificConfig(48000, 2)
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackAudio,
		Codec:        CodecAAC,
		Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
		CodecPrivate: private,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x21, 0x10, 0x56, 0xe5}
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     20_000_000,
		DurationNS: 20_000_000,
		Keyframe:   true,
		Data:       want,
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
	if tracks[0].Codec != CodecAAC ||
		tracks[0].Type != TrackAudio ||
		tracks[0].Audio.SampleRate != 48000 ||
		tracks[0].Audio.Channels != 2 ||
		!bytes.Equal(tracks[0].CodecPrivate, private) {
		t.Fatalf("track = %+v private=%x", tracks[0], tracks[0].CodecPrivate)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != trackID ||
		got.TimeNS != 20_000_000 ||
		got.DurationNS != 20_000_000 ||
		!bytes.Equal(got.Data, want) {
		t.Fatalf("packet = %+v data=%x, want track %d data=%x", got, got.Data, trackID, want)
	}
}

func TestMuxerRejectsInvalidAACCodecPrivate(t *testing.T) {
	tests := []struct {
		name    string
		private []byte
	}{
		{name: "missing", private: nil},
		{name: "short", private: []byte{0x11}},
		{name: "zero object type", private: []byte{0x01, 0x90}},
		{name: "reserved sample rate", private: []byte{0x17, 0x90}},
		{name: "zero explicit sample rate", private: []byte{0x17, 0x80, 0x00, 0x00, 0x00}},
		{name: "program config channels", private: []byte{0x11, 0x80}},
		{name: "reserved channel config", private: []byte{0x11, 0xf8}},
		{name: "sample rate mismatch", private: makeAACAudioSpecificConfig(44100, 2)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := muxer.AddTrack(Track{
				Type:         TrackAudio,
				Codec:        CodecAAC,
				Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
				CodecPrivate: tt.private,
			}); !errors.Is(err, ErrInvalidTrack) {
				t.Fatalf("err = %v, want ErrInvalidTrack", err)
			}
		})
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

func TestMuxerDemuxerPreservesCodecName(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:      TrackVideo,
		Codec:     CodecVP8,
		Name:      "camera-main",
		CodecName: "libvpx VP8",
		Video:     VideoConfig{Width: 640, Height: 360},
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
	if tracks[0].Name != "camera-main" || tracks[0].CodecName != "libvpx VP8" {
		t.Fatalf("track name=%q codec name=%q", tracks[0].Name, tracks[0].CodecName)
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

func TestMuxerDemuxerPreservesContentEncodings(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantEncodings := []ContentEncoding{
		{
			Order:          1,
			Scope:          ContentEncodingScopePrivate,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  []byte{0x00, 0x00, 0x01},
			},
		},
		{
			Order:         0,
			Scope:         ContentEncodingScopeNext,
			Type:          ContentEncodingTypeEncryption,
			EncryptionSet: true,
			Encryption: ContentEncryption{
				Algorithm:      ContentEncAlgoAES,
				KeyID:          []byte{0x10, 0x20},
				AESSettingsSet: true,
				AESSettings: ContentEncAESSettings{
					CipherMode: ContentEncAESCipherModeCTR,
				},
				Signature:              []byte{0xaa, 0xbb},
				SignatureKeyID:         []byte{0x30, 0x40},
				SignatureAlgorithm:     ContentSigAlgoRSA,
				SignatureHashAlgorithm: ContentSigHashAlgoSHA1,
			},
		},
	}
	track := Track{
		Type:             TrackVideo,
		Codec:            CodecVP8,
		ContentEncodings: cloneContentEncodings(wantEncodings),
		Video:            VideoConfig{Width: 640, Height: 360},
	}
	trackID, err := muxer.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	track.ContentEncodings[0].Compression.Settings[0] = 0xff
	track.ContentEncodings[1].Encryption.KeyID[0] = 0xff
	track.ContentEncodings[1].Encryption.Signature[0] = 0xff
	track.ContentEncodings[1].Encryption.SignatureKeyID[0] = 0xff
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
	if !reflect.DeepEqual(tracks[0].ContentEncodings, wantEncodings) {
		t.Fatalf("encodings = %+v, want %+v", tracks[0].ContentEncodings, wantEncodings)
	}
	tracks[0].ContentEncodings[0].Compression.Settings[0] = 0xee
	tracks[0].ContentEncodings[1].Encryption.KeyID[0] = 0xee
	tracks[0].ContentEncodings[1].Encryption.Signature[0] = 0xee
	tracks[0].ContentEncodings[1].Encryption.SignatureKeyID[0] = 0xee
	fresh := demuxer.Tracks()
	if !reflect.DeepEqual(fresh[0].ContentEncodings, wantEncodings) {
		t.Fatalf("content encoding alias was not protected: %+v", fresh[0].ContentEncodings)
	}
}

func TestMuxerDemuxerAppliesHeaderStrippingContentEncoding(t *testing.T) {
	settings := []byte("HS:")
	tests := []struct {
		name  string
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "simple block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, 32)}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "block group",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, 32)}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
		{
			name: "laced block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WriteLacedPacket(LacedPacket{
					TrackID:  trackID,
					TimeNS:   0,
					Keyframe: true,
					Lacing:   LacingXiph,
					Frames: [][]byte{
						data,
						append(append([]byte(nil), settings...), []byte("second")...),
					},
				})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, 32)}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("first packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, []byte("HS:second")) {
					t.Fatalf("second packet = %+v data=%q", packet, packet.Data)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			want := append(append([]byte(nil), settings...), []byte(tt.name)...)
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings: []ContentEncoding{{
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression: ContentCompression{
						Algorithm: ContentCompAlgoHeaderStripping,
						Settings:  settings,
					},
				}},
				Video: VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) {
				t.Fatalf("file still contains unstripped frame %q", want)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)
		})
	}
}

func TestMuxerDemuxerAppliesZlibContentEncoding(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "simple block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "block group",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
		{
			name: "laced block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WriteLacedPacket(LacedPacket{
					TrackID:  trackID,
					TimeNS:   0,
					Keyframe: true,
					Lacing:   LacingXiph,
					Frames: [][]byte{
						data,
						zlibTestPayload("second laced block"),
					},
				})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("first packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
				second := zlibTestPayload("second laced block")
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			want := zlibTestPayload(tt.name)
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings: []ContentEncoding{{
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression: ContentCompression{
						Algorithm: ContentCompAlgoZlib,
					},
				}},
				Video: VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) {
				t.Fatalf("file still contains uncompressed frame %q", want)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)
		})
	}
}

func TestMuxerDemuxerAppliesLZOContentEncoding(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "simple block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "block group",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
		{
			name: "laced block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WriteLacedPacket(LacedPacket{
					TrackID:  trackID,
					TimeNS:   0,
					Keyframe: true,
					Lacing:   LacingXiph,
					Frames: [][]byte{
						data,
						lzoTestPayload("second laced block"),
					},
				})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("first packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
				second := lzoTestPayload("second laced block")
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			want := lzoTestPayload(tt.name)
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings: []ContentEncoding{{
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoLZO1X},
				}},
				Video: VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) {
				t.Fatalf("file still contains uncompressed frame %q", want)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)
		})
	}
}

func TestMuxerDemuxerAppliesBzlibContentEncoding(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "simple block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "block group",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
		{
			name: "laced block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WriteLacedPacket(LacedPacket{
					TrackID:  trackID,
					TimeNS:   0,
					Keyframe: true,
					Lacing:   LacingXiph,
					Frames: [][]byte{
						data,
						bzlibTestPayload("second laced block"),
					},
				})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("first packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
				second := bzlibTestPayload("second laced block")
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			want := bzlibTestPayload(tt.name)
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings: []ContentEncoding{{
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoBzlib},
				}},
				Video: VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) {
				t.Fatalf("file still contains uncompressed frame %q", want)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)
		})
	}
}

func TestMuxerDemuxerAppliesChainedHeaderStrippingAndZlibContentEncoding(t *testing.T) {
	settings := []byte("HS:")
	tests := []struct {
		name  string
		track Track
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "vp8 simple block",
			track: Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				Video:             VideoConfig{Width: 640, Height: 360},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "vp9 block group",
			track: Track{
				Type:              TrackVideo,
				Codec:             CodecVP9,
				DefaultDurationNS: 20_000_000,
				Video:             VideoConfig{Width: 1280, Height: 720},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
		{
			name: "av1 laced block",
			track: Track{
				Type:              TrackVideo,
				Codec:             CodecAV1,
				CodecPrivate:      av1CodecConfig(),
				DefaultDurationNS: 20_000_000,
				Video:             VideoConfig{Width: 1920, Height: 1080},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WriteLacedPacket(LacedPacket{
					TrackID:  trackID,
					TimeNS:   0,
					Keyframe: true,
					Lacing:   LacingXiph,
					Frames: [][]byte{
						data,
						withContentEncodingHeader(settings, chainedContentPayload("av1 second laced block")),
					},
				})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("first packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
				second := withContentEncodingHeader(settings, chainedContentPayload("av1 second laced block"))
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
			},
		},
		{
			name: "opus simple block",
			track: Track{
				Type:              TrackAudio,
				Codec:             CodecOpus,
				DefaultDurationNS: 20_000_000,
				Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			track := tt.track
			track.ContentEncodings = chainedHeaderZlibContentEncodings(settings)
			trackID, err := muxer.AddTrack(track)
			if err != nil {
				t.Fatal(err)
			}
			want := withContentEncodingHeader(settings, chainedContentPayload(tt.name))
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) || bytes.Contains(buffer.Bytes(), want[len(settings):]) {
				t.Fatalf("file still contains unencoded frame payload")
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)
		})
	}
}

func TestMuxerDemuxerAppliesChainedHeaderStrippingAndLZOContentEncoding(t *testing.T) {
	settings := []byte("HS:")
	tests := []struct {
		name  string
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "simple block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "laced block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WriteLacedPacket(LacedPacket{
					TrackID:  trackID,
					TimeNS:   0,
					Keyframe: true,
					Lacing:   LacingXiph,
					Frames: [][]byte{
						data,
						withContentEncodingHeader(settings, chainedContentPayload("lzo second laced block")),
					},
				})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("first packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
				second := withContentEncodingHeader(settings, chainedContentPayload("lzo second laced block"))
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
			},
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
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings:  chainedHeaderLZOContentEncodings(settings),
				Video:             VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			want := withContentEncodingHeader(settings, chainedContentPayload(tt.name))
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) || bytes.Contains(buffer.Bytes(), want[len(settings):]) {
				t.Fatalf("file still contains unencoded frame payload")
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)
		})
	}
}

func TestMuxerDemuxerAppliesChainedHeaderStrippingAndBzlibContentEncoding(t *testing.T) {
	settings := []byte("HS:")
	tests := []struct {
		name  string
		track Track
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "vp8 simple block",
			track: Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				Video:             VideoConfig{Width: 640, Height: 360},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "vp9 block group",
			track: Track{
				Type:              TrackVideo,
				Codec:             CodecVP9,
				DefaultDurationNS: 20_000_000,
				Video:             VideoConfig{Width: 1280, Height: 720},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
		{
			name: "av1 laced block",
			track: Track{
				Type:              TrackVideo,
				Codec:             CodecAV1,
				CodecPrivate:      av1CodecConfig(),
				DefaultDurationNS: 20_000_000,
				Video:             VideoConfig{Width: 1920, Height: 1080},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WriteLacedPacket(LacedPacket{
					TrackID:  trackID,
					TimeNS:   0,
					Keyframe: true,
					Lacing:   LacingXiph,
					Frames: [][]byte{
						data,
						withContentEncodingHeader(settings, chainedContentPayload("bzlib av1 second laced block")),
					},
				})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("first packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
				second := withContentEncodingHeader(settings, chainedContentPayload("bzlib av1 second laced block"))
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
			},
		},
		{
			name: "opus simple block",
			track: Track{
				Type:              TrackAudio,
				Codec:             CodecOpus,
				DefaultDurationNS: 20_000_000,
				Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			track := tt.track
			track.ContentEncodings = chainedHeaderBzlibBlockContentEncodings(settings)
			trackID, err := muxer.AddTrack(track)
			if err != nil {
				t.Fatal(err)
			}
			want := withContentEncodingHeader(settings, chainedContentPayload(tt.name))
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) || bytes.Contains(buffer.Bytes(), want[len(settings):]) {
				t.Fatalf("file still contains unencoded frame payload")
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)
		})
	}
}

func TestDemuxerRetriesZlibLacedFrameAfterSmallBuffer(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecVP8,
		DefaultDurationNS: 20_000_000,
		ContentEncodings: []ContentEncoding{{
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := zlibTestPayload("first retry frame")
	second := zlibTestPayload("second retry frame")
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{first, second},
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
	packet := Packet{Data: make([]byte, 0, 4)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, len(first))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 0 || !bytes.Equal(packet.Data, first) {
		t.Fatalf("first retry packet = %+v data=%q", packet, packet.Data)
	}
	packet.Data = make([]byte, 0, len(second))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
		t.Fatalf("second packet = %+v data=%q", packet, packet.Data)
	}
}

func TestDemuxerRetriesChainedZlibLacedFrameAfterSmallBuffer(t *testing.T) {
	settings := []byte("HS:")
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecVP8,
		DefaultDurationNS: 20_000_000,
		ContentEncodings:  chainedHeaderZlibContentEncodings(settings),
		Video:             VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := withContentEncodingHeader(settings, chainedContentPayload("first retry frame"))
	second := withContentEncodingHeader(settings, chainedContentPayload("second retry frame"))
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{first, second},
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
	packet := Packet{Data: make([]byte, 0, 4)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, len(first))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 0 || !bytes.Equal(packet.Data, first) {
		t.Fatalf("first retry packet = %+v data=%q", packet, packet.Data)
	}
	packet.Data = make([]byte, 0, len(second))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
		t.Fatalf("second packet = %+v data=%q", packet, packet.Data)
	}
}

func TestDemuxerRetriesLZOLacedFrameAfterSmallBuffer(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecVP8,
		DefaultDurationNS: 20_000_000,
		ContentEncodings: []ContentEncoding{{
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoLZO1X},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := lzoTestPayload("first retry frame")
	second := lzoTestPayload("second retry frame")
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{first, second},
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
	packet := Packet{Data: make([]byte, 0, 4)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, len(first))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 0 || !bytes.Equal(packet.Data, first) {
		t.Fatalf("first retry packet = %+v data=%q", packet, packet.Data)
	}
	packet.Data = make([]byte, 0, len(second))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
		t.Fatalf("second packet = %+v data=%q", packet, packet.Data)
	}
}

func TestMuxerDemuxerAppliesZlibContentEncodingToWebRTCCodecs(t *testing.T) {
	tests := []struct {
		name  string
		track Track
		data  []byte
	}{
		{
			name:  "opus",
			track: Track{Type: TrackAudio, Codec: CodecOpus, Audio: AudioConfig{SampleRate: 48000, Channels: 2}},
			data:  zlibTestPayload("opus"),
		},
		{
			name:  "av1",
			track: Track{Type: TrackVideo, Codec: CodecAV1, CodecPrivate: av1CodecConfig(), Video: VideoConfig{Width: 640, Height: 360}},
			data:  zlibTestPayload("av1"),
		},
		{
			name:  "h264",
			track: Track{Type: TrackVideo, Codec: CodecH264, CodecPrivate: h264AVCDecoderConfigWithLengthSize(2), Video: VideoConfig{Width: 640, Height: 360}},
			data:  h264AnnexBAccessUnit(),
		},
		{
			name:  "vp9",
			track: Track{Type: TrackVideo, Codec: CodecVP9, Video: VideoConfig{Width: 640, Height: 360}},
			data:  zlibTestPayload("vp9"),
		},
		{
			name:  "vp8",
			track: Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 640, Height: 360}},
			data:  zlibTestPayload("vp8"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			track := tt.track
			track.ContentEncodings = []ContentEncoding{{
				Type:           ContentEncodingTypeCompression,
				CompressionSet: true,
				Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
			}}
			trackID, err := muxer.AddTrack(track)
			if err != nil {
				t.Fatal(err)
			}
			if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: tt.data}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), tt.data) {
				t.Fatalf("file still contains uncompressed %s payload", tt.name)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, len(tt.data))}
			if err := demuxer.ReadPacket(&packet); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(packet.Data, tt.data) {
				t.Fatalf("packet data = %v, want %v", packet.Data, tt.data)
			}
		})
	}
}

func TestMuxerDemuxerAppliesLZOContentEncodingToWebRTCCodecs(t *testing.T) {
	tests := []struct {
		name  string
		track Track
		data  []byte
	}{
		{
			name:  "opus",
			track: Track{Type: TrackAudio, Codec: CodecOpus, Audio: AudioConfig{SampleRate: 48000, Channels: 2}},
			data:  lzoTestPayload("opus"),
		},
		{
			name:  "av1",
			track: Track{Type: TrackVideo, Codec: CodecAV1, CodecPrivate: av1CodecConfig(), Video: VideoConfig{Width: 640, Height: 360}},
			data:  lzoTestPayload("av1"),
		},
		{
			name:  "h264",
			track: Track{Type: TrackVideo, Codec: CodecH264, CodecPrivate: h264AVCDecoderConfigWithLengthSize(2), Video: VideoConfig{Width: 640, Height: 360}},
			data:  h264AnnexBAccessUnit(),
		},
		{
			name:  "vp9",
			track: Track{Type: TrackVideo, Codec: CodecVP9, Video: VideoConfig{Width: 640, Height: 360}},
			data:  lzoTestPayload("vp9"),
		},
		{
			name:  "vp8",
			track: Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 640, Height: 360}},
			data:  lzoTestPayload("vp8"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			track := tt.track
			track.ContentEncodings = []ContentEncoding{{
				Type:           ContentEncodingTypeCompression,
				CompressionSet: true,
				Compression:    ContentCompression{Algorithm: ContentCompAlgoLZO1X},
			}}
			trackID, err := muxer.AddTrack(track)
			if err != nil {
				t.Fatal(err)
			}
			if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: tt.data}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), tt.data) {
				t.Fatalf("file still contains uncompressed %s payload", tt.name)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, len(tt.data))}
			if err := demuxer.ReadPacket(&packet); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(packet.Data, tt.data) {
				t.Fatalf("packet data = %v, want %v", packet.Data, tt.data)
			}
		})
	}
}

func TestMuxerDemuxerAppliesBzlibContentEncodingToWebRTCCodecs(t *testing.T) {
	tests := []struct {
		name  string
		track Track
		data  []byte
	}{
		{
			name:  "opus",
			track: Track{Type: TrackAudio, Codec: CodecOpus, Audio: AudioConfig{SampleRate: 48000, Channels: 2}},
			data:  bzlibTestPayload("opus"),
		},
		{
			name:  "av1",
			track: Track{Type: TrackVideo, Codec: CodecAV1, CodecPrivate: av1CodecConfig(), Video: VideoConfig{Width: 640, Height: 360}},
			data:  bzlibTestPayload("av1"),
		},
		{
			name:  "h264",
			track: Track{Type: TrackVideo, Codec: CodecH264, CodecPrivate: h264AVCDecoderConfigWithLengthSize(2), Video: VideoConfig{Width: 640, Height: 360}},
			data:  h264AnnexBAccessUnit(),
		},
		{
			name:  "vp9",
			track: Track{Type: TrackVideo, Codec: CodecVP9, Video: VideoConfig{Width: 640, Height: 360}},
			data:  bzlibTestPayload("vp9"),
		},
		{
			name:  "vp8",
			track: Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 640, Height: 360}},
			data:  bzlibTestPayload("vp8"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			track := tt.track
			track.ContentEncodings = []ContentEncoding{{
				Type:           ContentEncodingTypeCompression,
				CompressionSet: true,
				Compression:    ContentCompression{Algorithm: ContentCompAlgoBzlib},
			}}
			trackID, err := muxer.AddTrack(track)
			if err != nil {
				t.Fatal(err)
			}
			if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: tt.data}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), tt.data) {
				t.Fatalf("file still contains uncompressed %s payload", tt.name)
			}
			if tt.name == "h264" && bytes.Contains(buffer.Bytes(), h264AVCSampleWithLengthSize2()) {
				t.Fatalf("file still contains uncompressed h264 avc sample")
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, len(tt.data))}
			if err := demuxer.ReadPacket(&packet); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(packet.Data, tt.data) {
				t.Fatalf("packet data = %v, want %v", packet.Data, tt.data)
			}
		})
	}
}

func TestMuxerDemuxerAppliesAESCTRContentEncryption(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "simple block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "block group",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyID := aesCTRContentEncryptionKeyID()
			initialIV := aesCTRContentEncryptionInitialIV()
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{
				ContentEncryptionKeys:      aesCTRContentEncryptionKeys(keyID),
				ContentEncryptionInitialIV: initialIV,
			})
			if err != nil {
				t.Fatal(err)
			}
			want := encryptedTestPayload(tt.name)
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings:  aesCTRContentEncryptionTrackEncodings(keyID),
				Video:             VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) {
				t.Fatalf("file still contains unencrypted frame %q", want)
			}
			if !bytes.Contains(buffer.Bytes(), append([]byte{contentEncryptionSignalEncrypted}, initialIV...)) {
				t.Fatalf("file does not contain initial encrypted-frame signal and IV")
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{
				ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
			})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)

			missingKeyDemuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, len(want))}
			if err := missingKeyDemuxer.ReadPacket(&packet); !errors.Is(err, ErrUnsupportedContentEncoding) {
				t.Fatalf("missing key err = %v, want ErrUnsupportedContentEncoding", err)
			}
		})
	}
}

func TestMuxerDemuxerAppliesPartitionedAESCTRContentEncryption(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Muxer, uint32, Packet) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "simple block",
			write: func(muxer *Muxer, trackID uint32, packet Packet) error {
				packet.TrackID = trackID
				packet.TimeNS = 0
				packet.Keyframe = true
				return muxer.WritePacket(packet)
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "block group",
			write: func(muxer *Muxer, trackID uint32, packet Packet) error {
				packet.TrackID = trackID
				packet.TimeNS = 0
				packet.DurationNS = 20_000_000
				packet.Keyframe = true
				return muxer.WritePacket(packet)
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyID := aesCTRContentEncryptionKeyID()
			initialIV := aesCTRContentEncryptionInitialIV()
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{
				ContentEncryptionKeys:      aesCTRContentEncryptionKeys(keyID),
				ContentEncryptionInitialIV: initialIV,
			})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings:  aesCTRContentEncryptionTrackEncodings(keyID),
				Video:             VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			clearPrefix := []byte("partitioned aes ctr clear prefix:")
			secret := encryptedTestPayload("partitioned secret " + tt.name)
			clearSuffix := []byte(":partitioned aes ctr clear suffix")
			want := append(append(append([]byte(nil), clearPrefix...), secret...), clearSuffix...)
			partitions := []uint32{uint32(len(clearPrefix)), uint32(len(clearPrefix) + len(secret))}
			if err := tt.write(muxer, trackID, Packet{
				Data:                        want,
				ContentEncryptionPartitions: partitions,
			}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			encoded := buffer.Bytes()
			if !bytes.Contains(encoded, append([]byte{contentEncryptionSignalEncrypted | contentEncryptionSignalPartitioned}, initialIV...)) {
				t.Fatalf("file does not contain partitioned encrypted-frame signal and IV")
			}
			if !bytes.Contains(encoded, clearPrefix) {
				t.Fatalf("file does not contain clear prefix")
			}
			if !bytes.Contains(encoded, clearSuffix) {
				t.Fatalf("file does not contain clear suffix")
			}
			if bytes.Contains(encoded, secret) {
				t.Fatalf("file still contains encrypted partition %q", secret)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(encoded), DemuxerOptions{
				ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
			})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)

			missingKeyDemuxer, err := NewDemuxer(bytes.NewReader(encoded), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, len(want))}
			if err := missingKeyDemuxer.ReadPacket(&packet); !errors.Is(err, ErrUnsupportedContentEncoding) {
				t.Fatalf("missing key err = %v, want ErrUnsupportedContentEncoding", err)
			}
		})
	}
}

func TestMuxerDemuxerAppliesAESCTRContentEncryptionToWebRTCCodecs(t *testing.T) {
	tests := []struct {
		name  string
		track Track
		data  []byte
	}{
		{
			name:  "opus",
			track: Track{Type: TrackAudio, Codec: CodecOpus, Audio: AudioConfig{SampleRate: 48000, Channels: 2}},
			data:  encryptedTestPayload("opus"),
		},
		{
			name:  "av1",
			track: Track{Type: TrackVideo, Codec: CodecAV1, CodecPrivate: av1CodecConfig(), Video: VideoConfig{Width: 640, Height: 360}},
			data:  encryptedTestPayload("av1"),
		},
		{
			name:  "h264",
			track: Track{Type: TrackVideo, Codec: CodecH264, CodecPrivate: h264AVCDecoderConfigWithLengthSize(2), Video: VideoConfig{Width: 640, Height: 360}},
			data:  h264AnnexBAccessUnit(),
		},
		{
			name:  "vp9",
			track: Track{Type: TrackVideo, Codec: CodecVP9, Video: VideoConfig{Width: 640, Height: 360}},
			data:  encryptedTestPayload("vp9"),
		},
		{
			name:  "vp8",
			track: Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 640, Height: 360}},
			data:  encryptedTestPayload("vp8"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyID := aesCTRContentEncryptionKeyID()
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{
				ContentEncryptionKeys:      aesCTRContentEncryptionKeys(keyID),
				ContentEncryptionInitialIV: aesCTRContentEncryptionInitialIV(),
			})
			if err != nil {
				t.Fatal(err)
			}
			track := tt.track
			track.ContentEncodings = aesCTRContentEncryptionTrackEncodings(keyID)
			trackID, err := muxer.AddTrack(track)
			if err != nil {
				t.Fatal(err)
			}
			if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: tt.data}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), tt.data) {
				t.Fatalf("file still contains unencrypted %s payload", tt.name)
			}
			if tt.name == "h264" && bytes.Contains(buffer.Bytes(), h264AVCSampleWithLengthSize2()) {
				t.Fatalf("file still contains unencrypted h264 avc sample")
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{
				ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
			})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, len(tt.data))}
			if err := demuxer.ReadPacket(&packet); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(packet.Data, tt.data) {
				t.Fatalf("packet data = %v, want %v", packet.Data, tt.data)
			}
		})
	}
}

func TestMuxerDemuxerAppliesChainedCompressionAndAESCTRContentEncryption(t *testing.T) {
	settings := []byte("HS:")
	tests := []struct {
		name      string
		algorithm uint64
	}{
		{name: "zlib", algorithm: ContentCompAlgoZlib},
		{name: "bzlib", algorithm: ContentCompAlgoBzlib},
		{name: "lzo", algorithm: ContentCompAlgoLZO1X},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyID := aesCTRContentEncryptionKeyID()
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{
				ContentEncryptionKeys:      aesCTRContentEncryptionKeys(keyID),
				ContentEncryptionInitialIV: aesCTRContentEncryptionInitialIV(),
			})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings:  chainedHeaderCompressionAESCTRContentEncodings(settings, tt.algorithm, keyID),
				Video:             VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			want := withContentEncodingHeader(settings, chainedContentPayload("encrypted "+tt.name))
			if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: want}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) || bytes.Contains(buffer.Bytes(), want[len(settings):]) {
				t.Fatalf("file still contains unencoded frame payload")
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{
				ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
			})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, len(want))}
			if err := demuxer.ReadPacket(&packet); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(packet.Data, want) {
				t.Fatalf("packet data = %q, want %q", packet.Data, want)
			}
		})
	}
}

func TestMuxerRejectsPartitionedAESCTRContentEncryptionInvalidPartitions(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	manyPartitions := make([]uint32, 256)
	for i := range manyPartitions {
		manyPartitions[i] = uint32(i)
	}
	tests := []struct {
		name       string
		track      Track
		keys       []ContentEncryptionKey
		data       []byte
		partitions []uint32
	}{
		{
			name: "track without encryption",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 640, Height: 360},
			},
			data:       []byte("clear payload"),
			partitions: []uint32{0},
		},
		{
			name: "offset past payload",
			track: Track{
				Type:             TrackVideo,
				Codec:            CodecVP8,
				ContentEncodings: aesCTRContentEncryptionTrackEncodings(keyID),
				Video:            VideoConfig{Width: 640, Height: 360},
			},
			keys:       aesCTRContentEncryptionKeys(keyID),
			data:       []byte("payload"),
			partitions: []uint32{8},
		},
		{
			name: "duplicate offset",
			track: Track{
				Type:             TrackVideo,
				Codec:            CodecVP8,
				ContentEncodings: aesCTRContentEncryptionTrackEncodings(keyID),
				Video:            VideoConfig{Width: 640, Height: 360},
			},
			keys:       aesCTRContentEncryptionKeys(keyID),
			data:       []byte("payload"),
			partitions: []uint32{2, 2},
		},
		{
			name: "too many partitions",
			track: Track{
				Type:             TrackVideo,
				Codec:            CodecVP8,
				ContentEncodings: aesCTRContentEncryptionTrackEncodings(keyID),
				Video:            VideoConfig{Width: 640, Height: 360},
			},
			keys:       aesCTRContentEncryptionKeys(keyID),
			data:       make([]byte, 300),
			partitions: manyPartitions,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{
				ContentEncryptionKeys:      tt.keys,
				ContentEncryptionInitialIV: aesCTRContentEncryptionInitialIV(),
			})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(tt.track)
			if err != nil {
				t.Fatal(err)
			}
			err = muxer.WritePacket(Packet{
				TrackID:                     trackID,
				TimeNS:                      0,
				Keyframe:                    true,
				Data:                        tt.data,
				ContentEncryptionPartitions: tt.partitions,
			})
			if !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsAESCTRContentEncryptionWithoutKey(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{
		ContentEncryptionInitialIV: aesCTRContentEncryptionInitialIV(),
	})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:             TrackVideo,
		Codec:            CodecVP8,
		ContentEncodings: aesCTRContentEncryptionTrackEncodings(keyID),
		Video:            VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: encryptedTestPayload("missing key")}); !errors.Is(err, ErrUnsupportedContentEncoding) {
		t.Fatalf("err = %v, want ErrUnsupportedContentEncoding", err)
	}
}

func TestMuxerDemuxerAppliesAESCTRContentEncryptionToLacedFrames(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	frames := [][]byte{
		encryptedTestPayload("first laced"),
		encryptedTestPayload("second laced"),
		encryptedTestPayload("third laced"),
	}
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{
		ContentEncryptionKeys:      aesCTRContentEncryptionKeys(keyID),
		ContentEncryptionInitialIV: aesCTRContentEncryptionInitialIV(),
	})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecVP8,
		DefaultDurationNS: 20_000_000,
		ContentEncodings:  aesCTRContentEncryptionTrackEncodings(keyID),
		Video:             VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:         trackID,
		TimeNS:          0,
		FrameDurationNS: 20_000_000,
		Keyframe:        true,
		Lacing:          LacingEBML,
		Frames:          frames,
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	encoded := buffer.Bytes()
	for i := range frames {
		if bytes.Contains(encoded, frames[i]) {
			t.Fatalf("file still contains unencrypted laced frame %d", i)
		}
	}

	demuxer, err := NewDemuxer(bytes.NewReader(encoded), DemuxerOptions{
		ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
	})
	if err != nil {
		t.Fatal(err)
	}
	maxFrameSize := 0
	for i := range frames {
		if len(frames[i]) > maxFrameSize {
			maxFrameSize = len(frames[i])
		}
	}
	packet := Packet{Data: make([]byte, 0, maxFrameSize)}
	for i := range frames {
		if i == 1 {
			packet.Data = make([]byte, 0, len(frames[i])-1)
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
				t.Fatalf("small buffer err = %v, want ErrPayloadTooSmall", err)
			}
			packet.Data = make([]byte, 0, maxFrameSize)
		}
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%q", i, packet, packet.Data)
		}
	}

	missingKeyDemuxer, err := NewDemuxer(bytes.NewReader(encoded), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := missingKeyDemuxer.ReadPacket(&packet); !errors.Is(err, ErrUnsupportedContentEncoding) {
		t.Fatalf("missing key err = %v, want ErrUnsupportedContentEncoding", err)
	}
}

func TestDemuxerReadsUnencryptedFrameInEncryptedTrack(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	want := encryptedTestPayload("clear signal")
	frame := append([]byte{0}, want...)
	data := makeContentEncodedBlockMatroskaData(t, aesCTRContentEncryptionContentEncodings(t, keyID), frame)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(want))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packet.Data, want) {
		t.Fatalf("packet data = %q, want %q", packet.Data, want)
	}
}

func TestDemuxerRejectsUnsupportedAESCTRContentEncryptionSignal(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	frame := []byte{contentEncryptionSignalExtension}
	frame = append(frame, aesCTRContentEncryptionInitialIV()...)
	frame = append(frame, encryptedTestPayload("extension")...)
	data := makeContentEncodedBlockMatroskaData(t, aesCTRContentEncryptionContentEncodings(t, keyID), frame)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{
		ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 64)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrUnsupportedContentEncoding) {
		t.Fatalf("err = %v, want ErrUnsupportedContentEncoding", err)
	}
}

func TestDemuxerRejectsInvalidPartitionedAESCTRContentEncryption(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	iv := aesCTRContentEncryptionInitialIV()
	signal := contentEncryptionSignalEncrypted | contentEncryptionSignalPartitioned
	tests := []struct {
		name  string
		frame []byte
	}{
		{
			name:  "partitioned without encrypted flag",
			frame: []byte{contentEncryptionSignalPartitioned, 0x01, 0x02},
		},
		{
			name:  "missing iv",
			frame: []byte{signal, 0x01, 0x02},
		},
		{
			name:  "missing partition count",
			frame: append([]byte{signal}, iv...),
		},
		{
			name:  "truncated partition offsets",
			frame: append(append([]byte{signal}, iv...), 1, 0, 1),
		},
		{
			name:  "offset past payload",
			frame: partitionedAESCTRContentEncryptionFrame(signal, iv, []uint32{4}, []byte("abc")),
		},
		{
			name:  "unsorted offsets",
			frame: partitionedAESCTRContentEncryptionFrame(signal, iv, []uint32{3, 2}, []byte("abcd")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeContentEncodedBlockMatroskaData(t, aesCTRContentEncryptionContentEncodings(t, keyID), tt.frame)
			demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{
				ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
			})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, 64)}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerDemuxerAppliesZlibContentEncodingToH264(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH264,
		CodecPrivate: h264AVCDecoderConfigWithLengthSize(2),
		ContentEncodings: []ContentEncoding{{
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	annexB := h264AnnexBAccessUnit()
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: annexB}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buffer.Bytes(), annexB) || bytes.Contains(buffer.Bytes(), h264AVCSampleWithLengthSize2()) {
		t.Fatalf("file still contains uncompressed h264 sample")
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(annexB))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packet.Data, annexB) {
		t.Fatalf("packet data = %v, want %v", packet.Data, annexB)
	}
}

func TestMuxerRejectsHeaderStrippingPacketWithoutPrefix(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		ContentEncodings: []ContentEncoding{{
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  []byte("HS:"),
			},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte("missing")}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{[]byte("HS:first"), []byte("missing")},
	}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("laced err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerRejectsUnsupportedBlockContentEncoding(t *testing.T) {
	tests := []struct {
		name      string
		encodings []ContentEncoding
		wantError error
	}{
		{
			name: "bzlib compression settings",
			encodings: []ContentEncoding{{
				Type:           ContentEncodingTypeCompression,
				CompressionSet: true,
				Compression:    ContentCompression{Algorithm: ContentCompAlgoBzlib, Settings: []byte{1}},
			}},
			wantError: ErrUnsupportedContentEncoding,
		},
		{
			name: "lzo compression settings",
			encodings: []ContentEncoding{{
				Type:           ContentEncodingTypeCompression,
				CompressionSet: true,
				Compression:    ContentCompression{Algorithm: ContentCompAlgoLZO1X, Settings: []byte{1}},
			}},
			wantError: ErrUnsupportedContentEncoding,
		},
		{
			name: "cbc block encryption",
			encodings: []ContentEncoding{{
				Type:          ContentEncodingTypeEncryption,
				EncryptionSet: true,
				Encryption: ContentEncryption{
					Algorithm:      ContentEncAlgoAES,
					AESSettingsSet: true,
					AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCBC},
				},
			}},
			wantError: ErrUnsupportedContentEncoding,
		},
		{
			name: "multiple header stripping encodings",
			encodings: []ContentEncoding{
				{
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoHeaderStripping, Settings: []byte("A")},
				},
				{
					Order:          1,
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoHeaderStripping, Settings: []byte("B")},
				},
			},
			wantError: ErrUnsupportedContentEncoding,
		},
		{
			name: "multiple block compressions",
			encodings: []ContentEncoding{
				{
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
				},
				{
					Order:          1,
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoBzlib},
				},
			},
			wantError: ErrUnsupportedContentEncoding,
		},
		{
			name: "header stripping after zlib",
			encodings: []ContentEncoding{
				{
					Order:          0,
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
				},
				{
					Order:          1,
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoHeaderStripping, Settings: []byte("HS:")},
				},
			},
			wantError: ErrUnsupportedContentEncoding,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = muxer.AddTrack(Track{
				Type:             TrackVideo,
				Codec:            CodecVP8,
				ContentEncodings: tt.encodings,
				Video:            VideoConfig{Width: 16, Height: 16},
			})
			if !errors.Is(err, tt.wantError) {
				t.Fatalf("err = %v, want %v", err, tt.wantError)
			}
		})
	}
}

func TestDemuxerRejectsUnsupportedBlockContentEncoding(t *testing.T) {
	tests := []struct {
		name      string
		encodings []byte
		frame     []byte
	}{
		{
			name: "block encryption",
			encodings: contentEncodingsPayload(t,
				contentEncodingPayload(t, func(w *ebml.Writer) error {
					if err := w.WriteUInt(idContentEncodingType, ContentEncodingTypeEncryption); err != nil {
						return err
					}
					return w.WriteElement(idContentEncryption, contentEncryptionPayload(t, ContentEncAlgoAES, nil, ContentEncAESCipherModeCTR))
				}),
			),
			frame: append(append([]byte{contentEncryptionSignalEncrypted}, aesCTRContentEncryptionInitialIV()...), []byte("compressed")...),
		},
		{
			name: "header stripping after zlib",
			encodings: contentEncodingsPayload(t,
				contentEncodingPayload(t, func(w *ebml.Writer) error {
					if err := w.WriteUInt(idContentEncodingOrd, 0); err != nil {
						return err
					}
					return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoZlib, nil))
				}),
				contentEncodingPayload(t, func(w *ebml.Writer) error {
					if err := w.WriteUInt(idContentEncodingOrd, 1); err != nil {
						return err
					}
					return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoHeaderStripping, []byte("HS:")))
				}),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := tt.frame
			if frame == nil {
				frame = []byte("compressed")
			}
			data := makeContentEncodedBlockMatroskaData(t, tt.encodings, frame)
			demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, 32)}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrUnsupportedContentEncoding) {
				t.Fatalf("err = %v, want ErrUnsupportedContentEncoding", err)
			}
		})
	}
}

func TestDemuxerReadsCanonicalLZOContentEncoding(t *testing.T) {
	compressed := []byte{0x12, 0x00, 0x20, 0x00, 0xdf, 0x00, 0x00, 0x11, 0x00, 0x00}
	want := make([]byte, 512)
	data := makeContentEncodedBlockMatroskaData(t, lzoContentEncodings(t), compressed)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(want))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packet.Data, want) {
		t.Fatalf("packet data = %x, want %x", packet.Data, want)
	}
}

func TestDemuxerRejectsInvalidLZOContentEncodingPayload(t *testing.T) {
	data := makeContentEncodedBlockMatroskaData(t, lzoContentEncodings(t), []byte("not an lzo1x stream"))
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 32)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerRejectsTrailingLZOContentEncodingPayload(t *testing.T) {
	payload := append(lzoCompressedPayload(t, "trailing stream"), []byte("tail")...)
	data := makeContentEncodedBlockMatroskaData(t, lzoContentEncodings(t), payload)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(lzoTestPayload("trailing stream")))}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerReadsBzlibContentEncoding(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		read func(*Demuxer)
	}{
		{
			name: "simple block",
			data: makeContentEncodedBlockMatroskaData(t, bzlibContentEncodings(t), bzlibCompressedPayload(t, "simple block")),
			read: func(demuxer *Demuxer) {
				want := bzlibTestPayload("simple block")
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "laced block",
			data: makeContentEncodedLacedMatroskaData(t, bzlibContentEncodings(t), [][]byte{
				bzlibCompressedPayload(t, "laced first"),
				bzlibCompressedPayload(t, "laced second"),
			}),
			read: func(demuxer *Demuxer) {
				first := bzlibTestPayload("laced first")
				packet := Packet{Data: make([]byte, 0, len(first))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, first) {
					t.Fatalf("first packet = %+v data=%q, want %q", packet, packet.Data, first)
				}
				second := bzlibTestPayload("laced second")
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			demuxer, err := NewDemuxer(bytes.NewReader(tt.data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer)
		})
	}
}

func TestDemuxerReadsChainedHeaderStrippingAndBzlibContentEncoding(t *testing.T) {
	settings := []byte("HS:")
	tests := []struct {
		name string
		data []byte
		read func(*Demuxer)
	}{
		{
			name: "simple block",
			data: makeContentEncodedBlockMatroskaData(t, chainedHeaderBzlibContentEncodings(t, settings), bzlibCompressedPayload(t, "simple block")),
			read: func(demuxer *Demuxer) {
				want := withContentEncodingHeader(settings, bzlibTestPayload("simple block"))
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "laced block",
			data: makeContentEncodedLacedMatroskaData(t, chainedHeaderBzlibContentEncodings(t, settings), [][]byte{
				bzlibCompressedPayload(t, "laced first"),
				bzlibCompressedPayload(t, "laced second"),
			}),
			read: func(demuxer *Demuxer) {
				first := withContentEncodingHeader(settings, bzlibTestPayload("laced first"))
				packet := Packet{Data: make([]byte, 0, len(first))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, first) {
					t.Fatalf("first packet = %+v data=%q, want %q", packet, packet.Data, first)
				}
				second := withContentEncodingHeader(settings, bzlibTestPayload("laced second"))
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			demuxer, err := NewDemuxer(bytes.NewReader(tt.data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer)
		})
	}
}

func TestDemuxerRetriesBzlibLacedFrameAfterSmallBuffer(t *testing.T) {
	data := makeContentEncodedLacedMatroskaData(t, bzlibContentEncodings(t), [][]byte{
		bzlibCompressedPayload(t, "laced first"),
		bzlibCompressedPayload(t, "laced second"),
	})
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	first := bzlibTestPayload("laced first")
	packet := Packet{Data: make([]byte, 0, 4)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, len(first))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 0 || !bytes.Equal(packet.Data, first) {
		t.Fatalf("retry packet = %+v data=%q, want %q", packet, packet.Data, first)
	}
}

func TestDemuxerRejectsInvalidBzlibContentEncodingPayload(t *testing.T) {
	data := makeContentEncodedBlockMatroskaData(t, bzlibContentEncodings(t), []byte("not a bzip2 stream"))
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 32)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerRejectsInvalidZlibContentEncodingPayload(t *testing.T) {
	data := makeContentEncodedBlockMatroskaData(t, contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoZlib, nil))
		}),
	), []byte("not a zlib stream"))
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 32)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerRejectsTruncatedZlibContentEncodingPayload(t *testing.T) {
	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	if _, err := zw.Write(zlibTestPayload("truncated stream")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	payload := compressed.Bytes()
	data := makeContentEncodedBlockMatroskaData(t, contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoZlib, nil))
		}),
	), payload[:len(payload)-1])
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(zlibTestPayload("truncated stream")))}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func zlibTestPayload(label string) []byte {
	prefix := []byte("zlib content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*16)
	for i := 0; i < 16; i++ {
		payload = append(payload, prefix...)
	}
	return payload
}

func lzoContentEncodings(t testing.TB) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoLZO1X, nil))
		}),
	)
}

func lzoTestPayload(label string) []byte {
	prefix := []byte("lzo1x content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*16)
	for i := 0; i < 16; i++ {
		payload = append(payload, prefix...)
	}
	return payload
}

func lzoCompressedPayload(t testing.TB, label string) []byte {
	t.Helper()
	payload, err := lzo.Compress(lzoTestPayload(label), nil)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func chainedContentPayload(label string) []byte {
	prefix := []byte("ordered content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*16)
	for i := 0; i < 16; i++ {
		payload = append(payload, prefix...)
	}
	return payload
}

func withContentEncodingHeader(header, data []byte) []byte {
	out := make([]byte, 0, len(header)+len(data))
	out = append(out, header...)
	out = append(out, data...)
	return out
}

func chainedHeaderZlibContentEncodings(header []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
		},
	}
}

func chainedHeaderLZOContentEncodings(header []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoLZO1X},
		},
	}
}

func chainedHeaderBzlibBlockContentEncodings(header []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoBzlib},
		},
	}
}

func chainedHeaderBzlibContentEncodings(t testing.TB, header []byte) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			if err := w.WriteUInt(idContentEncodingOrd, 0); err != nil {
				return err
			}
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoHeaderStripping, header))
		}),
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			if err := w.WriteUInt(idContentEncodingOrd, 1); err != nil {
				return err
			}
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoBzlib, nil))
		}),
	)
}

func bzlibContentEncodings(t testing.TB) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoBzlib, nil))
		}),
	)
}

func bzlibTestPayload(label string) []byte {
	prefix := []byte("bzip2 content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*8)
	for i := 0; i < 8; i++ {
		payload = append(payload, prefix...)
	}
	return payload
}

func bzlibCompressedPayload(t testing.TB, label string) []byte {
	t.Helper()
	fixtures := map[string]string{
		"simple block": "425a6839314159265359c57d93a80000339980400010103eafcc302000902984d34069881554d3d4068f51934dc662e1a0e45854762c207236102c2074207a205435171518161d0c0b8f87c282824486e1a0918141b091228207a2a3f177245385090c57d93a80",
		"laced first":  "425a6839314159265359e931c9840000339980400010103fa5dc302000902984d340698815553f54dea6937a8d344f481e0d848fa286048d05c6048b0dc6050b8d0585c48dc205850c8c0919143c10287438091610351fc703210351d0c8f45dc914e14243a4c72610",
		"laced second": "425a6839314159265359c10c71800000339980400010103ea5cc3020009029a6462626205553351a34c9ea34a0f440a0e45c48a8644881617123c18141020587a24586438181a1b8d0ec5c68545448ec6db8b0702e30282064687e2ee48a70a1218218e300",
	}
	value, ok := fixtures[label]
	if !ok {
		t.Fatalf("missing bzlib fixture %q", label)
	}
	out, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func aesCTRContentEncryptionKeyID() []byte {
	return []byte("aes-ctr-key")
}

func aesCTRContentEncryptionKeys(keyID []byte) []ContentEncryptionKey {
	return []ContentEncryptionKey{{
		KeyID: append([]byte(nil), keyID...),
		Key: []byte{
			0x10, 0x11, 0x12, 0x13,
			0x14, 0x15, 0x16, 0x17,
			0x18, 0x19, 0x1a, 0x1b,
			0x1c, 0x1d, 0x1e, 0x1f,
		},
	}}
}

func aesCTRContentEncryptionInitialIV() []byte {
	return []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
}

func aesCTRContentEncryptionTrackEncodings(keyID []byte) []ContentEncoding {
	return []ContentEncoding{{
		Type:          ContentEncodingTypeEncryption,
		EncryptionSet: true,
		Encryption: ContentEncryption{
			Algorithm:      ContentEncAlgoAES,
			KeyID:          append([]byte(nil), keyID...),
			AESSettingsSet: true,
			AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCTR},
		},
	}}
}

func chainedHeaderCompressionAESCTRContentEncodings(header []byte, algorithm uint64, keyID []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: algorithm},
		},
		{
			Order:         2,
			Type:          ContentEncodingTypeEncryption,
			EncryptionSet: true,
			Encryption: ContentEncryption{
				Algorithm:      ContentEncAlgoAES,
				KeyID:          append([]byte(nil), keyID...),
				AESSettingsSet: true,
				AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCTR},
			},
		},
	}
}

func aesCTRContentEncryptionContentEncodings(t testing.TB, keyID []byte) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			if err := w.WriteUInt(idContentEncodingType, ContentEncodingTypeEncryption); err != nil {
				return err
			}
			return w.WriteElement(idContentEncryption, contentEncryptionPayload(t, ContentEncAlgoAES, keyID, ContentEncAESCipherModeCTR))
		}),
	)
}

func encryptedTestPayload(label string) []byte {
	prefix := []byte("aes ctr content encryption test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*12)
	for i := 0; i < 12; i++ {
		payload = append(payload, prefix...)
	}
	return payload
}

func partitionedAESCTRContentEncryptionFrame(signal byte, iv []byte, partitions []uint32, payload []byte) []byte {
	frame := append([]byte{signal}, iv...)
	frame = append(frame, byte(len(partitions)))
	var offset [4]byte
	for i := range partitions {
		binary.BigEndian.PutUint32(offset[:], partitions[i])
		frame = append(frame, offset[:]...)
	}
	return append(frame, payload...)
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

func TestFormatDemuxerPrefersLanguageBCP47(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:          TrackAudio,
		Codec:         CodecOpus,
		Language:      "eng",
		LanguageBCP47: "en-US",
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

	demuxer := &FormatDemuxer{}
	if err := demuxer.Open(context.Background(), format.Input{Reader: bytes.NewReader(buffer.Bytes())}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	streams := demuxer.Streams()
	if len(streams) != 1 {
		t.Fatalf("streams = %d, want 1", len(streams))
	}
	if streams[0].Language != "en-US" {
		t.Fatalf("stream language = %q, want en-US", streams[0].Language)
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

func TestDemuxerRejectsInvalidTrackFlags(t *testing.T) {
	tests := []struct {
		name string
		id   ebml.ID
	}{
		{name: "enabled", id: idFlagEnabled},
		{name: "default", id: idFlagDefault},
		{name: "forced", id: idFlagForced},
		{name: "hearing", id: idFlagHearingImpaired},
		{name: "visual", id: idFlagVisualImpaired},
		{name: "text descriptions", id: idFlagTextDescriptions},
		{name: "original", id: idFlagOriginal},
		{name: "commentary", id: idFlagCommentary},
		{name: "lacing", id: idFlagLacing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
				return writeTracksWithFlagValue(writer, tt.id, 2)
			})
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
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

func TestDemuxerRejectsLacedBlockWhenTrackDisablesLacing(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
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
	if err := muxer.writeHeader(); err != nil {
		t.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		t.Fatal(err)
	}
	if err := writeLacedSimpleBlock(muxer.ebml, trackID, simpleBlockLacingXiph, [][]byte{{1}, {2}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 4)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerTracksReturnsCodecPrivateCopies(t *testing.T) {
	data := makeH264AVCMatroskaData(t, 1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 || len(tracks[0].CodecPrivate) == 0 {
		t.Fatalf("tracks = %+v", tracks)
	}
	tracks[0].CodecPrivate[4] = 0xfe

	fresh := demuxer.Tracks()
	if len(fresh) != 1 || !bytes.Equal(fresh[0].CodecPrivate, h264AVCDecoderConfigWithLengthSize(2)) {
		t.Fatalf("fresh tracks = %+v", fresh)
	}
	packet := Packet{Data: make([]byte, 0, len(h264AnnexBAccessUnit()))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packet.Data, h264AnnexBAccessUnit()) {
		t.Fatalf("data = %v, want Annex B", packet.Data)
	}
}

func TestMuxerWritesDefaultOpusHead(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{1, 2, 3},
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
	if tracks[0].Codec != CodecOpus || tracks[0].Audio.Channels != 1 ||
		tracks[0].CodecDelayNS != 0 || tracks[0].SeekPreRollNS != opusDefaultSeekPreRollNS ||
		!bytes.Equal(tracks[0].CodecPrivate, expectedOpusHead(1, 48000)) {
		t.Fatalf("track = %+v private=%v", tracks[0], tracks[0].CodecPrivate)
	}
}

func TestMuxerDerivesOpusCodecTimingFromPrivateData(t *testing.T) {
	private := expectedOpusHeadWithPreSkip(2, 48000, 312)
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackAudio,
		Codec:        CodecOpus,
		Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
		CodecPrivate: private,
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
	if tracks[0].CodecDelayNS != 6_500_000 || tracks[0].SeekPreRollNS != opusDefaultSeekPreRollNS {
		t.Fatalf("track timing delay=%d preroll=%d", tracks[0].CodecDelayNS, tracks[0].SeekPreRollNS)
	}
}

func TestDemuxerReadsOpusCodecTiming(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	writeMatroskaSegmentPrefix(t, muxer)
	if err := writeTracksWithOpusPrivateAndTiming(muxer.ebml, expectedOpusHeadWithPreSkip(2, 48000, 960), 1_234, 5_678); err != nil {
		t.Fatal(err)
	}
	if err := muxer.ebml.WriteElement(idCluster, nil); err != nil {
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
	if tracks[0].CodecDelayNS != 1_234 || tracks[0].SeekPreRollNS != 5_678 {
		t.Fatalf("track timing delay=%d preroll=%d", tracks[0].CodecDelayNS, tracks[0].SeekPreRollNS)
	}
}

func TestDemuxerRejectsInvalidOpusHead(t *testing.T) {
	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: []byte("OpusHead")},
		{name: "bad magic", private: []byte("OpusHed\x01\x02\x00\x00\x80\xbb\x00\x00\x00\x00\x00")},
		{name: "zero channels", private: []byte("OpusHead\x01\x00\x00\x00\x80\xbb\x00\x00\x00\x00\x00")},
		{name: "family zero surround", private: []byte("OpusHead\x01\x03\x00\x00\x80\xbb\x00\x00\x00\x00\x00")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
				return writeTracksWithOpusPrivate(writer, tt.private)
			})
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMSACMWaveFormatValidation(t *testing.T) {
	format, err := parseMSACMWaveFormat(expectedMSACMWaveFormat(CodecPCMU, 2, 16000))
	if err != nil {
		t.Fatal(err)
	}
	if format.FormatTag != waveFormatMuLaw ||
		format.Channels != 2 ||
		format.SampleRate != 16000 ||
		format.AvgBytesPerSec != 32000 ||
		format.BlockAlign != 2 ||
		format.BitsPerSample != 8 ||
		format.ExtraSize != 0 {
		t.Fatalf("format = %+v", format)
	}

	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: expectedMSACMWaveFormat(CodecPCMU, 1, 8000)[:17]},
		{name: "trailing bytes without cbSize", private: append(expectedMSACMWaveFormat(CodecPCMU, 1, 8000), 0)},
		{name: "zero channels", private: msACMWaveFormatBytes(waveFormatMuLaw, 0, 8000, 8000, 1, 8, nil)},
		{name: "zero sample rate", private: msACMWaveFormatBytes(waveFormatMuLaw, 1, 0, 8000, 1, 8, nil)},
		{name: "zero average bytes", private: msACMWaveFormatBytes(waveFormatMuLaw, 1, 8000, 0, 1, 8, nil)},
		{name: "zero block align", private: msACMWaveFormatBytes(waveFormatMuLaw, 1, 8000, 8000, 0, 8, nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseMSACMWaveFormat(tt.private); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidMSACMG711CodecPrivate(t *testing.T) {
	avgMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	binary.LittleEndian.PutUint32(avgMismatch[8:12], 1)
	blockAlignMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	binary.LittleEndian.PutUint16(blockAlignMismatch[12:14], 2)

	tests := []struct {
		name  string
		track Track
	}{
		{
			name: "short",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecPCMU,
				Audio:        AudioConfig{SampleRate: 8000, Channels: 1},
				CodecPrivate: []byte{0x07, 0x00},
			},
		},
		{
			name: "wrong tag",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecPCMU,
				Audio:        AudioConfig{SampleRate: 8000, Channels: 1},
				CodecPrivate: expectedMSACMWaveFormat(CodecPCMA, 1, 8000),
			},
		},
		{
			name: "average bytes mismatch",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecPCMU,
				Audio:        AudioConfig{SampleRate: 8000, Channels: 1},
				CodecPrivate: avgMismatch,
			},
		},
		{
			name: "block align mismatch",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecPCMU,
				Audio:        AudioConfig{SampleRate: 8000, Channels: 1},
				CodecPrivate: blockAlignMismatch,
			},
		},
		{
			name: "audio sample rate mismatch",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecPCMU,
				Audio:        AudioConfig{SampleRate: 16000, Channels: 1},
				CodecPrivate: expectedMSACMWaveFormat(CodecPCMU, 1, 8000),
			},
		},
		{
			name: "default channel overflow",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecPCMU,
				Audio: AudioConfig{SampleRate: 8000, Channels: int(uint64(^uint16(0)) + 1)},
			},
		},
		{
			name: "default bit depth mismatch",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecPCMU,
				Audio: AudioConfig{SampleRate: 8000, Channels: 1, BitDepth: 16},
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

func TestDemuxerRejectsInvalidMSACMG711CodecPrivate(t *testing.T) {
	extraMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	extraMismatch = append(extraMismatch, 0)
	avgMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	binary.LittleEndian.PutUint32(avgMismatch[8:12], 1)
	blockAlignMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	binary.LittleEndian.PutUint16(blockAlignMismatch[12:14], 2)
	bitDepthMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	binary.LittleEndian.PutUint16(bitDepthMismatch[14:16], 16)

	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: []byte{0x07, 0x00}},
		{name: "extra size mismatch", private: extraMismatch},
		{name: "zero sample rate", private: msACMWaveFormatBytes(waveFormatMuLaw, 1, 0, 8000, 1, 8, nil)},
		{name: "average bytes mismatch", private: avgMismatch},
		{name: "block align mismatch", private: blockAlignMismatch},
		{name: "bit depth mismatch", private: bitDepthMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
				return writeTracksWithMSACMPrivate(writer, tt.private, AudioConfig{SampleRate: 8000, Channels: 1, BitDepth: 8})
			})
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestDemuxerReadsUnsupportedMSACMAsUnknown(t *testing.T) {
	private := msACMWaveFormatBytes(0x0055, 2, 44100, 16000, 1, 0, nil)
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	writeMatroskaSegmentPrefix(t, muxer)
	if err := writeTracksWithMSACMPrivate(muxer.ebml, private, AudioConfig{SampleRate: 44100, Channels: 2}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.ebml.WriteElement(idCluster, nil); err != nil {
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
	if tracks[0].Codec != CodecUnknown ||
		tracks[0].Audio.SampleRate != 44100 ||
		tracks[0].Audio.Channels != 2 ||
		tracks[0].Audio.BitDepth != 0 ||
		!bytes.Equal(tracks[0].CodecPrivate, private) {
		t.Fatalf("track = %+v private=%x", tracks[0], tracks[0].CodecPrivate)
	}
}

func TestAV1CodecConfigurationRecordValidation(t *testing.T) {
	config, err := parseAV1CodecConfigurationRecord(av1CodecConfig())
	if err != nil {
		t.Fatal(err)
	}
	if config.SeqProfile != 0 || config.SeqLevelIdx0 != 5 || config.SeqTier0 ||
		config.HighBitDepth || config.TwelveBit || !config.Monochrome ||
		config.ChromaSubsamplingX || config.ChromaSubsamplingY ||
		config.ChromaSamplePosition != 0 || config.InitialPresentationDelaySet ||
		config.ConfigOBUCount != 1 || !config.SequenceHeaderOBUPresent {
		t.Fatalf("config = %+v", config)
	}

	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: []byte{0x81, 0x05, 0x10}},
		{name: "bad marker", private: av1CodecConfigWithByte(0, 0x01)},
		{name: "bad version", private: av1CodecConfigWithByte(0, 0x82)},
		{name: "bad profile", private: av1CodecConfigWithByte(1, 0xe5)},
		{name: "twelve bit without high bit depth", private: av1CodecConfigWithByte(2, 0x20)},
		{name: "reserved chroma sample position", private: av1CodecConfigWithByte(2, 0x03)},
		{name: "reserved fixed bits", private: av1CodecConfigWithByte(3, 0xe0)},
		{name: "delay bits without presence", private: av1CodecConfigWithByte(3, 0x01)},
		{name: "obu missing size", private: av1CodecConfigWithByte(4, 0x08)},
		{name: "sequence not first", private: av1CodecConfigWithPrefixOBU([]byte{0x2a, 0x00})},
		{name: "duplicate sequence", private: append(av1CodecConfig(), av1SequenceHeaderOBU()...)},
		{name: "truncated leb128", private: append([]byte{0x81, 0x05, 0x10, 0x00, 0x0a}, 0x80)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseAV1CodecConfigurationRecord(tt.private); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidAV1CodecPrivate(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecAV1,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: []byte{0x81, 0x05, 0x10, 0x00, 0x0a},
	})
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
}

func TestDemuxerRejectsInvalidAV1CodecPrivate(t *testing.T) {
	data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
		return writeTracksWithAV1Private(writer, []byte{0x81, 0x05, 0x10, 0x00, 0x0a})
	})
	if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestAV1CodecConfigurationRecordFromSequenceHeader(t *testing.T) {
	private, err := av1CodecConfigurationRecordFromFrames([][]byte{av1SequenceHeaderOBU()})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(private, av1CodecConfig()) {
		t.Fatalf("private = %v, want %v", private, av1CodecConfig())
	}
	if _, err := parseAV1CodecConfigurationRecord(private); err != nil {
		t.Fatal(err)
	}

	_, err = av1CodecConfigurationRecordFromFrames([][]byte{av1TemporalDelimiterOBU()})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerGeneratesAV1CodecPrivateFromFirstPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecAV1,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	data := av1SequenceHeaderOBU()
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data}); err != nil {
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
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, av1CodecConfig()) {
		t.Fatalf("tracks = %+v", tracks)
	}
	packet := Packet{Data: make([]byte, 0, len(data))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != trackID || packet.TimeNS != 0 || !packet.Keyframe || !bytes.Equal(packet.Data, data) {
		t.Fatalf("packet = %+v data=%v, want %v", packet, packet.Data, data)
	}
}

func TestMuxerGeneratesAV1CodecPrivateFromFirstLacedPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecAV1,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{av1SequenceHeaderOBU(), av1TemporalDelimiterOBU()}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   frames,
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
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, av1CodecConfig()) {
		t.Fatalf("tracks = %+v", tracks)
	}
	packet := Packet{Data: make([]byte, 0, len(frames[0]))}
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("frame %d read: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, frames[i])
		}
	}
}

func TestMuxerDefersHeaderWhenAV1CodecPrivateIsMissing(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecAV1,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	audioTrack, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = muxer.WritePacket(Packet{TrackID: audioTrack, TimeNS: 0, Keyframe: true, Data: []byte{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before AV1 private data was available", buffer.Len())
	}
	err = muxer.Close()
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing AV1 private data", buffer.Len())
	}
}

func TestMuxerDefersHeaderUntilGeneratedCodecPrivateTracksReady(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h264ID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH264,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	av1ID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecAV1,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	opusID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	h264Data := h264AnnexBParameterAccessUnit()
	if err := muxer.WritePacket(Packet{TrackID: h264ID, TimeNS: 0, Keyframe: true, Data: h264Data}); err != nil {
		t.Fatal(err)
	}
	h264Data[0] = 0xff
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before all generated private data was available", buffer.Len())
	}
	if _, err := muxer.AddTrack(Track{Type: TrackAudio, Codec: CodecOpus, Audio: AudioConfig{SampleRate: 48000, Channels: 1}}); !errors.Is(err, ErrTrackAfterWrite) {
		t.Fatalf("err = %v, want ErrTrackAfterWrite", err)
	}

	opusData := []byte{0xf8, 0xff, 0xfe}
	if err := muxer.WritePacket(Packet{TrackID: opusID, TimeNS: 20_000_000, Keyframe: true, Data: opusData}); err != nil {
		t.Fatal(err)
	}
	opusData[0] = 0
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before all generated private data was available", buffer.Len())
	}

	av1Data := av1SequenceHeaderOBU()
	if err := muxer.WritePacket(Packet{TrackID: av1ID, TimeNS: 40_000_000, Keyframe: true, Data: av1Data}); err != nil {
		t.Fatal(err)
	}
	av1Data[0] = 0
	if buffer.Len() == 0 {
		t.Fatalf("header was not written after generated private data became available")
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 3 {
		t.Fatalf("tracks = %d, want 3", len(tracks))
	}
	if !bytes.Equal(tracks[0].CodecPrivate, h264AVCDecoderConfig()) {
		t.Fatalf("h264 private = %x, want %x", tracks[0].CodecPrivate, h264AVCDecoderConfig())
	}
	if !bytes.Equal(tracks[1].CodecPrivate, av1CodecConfig()) {
		t.Fatalf("av1 private = %x, want %x", tracks[1].CodecPrivate, av1CodecConfig())
	}

	wantPackets := []Packet{
		{TrackID: h264ID, TimeNS: 0, Keyframe: true, Data: h264AnnexBParameterAccessUnit()},
		{TrackID: opusID, TimeNS: 20_000_000, Keyframe: true, Data: []byte{0xf8, 0xff, 0xfe}},
		{TrackID: av1ID, TimeNS: 40_000_000, Keyframe: true, Data: av1SequenceHeaderOBU()},
	}
	got := Packet{Data: make([]byte, 0, len(h264AnnexBParameterAccessUnit()))}
	for i := range wantPackets {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatalf("packet %d read: %v", i, err)
		}
		if got.TrackID != wantPackets[i].TrackID || got.TimeNS != wantPackets[i].TimeNS ||
			got.Keyframe != wantPackets[i].Keyframe || !bytes.Equal(got.Data, wantPackets[i].Data) {
			t.Fatalf("packet %d = %+v data=%x, want %+v data=%x", i, got, got.Data, wantPackets[i], wantPackets[i].Data)
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestMuxerDefersLacedHeaderPacketUntilGeneratedCodecPrivateTracksReady(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	av1ID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecAV1,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	h264ID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH264,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}

	frames := [][]byte{av1SequenceHeaderOBU(), av1TemporalDelimiterOBU()}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  av1ID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   frames,
	}); err != nil {
		t.Fatal(err)
	}
	frames[0][0] = 0
	frames[1][0] = 0
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before all generated private data was available", buffer.Len())
	}
	if err := muxer.WritePacket(Packet{TrackID: h264ID, TimeNS: 40_000_000, Keyframe: true, Data: h264AnnexBParameterAccessUnit()}); err != nil {
		t.Fatal(err)
	}
	if buffer.Len() == 0 {
		t.Fatalf("header was not written after generated private data became available")
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 2 {
		t.Fatalf("tracks = %d, want 2", len(tracks))
	}
	if !bytes.Equal(tracks[0].CodecPrivate, av1CodecConfig()) {
		t.Fatalf("av1 private = %x, want %x", tracks[0].CodecPrivate, av1CodecConfig())
	}
	if !bytes.Equal(tracks[1].CodecPrivate, h264AVCDecoderConfig()) {
		t.Fatalf("h264 private = %x, want %x", tracks[1].CodecPrivate, h264AVCDecoderConfig())
	}

	wantPackets := []Packet{
		{TrackID: av1ID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: av1SequenceHeaderOBU()},
		{TrackID: av1ID, TimeNS: 20_000_000, DurationNS: 20_000_000, Keyframe: true, Data: av1TemporalDelimiterOBU()},
		{TrackID: h264ID, TimeNS: 40_000_000, Keyframe: true, Data: h264AnnexBParameterAccessUnit()},
	}
	got := Packet{Data: make([]byte, 0, len(h264AnnexBParameterAccessUnit()))}
	for i := range wantPackets {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatalf("packet %d read: %v", i, err)
		}
		if got.TrackID != wantPackets[i].TrackID || got.TimeNS != wantPackets[i].TimeNS ||
			got.DurationNS != wantPackets[i].DurationNS || got.Keyframe != wantPackets[i].Keyframe ||
			!bytes.Equal(got.Data, wantPackets[i].Data) {
			t.Fatalf("packet %d = %+v data=%x, want %+v data=%x", i, got, got.Data, wantPackets[i], wantPackets[i].Data)
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestMuxerCloseRejectsMissingAV1CodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecAV1,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing AV1 private data", buffer.Len())
	}
}

func TestAVCDecoderConfigurationRecordValidation(t *testing.T) {
	config, err := parseAVCDecoderConfigurationRecord(h264AVCDecoderConfig())
	if err != nil {
		t.Fatal(err)
	}
	if config.ProfileIDC != 0x42 || config.ProfileCompatibility != 0x00 || config.LevelIDC != 0x0a ||
		config.NALULengthSize != 4 || config.SPSCount != 1 || config.PPSCount != 1 {
		t.Fatalf("config = %+v", config)
	}

	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: []byte{0x01, 0x42, 0x00, 0x0a, 0xff, 0xe1}},
		{name: "bad version", private: h264AVCDecoderConfigWithByte(0, 0x00)},
		{name: "bad reserved length bits", private: h264AVCDecoderConfigWithByte(4, 0x03)},
		{name: "invalid length size", private: h264AVCDecoderConfigWithByte(4, 0xfe)},
		{name: "no sps", private: h264AVCDecoderConfigWithByte(5, 0xe0)},
		{name: "wrong sps type", private: h264AVCDecoderConfigWithByte(8, 0x68)},
		{name: "missing pps", private: h264AVCDecoderConfig()[:15]},
		{name: "wrong pps type", private: h264AVCDecoderConfigWithByte(18, 0x67)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseAVCDecoderConfigurationRecord(tt.private); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidH264CodecPrivate(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH264,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: []byte{0x01, 0x64, 0x00, 0x1f},
	})
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
}

func TestDemuxerRejectsInvalidH264CodecPrivate(t *testing.T) {
	data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
		return writeTracksWithH264Private(writer, []byte{0x01, 0x64, 0x00, 0x1f})
	})
	if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestH264AVCDecoderConfigurationRecordFromAnnexB(t *testing.T) {
	private, err := h264AVCDecoderConfigurationRecordFromAnnexBFrames([][]byte{h264AnnexBParameterAccessUnit()})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(private, h264AVCDecoderConfig()) {
		t.Fatalf("private = %v, want %v", private, h264AVCDecoderConfig())
	}
	if _, err := parseAVCDecoderConfigurationRecord(private); err != nil {
		t.Fatal(err)
	}

	_, err = h264AVCDecoderConfigurationRecordFromAnnexBFrames([][]byte{h264AnnexBAccessUnit()})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerGeneratesH264CodecPrivateFromFirstAnnexBPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH264,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	annexB := h264AnnexBParameterAccessUnit()
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: annexB}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	avc := h264AVCSampleWithParameterSetsLengthSize4()
	if !bytes.Contains(buffer.Bytes(), h264AVCDecoderConfig()) {
		t.Fatalf("muxed data does not contain generated AVC config")
	}
	if !bytes.Contains(buffer.Bytes(), avc) {
		t.Fatalf("muxed data does not contain AVC sample %v", avc)
	}
	if bytes.Contains(buffer.Bytes(), annexB) {
		t.Fatalf("muxed data still contains Annex B access unit")
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, h264AVCDecoderConfig()) {
		t.Fatalf("tracks = %+v", tracks)
	}
	packet := Packet{Data: make([]byte, 0, len(annexB))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != trackID || packet.TimeNS != 0 || !packet.Keyframe || !bytes.Equal(packet.Data, annexB) {
		t.Fatalf("packet = %+v data=%v, want Annex B %v", packet, packet.Data, annexB)
	}
}

func TestMuxerGeneratesH264CodecPrivateFromFirstLacedAnnexBPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH264,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{h264AnnexBParameterAccessUnit(), h264AnnexBInterFrame()}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   frames,
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
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, h264AVCDecoderConfig()) {
		t.Fatalf("tracks = %+v", tracks)
	}
	packet := Packet{Data: make([]byte, 0, len(frames[0]))}
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("frame %d read: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, frames[i])
		}
	}
}

func TestMuxerDefersHeaderWhenH264CodecPrivateIsMissing(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH264,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	audioTrack, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = muxer.WritePacket(Packet{TrackID: audioTrack, TimeNS: 0, Keyframe: true, Data: []byte{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before H.264 private data was available", buffer.Len())
	}
	err = muxer.Close()
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing H.264 private data", buffer.Len())
	}
}

func TestMuxerCloseRejectsMissingH264CodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH264,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing H.264 private data", buffer.Len())
	}
}

func TestMuxerWritesH264AnnexBPacketsAsAVCSamples(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH264,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: h264AVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	annexB := h264AnnexBAccessUnit()
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: annexB}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	avc := h264AVCSampleWithLengthSize2()
	if !bytes.Contains(buffer.Bytes(), avc) {
		t.Fatalf("muxed data does not contain AVC sample %v", avc)
	}
	if bytes.Contains(buffer.Bytes(), annexB) {
		t.Fatalf("muxed data still contains Annex B access unit")
	}

	small, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(avc))}
	if err := small.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet.Data = make([]byte, 0, len(annexB))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != trackID || packet.TimeNS != 0 || !packet.Keyframe || !bytes.Equal(packet.Data, annexB) {
		t.Fatalf("packet = %+v data=%v, want Annex B %v", packet, packet.Data, annexB)
	}
}

func TestMuxerWritesLacedH264AnnexBFramesAsAVCSamples(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH264,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
		CodecPrivate:      h264AVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{h264AnnexBAccessUnit(), h264AnnexBInterFrame()}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   frames,
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	avc := append(h264AVCSampleWithLengthSize2(), h264AVCInterFrameWithLengthSize2()...)
	if !bytes.Contains(buffer.Bytes(), avc) {
		t.Fatalf("muxed data does not contain laced AVC samples %v", avc)
	}
	for i := range frames {
		if bytes.Contains(buffer.Bytes(), frames[i]) {
			t.Fatalf("muxed data still contains Annex B frame %d", i)
		}
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 16)}
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("frame %d read: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, frames[i])
		}
	}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestHEVCDecoderConfigurationRecordValidation(t *testing.T) {
	config, err := parseHEVCDecoderConfigurationRecord(h265HEVCDecoderConfig())
	if err != nil {
		t.Fatal(err)
	}
	if config.GeneralProfileSpace != 0 || config.GeneralTierFlag ||
		config.GeneralProfileIDC != 1 || config.GeneralProfileCompatibilityFlags != 0x60000000 ||
		config.GeneralConstraintIndicatorFlags != 0 || config.GeneralLevelIDC != 120 ||
		config.ChromaFormat != 1 || config.NALULengthSize != 4 ||
		config.VPSCount != 1 || config.SPSCount != 1 || config.PPSCount != 1 ||
		config.NumTemporalLayers != 1 || !config.TemporalIDNested {
		t.Fatalf("config = %+v", config)
	}

	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: []byte{0x01, 0x01}},
		{name: "bad version", private: h265HEVCDecoderConfigWithByte(0, 0x00)},
		{name: "bad min spatial reserved", private: h265HEVCDecoderConfigWithByte(13, 0x0f)},
		{name: "bad parallelism reserved", private: h265HEVCDecoderConfigWithByte(15, 0x03)},
		{name: "bad chroma reserved", private: h265HEVCDecoderConfigWithByte(16, 0x03)},
		{name: "bad luma reserved", private: h265HEVCDecoderConfigWithByte(17, 0x07)},
		{name: "bad chroma depth reserved", private: h265HEVCDecoderConfigWithByte(18, 0x07)},
		{name: "invalid length size", private: h265HEVCDecoderConfigWithLengthSize(3)},
		{name: "no arrays", private: h265HEVCDecoderConfigWithByte(22, 0)},
		{name: "bad array reserved", private: h265HEVCDecoderConfigWithByte(23, 0xe0)},
		{name: "wrong nalu type", private: h265HEVCDecoderConfigWithByte(28, 0x42)},
		{name: "missing pps", private: h265HEVCDecoderConfig()[:len(h265HEVCDecoderConfig())-len(h265PPS())-2]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseHEVCDecoderConfigurationRecord(tt.private); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidH265CodecPrivate(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH265,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: []byte{0x01, 0x01},
	})
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
}

func TestDemuxerRejectsInvalidH265CodecPrivate(t *testing.T) {
	data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
		return writeTracksWithH265Private(writer, []byte{0x01, 0x01})
	})
	if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestH265HEVCDecoderConfigurationRecordFromAnnexB(t *testing.T) {
	private, err := h265HEVCDecoderConfigurationRecordFromAnnexBFrames([][]byte{h265AnnexBParameterAccessUnit()})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(private, h265HEVCDecoderConfig()) {
		t.Fatalf("private = %v, want %v", private, h265HEVCDecoderConfig())
	}
	if _, err := parseHEVCDecoderConfigurationRecord(private); err != nil {
		t.Fatal(err)
	}

	_, err = h265HEVCDecoderConfigurationRecordFromAnnexBFrames([][]byte{h265AnnexBAccessUnit()})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerGeneratesH265CodecPrivateFromFirstAnnexBPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH265,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	annexB := h265AnnexBParameterAccessUnit()
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: annexB}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	hvcc := h265LengthPrefixedParameterSample4()
	if !bytes.Contains(buffer.Bytes(), h265HEVCDecoderConfig()) {
		t.Fatalf("muxed data does not contain generated HEVC config")
	}
	if !bytes.Contains(buffer.Bytes(), hvcc) {
		t.Fatalf("muxed data does not contain HEVC sample %v", hvcc)
	}
	if bytes.Contains(buffer.Bytes(), annexB) {
		t.Fatalf("muxed data still contains Annex B access unit")
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, h265HEVCDecoderConfig()) {
		t.Fatalf("tracks = %+v", tracks)
	}
	packet := Packet{Data: make([]byte, 0, len(annexB))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != trackID || packet.TimeNS != 0 || !packet.Keyframe || !bytes.Equal(packet.Data, annexB) {
		t.Fatalf("packet = %+v data=%v, want Annex B %v", packet, packet.Data, annexB)
	}
}

func TestMuxerGeneratesH265CodecPrivateFromFirstLacedAnnexBPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH265,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{h265AnnexBParameterAccessUnit(), h265AnnexBInterFrame()}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   frames,
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
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, h265HEVCDecoderConfig()) {
		t.Fatalf("tracks = %+v", tracks)
	}
	packet := Packet{Data: make([]byte, 0, len(frames[0]))}
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("frame %d read: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, frames[i])
		}
	}
}

func TestMuxerDefersHeaderWhenH265CodecPrivateIsMissing(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH265,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	audioTrack, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = muxer.WritePacket(Packet{TrackID: audioTrack, TimeNS: 0, Keyframe: true, Data: []byte{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before H.265 private data was available", buffer.Len())
	}
	err = muxer.Close()
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing H.265 private data", buffer.Len())
	}
}

func TestMuxerCloseRejectsMissingH265CodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH265,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing H.265 private data", buffer.Len())
	}
}

func TestMuxerWritesH265AnnexBPacketsAsHEVCSamples(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH265,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: h265HEVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	annexB := h265AnnexBAccessUnit()
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: annexB}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	hvcc := h265LengthPrefixedSample2()
	if !bytes.Contains(buffer.Bytes(), hvcc) {
		t.Fatalf("muxed data does not contain HEVC sample %v", hvcc)
	}
	if bytes.Contains(buffer.Bytes(), annexB) {
		t.Fatalf("muxed data still contains Annex B access unit")
	}

	small, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(hvcc))}
	if err := small.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet.Data = make([]byte, 0, len(annexB))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != trackID || packet.TimeNS != 0 || !packet.Keyframe || !bytes.Equal(packet.Data, annexB) {
		t.Fatalf("packet = %+v data=%v, want Annex B %v", packet, packet.Data, annexB)
	}
}

func TestMuxerWritesLacedH265AnnexBFramesAsHEVCSamples(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH265,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
		CodecPrivate:      h265HEVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{h265AnnexBAccessUnit(), h265AnnexBInterFrame()}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   frames,
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	hvcc := append(h265LengthPrefixedSample2(), h265LengthPrefixedInterFrame2()...)
	if !bytes.Contains(buffer.Bytes(), hvcc) {
		t.Fatalf("muxed data does not contain laced HEVC samples %v", hvcc)
	}
	for i := range frames {
		if bytes.Contains(buffer.Bytes(), frames[i]) {
			t.Fatalf("muxed data still contains Annex B frame %d", i)
		}
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 32)}
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("frame %d read: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, frames[i])
		}
	}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
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

func TestSeekableMuxerPatchesSegmentClusterAndDuration(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{})
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
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     40_000_000,
		DurationNS: 20_000_000,
		Keyframe:   true,
		Data:       []byte{1, 2, 3},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := ebmlReader(t, ws.bytes)
	header, err := reader.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	if header.ID != idEBML {
		t.Fatalf("first id = 0x%x, want EBML", uint64(header.ID))
	}
	if err := reader.Skip(header.Size.Value); err != nil {
		t.Fatal(err)
	}
	segment, err := reader.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	if segment.ID != idSegment || segment.Size.Unknown {
		t.Fatalf("segment = %+v, want known-size segment", segment)
	}
	var sawKnownCluster bool
	var sawPatchedDuration bool
	segmentEnd := segment.DataOffset + int64(segment.Size.Value)
	for reader.Offset() < segmentEnd {
		child, err := reader.ReadHeader()
		if err != nil {
			t.Fatal(err)
		}
		if child.ID == idCluster {
			if child.Size.Unknown {
				t.Fatalf("cluster size is unknown in seekable mode")
			}
			sawKnownCluster = true
			break
		}
		if child.ID == idInfo {
			duration, ok := readInfoDuration(t, ws.bytes[child.DataOffset:child.DataOffset+int64(child.Size.Value)])
			if !ok {
				t.Fatalf("seekable Info did not contain Duration")
			}
			if duration != 60 {
				t.Fatalf("duration = %f, want 60 timestamp-scale ticks", duration)
			}
			sawPatchedDuration = true
		}
		if child.Size.Unknown {
			t.Fatalf("unexpected unknown child before cluster: %+v", child)
		}
		if err := reader.Skip(child.Size.Value); err != nil {
			t.Fatal(err)
		}
	}
	if !sawKnownCluster {
		t.Fatalf("did not find known-size cluster")
	}
	if !sawPatchedDuration {
		t.Fatalf("did not find patched duration")
	}
	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	info := demuxer.Info()
	if !info.DurationSet || info.DurationNS != 60_000_000 {
		t.Fatalf("info duration = %d set=%v, want 60000000 true", info.DurationNS, info.DurationSet)
	}
}

func TestSeekableMuxerWritesSeekHeadAndCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 1_000_000})
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 2_000_000, Keyframe: true, Data: []byte{2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	positions := collectTopLevelPositions(t, ws.bytes)
	for _, id := range []ebml.ID{idSeekHead, idInfo, idTracks, idCluster, idCues} {
		if _, ok := positions[id]; !ok {
			t.Fatalf("missing top-level element 0x%x in positions %+v", uint64(id), positions)
		}
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entries := demuxer.SeekEntries()
	assertSeekEntry(t, entries, idInfo, positions[idInfo])
	assertSeekEntry(t, entries, idTracks, positions[idTracks])
	assertSeekEntry(t, entries, idCues, positions[idCues])

	got := Packet{Data: make([]byte, 0, 8)}
	for i := range packets {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatal(err)
		}
		if got.TimeNS != packets[i].TimeNS || !bytes.Equal(got.Data, packets[i].Data) {
			t.Fatalf("packet %d = %+v data=%v", i, got, got.Data)
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
	cues := demuxer.Cues()
	if len(cues) != len(packets) {
		t.Fatalf("cues = %+v, want %d cues", cues, len(packets))
	}
	for i := range cues {
		if cues[i].TrackID != trackID || cues[i].TimeNS != packets[i].TimeNS {
			t.Fatalf("cue %d = %+v, want track=%d time=%d", i, cues[i], trackID, packets[i].TimeNS)
		}
		if cues[i].ClusterPosition == 0 {
			t.Fatalf("cue %d cluster position = 0, positions=%+v", i, positions)
		}
		if !cues[i].RelativePositionSet {
			t.Fatalf("cue %d missing relative position: %+v", i, cues[i])
		}
	}
}

func TestSeekableMuxerCuesAudioPacketsWithoutKeyframe(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 1_000_000})
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 20_000_000, DurationNS: 20_000_000, Data: []byte{2}},
		{TrackID: trackID, TimeNS: 40_000_000, DurationNS: 20_000_000, Data: []byte{3}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(30_000_000); err != nil {
		t.Fatal(err)
	}
	cues := demuxer.Cues()
	if len(cues) != len(packets) {
		t.Fatalf("cues = %+v, want %d audio cues", cues, len(packets))
	}
	for i := range cues {
		if cues[i].TrackID != trackID || cues[i].TimeNS != packets[i].TimeNS || cues[i].DurationNS != packets[i].DurationNS {
			t.Fatalf("cue %d = %+v, want track=%d time=%d duration=%d", i, cues[i], trackID, packets[i].TimeNS, packets[i].DurationNS)
		}
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[1].TimeNS || !bytes.Equal(got.Data, packets[1].Data) {
		t.Fatalf("packet after seek = %+v data=%v, want %+v data=%v", got, got.Data, packets[1], packets[1].Data)
	}
	if err := demuxer.ReadPacketAtTime(30_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[2].TimeNS || !bytes.Equal(got.Data, packets[2].Data) {
		t.Fatalf("packet at time = %+v data=%v, want %+v data=%v", got, got.Data, packets[2], packets[2].Data)
	}
}

func TestSeekableMuxerCoalescesSameTimeCueTrackPositions(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 40_000_000})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: audioID, TimeNS: 0, DurationNS: 20_000_000, Data: []byte{1}},
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{2}},
		{TrackID: audioID, TimeNS: 20_000_000, DurationNS: 20_000_000, Data: []byte{3}},
		{TrackID: videoID, TimeNS: 20_000_000, Keyframe: true, Data: []byte{4}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		t.Fatal(err)
	}
	cues := demuxer.Cues()
	if len(cues) != 2 {
		t.Fatalf("cues = %+v, want 2 coalesced cue points", cues)
	}
	for i := range cues {
		if cues[i].TimeNS != int64(i)*20_000_000 {
			t.Fatalf("cue %d time = %d, want %d", i, cues[i].TimeNS, int64(i)*20_000_000)
		}
		if len(cues[i].Positions) != 2 {
			t.Fatalf("cue %d positions = %+v, want audio and video positions", i, cues[i].Positions)
		}
		audioPosition, ok := cuePositionForTrack(cues[i], audioID)
		if !ok {
			t.Fatalf("cue %d missing audio position: %+v", i, cues[i])
		}
		if !audioPosition.DurationSet || audioPosition.DurationNS != 20_000_000 {
			t.Fatalf("cue %d audio duration = %d set=%v, want 20000000 true", i, audioPosition.DurationNS, audioPosition.DurationSet)
		}
		if _, ok := cuePositionForTrack(cues[i], videoID); !ok {
			t.Fatalf("cue %d missing video position: %+v", i, cues[i])
		}
	}

	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadCuedTrackPacketAtTime(videoID, 0, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != videoID || got.TimeNS != 0 || !bytes.Equal(got.Data, []byte{2}) {
		t.Fatalf("video packet = track %d time %d data %v, want track %d time 0 data [2]", got.TrackID, got.TimeNS, got.Data, videoID)
	}
	if err := demuxer.ReadCuedTrackPacketAtTime(audioID, 20_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != audioID || got.TimeNS != 20_000_000 || !bytes.Equal(got.Data, []byte{3}) {
		t.Fatalf("audio packet = track %d time %d data %v, want track %d time 20000000 data [3]", got.TrackID, got.TimeNS, got.Data, audioID)
	}
}

func TestSeekableMuxerWritesCuesSortedByTime(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 100_000_000})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: videoID, TimeNS: 40_000_000, Keyframe: true, Data: []byte{4}},
		{TrackID: audioID, TimeNS: 20_000_000, DurationNS: 20_000_000, Data: []byte{2}},
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{0}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		t.Fatal(err)
	}
	if !demuxer.cuesSorted {
		t.Fatalf("generated cues were parsed as unsorted: %+v", demuxer.Cues())
	}
	cues := demuxer.Cues()
	wantTimes := []int64{0, 20_000_000, 40_000_000}
	if len(cues) != len(wantTimes) {
		t.Fatalf("cues = %+v, want %d sorted cues", cues, len(wantTimes))
	}
	for i := range wantTimes {
		if cues[i].TimeNS != wantTimes[i] {
			t.Fatalf("cue %d time = %d, want %d; cues=%+v", i, cues[i].TimeNS, wantTimes[i], cues)
		}
	}

	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadCuedPacketAtTime(0, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != videoID || got.TimeNS != 0 || !bytes.Equal(got.Data, []byte{0}) {
		t.Fatalf("cued packet at 0 = track %d time %d data %v, want video 0 [0]", got.TrackID, got.TimeNS, got.Data)
	}
	if err := demuxer.ReadCuedTrackPacketAtTime(audioID, 0, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != audioID || got.TimeNS != 20_000_000 || !bytes.Equal(got.Data, []byte{2}) {
		t.Fatalf("audio cued packet = track %d time %d data %v, want audio 20000000 [2]", got.TrackID, got.TimeNS, got.Data)
	}
	if err := demuxer.ReadCuedPacketAtTime(30_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != videoID || got.TimeNS != 40_000_000 || !bytes.Equal(got.Data, []byte{4}) {
		t.Fatalf("cued packet at 30000000 = track %d time %d data %v, want video 40000000 [4]", got.TrackID, got.TimeNS, got.Data)
	}
}

func TestSeekableMuxerWritesCueReferencesForReferencedPackets(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{
		ClusterMaxDurationNS: 100_000_000,
		CuePolicy:            CuePolicyAllPackets,
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 20_000_000, DurationNS: 20_000_000, ReferenceBlockTimeNS: []int64{-20_000_000}, Data: []byte{2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		t.Fatal(err)
	}
	cues := demuxer.Cues()
	if len(cues) != 2 {
		t.Fatalf("cues = %+v, want keyframe and referenced packet cues", cues)
	}
	referenced, err := firstCuePosition(cues[0])
	if err != nil {
		t.Fatal(err)
	}
	position, ok := cuePositionForTrack(cues[1], trackID)
	if !ok {
		t.Fatalf("cue missing track position: %+v", cues[1])
	}
	if len(position.References) != 1 {
		t.Fatalf("cue references = %+v, want one reference", position.References)
	}
	reference := position.References[0]
	if reference.TimeNS != packets[0].TimeNS || reference.ClusterPosition != referenced.ClusterPosition ||
		reference.BlockNumber != referenced.BlockNumber || reference.BlockNumberSet != referenced.BlockNumberSet {
		t.Fatalf("cue reference = %+v, want time=%d cluster=%d block=%d set=%v", reference, packets[0].TimeNS, referenced.ClusterPosition, referenced.BlockNumber, referenced.BlockNumberSet)
	}
	if !equalCueReferences(cues[1].References, position.References) {
		t.Fatalf("legacy cue references = %+v, want %+v", cues[1].References, position.References)
	}
}

func TestSeekableMuxerWritesCueReferencesForReferencedLacedPackets(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{
		ClusterMaxDurationNS: 100_000_000,
		CuePolicy:            CuePolicyAllPackets,
	})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecVP8,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     0,
		DurationNS: 20_000_000,
		Keyframe:   true,
		Data:       []byte{1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:              trackID,
		TimeNS:               20_000_000,
		FrameDurationNS:      20_000_000,
		ReferenceBlockTimeNS: []int64{-20_000_000},
		Lacing:               LacingXiph,
		Frames:               [][]byte{{2}, {3}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		t.Fatal(err)
	}
	cues := demuxer.Cues()
	if len(cues) != 2 {
		t.Fatalf("cues = %+v, want keyframe and referenced laced cues", cues)
	}
	referenced, err := firstCuePosition(cues[0])
	if err != nil {
		t.Fatal(err)
	}
	position, ok := cuePositionForTrack(cues[1], trackID)
	if !ok {
		t.Fatalf("cue missing track position: %+v", cues[1])
	}
	if len(position.References) != 1 {
		t.Fatalf("laced cue references = %+v, want one reference", position.References)
	}
	reference := position.References[0]
	if reference.TimeNS != 0 || reference.ClusterPosition != referenced.ClusterPosition ||
		reference.BlockNumber != referenced.BlockNumber || reference.BlockNumberSet != referenced.BlockNumberSet {
		t.Fatalf("laced cue reference = %+v, want time=0 cluster=%d block=%d set=%v", reference, referenced.ClusterPosition, referenced.BlockNumber, referenced.BlockNumberSet)
	}
	if !position.DurationSet || position.DurationNS != 40_000_000 {
		t.Fatalf("laced cue duration = %d set=%v, want 40000000 true", position.DurationNS, position.DurationSet)
	}
}

func TestMuxerCuePolicyControlsIndexing(t *testing.T) {
	tests := []struct {
		name     string
		policy   CuePolicy
		wantCues int
	}{
		{name: "default skips nonkeyframe video", policy: CuePolicyDefault, wantCues: 0},
		{name: "keyframes skips nonkeyframe video", policy: CuePolicyKeyframes, wantCues: 0},
		{name: "all packets indexes nonkeyframe video", policy: CuePolicyAllPackets, wantCues: 2},
		{name: "none disables keyframe cues", policy: CuePolicyNone, wantCues: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &memoryWriteSeeker{}
			muxer, err := NewMuxer(ws, MuxerOptions{
				ClusterMaxDurationNS: 1_000_000,
				CuePolicy:            tt.policy,
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
			for _, packet := range []Packet{
				{TrackID: trackID, TimeNS: 0, Keyframe: tt.policy == CuePolicyNone, Data: []byte{1}},
				{TrackID: trackID, TimeNS: 20_000_000, Keyframe: false, Data: []byte{2}},
			} {
				if err := muxer.WritePacket(packet); err != nil {
					t.Fatal(err)
				}
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if tt.wantCues != 0 {
				if err := demuxer.SeekToTime(0); err != nil {
					t.Fatal(err)
				}
			}
			cues := demuxer.Cues()
			if len(cues) != tt.wantCues {
				t.Fatalf("cues = %+v, want %d", cues, tt.wantCues)
			}
		})
	}
}

func TestNewMuxerRejectsInvalidCuePolicy(t *testing.T) {
	if _, err := NewMuxer(discardWriter{}, MuxerOptions{CuePolicy: CuePolicy(99)}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerSeekToTimeUsesCueRelativePosition(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 20_000_000, DurationNS: 10_000_000, Keyframe: true, Data: []byte{2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(20_000_000); err != nil {
		t.Fatal(err)
	}
	cues := demuxer.Cues()
	if len(cues) != len(packets) {
		t.Fatalf("cues = %+v, want %d cues", cues, len(packets))
	}
	if cues[0].ClusterPosition != cues[1].ClusterPosition {
		t.Fatalf("cues are not in the same cluster: %+v", cues)
	}
	if !cues[1].RelativePositionSet || cues[1].RelativePosition <= cues[0].RelativePosition {
		t.Fatalf("cues did not preserve relative block positions: %+v", cues)
	}
	if cues[0].BlockNumber != 1 || !cues[0].BlockNumberSet ||
		cues[1].BlockNumber != 2 || !cues[1].BlockNumberSet {
		t.Fatalf("cues did not preserve cluster block numbers: %+v", cues)
	}
	if len(cues[1].Positions) != 1 || cues[1].Positions[0].BlockNumber != 2 {
		t.Fatalf("cue positions = %+v, want second block number", cues[1].Positions)
	}
	if !cues[1].DurationSet || cues[1].DurationNS != packets[1].DurationNS ||
		!cues[1].Positions[0].DurationSet || cues[1].Positions[0].DurationNS != packets[1].DurationNS {
		t.Fatalf("cue duration = %+v positions=%+v, want %d", cues[1], cues[1].Positions, packets[1].DurationNS)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[1].TimeNS || got.DurationNS != packets[1].DurationNS || !bytes.Equal(got.Data, packets[1].Data) {
		t.Fatalf("packet after seek = %+v data=%v, want %+v data=%v", got, got.Data, packets[1], packets[1].Data)
	}
}

func TestDemuxerSeekToTimeUsesCueBlockNumberWithoutRelativePosition(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 20_000_000, DurationNS: 10_000_000, Keyframe: true, Data: []byte{2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	muxer.cues = []CuePoint{{
		TimeNS: packets[1].TimeNS,
		Positions: []CueTrackPosition{{
			TrackID:         trackID,
			ClusterPosition: muxer.clusterPosition,
			DurationNS:      packets[1].DurationNS,
			DurationSet:     true,
			BlockNumber:     2,
			BlockNumberSet:  true,
		}},
	}}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(20_000_000); err != nil {
		t.Fatal(err)
	}
	cues := demuxer.Cues()
	if len(cues) != 1 || cues[0].RelativePositionSet || !cues[0].BlockNumberSet || cues[0].BlockNumber != 2 {
		t.Fatalf("cue = %+v, want block-number cue without relative position", cues)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[1].TimeNS || got.DurationNS != packets[1].DurationNS || !bytes.Equal(got.Data, packets[1].Data) {
		t.Fatalf("packet after seek = %+v data=%v, want %+v data=%v", got, got.Data, packets[1], packets[1].Data)
	}
}

func TestDemuxerRejectsCueBlockNumberPastCluster(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
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
	muxer.cues = []CuePoint{{
		TimeNS: 0,
		Positions: []CueTrackPosition{{
			TrackID:         trackID,
			ClusterPosition: muxer.clusterPosition,
			BlockNumber:     2,
			BlockNumberSet:  true,
		}},
	}}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerPreservesCueTrackPositionMetadata(t *testing.T) {
	want := CuePoint{
		TimeNS: 20_000_000,
		Positions: []CueTrackPosition{
			{
				TrackID:             1,
				ClusterPosition:     128,
				RelativePosition:    12,
				RelativePositionSet: true,
				DurationNS:          5_000_000,
				DurationSet:         true,
				BlockNumber:         3,
				BlockNumberSet:      true,
				CodecStatePosition:  64,
				CodecStateSet:       true,
				References: []CueReference{{
					TimeNS:             10_000_000,
					ClusterPosition:    96,
					BlockNumber:        2,
					BlockNumberSet:     true,
					CodecStatePosition: 32,
					CodecStateSet:      true,
				}},
			},
			{
				TrackID:             1,
				ClusterPosition:     256,
				RelativePosition:    24,
				RelativePositionSet: true,
				BlockNumber:         4,
				BlockNumberSet:      true,
			},
		},
	}
	applyCuePosition(&want, want.Positions[0])
	data := makeCueMetadataMatroskaData(t, func(writer *ebml.Writer) error {
		var cues bytes.Buffer
		cw := ebml.NewWriter(&cues)
		if err := writeCuePoint(cw, want, defaultTimecodeScaleNS); err != nil {
			return err
		}
		return writer.WriteElement(idCues, cues.Bytes())
	})
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cues := demuxer.Cues()
	if len(cues) != 1 {
		t.Fatalf("cues = %+v, want one cue", cues)
	}
	if !equalCuePoint(cues[0], want) {
		t.Fatalf("cue = %+v, want %+v", cues[0], want)
	}
	cues[0].Positions[0].References[0].TimeNS = 0
	fresh := demuxer.Cues()
	if fresh[0].Positions[0].References[0].TimeNS != 10_000_000 {
		t.Fatalf("cue references alias was not protected: %+v", fresh[0].Positions[0].References)
	}
}

func TestDemuxerCueForTime(t *testing.T) {
	sorted := Demuxer{
		cuesSorted: true,
		cues: []CuePoint{
			{TimeNS: 10_000_000, TrackID: 1},
			{TimeNS: 20_000_000, TrackID: 1},
			{TimeNS: 40_000_000, TrackID: 1},
		},
	}
	if got := sorted.cueForTime(5_000_000); got.TimeNS != 10_000_000 {
		t.Fatalf("early sorted cue time = %d, want 10000000", got.TimeNS)
	}
	if got := sorted.cueForTime(30_000_000); got.TimeNS != 20_000_000 {
		t.Fatalf("middle sorted cue time = %d, want 20000000", got.TimeNS)
	}
	if got := sorted.cueForTime(40_000_000); got.TimeNS != 40_000_000 {
		t.Fatalf("exact sorted cue time = %d, want 40000000", got.TimeNS)
	}

	unsorted := Demuxer{
		cuesSorted: false,
		cues: []CuePoint{
			{TimeNS: 20_000_000, TrackID: 1},
			{TimeNS: 10_000_000, TrackID: 1},
			{TimeNS: 40_000_000, TrackID: 1},
		},
	}
	if got := unsorted.cueForTime(30_000_000); got.TimeNS != 20_000_000 {
		t.Fatalf("unsorted fallback cue time = %d, want 20000000", got.TimeNS)
	}
	if got := unsorted.cueForTime(5_000_000); got.TimeNS != 10_000_000 {
		t.Fatalf("early unsorted fallback cue time = %d, want 10000000", got.TimeNS)
	}

	trackCues := Demuxer{
		cuesSorted: true,
		cues: []CuePoint{
			{
				TimeNS: 10_000_000,
				Positions: []CueTrackPosition{{
					TrackID:         1,
					ClusterPosition: 10,
				}},
			},
			{
				TimeNS: 20_000_000,
				Positions: []CueTrackPosition{{
					TrackID:         2,
					ClusterPosition: 20,
				}},
			},
			{
				TimeNS: 40_000_000,
				Positions: []CueTrackPosition{
					{TrackID: 1, ClusterPosition: 40},
					{TrackID: 2, ClusterPosition: 41},
				},
			},
		},
	}
	cue, position, ok := trackCues.cueForTrackTime(2, 30_000_000)
	if !ok || cue.TimeNS != 20_000_000 || position.ClusterPosition != 20 {
		t.Fatalf("track 2 cue at 30000000 = %+v position=%+v ok=%v, want cue 20000000 position 20", cue, position, ok)
	}
	cue, position, ok = trackCues.cueForTrackTime(2, 5_000_000)
	if !ok || cue.TimeNS != 20_000_000 || position.ClusterPosition != 20 {
		t.Fatalf("early track 2 cue = %+v position=%+v ok=%v, want first track 2 cue", cue, position, ok)
	}
	cue, position, ok = trackCues.cueForTrackTime(1, 50_000_000)
	if !ok || cue.TimeNS != 40_000_000 || position.ClusterPosition != 40 {
		t.Fatalf("late track 1 cue = %+v position=%+v ok=%v, want multi-position cue", cue, position, ok)
	}
	if _, _, ok := trackCues.cueForTrackTime(3, 50_000_000); ok {
		t.Fatalf("track 3 unexpectedly had a cue")
	}

	unsortedTrackCues := Demuxer{
		cuesSorted: false,
		cues: []CuePoint{
			{
				TimeNS: 40_000_000,
				Positions: []CueTrackPosition{{
					TrackID:         1,
					ClusterPosition: 40,
				}},
			},
			{
				TimeNS: 10_000_000,
				Positions: []CueTrackPosition{{
					TrackID:         2,
					ClusterPosition: 10,
				}},
			},
			{
				TimeNS: 20_000_000,
				Positions: []CueTrackPosition{{
					TrackID:         2,
					ClusterPosition: 20,
				}},
			},
			{
				TimeNS: 30_000_000,
				Positions: []CueTrackPosition{{
					TrackID:         1,
					ClusterPosition: 30,
				}},
			},
		},
	}
	cue, position, ok = unsortedTrackCues.cueForTrackTime(2, 25_000_000)
	if !ok || cue.TimeNS != 20_000_000 || position.ClusterPosition != 20 {
		t.Fatalf("unsorted track 2 cue = %+v position=%+v ok=%v, want cue 20000000 position 20", cue, position, ok)
	}
	cue, position, ok = unsortedTrackCues.cueForTrackTime(2, 5_000_000)
	if !ok || cue.TimeNS != 10_000_000 || position.ClusterPosition != 10 {
		t.Fatalf("early unsorted track 2 cue = %+v position=%+v ok=%v, want first track cue", cue, position, ok)
	}
	cue, position, ok = unsortedTrackCues.cueForTrackTime(1, 35_000_000)
	if !ok || cue.TimeNS != 30_000_000 || position.ClusterPosition != 30 {
		t.Fatalf("unsorted track 1 cue = %+v position=%+v ok=%v, want cue 30000000 position 30", cue, position, ok)
	}
}

func TestMuxerFailedPacketDoesNotAdvanceCuesOrDuration(t *testing.T) {
	ws := &failToggleWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{})
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
		TrackID:    trackID,
		TimeNS:     0,
		DurationNS: 10_000_000,
		Keyframe:   true,
		Data:       []byte{1},
	}); err != nil {
		t.Fatal(err)
	}
	if len(muxer.cues) != 1 || muxer.maxTimeNS != 10_000_000 {
		t.Fatalf("cues=%+v maxTimeNS=%d after first packet", muxer.cues, muxer.maxTimeNS)
	}
	ws.fail = true
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     20_000_000,
		DurationNS: 10_000_000,
		Keyframe:   true,
		Data:       []byte{2},
	}); !errors.Is(err, errFailingWriter) {
		t.Fatalf("err = %v, want errFailingWriter", err)
	}
	if len(muxer.cues) != 1 {
		t.Fatalf("cues = %+v, want only the successfully written packet", muxer.cues)
	}
	if muxer.maxTimeNS != 10_000_000 {
		t.Fatalf("maxTimeNS = %d, want 10000000", muxer.maxTimeNS)
	}
}

func TestDemuxerSeekToTimeUsesCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 1_000_000})
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 2_000_000, Keyframe: true, Data: []byte{2}},
		{TrackID: trackID, TimeNS: 4_000_000, Keyframe: true, Data: []byte{3}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(3_000_000); err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[1].TimeNS || !bytes.Equal(got.Data, packets[1].Data) {
		t.Fatalf("packet after seek = %+v data=%v, want %+v data=%v", got, got.Data, packets[1], packets[1].Data)
	}
}

func TestDemuxerReadPacketAtTimeUsesCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 1_000_000})
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 2_000_000, Keyframe: true, Data: []byte{2}},
		{TrackID: trackID, TimeNS: 4_000_000, Keyframe: true, Data: []byte{3}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacketAtTime(3_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[2].TimeNS || !bytes.Equal(got.Data, packets[2].Data) {
		t.Fatalf("packet at time = %+v data=%v, want %+v data=%v", got, got.Data, packets[2], packets[2].Data)
	}
}

func TestDemuxerReadPacketAtTimeUsesPacketIndexWithSparseCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 20_000_000, Data: []byte{2}},
		{TrackID: trackID, TimeNS: 40_000_000, Keyframe: true, Data: []byte{3}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacketAtTime(10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if !demuxer.packetIndexBuilt {
		t.Fatal("packet index was not built for sparse cue read")
	}
	if got.TimeNS != packets[1].TimeNS || !bytes.Equal(got.Data, packets[1].Data) {
		t.Fatalf("packet at sparse-cue time = %+v data=%v, want uncued packet %+v data=%v", got, got.Data, packets[1], packets[1].Data)
	}

	cued := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadCuedPacketAtTime(10_000_000, &cued); err != nil {
		t.Fatal(err)
	}
	if cued.TimeNS != packets[2].TimeNS || !bytes.Equal(cued.Data, packets[2].Data) {
		t.Fatalf("cued packet at sparse-cue time = %+v data=%v, want next cue %+v data=%v", cued, cued.Data, packets[2], packets[2].Data)
	}
}

func TestDemuxerSeekToTimeUsesClusterIndexWithoutCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{
		ClusterMaxDurationNS: 60_000_000,
		CuePolicy:            CuePolicyNone,
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 2_000_000, Keyframe: true, Data: []byte{2}},
		{TrackID: trackID, TimeNS: 4_000_000, Keyframe: true, Data: []byte{3}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(3_000_000); err != nil {
		t.Fatal(err)
	}
	if !demuxer.packetIndexBuilt {
		t.Fatal("packet index was not built for cue-free seek")
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[1].TimeNS || !bytes.Equal(got.Data, packets[1].Data) {
		t.Fatalf("packet after no-cue seek = %+v data=%v, want %+v data=%v", got, got.Data, packets[1], packets[1].Data)
	}
}

func TestDemuxerReadPacketAtTimeUsesClusterIndexWithoutCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{
		ClusterMaxDurationNS: 60_000_000,
		CuePolicy:            CuePolicyNone,
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 2_000_000, Keyframe: true, Data: []byte{2}},
		{TrackID: trackID, TimeNS: 4_000_000, Keyframe: true, Data: []byte{3}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacketAtTime(3_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if !demuxer.packetIndexBuilt {
		t.Fatal("packet index was not built for cue-free read")
	}
	if got.TimeNS != packets[2].TimeNS || !bytes.Equal(got.Data, packets[2].Data) {
		t.Fatalf("packet at time without cues = %+v data=%v, want %+v data=%v", got, got.Data, packets[2], packets[2].Data)
	}
}

func TestDemuxerReadTrackPacketAtTimeUsesClusterIndexWithoutTrackCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{
		ClusterMaxDurationNS: 60_000_000,
		CuePolicy:            CuePolicyNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: audioID, TimeNS: 0, DurationNS: 20_000_000, Data: []byte{0xa0}},
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{0xb0}},
		{TrackID: audioID, TimeNS: 20_000_000, DurationNS: 20_000_000, Data: []byte{0xa1}},
		{TrackID: videoID, TimeNS: 20_000_000, Data: []byte{0xb1}},
		{TrackID: audioID, TimeNS: 40_000_000, DurationNS: 20_000_000, Data: []byte{0xa2}},
		{TrackID: videoID, TimeNS: 40_000_000, Data: []byte{0xb2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.SeekToTrackTime(videoID, 30_000_000); err != nil {
		t.Fatal(err)
	}
	if !demuxer.packetIndexBuilt {
		t.Fatal("packet index was not built for cue-free track seek")
	}
	requirePacketTrackIndex(t, demuxer, audioID, []int64{0, 20_000_000, 40_000_000})
	requirePacketTrackIndex(t, demuxer, videoID, []int64{0, 20_000_000, 40_000_000})
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != videoID || got.TimeNS != 20_000_000 || !bytes.Equal(got.Data, []byte{0xb1}) {
		t.Fatalf("packet after cue-free video seek = %+v data=%v, want video at 20000000", got, got.Data)
	}
	if err := demuxer.ReadTrackPacketAtTime(audioID, 30_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != audioID || got.TimeNS != 40_000_000 || !bytes.Equal(got.Data, []byte{0xa2}) {
		t.Fatalf("audio packet at time without audio cues = %+v data=%v, want audio at 40000000", got, got.Data)
	}
	if err := demuxer.ReadTrackPacketAtTime(videoID, 30_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != videoID || got.TimeNS != 40_000_000 || !bytes.Equal(got.Data, []byte{0xb2}) {
		t.Fatalf("video packet at time from sparse cues = %+v data=%v, want video at 40000000", got, got.Data)
	}
}

func TestDemuxerReadTrackPacketAtTimeUsesPacketIndexWithSparseTrackCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: audioID, TimeNS: 0, DurationNS: 20_000_000, Data: []byte{0xa0}},
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{0xb0}},
		{TrackID: audioID, TimeNS: 20_000_000, DurationNS: 20_000_000, Data: []byte{0xa1}},
		{TrackID: videoID, TimeNS: 20_000_000, Data: []byte{0xb1}},
		{TrackID: audioID, TimeNS: 40_000_000, DurationNS: 20_000_000, Data: []byte{0xa2}},
		{TrackID: videoID, TimeNS: 40_000_000, Keyframe: true, Data: []byte{0xb2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadTrackPacketAtTime(videoID, 10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if !demuxer.packetIndexBuilt {
		t.Fatal("packet index was not built for sparse track cue read")
	}
	requirePacketTrackIndex(t, demuxer, audioID, []int64{0, 20_000_000, 40_000_000})
	requirePacketTrackIndex(t, demuxer, videoID, []int64{0, 20_000_000, 40_000_000})
	if got.TrackID != videoID || got.TimeNS != 20_000_000 || !bytes.Equal(got.Data, []byte{0xb1}) {
		t.Fatalf("video packet at sparse-track-cue time = %+v data=%v, want video at 20000000", got, got.Data)
	}

	if err := demuxer.ReadCuedTrackPacketAtTime(videoID, 10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != videoID || got.TimeNS != 40_000_000 || !bytes.Equal(got.Data, []byte{0xb2}) {
		t.Fatalf("video cued packet = %+v data=%v, want next video cue at 40000000", got, got.Data)
	}
}

func TestDemuxerReadPacketAtTimeFallsBackBeforeFirstCue(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{
		ClusterMaxDurationNS: 1_000_000,
		CuePolicy:            CuePolicyKeyframes,
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 20_000_000, Data: []byte{2}},
		{TrackID: trackID, TimeNS: 40_000_000, Keyframe: true, Data: []byte{3}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacketAtTime(10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[1].TimeNS || !bytes.Equal(got.Data, packets[1].Data) {
		t.Fatalf("packet before first cue = %+v data=%v, want %+v data=%v", got, got.Data, packets[1], packets[1].Data)
	}
}

func TestDemuxerReadCuedPacketAtTimeSkipsUncuedPackets(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 20_000_000, Data: []byte{2}},
		{TrackID: trackID, TimeNS: 40_000_000, Keyframe: true, Data: []byte{3}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	scanned := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacketAtTime(10_000_000, &scanned); err != nil {
		t.Fatal(err)
	}
	if scanned.TimeNS != packets[1].TimeNS || !bytes.Equal(scanned.Data, packets[1].Data) {
		t.Fatalf("packet at time = %+v data=%v, want uncued packet %+v data=%v", scanned, scanned.Data, packets[1], packets[1].Data)
	}

	cued := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadCuedPacketAtTime(10_000_000, &cued); err != nil {
		t.Fatal(err)
	}
	if cued.TimeNS != packets[2].TimeNS || !bytes.Equal(cued.Data, packets[2].Data) {
		t.Fatalf("cued packet at time = %+v data=%v, want next cue %+v data=%v", cued, cued.Data, packets[2], packets[2].Data)
	}
}

func TestDemuxerReadCuedTrackPacketAtTimeUsesTrackCue(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: audioID, TimeNS: 0, DurationNS: 20_000_000, Data: []byte{0xa0}},
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{0xb0}},
		{TrackID: audioID, TimeNS: 20_000_000, DurationNS: 20_000_000, Data: []byte{0xa1}},
		{TrackID: videoID, TimeNS: 20_000_000, Data: []byte{0xb1}},
		{TrackID: audioID, TimeNS: 40_000_000, DurationNS: 20_000_000, Data: []byte{0xa2}},
		{TrackID: videoID, TimeNS: 40_000_000, Keyframe: true, Data: []byte{0xb2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadCuedTrackPacketAtTime(audioID, 10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != audioID || got.TimeNS != 20_000_000 || !bytes.Equal(got.Data, []byte{0xa1}) {
		t.Fatalf("audio cued packet = %+v data=%v, want audio at 20000000", got, got.Data)
	}
	if err := demuxer.ReadCuedTrackPacketAtTime(videoID, 10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != videoID || got.TimeNS != 40_000_000 || !bytes.Equal(got.Data, []byte{0xb2}) {
		t.Fatalf("video cued packet = %+v data=%v, want video cue at 40000000", got, got.Data)
	}
}

func TestDemuxerReadCuedPacketAtTimeResolvesClusterOnlyCue(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 20_000_000, Data: []byte{2}},
		{TrackID: trackID, TimeNS: 40_000_000, Keyframe: true, Data: []byte{3}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	muxer.cues = []CuePoint{{
		TrackID:         trackID,
		TimeNS:          packets[2].TimeNS,
		ClusterPosition: muxer.clusterPosition,
		Positions: []CueTrackPosition{{
			TrackID:         trackID,
			ClusterPosition: muxer.clusterPosition,
		}},
	}}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadCuedPacketAtTime(10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[2].TimeNS || !bytes.Equal(got.Data, packets[2].Data) {
		t.Fatalf("cued packet = %+v data=%v, want %+v data=%v", got, got.Data, packets[2], packets[2].Data)
	}
}

func TestDemuxerReadCuedTrackPacketAtTimeResolvesClusterOnlyCue(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: audioID, TimeNS: 0, DurationNS: 20_000_000, Data: []byte{0xa0}},
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{0xb0}},
		{TrackID: audioID, TimeNS: 20_000_000, DurationNS: 20_000_000, Data: []byte{0xa1}},
		{TrackID: videoID, TimeNS: 20_000_000, Data: []byte{0xb1}},
		{TrackID: audioID, TimeNS: 40_000_000, DurationNS: 20_000_000, Data: []byte{0xa2}},
		{TrackID: videoID, TimeNS: 40_000_000, Keyframe: true, Data: []byte{0xb2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	muxer.cues = []CuePoint{
		{
			TrackID:         audioID,
			TimeNS:          20_000_000,
			ClusterPosition: muxer.clusterPosition,
			Positions: []CueTrackPosition{{
				TrackID:         audioID,
				ClusterPosition: muxer.clusterPosition,
			}},
		},
		{
			TrackID:         videoID,
			TimeNS:          40_000_000,
			ClusterPosition: muxer.clusterPosition,
			Positions: []CueTrackPosition{{
				TrackID:         videoID,
				ClusterPosition: muxer.clusterPosition,
			}},
		},
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadCuedTrackPacketAtTime(audioID, 10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != audioID || got.TimeNS != 20_000_000 || !bytes.Equal(got.Data, []byte{0xa1}) {
		t.Fatalf("audio cued packet = %+v data=%v, want audio at 20000000", got, got.Data)
	}
	if err := demuxer.ReadCuedTrackPacketAtTime(videoID, 10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != videoID || got.TimeNS != 40_000_000 || !bytes.Equal(got.Data, []byte{0xb2}) {
		t.Fatalf("video cued packet = %+v data=%v, want video at 40000000", got, got.Data)
	}
}

func TestDemuxerReadCuedPacketAtTimeResolvesClusterOnlyLacedCue(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
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
		TrackID: trackID,
		TimeNS:  0,
		Frames:  [][]byte{{0xa0}, {0xa1}, {0xa2}},
	}); err != nil {
		t.Fatal(err)
	}
	muxer.cues = []CuePoint{{
		TrackID:         trackID,
		TimeNS:          20_000_000,
		ClusterPosition: muxer.clusterPosition,
		Positions: []CueTrackPosition{{
			TrackID:         trackID,
			ClusterPosition: muxer.clusterPosition,
		}},
	}}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadCuedPacketAtTime(10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != trackID || got.TimeNS != 20_000_000 || got.DurationNS != 20_000_000 || !bytes.Equal(got.Data, []byte{0xa1}) {
		t.Fatalf("laced cued packet = %+v data=%v, want second frame at 20000000", got, got.Data)
	}
}

func TestDemuxerReadCuedPacketAtTimeRejectsInvalidInputs(t *testing.T) {
	data := makeMatroskaData(t, 1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.ReadCuedPacketAtTime(0, nil); !errors.Is(err, ErrNilPacket) {
		t.Fatalf("err = %v, want ErrNilPacket", err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadCuedPacketAtTime(-1, &packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
	if err := demuxer.ReadCuedTrackPacketAtTime(0, 0, &packet); !errors.Is(err, ErrUnknownTrack) {
		t.Fatalf("err = %v, want ErrUnknownTrack", err)
	}
	if err := demuxer.ReadCuedTrackPacketAtTime(99, 0, &packet); !errors.Is(err, ErrUnknownTrack) {
		t.Fatalf("err = %v, want ErrUnknownTrack", err)
	}
	if err := demuxer.ReadCuedTrackPacketAtTime(1, -1, &packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerSeekToTrackTimeUsesTrackCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: audioID, TimeNS: 0, DurationNS: 20_000_000, Data: []byte{0xa0}},
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{0xb0}},
		{TrackID: audioID, TimeNS: 20_000_000, DurationNS: 20_000_000, Data: []byte{0xa1}},
		{TrackID: videoID, TimeNS: 20_000_000, Data: []byte{0xb1}},
		{TrackID: audioID, TimeNS: 40_000_000, DurationNS: 20_000_000, Data: []byte{0xa2}},
		{TrackID: videoID, TimeNS: 40_000_000, Keyframe: true, Data: []byte{0xb2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTrackTime(videoID, 30_000_000); err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != videoID || got.TimeNS != 0 || !bytes.Equal(got.Data, []byte{0xb0}) {
		t.Fatalf("packet after video seek = %+v data=%v, want first video keyframe", got, got.Data)
	}
	if err := demuxer.SeekToTrackTime(audioID, 30_000_000); err != nil {
		t.Fatal(err)
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != audioID || got.TimeNS != 20_000_000 || !bytes.Equal(got.Data, []byte{0xa1}) {
		t.Fatalf("packet after audio seek = %+v data=%v, want preceding audio cue", got, got.Data)
	}
	if err := demuxer.ReadTrackPacketAtTime(videoID, 30_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != videoID || got.TimeNS != 40_000_000 || !bytes.Equal(got.Data, []byte{0xb2}) {
		t.Fatalf("video packet at time = %+v data=%v, want video at 40000000", got, got.Data)
	}
	if err := demuxer.ReadTrackPacketAtTime(audioID, 30_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != audioID || got.TimeNS != 40_000_000 || !bytes.Equal(got.Data, []byte{0xa2}) {
		t.Fatalf("audio packet at time = %+v data=%v, want audio at 40000000", got, got.Data)
	}
}

func TestDemuxerReadTrackPacketAtTimeRejectsInvalidInputs(t *testing.T) {
	data := makeMatroskaData(t, 1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.ReadTrackPacketAtTime(1, 0, nil); !errors.Is(err, ErrNilPacket) {
		t.Fatalf("err = %v, want ErrNilPacket", err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadTrackPacketAtTime(0, 0, &packet); !errors.Is(err, ErrUnknownTrack) {
		t.Fatalf("err = %v, want ErrUnknownTrack", err)
	}
	if err := demuxer.ReadTrackPacketAtTime(99, 0, &packet); !errors.Is(err, ErrUnknownTrack) {
		t.Fatalf("err = %v, want ErrUnknownTrack", err)
	}
	if err := demuxer.ReadTrackPacketAtTime(1, -1, &packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerReadPacketAtTimeFindsLacedFrame(t *testing.T) {
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
	frames := [][]byte{{1}, {2}, {3}}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   frames,
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacketAtTime(20_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != 20_000_000 || got.DurationNS != 20_000_000 || !bytes.Equal(got.Data, frames[1]) {
		t.Fatalf("packet at time = %+v data=%v, want second laced frame", got, got.Data)
	}
}

func TestDemuxerReadPacketAtTimeFindsLacedFrameWithoutCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{CuePolicy: CuePolicyNone})
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
	frames := [][]byte{{1}, {2}, {3}}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   frames,
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.SeekToTime(20_000_000); err != nil {
		t.Fatal(err)
	}
	if !demuxer.packetIndexBuilt {
		t.Fatal("packet index was not built for cue-free laced seek")
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != 20_000_000 || got.DurationNS != 20_000_000 || !bytes.Equal(got.Data, frames[1]) {
		t.Fatalf("packet after laced seek without cues = %+v data=%v, want second laced frame", got, got.Data)
	}
	demuxer, err = NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.ReadPacketAtTime(20_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != 20_000_000 || got.DurationNS != 20_000_000 || !bytes.Equal(got.Data, frames[1]) {
		t.Fatalf("packet at time without cues = %+v data=%v, want second laced frame", got, got.Data)
	}
}

func TestDemuxerReadTrackPacketAtTimeFindsLacedFrameWithoutCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{CuePolicy: CuePolicyNone})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{{1}, {2}, {3}}
	if err := muxer.WritePacket(Packet{
		TrackID:  videoID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0xb0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  audioID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   frames,
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  videoID,
		TimeNS:   40_000_000,
		Keyframe: true,
		Data:     []byte{0xb1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadTrackPacketAtTime(audioID, 20_000_000, &got); err != nil {
		t.Fatal(err)
	}
	requirePacketTrackIndex(t, demuxer, audioID, []int64{0, 20_000_000, 40_000_000})
	requirePacketTrackIndex(t, demuxer, videoID, []int64{0, 40_000_000})
	if got.TrackID != audioID || got.TimeNS != 20_000_000 || got.DurationNS != 20_000_000 || !bytes.Equal(got.Data, frames[1]) {
		t.Fatalf("track packet at time without cues = %+v data=%v, want second laced audio frame", got, got.Data)
	}
}

func TestDemuxerReadPacketAtTimeFindsLacedBlockGroupFrameWithoutCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{CuePolicy: CuePolicyNone})
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
	frames := [][]byte{{0xa0}, {0xa1}, {0xa2}}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:         trackID,
		TimeNS:          0,
		Keyframe:        true,
		Lacing:          LacingXiph,
		FrameDurationNS: 20_000_000,
		Frames:          frames,
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.SeekToTime(20_000_000); err != nil {
		t.Fatal(err)
	}
	if !demuxer.packetIndexBuilt {
		t.Fatal("packet index was not built for cue-free laced BlockGroup seek")
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != 20_000_000 || got.DurationNS != 20_000_000 || !got.Keyframe || !bytes.Equal(got.Data, frames[1]) {
		t.Fatalf("packet after laced BlockGroup seek without cues = %+v data=%v, want second frame", got, got.Data)
	}
}

func TestDemuxerReadPacketAtTimeRejectsNilPacket(t *testing.T) {
	data := makeMatroskaData(t, 1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.ReadPacketAtTime(0, nil); !errors.Is(err, ErrNilPacket) {
		t.Fatalf("err = %v, want ErrNilPacket", err)
	}
}

func TestDemuxerSeekToTimeClearsPendingLacedFrames(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 1_000_000})
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
	if err := muxer.writeHeader(); err != nil {
		t.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		t.Fatal(err)
	}
	if err := writeLacedSimpleBlock(muxer.ebml, trackID, simpleBlockLacingXiph, [][]byte{{1}, {2}, {3}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   2_000_000,
		Keyframe: true,
		Data:     []byte{9},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Data, []byte{1}) {
		t.Fatalf("first packet data = %v, want laced first frame", got.Data)
	}
	if err := demuxer.SeekToTime(2_000_000); err != nil {
		t.Fatal(err)
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != 2_000_000 || !bytes.Equal(got.Data, []byte{9}) {
		t.Fatalf("packet after seek = %+v data=%v, want time=2000000 data=[9]", got, got.Data)
	}
}

func TestDemuxerSeekToTimeRequiresSeekableReader(t *testing.T) {
	data := makeMatroskaData(t, 1)
	demuxer, err := NewDemuxer(bytes.NewBuffer(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); !errors.Is(err, ErrNonSeekableReader) {
		t.Fatalf("err = %v, want ErrNonSeekableReader", err)
	}
}

func TestDemuxerReadsLacedSimpleBlocks(t *testing.T) {
	tests := []struct {
		name   string
		lacing byte
		frames [][]byte
	}{
		{
			name:   "xiph",
			lacing: simpleBlockLacingXiph,
			frames: [][]byte{{1, 2}, {3, 4, 5}, {6}},
		},
		{
			name:   "fixed",
			lacing: simpleBlockLacingFixed,
			frames: [][]byte{{1, 2}, {3, 4}, {5, 6}},
		},
		{
			name:   "ebml",
			lacing: simpleBlockLacingEBML,
			frames: [][]byte{{1, 2}, {3, 4, 5}, {6}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeLacedMatroskaData(t, tt.lacing, tt.frames, 20_000_000)
			demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, 8)}
			for i := range tt.frames {
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatalf("read frame %d: %v", i, err)
				}
				if packet.TrackID != 1 || packet.TimeNS != int64(i)*20_000_000 ||
					packet.DurationNS != 20_000_000 || !packet.Keyframe ||
					!bytes.Equal(packet.Data, tt.frames[i]) {
					t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, tt.frames[i])
				}
			}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
				t.Fatalf("err = %v, want EOF", err)
			}
		})
	}
}

func TestDemuxerReadsSpecLacingFlags(t *testing.T) {
	const (
		specLacingXiph  = 0x02
		specLacingFixed = 0x04
		specLacingEBML  = 0x06
	)
	tests := []struct {
		name   string
		lacing byte
		frames [][]byte
	}{
		{
			name:   "xiph",
			lacing: specLacingXiph,
			frames: [][]byte{{1, 2}, {3, 4, 5}, {6}},
		},
		{
			name:   "fixed",
			lacing: specLacingFixed,
			frames: [][]byte{{1, 2}, {3, 4}, {5, 6}},
		},
		{
			name:   "ebml",
			lacing: specLacingEBML,
			frames: [][]byte{{1, 2}, {3, 4, 5}, {6}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeSpecLacedMatroskaData(t, tt.lacing, tt.frames, 20_000_000)
			demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, 8)}
			for i := range tt.frames {
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatalf("read frame %d: %v", i, err)
				}
				if packet.TrackID != 1 || packet.TimeNS != int64(i)*20_000_000 ||
					packet.DurationNS != 20_000_000 || !packet.Keyframe ||
					!bytes.Equal(packet.Data, tt.frames[i]) {
					t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, tt.frames[i])
				}
			}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
				t.Fatalf("err = %v, want EOF", err)
			}
		})
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

func TestDemuxerPreservesLacedBlockGroupMetadataAcrossFrames(t *testing.T) {
	frames := [][]byte{{1, 2}, {3}, {4, 5, 6}}
	data := makeLacedBlockGroupMatroskaData(t, simpleBlockLacingEBML, frames)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantAdditions := []BlockAddition{{ID: 2, Data: []byte{0xaa, 0xbb}}}
	packet := Packet{
		Data:                 make([]byte, 0, 16),
		ReferenceBlockTimeNS: make([]int64, 0, 1),
		CodecState:           make([]byte, 0, 4),
		BlockAdditions:       make([]BlockAddition, 0, 1),
	}
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if packet.TrackID != 1 || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || packet.Keyframe || !packet.Invisible ||
			packet.ReferencePriority != 2 || packet.DiscardPaddingNS != -3_000_000 ||
			!equalInt64s(packet.ReferenceBlockTimeNS, []int64{-20_000_000}) ||
			!bytes.Equal(packet.CodecState, []byte{0xcc, 0xdd}) ||
			!equalBlockAdditions(packet.BlockAdditions, wantAdditions) ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%v", i, packet, packet.Data)
		}
		if i == 0 {
			packet.ReferenceBlockTimeNS[0] = 0
			packet.CodecState[0] = 0
			packet.BlockAdditions[0].Data[0] = 0
		}
	}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
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

func TestDemuxerRejectsLaceFrameCountOverLimit(t *testing.T) {
	data := makeLacedMatroskaData(t, simpleBlockLacingFixed, [][]byte{{1}, {2}, {3}}, 20_000_000)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{MaxLaceFrames: 2})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrLaceTooLarge) {
		t.Fatalf("err = %v, want ErrLaceTooLarge", err)
	}
}

func TestDemuxerRejectsOversizedTrackIDs(t *testing.T) {
	oversized := maxTrackID + 1
	t.Run("track entry", func(t *testing.T) {
		data := makeTrackNumberMatroskaData(t, oversized)
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("cue track", func(t *testing.T) {
		data := makeCueTrackNumberMatroskaData(t, oversized)
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("block track", func(t *testing.T) {
		data := makeBlockTrackNumberMatroskaData(t, oversized)
		demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		packet := Packet{Data: make([]byte, 0, 8)}
		if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
}

func TestDemuxerRejectsInvalidCueMetadata(t *testing.T) {
	tests := []struct {
		name     string
		writeCue func(*ebml.Writer) error
	}{
		{
			name: "missing cue time",
			writeCue: func(w *ebml.Writer) error {
				var point bytes.Buffer
				pw := ebml.NewWriter(&point)
				if err := pw.WriteElement(idCueTrackPositions, cueTrackPositionsPayload(t)); err != nil {
					return err
				}
				return w.WriteElement(idCuePoint, point.Bytes())
			},
		},
		{
			name: "missing cue track positions",
			writeCue: func(w *ebml.Writer) error {
				var point bytes.Buffer
				pw := ebml.NewWriter(&point)
				if err := pw.WriteUInt(idCueTime, 0); err != nil {
					return err
				}
				return w.WriteElement(idCuePoint, point.Bytes())
			},
		},
		{
			name: "missing cue track",
			writeCue: func(w *ebml.Writer) error {
				var positions bytes.Buffer
				tw := ebml.NewWriter(&positions)
				if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
					return err
				}
				return writeCuePointWithPositionsPayload(w, positions.Bytes())
			},
		},
		{
			name: "missing cue cluster position",
			writeCue: func(w *ebml.Writer) error {
				var positions bytes.Buffer
				tw := ebml.NewWriter(&positions)
				if err := tw.WriteUInt(idCueTrack, 1); err != nil {
					return err
				}
				return writeCuePointWithPositionsPayload(w, positions.Bytes())
			},
		},
		{
			name: "zero cue block number",
			writeCue: func(w *ebml.Writer) error {
				var positions bytes.Buffer
				tw := ebml.NewWriter(&positions)
				if err := tw.WriteUInt(idCueTrack, 1); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueBlockNumber, 0); err != nil {
					return err
				}
				return writeCuePointWithPositionsPayload(w, positions.Bytes())
			},
		},
		{
			name: "cue reference missing time",
			writeCue: func(w *ebml.Writer) error {
				var reference bytes.Buffer
				rw := ebml.NewWriter(&reference)
				if err := rw.WriteUInt(idCueRefCluster, 0); err != nil {
					return err
				}
				return writeCuePointWithReferencePayload(w, reference.Bytes())
			},
		},
		{
			name: "cue reference missing cluster",
			writeCue: func(w *ebml.Writer) error {
				var reference bytes.Buffer
				rw := ebml.NewWriter(&reference)
				if err := rw.WriteUInt(idCueRefTime, 0); err != nil {
					return err
				}
				return writeCuePointWithReferencePayload(w, reference.Bytes())
			},
		},
		{
			name: "zero cue reference number",
			writeCue: func(w *ebml.Writer) error {
				var reference bytes.Buffer
				rw := ebml.NewWriter(&reference)
				if err := rw.WriteUInt(idCueRefTime, 0); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefCluster, 0); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefNumber, 0); err != nil {
					return err
				}
				return writeCuePointWithReferencePayload(w, reference.Bytes())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeCueMetadataMatroskaData(t, func(writer *ebml.Writer) error {
				var cues bytes.Buffer
				cw := ebml.NewWriter(&cues)
				if err := tt.writeCue(cw); err != nil {
					return err
				}
				return writer.WriteElement(idCues, cues.Bytes())
			})
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestDemuxerRejectsDuplicateCueMetadata(t *testing.T) {
	tests := []struct {
		name     string
		writeCue func(*ebml.Writer) error
	}{
		{
			name: "duplicate cue time",
			writeCue: func(w *ebml.Writer) error {
				var point bytes.Buffer
				pw := ebml.NewWriter(&point)
				if err := pw.WriteUInt(idCueTime, 0); err != nil {
					return err
				}
				if err := pw.WriteUInt(idCueTime, 1); err != nil {
					return err
				}
				if err := pw.WriteElement(idCueTrackPositions, cueTrackPositionsPayload(t)); err != nil {
					return err
				}
				return w.WriteElement(idCuePoint, point.Bytes())
			},
		},
		{
			name: "duplicate cue track",
			writeCue: func(w *ebml.Writer) error {
				var positions bytes.Buffer
				tw := ebml.NewWriter(&positions)
				if err := tw.WriteUInt(idCueTrack, 1); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueTrack, 2); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
					return err
				}
				return writeCuePointWithPositionsPayload(w, positions.Bytes())
			},
		},
		{
			name: "duplicate cue cluster position",
			writeCue: func(w *ebml.Writer) error {
				var positions bytes.Buffer
				tw := ebml.NewWriter(&positions)
				if err := tw.WriteUInt(idCueTrack, 1); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueClusterPosition, 1); err != nil {
					return err
				}
				return writeCuePointWithPositionsPayload(w, positions.Bytes())
			},
		},
		{
			name: "duplicate cue relative position",
			writeCue: func(w *ebml.Writer) error {
				var positions bytes.Buffer
				tw := ebml.NewWriter(&positions)
				if err := tw.WriteUInt(idCueTrack, 1); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueRelativePos, 1); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueRelativePos, 2); err != nil {
					return err
				}
				return writeCuePointWithPositionsPayload(w, positions.Bytes())
			},
		},
		{
			name: "duplicate cue duration",
			writeCue: func(w *ebml.Writer) error {
				var positions bytes.Buffer
				tw := ebml.NewWriter(&positions)
				if err := tw.WriteUInt(idCueTrack, 1); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueDuration, 1); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueDuration, 2); err != nil {
					return err
				}
				return writeCuePointWithPositionsPayload(w, positions.Bytes())
			},
		},
		{
			name: "duplicate cue block number",
			writeCue: func(w *ebml.Writer) error {
				var positions bytes.Buffer
				tw := ebml.NewWriter(&positions)
				if err := tw.WriteUInt(idCueTrack, 1); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueBlockNumber, 1); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueBlockNumber, 2); err != nil {
					return err
				}
				return writeCuePointWithPositionsPayload(w, positions.Bytes())
			},
		},
		{
			name: "duplicate cue codec state",
			writeCue: func(w *ebml.Writer) error {
				var positions bytes.Buffer
				tw := ebml.NewWriter(&positions)
				if err := tw.WriteUInt(idCueTrack, 1); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueCodecState, 1); err != nil {
					return err
				}
				if err := tw.WriteUInt(idCueCodecState, 2); err != nil {
					return err
				}
				return writeCuePointWithPositionsPayload(w, positions.Bytes())
			},
		},
		{
			name: "duplicate cue reference time",
			writeCue: func(w *ebml.Writer) error {
				var reference bytes.Buffer
				rw := ebml.NewWriter(&reference)
				if err := rw.WriteUInt(idCueRefTime, 0); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefTime, 1); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefCluster, 0); err != nil {
					return err
				}
				return writeCuePointWithReferencePayload(w, reference.Bytes())
			},
		},
		{
			name: "duplicate cue reference cluster",
			writeCue: func(w *ebml.Writer) error {
				var reference bytes.Buffer
				rw := ebml.NewWriter(&reference)
				if err := rw.WriteUInt(idCueRefTime, 0); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefCluster, 0); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefCluster, 1); err != nil {
					return err
				}
				return writeCuePointWithReferencePayload(w, reference.Bytes())
			},
		},
		{
			name: "duplicate cue reference number",
			writeCue: func(w *ebml.Writer) error {
				var reference bytes.Buffer
				rw := ebml.NewWriter(&reference)
				if err := rw.WriteUInt(idCueRefTime, 0); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefCluster, 0); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefNumber, 1); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefNumber, 2); err != nil {
					return err
				}
				return writeCuePointWithReferencePayload(w, reference.Bytes())
			},
		},
		{
			name: "duplicate cue reference codec state",
			writeCue: func(w *ebml.Writer) error {
				var reference bytes.Buffer
				rw := ebml.NewWriter(&reference)
				if err := rw.WriteUInt(idCueRefTime, 0); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefCluster, 0); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefCodecState, 1); err != nil {
					return err
				}
				if err := rw.WriteUInt(idCueRefCodecState, 2); err != nil {
					return err
				}
				return writeCuePointWithReferencePayload(w, reference.Bytes())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeCueMetadataMatroskaData(t, func(writer *ebml.Writer) error {
				var cues bytes.Buffer
				cw := ebml.NewWriter(&cues)
				if err := tt.writeCue(cw); err != nil {
					return err
				}
				return writer.WriteElement(idCues, cues.Bytes())
			})
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestDemuxerRejectsInvalidEBMLHeaderMetadata(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ebmlHeaderFixture)
	}{
		{
			name: "missing doc type",
			mutate: func(header *ebmlHeaderFixture) {
				header.DocTypeSet = false
			},
		},
		{
			name: "empty doc type",
			mutate: func(header *ebmlHeaderFixture) {
				header.DocType = ""
			},
		},
		{
			name: "unsupported doc type",
			mutate: func(header *ebmlHeaderFixture) {
				header.DocType = "notmatroska"
			},
		},
		{
			name: "duplicate ebml version",
			mutate: func(header *ebmlHeaderFixture) {
				header.DuplicateEBMLVersion = true
			},
		},
		{
			name: "duplicate ebml read version",
			mutate: func(header *ebmlHeaderFixture) {
				header.DuplicateEBMLReadVersion = true
			},
		},
		{
			name: "duplicate max id length",
			mutate: func(header *ebmlHeaderFixture) {
				header.DuplicateEBMLMaxIDLength = true
			},
		},
		{
			name: "duplicate max size length",
			mutate: func(header *ebmlHeaderFixture) {
				header.DuplicateEBMLMaxSizeLength = true
			},
		},
		{
			name: "duplicate doc type",
			mutate: func(header *ebmlHeaderFixture) {
				header.DuplicateDocType = true
			},
		},
		{
			name: "duplicate doc type version",
			mutate: func(header *ebmlHeaderFixture) {
				header.DuplicateDocTypeVersion = true
			},
		},
		{
			name: "duplicate doc type read version",
			mutate: func(header *ebmlHeaderFixture) {
				header.DuplicateDocTypeReadVersion = true
			},
		},
		{
			name: "zero ebml version",
			mutate: func(header *ebmlHeaderFixture) {
				header.EBMLVersion = 0
			},
		},
		{
			name: "zero ebml read version",
			mutate: func(header *ebmlHeaderFixture) {
				header.EBMLReadVersion = 0
			},
		},
		{
			name: "unsupported ebml read version",
			mutate: func(header *ebmlHeaderFixture) {
				header.EBMLVersion = 2
				header.EBMLReadVersion = 2
			},
		},
		{
			name: "short max id length",
			mutate: func(header *ebmlHeaderFixture) {
				header.EBMLMaxIDLength = ebml.MaxIDWidth - 1
			},
		},
		{
			name: "long max id length",
			mutate: func(header *ebmlHeaderFixture) {
				header.EBMLMaxIDLength = ebml.MaxIDWidth + 1
			},
		},
		{
			name: "short max size length",
			mutate: func(header *ebmlHeaderFixture) {
				header.EBMLMaxSizeLength = ebml.MaxSizeWidth - 1
			},
		},
		{
			name: "long max size length",
			mutate: func(header *ebmlHeaderFixture) {
				header.EBMLMaxSizeLength = ebml.MaxSizeWidth + 1
			},
		},
		{
			name: "zero doc type version",
			mutate: func(header *ebmlHeaderFixture) {
				header.DocTypeVersion = 0
			},
		},
		{
			name: "zero doc type read version",
			mutate: func(header *ebmlHeaderFixture) {
				header.DocTypeReadVersion = 0
			},
		},
		{
			name: "doc type read version past document version",
			mutate: func(header *ebmlHeaderFixture) {
				header.DocTypeVersion = 2
				header.DocTypeReadVersion = 3
			},
		},
		{
			name: "unsupported doc type read version",
			mutate: func(header *ebmlHeaderFixture) {
				header.DocTypeVersion = defaultDocTypeVersion + 1
				header.DocTypeReadVersion = defaultDocTypeVersion + 1
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := defaultEBMLHeaderFixture()
			tt.mutate(&header)
			data := makeEBMLHeaderMatroskaData(t, header)
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestDemuxerAcceptsEBMLHeaderDefaults(t *testing.T) {
	header := defaultEBMLHeaderFixture()
	header.EBMLVersionSet = false
	header.EBMLReadVersionSet = false
	header.EBMLMaxIDLengthSet = false
	header.EBMLMaxSizeLengthSet = false
	header.DocTypeVersionSet = false
	header.DocTypeReadVersionSet = false
	data := makeEBMLHeaderMatroskaData(t, header)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := demuxer.DocType(); got != "matroska" {
		t.Fatalf("doctype = %q, want matroska", got)
	}
}

func TestDemuxerRejectsInvalidSeekHeadMetadata(t *testing.T) {
	tests := []struct {
		name      string
		writeSeek func(*ebml.Writer) error
	}{
		{
			name:      "empty seek entry",
			writeSeek: func(*ebml.Writer) error { return nil },
		},
		{
			name: "missing seek id",
			writeSeek: func(w *ebml.Writer) error {
				return w.WriteUInt(idSeekPosition, 0)
			},
		},
		{
			name: "missing seek position",
			writeSeek: func(w *ebml.Writer) error {
				return writeSeekIDElement(w, idInfo)
			},
		},
		{
			name: "duplicate seek id",
			writeSeek: func(w *ebml.Writer) error {
				if err := writeSeekIDElement(w, idInfo); err != nil {
					return err
				}
				if err := writeSeekIDElement(w, idTracks); err != nil {
					return err
				}
				return w.WriteUInt(idSeekPosition, 0)
			},
		},
		{
			name: "duplicate seek position",
			writeSeek: func(w *ebml.Writer) error {
				if err := writeSeekIDElement(w, idInfo); err != nil {
					return err
				}
				if err := w.WriteUInt(idSeekPosition, 0); err != nil {
					return err
				}
				return w.WriteUInt(idSeekPosition, 1)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeSeekHeadMetadataMatroskaData(t, func(w *ebml.Writer) error {
				var payload bytes.Buffer
				sw := ebml.NewWriter(&payload)
				if err := tt.writeSeek(sw); err != nil {
					return err
				}
				return w.WriteElement(idSeek, payload.Bytes())
			})
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestDemuxerKeepsZeroSeekPosition(t *testing.T) {
	data := makeSeekHeadMetadataMatroskaData(t, func(w *ebml.Writer) error {
		return writeSeekEntry(w, idInfo, 0)
	})
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entries := demuxer.SeekEntries()
	if len(entries) == 0 {
		t.Fatal("missing seek entries")
	}
	assertSeekEntry(t, entries, idInfo, 0)
}

func TestDemuxerRejectsDuplicateTopLevelSegmentMetadata(t *testing.T) {
	tests := []struct {
		name          string
		writeMetadata func(*ebml.Writer) error
	}{
		{
			name: "third seekhead",
			writeMetadata: func(w *ebml.Writer) error {
				for i := 0; i < 3; i++ {
					if err := w.WriteElement(idSeekHead, nil); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			name: "duplicate info",
			writeMetadata: func(w *ebml.Writer) error {
				return writeInfoWithElements(w, nil)
			},
		},
		{
			name: "duplicate tracks",
			writeMetadata: func(w *ebml.Writer) error {
				return writeTracksWithVideoDimensions(w, 16, 16)
			},
		},
		{
			name: "duplicate cues",
			writeMetadata: func(w *ebml.Writer) error {
				if err := writeCuesWithTrackNumber(w, 1); err != nil {
					return err
				}
				return writeCuesWithTrackNumber(w, 1)
			},
		},
		{
			name: "duplicate attachments",
			writeMetadata: func(w *ebml.Writer) error {
				if err := writeAttachmentsElement(w, Attachment{
					UID:      1,
					Filename: "one.txt",
					MIMEType: "text/plain",
					Data:     []byte("one"),
				}); err != nil {
					return err
				}
				return writeAttachmentsElement(w, Attachment{
					UID:      2,
					Filename: "two.txt",
					MIMEType: "text/plain",
					Data:     []byte("two"),
				})
			},
		},
		{
			name: "duplicate chapters",
			writeMetadata: func(w *ebml.Writer) error {
				if err := writeChaptersElement(w,
					ChapterEdition{UID: 1, Chapters: []Chapter{metadataValidationChapter(1, 0)}},
				); err != nil {
					return err
				}
				return writeChaptersElement(w,
					ChapterEdition{UID: 2, Chapters: []Chapter{metadataValidationChapter(2, 1)}},
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

func TestDemuxerLoadsRequiredMetadataFromSeekHeadBeforeFirstCluster(t *testing.T) {
	data := makeDeferredRequiredMetadataMatroskaData(t, true)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if tracks := demuxer.Tracks(); len(tracks) != 1 || tracks[0].Codec != CodecVP8 {
		t.Fatalf("tracks = %+v, want deferred VP8 track", tracks)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != 1 || packet.TimeNS != 0 || !bytes.Equal(packet.Data, []byte{0x77}) {
		t.Fatalf("packet = %+v data=%x", packet, packet.Data)
	}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestDemuxerLoadsOptionalMetadataFromSeekHeadBeforeFirstCluster(t *testing.T) {
	attachments := []Attachment{{
		UID:         9,
		Filename:    "late-cover.png",
		MIMEType:    "image/png",
		Description: "late cover",
		Data:        []byte{0x89, 0x50, 0x4e, 0x47},
	}}
	chapters := []ChapterEdition{{
		UID:     17,
		Default: true,
		Chapters: []Chapter{{
			UID:        18,
			StartNS:    0,
			EndNS:      1_000_000_000,
			EndSet:     true,
			Enabled:    true,
			EnabledSet: true,
			Displays: []ChapterDisplay{{
				String:   "Late Chapter",
				Language: "eng",
			}},
		}},
	}}
	tags := []Tag{{
		Target: TagTarget{
			TypeValue:      50,
			AttachmentUIDs: []uint64{9},
			EditionUIDs:    []uint64{17},
			ChapterUIDs:    []uint64{18},
		},
		Simple: []SimpleTag{{
			Name:       "TITLE",
			Default:    true,
			DefaultSet: true,
			String:     "Late Metadata",
			StringSet:  true,
		}},
	}}
	wantAttachments, err := normalizeAttachments(attachments)
	if err != nil {
		t.Fatal(err)
	}
	wantChapters, err := normalizeChapters(chapters)
	if err != nil {
		t.Fatal(err)
	}
	wantTags, err := normalizeTags(tags)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name                   string
		requiredMetadataBefore bool
	}{
		{name: "required metadata late", requiredMetadataBefore: false},
		{name: "required metadata before cluster", requiredMetadataBefore: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeDeferredOptionalMetadataMatroskaData(t, tt.requiredMetadataBefore, attachments, chapters, tags)
			demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if got := demuxer.Attachments(); !reflect.DeepEqual(got, wantAttachments) {
				t.Fatalf("attachments = %+v, want %+v", got, wantAttachments)
			}
			if got := demuxer.Chapters(); !reflect.DeepEqual(got, wantChapters) {
				t.Fatalf("chapters = %+v, want %+v", got, wantChapters)
			}
			if got := demuxer.Tags(); !reflect.DeepEqual(got, wantTags) {
				t.Fatalf("tags = %+v, want %+v", got, wantTags)
			}

			packet := Packet{Data: make([]byte, 0, 8)}
			if err := demuxer.ReadPacket(&packet); err != nil {
				t.Fatal(err)
			}
			if packet.TrackID != 1 || packet.TimeNS != 0 || !bytes.Equal(packet.Data, []byte{0x77}) {
				t.Fatalf("packet = %+v data=%x", packet, packet.Data)
			}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
				t.Fatalf("err = %v, want EOF", err)
			}
		})
	}
}

func TestDemuxerRejectsClusterBeforeRequiredMetadataWithoutSeekHead(t *testing.T) {
	data := makeDeferredRequiredMetadataMatroskaData(t, false)
	if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerRejectsBlockForUnknownTrack(t *testing.T) {
	data := makeBlockTrackNumberMatroskaData(t, 2)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrUnknownTrack) {
		t.Fatalf("err = %v, want ErrUnknownTrack", err)
	}
}

func TestDemuxerRejectsInvalidBlockAdditions(t *testing.T) {
	tests := []struct {
		name           string
		writeAdditions func(*ebml.Writer) error
	}{
		{
			name: "empty block additions",
			writeAdditions: func(w *ebml.Writer) error {
				return w.WriteElement(idBlockAdditions, nil)
			},
		},
		{
			name: "zero block add id",
			writeAdditions: func(w *ebml.Writer) error {
				var additions bytes.Buffer
				aw := ebml.NewWriter(&additions)
				if err := writeBlockMoreFixture(aw, 0, []byte{1}, true); err != nil {
					return err
				}
				return w.WriteElement(idBlockAdditions, additions.Bytes())
			},
		},
		{
			name: "duplicate block add id",
			writeAdditions: func(w *ebml.Writer) error {
				var additions bytes.Buffer
				aw := ebml.NewWriter(&additions)
				if err := writeBlockMoreFixture(aw, 2, []byte{1}, true); err != nil {
					return err
				}
				if err := writeBlockMoreFixture(aw, 2, []byte{2}, true); err != nil {
					return err
				}
				return w.WriteElement(idBlockAdditions, additions.Bytes())
			},
		},
		{
			name: "duplicate block add id in one block more",
			writeAdditions: func(w *ebml.Writer) error {
				var more bytes.Buffer
				mw := ebml.NewWriter(&more)
				if err := mw.WriteUInt(idBlockAddID, 2); err != nil {
					return err
				}
				if err := mw.WriteUInt(idBlockAddID, 3); err != nil {
					return err
				}
				if err := writeBinary(mw, idBlockAdditional, []byte{1}); err != nil {
					return err
				}
				var additions bytes.Buffer
				aw := ebml.NewWriter(&additions)
				if err := aw.WriteElement(idBlockMore, more.Bytes()); err != nil {
					return err
				}
				return w.WriteElement(idBlockAdditions, additions.Bytes())
			},
		},
		{
			name: "missing block additional",
			writeAdditions: func(w *ebml.Writer) error {
				var more bytes.Buffer
				mw := ebml.NewWriter(&more)
				if err := mw.WriteUInt(idBlockAddID, 2); err != nil {
					return err
				}
				var additions bytes.Buffer
				aw := ebml.NewWriter(&additions)
				if err := aw.WriteElement(idBlockMore, more.Bytes()); err != nil {
					return err
				}
				return w.WriteElement(idBlockAdditions, additions.Bytes())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeBlockAdditionsMatroskaData(t, tt.writeAdditions)
			demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, 8)}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestDemuxerRejectsDuplicateBlockGroupMetadata(t *testing.T) {
	tests := []struct {
		name       string
		writeGroup func(*ebml.Writer, uint32) error
	}{
		{
			name: "duplicate block",
			writeGroup: func(w *ebml.Writer, trackID uint32) error {
				if err := writeBlockWithTrackNumber(w, uint64(trackID), []byte{1}); err != nil {
					return err
				}
				return writeBlockWithTrackNumber(w, uint64(trackID), []byte{2})
			},
		},
		{
			name: "duplicate block duration",
			writeGroup: func(w *ebml.Writer, trackID uint32) error {
				if err := writeBlockWithTrackNumber(w, uint64(trackID), []byte{1}); err != nil {
					return err
				}
				if err := w.WriteUInt(idBlockDuration, 1); err != nil {
					return err
				}
				return w.WriteUInt(idBlockDuration, 2)
			},
		},
		{
			name: "duplicate block additions",
			writeGroup: func(w *ebml.Writer, trackID uint32) error {
				if err := writeBlockWithTrackNumber(w, uint64(trackID), []byte{1}); err != nil {
					return err
				}
				if err := writeBlockAdditions(w, []BlockAddition{{ID: 2, Data: []byte{1}}}); err != nil {
					return err
				}
				return writeBlockAdditions(w, []BlockAddition{{ID: 3, Data: []byte{2}}})
			},
		},
		{
			name: "duplicate reference priority",
			writeGroup: func(w *ebml.Writer, trackID uint32) error {
				if err := writeBlockWithTrackNumber(w, uint64(trackID), []byte{1}); err != nil {
					return err
				}
				if err := w.WriteUInt(idReferencePriority, 1); err != nil {
					return err
				}
				return w.WriteUInt(idReferencePriority, 2)
			},
		},
		{
			name: "duplicate discard padding",
			writeGroup: func(w *ebml.Writer, trackID uint32) error {
				if err := writeBlockWithTrackNumber(w, uint64(trackID), []byte{1}); err != nil {
					return err
				}
				if err := w.WriteInt(idDiscardPad, -1); err != nil {
					return err
				}
				return w.WriteInt(idDiscardPad, -2)
			},
		},
		{
			name: "duplicate codec state",
			writeGroup: func(w *ebml.Writer, trackID uint32) error {
				if err := writeBlockWithTrackNumber(w, uint64(trackID), []byte{1}); err != nil {
					return err
				}
				if err := writeBinary(w, idCodecState, []byte{1}); err != nil {
					return err
				}
				return writeBinary(w, idCodecState, []byte{2})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeBlockGroupMetadataMatroskaData(t, tt.writeGroup)
			demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, 8)}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestDemuxerRejectsDuplicateTrackSingletonMetadata(t *testing.T) {
	validCompressionEncodings := func(tb testing.TB) []byte {
		tb.Helper()
		return contentEncodingsPayload(tb, contentEncodingPayload(tb, func(w *ebml.Writer) error {
			return w.WriteElement(idContentCompression, contentCompressionPayload(tb, ContentCompAlgoZlib, nil))
		}))
	}
	tests := []struct {
		name        string
		writeTracks func(*ebml.Writer) error
	}{
		{
			name: "duplicate track flag lacing",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackMetadata(w,
					trackUIntElement{idFlagLacing, 1},
					trackUIntElement{idFlagLacing, 0},
				)
			},
		},
		{
			name: "duplicate track language",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackExtra(w, func(ew *ebml.Writer) error {
					if err := ew.WriteString(idLanguage, "eng"); err != nil {
						return err
					}
					return ew.WriteString(idLanguage, "fra")
				})
			},
		},
		{
			name: "duplicate codec private",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackExtra(w, func(ew *ebml.Writer) error {
					if err := writeBinary(ew, idCodecPrivate, []byte{1}); err != nil {
						return err
					}
					return writeBinary(ew, idCodecPrivate, []byte{2})
				})
			},
		},
		{
			name: "duplicate default duration",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackMetadata(w,
					trackUIntElement{idDefaultDur, 1},
					trackUIntElement{idDefaultDur, 2},
				)
			},
		},
		{
			name: "duplicate codec delay",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackMetadata(w,
					trackUIntElement{idCodecDelay, 1},
					trackUIntElement{idCodecDelay, 2},
				)
			},
		},
		{
			name: "duplicate content encodings",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackExtra(w, func(ew *ebml.Writer) error {
					encodings := validCompressionEncodings(t)
					if err := ew.WriteElement(idContentEncodings, encodings); err != nil {
						return err
					}
					return ew.WriteElement(idContentEncodings, encodings)
				})
			},
		},
		{
			name: "duplicate track translate id",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackExtra(w, func(ew *ebml.Writer) error {
					var payload bytes.Buffer
					tw := ebml.NewWriter(&payload)
					if err := writeBinary(tw, idTrackTranslateTrack, []byte{1}); err != nil {
						return err
					}
					if err := writeBinary(tw, idTrackTranslateTrack, []byte{2}); err != nil {
						return err
					}
					if err := tw.WriteUInt(idTrackTranslateCodec, 1); err != nil {
						return err
					}
					return ew.WriteElement(idTrackTranslate, payload.Bytes())
				})
			},
		},
		{
			name: "duplicate block addition mapping name",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackExtra(w, func(ew *ebml.Writer) error {
					if err := ew.WriteUInt(idMaxBlockAdditionID, 2); err != nil {
						return err
					}
					var payload bytes.Buffer
					mw := ebml.NewWriter(&payload)
					if err := mw.WriteUInt(idBlockAddIDValue, 2); err != nil {
						return err
					}
					if err := mw.WriteString(idBlockAddIDName, "first"); err != nil {
						return err
					}
					if err := mw.WriteString(idBlockAddIDName, "second"); err != nil {
						return err
					}
					if err := mw.WriteUInt(idBlockAddIDType, 1); err != nil {
						return err
					}
					return ew.WriteElement(idBlockAdditionMapping, payload.Bytes())
				})
			},
		},
		{
			name: "duplicate content encoding scope",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackExtra(w, func(ew *ebml.Writer) error {
					return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
						contentEncodingPayload(t, func(cw *ebml.Writer) error {
							if err := cw.WriteUInt(idContentEncodingScope, ContentEncodingScopeBlock); err != nil {
								return err
							}
							if err := cw.WriteUInt(idContentEncodingScope, ContentEncodingScopePrivate); err != nil {
								return err
							}
							return cw.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoZlib, nil))
						}),
					))
				})
			},
		},
		{
			name: "duplicate content compression algorithm",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackExtra(w, func(ew *ebml.Writer) error {
					var compression bytes.Buffer
					cw := ebml.NewWriter(&compression)
					if err := cw.WriteUInt(idContentCompAlgo, ContentCompAlgoZlib); err != nil {
						return err
					}
					if err := cw.WriteUInt(idContentCompAlgo, ContentCompAlgoHeaderStripping); err != nil {
						return err
					}
					return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
						contentEncodingPayload(t, func(encw *ebml.Writer) error {
							return encw.WriteElement(idContentCompression, compression.Bytes())
						}),
					))
				})
			},
		},
		{
			name: "duplicate content encryption key id",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackExtra(w, func(ew *ebml.Writer) error {
					var encryption bytes.Buffer
					enw := ebml.NewWriter(&encryption)
					if err := enw.WriteUInt(idContentEncAlgo, ContentEncAlgoAES); err != nil {
						return err
					}
					if err := writeBinary(enw, idContentEncKeyID, []byte{1}); err != nil {
						return err
					}
					if err := writeBinary(enw, idContentEncKeyID, []byte{2}); err != nil {
						return err
					}
					return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
						contentEncodingPayload(t, func(encw *ebml.Writer) error {
							if err := encw.WriteUInt(idContentEncodingType, ContentEncodingTypeEncryption); err != nil {
								return err
							}
							return encw.WriteElement(idContentEncryption, encryption.Bytes())
						}),
					))
				})
			},
		},
		{
			name: "duplicate aes cipher",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithTrackExtra(w, func(ew *ebml.Writer) error {
					var aes bytes.Buffer
					aw := ebml.NewWriter(&aes)
					if err := aw.WriteUInt(idContentEncAESCipher, ContentEncAESCipherModeCTR); err != nil {
						return err
					}
					if err := aw.WriteUInt(idContentEncAESCipher, ContentEncAESCipherModeCTR); err != nil {
						return err
					}
					var encryption bytes.Buffer
					enw := ebml.NewWriter(&encryption)
					if err := enw.WriteUInt(idContentEncAlgo, ContentEncAlgoAES); err != nil {
						return err
					}
					if err := enw.WriteElement(idContentEncAES, aes.Bytes()); err != nil {
						return err
					}
					return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
						contentEncodingPayload(t, func(encw *ebml.Writer) error {
							if err := encw.WriteUInt(idContentEncodingType, ContentEncodingTypeEncryption); err != nil {
								return err
							}
							return encw.WriteElement(idContentEncryption, encryption.Bytes())
						}),
					))
				})
			},
		},
		{
			name: "duplicate video pixel width",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithVideoElements(w,
					videoUIntElement{idPixelWidth, 16},
					videoUIntElement{idPixelWidth, 32},
					videoUIntElement{idPixelHeight, 16},
				)
			},
		},
		{
			name: "duplicate video colour max cll",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithVideoColourElements(w,
					colourUIntElement{idMaxCLL, 1},
					colourUIntElement{idMaxCLL, 2},
				)
			},
		},
		{
			name: "duplicate mastering luminance max",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithVideoMasteringFloatElements(w,
					colourFloatElement{idLuminanceMax, 1},
					colourFloatElement{idLuminanceMax, 2},
				)
			},
		},
		{
			name: "duplicate projection pose yaw",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithVideoProjectionMetadata(w,
					[]projectionUIntElement{{idProjectionType, 0}},
					[]projectionFloatElement{{idProjectionPoseYaw, 0}, {idProjectionPoseYaw, 1}},
					nil,
				)
			},
		},
		{
			name: "duplicate audio sampling frequency",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithAudioElements(w, func(aw *ebml.Writer) error {
					if err := aw.WriteFloat64(idSamplingFreq, 48000); err != nil {
						return err
					}
					if err := aw.WriteFloat64(idSamplingFreq, 44100); err != nil {
						return err
					}
					if err := aw.WriteUInt(idChannels, 2); err != nil {
						return err
					}
					return aw.WriteUInt(idBitDepth, 16)
				})
			},
		},
		{
			name: "duplicate audio channels",
			writeTracks: func(w *ebml.Writer) error {
				return writeTracksWithAudioElements(w, func(aw *ebml.Writer) error {
					if err := aw.WriteFloat64(idSamplingFreq, 48000); err != nil {
						return err
					}
					if err := aw.WriteUInt(idChannels, 2); err != nil {
						return err
					}
					if err := aw.WriteUInt(idChannels, 1); err != nil {
						return err
					}
					return aw.WriteUInt(idBitDepth, 16)
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeTrackMetadataMatroskaData(t, tt.writeTracks)
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestDemuxerRejectsInvalidTrackMetadata(t *testing.T) {
	t.Run("missing track number", func(t *testing.T) {
		entry := defaultTrackEntryFixture()
		entry.NumberSet = false
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, entry)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("missing track type", func(t *testing.T) {
		entry := defaultTrackEntryFixture()
		entry.TypeSet = false
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, entry)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("missing codec id", func(t *testing.T) {
		entry := defaultTrackEntryFixture()
		entry.CodecIDSet = false
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, entry)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("missing video settings", func(t *testing.T) {
		entry := defaultTrackEntryFixture()
		entry.MediaSet = false
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, entry)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("missing audio settings", func(t *testing.T) {
		entry := defaultTrackEntryFixture()
		entry.Type = matroskaTrackAudio
		entry.CodecID = codecIDOpus
		entry.MediaSet = false
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, entry)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("duplicate track number", func(t *testing.T) {
		first := defaultTrackEntryFixture()
		second := defaultTrackEntryFixture()
		second.UID = 2
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, first, second)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("duplicate track uid", func(t *testing.T) {
		first := defaultTrackEntryFixture()
		second := defaultTrackEntryFixture()
		second.Number = 2
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, first, second)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("duplicate track number element", func(t *testing.T) {
		entry := defaultTrackEntryFixture()
		entry.DuplicateNumber = true
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, entry)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("duplicate track uid element", func(t *testing.T) {
		entry := defaultTrackEntryFixture()
		entry.DuplicateUID = true
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, entry)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("duplicate track type element", func(t *testing.T) {
		entry := defaultTrackEntryFixture()
		entry.DuplicateType = true
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, entry)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("duplicate codec id element", func(t *testing.T) {
		entry := defaultTrackEntryFixture()
		entry.DuplicateCodecID = true
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, entry)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("duplicate video element", func(t *testing.T) {
		entry := defaultTrackEntryFixture()
		entry.DuplicateMedia = true
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackEntryFixtures(writer, entry)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video dimension overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoDimensions(writer, maxIntValue+1, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video zero pixel width", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoDimensions(writer, 0, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video stereo mode overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoElements(writer,
				videoUIntElement{idPixelWidth, 16},
				videoUIntElement{idPixelHeight, 16},
				videoUIntElement{idStereoMode, maxIntValue + 1},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video interlaced flag overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoElements(writer,
				videoUIntElement{idPixelWidth, 16},
				videoUIntElement{idPixelHeight, 16},
				videoUIntElement{idFlagInterlaced, maxIntValue + 1},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video field order overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoElements(writer,
				videoUIntElement{idPixelWidth, 16},
				videoUIntElement{idPixelHeight, 16},
				videoUIntElement{idFieldOrder, maxIntValue + 1},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video alpha mode overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoElements(writer,
				videoUIntElement{idPixelWidth, 16},
				videoUIntElement{idPixelHeight, 16},
				videoUIntElement{idAlphaMode, maxIntValue + 1},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video display width overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoElements(writer,
				videoUIntElement{idPixelWidth, 16},
				videoUIntElement{idPixelHeight, 16},
				videoUIntElement{idDisplayWidth, maxIntValue + 1},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video zero display width", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoElements(writer,
				videoUIntElement{idPixelWidth, 16},
				videoUIntElement{idPixelHeight, 16},
				videoUIntElement{idDisplayWidth, 0},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video aspect ratio type overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoElements(writer,
				videoUIntElement{idPixelWidth, 16},
				videoUIntElement{idPixelHeight, 16},
				videoUIntElement{idAspectRatioType, maxIntValue + 1},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video crop overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoElements(writer,
				videoUIntElement{idPixelWidth, 16},
				videoUIntElement{idPixelHeight, 16},
				videoUIntElement{idPixelCropLeft, maxIntValue + 1},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video display unit overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoElements(writer,
				videoUIntElement{idPixelWidth, 16},
				videoUIntElement{idPixelHeight, 16},
				videoUIntElement{idDisplayUnit, maxIntValue + 1},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video colour overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoColourElements(writer,
				colourUIntElement{idMaxCLL, maxIntValue + 1},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video colour chromaticity range", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoMasteringFloatElements(writer,
				colourFloatElement{idPrimaryRX, 1.1},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video colour luminance range", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoMasteringFloatElements(writer,
				colourFloatElement{idLuminanceMin, -0.1},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video projection type overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoProjectionMetadata(writer,
				[]projectionUIntElement{{idProjectionType, maxIntValue + 1}},
				nil,
				nil,
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video projection private on rectangular", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoProjectionMetadata(writer,
				[]projectionUIntElement{{idProjectionType, 0}},
				nil,
				[]byte{0},
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video projection private missing", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoProjectionMetadata(writer,
				[]projectionUIntElement{{idProjectionType, 1}},
				nil,
				nil,
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("video projection pose range", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoProjectionMetadata(writer,
				[]projectionUIntElement{{idProjectionType, 0}},
				[]projectionFloatElement{{idProjectionPosePitch, 90.1}},
				nil,
			)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("zero default duration", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackMetadata(writer, trackUIntElement{idDefaultDur, 0})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("default duration overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackMetadata(writer, trackUIntElement{idDefaultDur, uint64(math.MaxInt64) + 1})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("zero decoded field duration", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackMetadata(writer, trackUIntElement{idDefaultDecodedDur, 0})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("decoded field duration overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackMetadata(writer, trackUIntElement{idDefaultDecodedDur, uint64(math.MaxInt64) + 1})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("zero track overlay", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackMetadata(writer, trackUIntElement{idTrackOverlay, 0})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("track translate missing id", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idTrackTranslate, trackTranslatePayload(t, false, true))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("track translate missing codec", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idTrackTranslate, trackTranslatePayload(t, true, false))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("block addition mapping missing id", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idBlockAdditionMapping, blockAdditionMappingPayload(t, 0, false, 1, nil))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("block addition mapping id below minimum", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idBlockAdditionMapping, blockAdditionMappingPayload(t, 1, true, 1, nil))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("duplicate block addition mapping id", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				if err := ew.WriteUInt(idMaxBlockAdditionID, 2); err != nil {
					return err
				}
				if err := ew.WriteElement(idBlockAdditionMapping, blockAdditionMappingPayload(t, 2, true, 1, nil)); err != nil {
					return err
				}
				return ew.WriteElement(idBlockAdditionMapping, blockAdditionMappingPayload(t, 2, true, 2, nil))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("max block addition id below mapping", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				if err := ew.WriteUInt(idMaxBlockAdditionID, 1); err != nil {
					return err
				}
				return ew.WriteElement(idBlockAdditionMapping, blockAdditionMappingPayload(t, 2, true, 1, nil))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("empty content encodings", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idContentEncodings, nil)
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("duplicate content encoding order", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
					contentEncodingPayload(t, func(w *ebml.Writer) error {
						if err := w.WriteUInt(idContentEncodingOrd, 1); err != nil {
							return err
						}
						return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoZlib, nil))
					}),
					contentEncodingPayload(t, func(w *ebml.Writer) error {
						if err := w.WriteUInt(idContentEncodingOrd, 1); err != nil {
							return err
						}
						return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoHeaderStripping, []byte{0}))
					}),
				))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("content encoding invalid type", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
					contentEncodingPayload(t, func(w *ebml.Writer) error {
						return w.WriteUInt(idContentEncodingType, 2)
					}),
				))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("content encoding invalid scope", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
					contentEncodingPayload(t, func(w *ebml.Writer) error {
						if err := w.WriteUInt(idContentEncodingScope, 8); err != nil {
							return err
						}
						return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoZlib, nil))
					}),
				))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("content compression missing", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
					contentEncodingPayload(t, func(w *ebml.Writer) error {
						return w.WriteUInt(idContentEncodingType, ContentEncodingTypeCompression)
					}),
				))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("content encryption missing", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
					contentEncodingPayload(t, func(w *ebml.Writer) error {
						return w.WriteUInt(idContentEncodingType, ContentEncodingTypeEncryption)
					}),
				))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("content compression invalid algorithm", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
					contentEncodingPayload(t, func(w *ebml.Writer) error {
						return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoHeaderStripping+1, nil))
					}),
				))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("content aes settings missing cipher", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
					contentEncodingPayload(t, func(w *ebml.Writer) error {
						if err := w.WriteUInt(idContentEncodingType, ContentEncodingTypeEncryption); err != nil {
							return err
						}
						return w.WriteElement(idContentEncryption, contentEncryptionPayload(t, ContentEncAlgoAES, nil, contentAESCipherUnset))
					}),
				))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("content aes settings invalid cipher", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
				return ew.WriteElement(idContentEncodings, contentEncodingsPayload(t,
					contentEncodingPayload(t, func(w *ebml.Writer) error {
						if err := w.WriteUInt(idContentEncodingType, ContentEncodingTypeEncryption); err != nil {
							return err
						}
						return w.WriteElement(idContentEncryption, contentEncryptionPayload(t, ContentEncAlgoAES, nil, 3))
					}),
				))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("audio channels overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithAudioMetadata(writer, 48000, maxIntValue+1, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("audio negative sample rate", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithAudioMetadata(writer, -1, 2, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("audio zero sample rate", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithAudioMetadata(writer, 0, 2, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("audio nan sample rate", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithAudioMetadata(writer, math.NaN(), 2, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("audio negative output sample rate", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithAudioOutputMetadata(writer, 48000, -1, 2, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("audio zero output sample rate", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithAudioOutputMetadata(writer, 48000, 0, 2, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("audio nan output sample rate", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithAudioOutputMetadata(writer, 48000, math.NaN(), 2, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
}

func TestDemuxerValidatesSegmentInfoCRC32(t *testing.T) {
	t.Run("valid crc32", func(t *testing.T) {
		data := makeInfoMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeInfoWithCRC32(writer, nil)
		})
		demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := demuxer.Info(); got.Title != "crc protected" || got.MuxingApp != "crc-mux" || got.WritingApp != "crc-write" {
			t.Fatalf("info = %+v", got)
		}
	})
	t.Run("crc32 mismatch", func(t *testing.T) {
		data := makeInfoMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeInfoWithCRC32(writer, func(payload []byte) {
				payload[len(payload)-1] ^= 0x01
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("short crc32", func(t *testing.T) {
		data := makeInfoMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeInfoWithElements(writer, func(w *ebml.Writer) error {
				if err := w.WriteHeader(idCRC32, 3); err != nil {
					return err
				}
				_, err := w.Write([]byte{0, 0, 0})
				return err
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("misplaced crc32", func(t *testing.T) {
		data := makeInfoMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeInfoWithElements(writer, func(w *ebml.Writer) error {
				if err := w.WriteString(idTitle, "before crc"); err != nil {
					return err
				}
				return w.WriteCRC32([]byte{})
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
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

func TestDemuxerValidatesTopLevelMetadataCRC32(t *testing.T) {
	tests := []struct {
		name string
		id   ebml.ID
	}{
		{name: "seekhead", id: idSeekHead},
		{name: "tracks", id: idTracks},
		{name: "cues", id: idCues},
		{name: "attachments", id: idAttachments},
		{name: "chapters", id: idChapters},
		{name: "tags", id: idTags},
	}
	for _, tt := range tests {
		t.Run(tt.name+" valid crc32", func(t *testing.T) {
			data := makeMetadataCRCMatroskaData(t, tt.id, nil)
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); err != nil {
				t.Fatal(err)
			}
		})
		t.Run(tt.name+" crc32 mismatch", func(t *testing.T) {
			data := makeMetadataCRCMatroskaData(t, tt.id, func(payload []byte) {
				payload[len(payload)-1] ^= 0x01
			})
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestDemuxerValidatesNestedMetadataCRC32(t *testing.T) {
	tests := []string{
		"seek",
		"track",
		"video",
		"audio",
		"colour",
		"mastering",
		"projection",
		"cuepoint",
		"cuepositions",
		"attachedfile",
		"edition",
		"chapter",
		"chaptertrack",
		"chapterdisplay",
		"tag",
		"targets",
		"simpletag",
		"childsimpletag",
	}
	for _, name := range tests {
		t.Run(name+" valid crc32", func(t *testing.T) {
			data := makeNestedCRCMatroskaData(t, name, nil)
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); err != nil {
				t.Fatal(err)
			}
		})
		t.Run(name+" crc32 mismatch", func(t *testing.T) {
			data := makeNestedCRCMatroskaData(t, name, func(payload []byte) {
				payload[len(payload)-1] ^= 0x01
			})
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestDemuxerRejectsInvalidSegmentInfoMetadata(t *testing.T) {
	t.Run("short segment uuid", func(t *testing.T) {
		data := makeInfoMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeInfoWithElements(writer, func(w *ebml.Writer) error {
				return writeBinary(w, idSegmentUUID, []byte{1, 2, 3})
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("zero segment uuid", func(t *testing.T) {
		data := makeInfoMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeInfoWithElements(writer, func(w *ebml.Writer) error {
				return writeBinary(w, idSegmentUUID, make([]byte, 16))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("invalid date size", func(t *testing.T) {
		data := makeInfoMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeInfoWithElements(writer, func(w *ebml.Writer) error {
				if err := w.WriteHeader(idDateUTC, 4); err != nil {
					return err
				}
				_, err := w.Write([]byte{0, 0, 0, 0})
				return err
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("negative duration", func(t *testing.T) {
		data := makeInfoMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeInfoWithElements(writer, func(w *ebml.Writer) error {
				return w.WriteFloat64(idDuration, -1)
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("non finite duration", func(t *testing.T) {
		data := makeInfoMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeInfoWithElements(writer, func(w *ebml.Writer) error {
				return w.WriteFloat64(idDuration, math.Inf(1))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("duration overflow", func(t *testing.T) {
		data := makeInfoMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeInfoWithElements(writer, func(w *ebml.Writer) error {
				return w.WriteFloat64(idDuration, float64(math.MaxInt64))
			})
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
}

func TestDemuxerRejectsDuplicateKnownMetadataFields(t *testing.T) {
	tests := []struct {
		name string
		data func(testing.TB) []byte
	}{
		{
			name: "info title",
			data: func(tb testing.TB) []byte {
				return makeInfoMetadataMatroskaData(tb, func(writer *ebml.Writer) error {
					return writeInfoWithElements(writer, func(w *ebml.Writer) error {
						if err := w.WriteString(idTitle, "first"); err != nil {
							return err
						}
						return w.WriteString(idTitle, "second")
					})
				})
			},
		},
		{
			name: "attached file filename",
			data: func(tb testing.TB) []byte {
				return makeTopLevelMetadataMatroskaData(tb, func(w *ebml.Writer) error {
					return writeAttachmentsWithAttachedFilePayload(w, func(fw *ebml.Writer) error {
						if err := fw.WriteString(idFileName, "one.txt"); err != nil {
							return err
						}
						if err := fw.WriteString(idFileName, "two.txt"); err != nil {
							return err
						}
						if err := fw.WriteString(idFileMediaType, "text/plain"); err != nil {
							return err
						}
						if err := writeBinary(fw, idFileData, []byte("payload")); err != nil {
							return err
						}
						return fw.WriteUInt(idFileUID, 1)
					})
				})
			},
		},
		{
			name: "edition flag",
			data: func(tb testing.TB) []byte {
				return makeTopLevelMetadataMatroskaData(tb, func(w *ebml.Writer) error {
					return writeChaptersWithEditionPayload(w, func(ew *ebml.Writer) error {
						if err := ew.WriteUInt(idEditionFlagDefault, 0); err != nil {
							return err
						}
						if err := ew.WriteUInt(idEditionFlagDefault, 1); err != nil {
							return err
						}
						return ew.WriteElement(idChapterAtom, crcChapterPayload(tb, "", nil))
					})
				})
			},
		},
		{
			name: "chapter time start",
			data: func(tb testing.TB) []byte {
				return makeTopLevelMetadataMatroskaData(tb, func(w *ebml.Writer) error {
					return writeChaptersWithEditionPayload(w, func(ew *ebml.Writer) error {
						return ew.WriteElement(idChapterAtom, duplicateChapterTimeStartPayload(tb))
					})
				})
			},
		},
		{
			name: "chapter display string",
			data: func(tb testing.TB) []byte {
				return makeTopLevelMetadataMatroskaData(tb, func(w *ebml.Writer) error {
					return writeChaptersWithEditionPayload(w, func(ew *ebml.Writer) error {
						return ew.WriteElement(idChapterAtom, duplicateChapterDisplayStringChapterPayload(tb))
					})
				})
			},
		},
		{
			name: "tag targets",
			data: func(tb testing.TB) []byte {
				return makeTopLevelMetadataMatroskaData(tb, func(w *ebml.Writer) error {
					return writeTagsWithTagPayload(w, func(tw *ebml.Writer) error {
						if err := tw.WriteElement(idTargets, validTagTargetsPayload(tb)); err != nil {
							return err
						}
						if err := tw.WriteElement(idTargets, validTagTargetsPayload(tb)); err != nil {
							return err
						}
						return tw.WriteElement(idSimpleTag, validSimpleTagPayload(tb))
					})
				})
			},
		},
		{
			name: "target type",
			data: func(tb testing.TB) []byte {
				return makeTopLevelMetadataMatroskaData(tb, func(w *ebml.Writer) error {
					return writeTagsWithTagPayload(w, func(tw *ebml.Writer) error {
						if err := tw.WriteElement(idTargets, duplicateTagTargetTypePayload(tb)); err != nil {
							return err
						}
						return tw.WriteElement(idSimpleTag, validSimpleTagPayload(tb))
					})
				})
			},
		},
		{
			name: "simple tag name",
			data: func(tb testing.TB) []byte {
				return makeTopLevelMetadataMatroskaData(tb, func(w *ebml.Writer) error {
					return writeTagsWithTagPayload(w, func(tw *ebml.Writer) error {
						if err := tw.WriteElement(idTargets, validTagTargetsPayload(tb)); err != nil {
							return err
						}
						return tw.WriteElement(idSimpleTag, duplicateSimpleTagNamePayload(tb))
					})
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewDemuxer(bytes.NewReader(tt.data(t)), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
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
}

func TestFormatMuxerDemuxerSupportsVorbis(t *testing.T) {
	ctx := context.Background()
	private := validVorbisCodecPrivate()
	stream := av.Stream{
		ID:       "audio",
		Index:    0,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecVorbis,
			Type:       av.MediaAudio,
			SampleRate: 48000,
			Channels:   2,
			ExtraData:  av.Buffer{Bytes: private},
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{0xaa, 0xbb}},
		PTS:      av.Timestamp{Value: 960, Base: stream.TimeBase},
		Duration: av.Duration{Value: 960, Base: stream.TimeBase},
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
	if len(streams) != 1 ||
		streams[0].Codec.ID != av.CodecVorbis ||
		streams[0].Codec.SampleRate != 48000 ||
		streams[0].Codec.Channels != 2 ||
		!bytes.Equal(streams[0].Codec.ExtraData.Bytes, private) {
		t.Fatalf("streams = %+v", streams)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 8)}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady ||
		result.Packet.StreamID != "1" ||
		result.Packet.PTS.Value != 20_000_000 ||
		result.Packet.Duration.Value != 20_000_000 ||
		!bytes.Equal(result.Packet.Payload.Bytes, []byte{0xaa, 0xbb}) {
		t.Fatalf("result = %+v packet=%+v", result, result.Packet)
	}
}

func TestFormatMuxerDemuxerSupportsFLAC(t *testing.T) {
	ctx := context.Background()
	private := validFLACCodecPrivate()
	stream := av.Stream{
		ID:       "audio",
		Index:    0,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecFLAC,
			Type:       av.MediaAudio,
			SampleRate: 48000,
			Channels:   2,
			ExtraData:  av.Buffer{Bytes: private},
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{0xff, 0xf8, 0x69, 0x18}},
		PTS:      av.Timestamp{Value: 960, Base: stream.TimeBase},
		Duration: av.Duration{Value: 960, Base: stream.TimeBase},
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
	if len(streams) != 1 ||
		streams[0].Codec.ID != av.CodecFLAC ||
		streams[0].Codec.SampleRate != 48000 ||
		streams[0].Codec.Channels != 2 ||
		!bytes.Equal(streams[0].Codec.ExtraData.Bytes, private) {
		t.Fatalf("streams = %+v", streams)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 8)}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady ||
		result.Packet.StreamID != "1" ||
		result.Packet.PTS.Value != 20_000_000 ||
		result.Packet.Duration.Value != 20_000_000 ||
		!bytes.Equal(result.Packet.Payload.Bytes, []byte{0xff, 0xf8, 0x69, 0x18}) {
		t.Fatalf("result = %+v packet=%+v", result, result.Packet)
	}
}

func TestFormatMuxerDemuxerSupportsAAC(t *testing.T) {
	ctx := context.Background()
	private := makeAACAudioSpecificConfig(48000, 2)
	stream := av.Stream{
		ID:       "audio",
		Index:    0,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecAAC,
			Type:       av.MediaAudio,
			SampleRate: 48000,
			Channels:   2,
			ExtraData:  av.Buffer{Bytes: private},
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{0x21, 0x10, 0x56, 0xe5}},
		PTS:      av.Timestamp{Value: 960, Base: stream.TimeBase},
		Duration: av.Duration{Value: 960, Base: stream.TimeBase},
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
	if len(streams) != 1 ||
		streams[0].Codec.ID != av.CodecAAC ||
		streams[0].Codec.SampleRate != 48000 ||
		streams[0].Codec.Channels != 2 ||
		!bytes.Equal(streams[0].Codec.ExtraData.Bytes, private) {
		t.Fatalf("streams = %+v", streams)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 8)}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady ||
		result.Packet.StreamID != "1" ||
		result.Packet.PTS.Value != 20_000_000 ||
		result.Packet.Duration.Value != 20_000_000 ||
		!bytes.Equal(result.Packet.Payload.Bytes, []byte{0x21, 0x10, 0x56, 0xe5}) {
		t.Fatalf("result = %+v packet=%+v", result, result.Packet)
	}
}

func TestFormatDemuxerStreamsReturnsExtraDataCopies(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Index:    0,
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: timeNS},
		Codec: av.CodecParameters{
			ID:        av.CodecH264,
			Type:      av.MediaVideo,
			Width:     16,
			Height:    16,
			ExtraData: av.Buffer{Bytes: h264AVCDecoderConfigWithLengthSize(2)},
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: h264AnnexBAccessUnit()},
		PTS:      av.Timestamp{Value: 0, Base: stream.TimeBase},
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
	if len(streams) != 1 || len(streams[0].Codec.ExtraData.Bytes) == 0 {
		t.Fatalf("streams = %+v", streams)
	}
	streams[0].Codec.ExtraData.Bytes[4] = 0xfe

	fresh := demuxer.Streams()
	if len(fresh) != 1 || !bytes.Equal(fresh[0].Codec.ExtraData.Bytes, h264AVCDecoderConfigWithLengthSize(2)) {
		t.Fatalf("fresh streams = %+v", fresh)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, len(h264AnnexBAccessUnit()))}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.Packet.Payload.Bytes, h264AnnexBAccessUnit()) {
		t.Fatalf("payload = %v, want Annex B", result.Packet.Payload.Bytes)
	}
}

func TestFormatMuxerDemuxerSupportsWebRTCCodecs(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{
		{
			ID:       "opus",
			Index:    0,
			Type:     av.MediaAudio,
			TimeBase: av.TimeBase{Num: 1, Den: 1000},
			Codec: av.CodecParameters{
				ID:         av.CodecOpus,
				Type:       av.MediaAudio,
				SampleRate: 48000,
				Channels:   2,
			},
		},
		{
			ID:       "av1",
			Index:    1,
			Type:     av.MediaVideo,
			TimeBase: av.TimeBase{Num: 1, Den: 1000},
			Codec: av.CodecParameters{
				ID:        av.CodecAV1,
				Type:      av.MediaVideo,
				Width:     640,
				Height:    360,
				ExtraData: av.Buffer{Bytes: av1CodecConfig()},
			},
		},
		{
			ID:       "h264",
			Index:    2,
			Type:     av.MediaVideo,
			TimeBase: av.TimeBase{Num: 1, Den: 1000},
			Codec: av.CodecParameters{
				ID:        av.CodecH264,
				Type:      av.MediaVideo,
				Width:     640,
				Height:    360,
				ExtraData: av.Buffer{Bytes: h264AVCDecoderConfig()},
			},
		},
		{
			ID:       "vp9",
			Index:    3,
			Type:     av.MediaVideo,
			TimeBase: av.TimeBase{Num: 1, Den: 1000},
			Codec: av.CodecParameters{
				ID:     av.CodecVP9,
				Type:   av.MediaVideo,
				Width:  640,
				Height: 360,
			},
		},
		{
			ID:       "vp8",
			Index:    4,
			Type:     av.MediaVideo,
			TimeBase: av.TimeBase{Num: 1, Den: 1000},
			Codec: av.CodecParameters{
				ID:     av.CodecVP8,
				Type:   av.MediaVideo,
				Width:  640,
				Height: 360,
			},
		},
	}
	payloads := [][]byte{
		{0x01, 0x02},
		{0x12, 0x00, 0x0a},
		h264AnnexBAccessUnit(),
		{0x83, 0x49, 0x83},
		{0x9d, 0x01, 0x2a},
	}

	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := range streams {
		if err := muxer.Write(ctx, &av.Packet{
			StreamID: streams[i].ID,
			Payload:  av.Buffer{Bytes: payloads[i]},
			PTS:      av.Timestamp{Value: int64(i) * 20, Base: streams[i].TimeBase},
			Keyframe: true,
		}, nil); err != nil {
			t.Fatalf("%s write packet: %v", streams[i].ID, err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer := &FormatDemuxer{}
	if err := demuxer.Open(ctx, format.Input{Reader: bytes.NewReader(buffer.Bytes())}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	gotStreams := demuxer.Streams()
	if len(gotStreams) != len(streams) {
		t.Fatalf("streams = %d, want %d", len(gotStreams), len(streams))
	}
	for i := range streams {
		wantPrivate := streams[i].Codec.ExtraData.Bytes
		if streams[i].Codec.ID == av.CodecOpus && len(wantPrivate) == 0 {
			wantPrivate = expectedOpusHead(streams[i].Codec.Channels, streams[i].Codec.SampleRate)
		}
		if gotStreams[i].Codec.ID != streams[i].Codec.ID || gotStreams[i].Type != streams[i].Type ||
			!bytes.Equal(gotStreams[i].Codec.ExtraData.Bytes, wantPrivate) {
			t.Fatalf("%s stream = %+v, want %+v", streams[i].ID, gotStreams[i], streams[i])
		}
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 16)}}}
	for i := range streams {
		if err := demuxer.ReadInto(ctx, &result); err != nil {
			t.Fatalf("%s read packet: %v", streams[i].ID, err)
		}
		if !result.PacketReady || result.Packet.StreamID != av.StreamID(strconv.Itoa(i+1)) ||
			result.Packet.PTS.Value != int64(i)*20_000_000 ||
			!bytes.Equal(result.Packet.Payload.Bytes, payloads[i]) {
			t.Fatalf("%s result = %+v packet=%+v", streams[i].ID, result, result.Packet)
		}
	}
	if err := demuxer.ReadInto(ctx, &result); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
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

func TestReadPacketRequiresCapacity(t *testing.T) {
	data := makeMatroskaData(t, 1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 1)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
}

func TestReadPacketSmallBufferSkipsSimpleBlock(t *testing.T) {
	data := makeMatroskaData(t, 2)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 1)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, 8)
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 20_000_000 || !bytes.Equal(packet.Data, []byte{1, 2, 3, 4}) {
		t.Fatalf("packet = %+v data=%v", packet, packet.Data)
	}
}

func TestReadPacketSmallBufferSkipsBlockGroup(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
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
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     0,
		DurationNS: 20_000_000,
		Keyframe:   true,
		Data:       []byte{1, 2, 3},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   40_000_000,
		Keyframe: true,
		Data:     []byte{4, 5},
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
	packet := Packet{Data: make([]byte, 0, 1)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, 8)
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 40_000_000 || !bytes.Equal(packet.Data, []byte{4, 5}) {
		t.Fatalf("packet = %+v data=%v", packet, packet.Data)
	}
}

func TestReadPacketSmallBufferRetriesLacedFrame(t *testing.T) {
	frames := [][]byte{{1, 2}, {3}, {4}}
	data := makeLacedMatroskaData(t, simpleBlockLacingXiph, frames, 20_000_000)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 1)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, 8)
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if packet.TimeNS != int64(i)*20_000_000 || !bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet = %+v data=%v", i, packet, packet.Data)
		}
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

func TestWriteH264LacedPacketAllocs(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH264,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
		CodecPrivate:      h264AVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := LacedPacket{
		TrackID:  id,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   [][]byte{h264AnnexBAccessUnit(), h264AnnexBInterFrame()},
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

func TestWriteH265LacedPacketAllocs(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH265,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
		CodecPrivate:      h265HEVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := LacedPacket{
		TrackID:  id,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   [][]byte{h265AnnexBAccessUnit(), h265AnnexBInterFrame()},
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

func TestH264AVCToAnnexBInPlaceAllocs(t *testing.T) {
	tests := []struct {
		name       string
		lengthSize int
		input      []byte
	}{
		{name: "length1", lengthSize: 1, input: h264AVCSampleWithLengthSize1()},
		{name: "length2", lengthSize: 2, input: h264AVCSampleWithLengthSize2()},
	}
	want := h264AnnexBAccessUnit()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := make([]byte, len(want))
			allocs := testing.AllocsPerRun(1000, func() {
				data := buffer[:len(tt.input)]
				copy(data, tt.input)
				out, err := h264AVCToAnnexBInPlace(data, len(want), tt.lengthSize)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(out, want) {
					t.Fatalf("out = %v, want %v", out, want)
				}
			})
			if allocs != 0 {
				t.Fatalf("allocs = %f, want 0", allocs)
			}
		})
	}
}

func TestH265LengthPrefixedToAnnexBInPlaceAllocs(t *testing.T) {
	tests := []struct {
		name       string
		lengthSize int
		input      []byte
	}{
		{name: "length2", lengthSize: 2, input: h265LengthPrefixedSample2()},
	}
	want := h265AnnexBAccessUnit()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := make([]byte, len(want))
			allocs := testing.AllocsPerRun(1000, func() {
				data := buffer[:len(tt.input)]
				copy(data, tt.input)
				out, err := h264AVCToAnnexBInPlace(data, len(want), tt.lengthSize)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(out, want) {
					t.Fatalf("out = %v, want %v", out, want)
				}
			})
			if allocs != 0 {
				t.Fatalf("allocs = %f, want 0", allocs)
			}
		})
	}
}

func TestReadPacketAllocs(t *testing.T) {
	data := makeMatroskaData(t, 1200)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func TestReadH264AVCToAnnexBAllocs(t *testing.T) {
	data := makeH264AVCMatroskaData(t, 1200)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(h264AnnexBAccessUnit()))}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(packet.Data, h264AnnexBAccessUnit()) {
			t.Fatalf("data = %v, want Annex B", packet.Data)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func TestReadH265HEVCToAnnexBAllocs(t *testing.T) {
	data := makeH265HEVCMatroskaData(t, 1200)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(h265AnnexBAccessUnit()))}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(packet.Data, h265AnnexBAccessUnit()) {
			t.Fatalf("data = %v, want Annex B", packet.Data)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func FuzzDemuxer(f *testing.F) {
	f.Add(makeMatroskaData(&testing.T{}, 1))
	f.Add([]byte{0x1a, 0x45, 0xdf, 0xa3, 0x80})
	f.Fuzz(func(t *testing.T, data []byte) {
		demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{MaxElementSize: 1 << 20})
		if err != nil {
			return
		}
		packet := Packet{Data: make([]byte, 0, 64)}
		for i := 0; i < 16; i++ {
			err := demuxer.ReadPacket(&packet)
			if err != nil {
				return
			}
		}
	})
}

func BenchmarkWriteSimpleBlock(b *testing.B) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{TrackID: id, TimeNS: 0, Keyframe: true, Data: make([]byte, 1200)}
	if err := muxer.WritePacket(packet); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(packet.Data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packet.TimeNS = int64(i) * 20_000_000
		if err := muxer.WritePacket(packet); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteXiphLacedSimpleBlock(b *testing.B) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		b.Fatal(err)
	}
	packet := LacedPacket{
		TrackID:  id,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames: [][]byte{
			make([]byte, 400),
			make([]byte, 400),
			make([]byte, 400),
		},
	}
	if err := muxer.WriteLacedPacket(packet); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(1200)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packet.TimeNS = int64(i) * 60_000_000
		if err := muxer.WriteLacedPacket(packet); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSimpleBlock(b *testing.B) {
	data := makeMatroskaData(b, b.N+1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	b.ReportAllocs()
	b.SetBytes(4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := demuxer.ReadPacket(&packet); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadH264AVCToAnnexB(b *testing.B) {
	data := makeH264AVCMatroskaData(b, b.N+1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(h264AnnexBAccessUnit()))}
	b.ReportAllocs()
	b.SetBytes(int64(len(h264AnnexBAccessUnit())))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := demuxer.ReadPacket(&packet); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadH265HEVCToAnnexB(b *testing.B) {
	data := makeH265HEVCMatroskaData(b, b.N+1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(h265AnnexBAccessUnit()))}
	b.ReportAllocs()
	b.SetBytes(int64(len(h265AnnexBAccessUnit())))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := demuxer.ReadPacket(&packet); err != nil {
			b.Fatal(err)
		}
	}
}

func TestDemuxerLargeWebRTCCorpusSeekIndex(t *testing.T) {
	payloads := benchmarkWebRTCPayloads()
	tests := []struct {
		name            string
		data            []byte
		hasDirectCues   bool
		wantPacketIndex bool
	}{
		{
			name:          "relative cues",
			data:          makeBenchmarkWebRTCCorpusSeekableMatroskaData(t, largeWebRTCCorpusRegressionCycles, payloads),
			hasDirectCues: true,
		},
		{
			name:          "cluster only cues",
			data:          makeBenchmarkWebRTCCorpusClusterOnlyCueMatroskaData(t, largeWebRTCCorpusRegressionCycles, payloads),
			hasDirectCues: true,
		},
		{
			name:            "no cues",
			data:            makeBenchmarkWebRTCCorpusNoCueMatroskaData(t, largeWebRTCCorpusRegressionCycles, payloads),
			wantPacketIndex: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertLargeWebRTCCorpusSeekIndex(t, tt.data, payloads, tt.hasDirectCues, tt.wantPacketIndex)
		})
	}
}

func assertLargeWebRTCCorpusSeekIndex(t testing.TB, data []byte, payloads benchmarkWebRTCPayloadSet, hasDirectCues bool, wantPacketIndex bool) {
	t.Helper()
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	for _, cycle := range []int{0, 1, 127, 511, largeWebRTCCorpusRegressionCycles - 1} {
		timeNS := int64(cycle) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadPacketAtTime(timeNS, &packet); err != nil {
			t.Fatalf("read packet at cycle %d: %v", cycle, err)
		}
		if packet.TimeNS != timeNS || packet.DurationNS != benchmarkWebRTCCorpusFrameDurationNS || len(packet.Data) == 0 {
			t.Fatalf("packet at cycle %d = %+v data=%d", cycle, packet, len(packet.Data))
		}
		assertLargeWebRTCCorpusTrackPacket(t, demuxer, CodecOpus, timeNS, payloads.opus, payloads.maxPayload)
		assertLargeWebRTCCorpusTrackPacket(t, demuxer, CodecAV1, timeNS, payloads.av1, payloads.maxPayload)
		assertLargeWebRTCCorpusTrackPacket(t, demuxer, CodecH264, timeNS, payloads.h264, payloads.maxPayload)
		assertLargeWebRTCCorpusTrackPacket(t, demuxer, CodecVP9, timeNS, payloads.vp9, payloads.maxPayload)
		assertLargeWebRTCCorpusTrackPacket(t, demuxer, CodecVP8, timeNS, payloads.vp8, payloads.maxPayload)
	}
	for _, cycle := range []int{0, 127, 511, largeWebRTCCorpusRegressionCycles - 2} {
		targetNS := int64(cycle)*benchmarkWebRTCCorpusFrameDurationNS + benchmarkWebRTCCorpusFrameDurationNS/2
		wantTimeNS := int64(cycle+1) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadPacketAtTime(targetNS, &packet); err != nil {
			t.Fatalf("read packet after midpoint cycle %d: %v", cycle, err)
		}
		if packet.TimeNS != wantTimeNS {
			t.Fatalf("midpoint packet at cycle %d time = %d, want %d", cycle, packet.TimeNS, wantTimeNS)
		}
		if hasDirectCues {
			if err := demuxer.ReadCuedPacketAtTime(targetNS, &packet); err != nil {
				t.Fatalf("read cued packet after midpoint cycle %d: %v", cycle, err)
			}
			if packet.TimeNS != wantTimeNS {
				t.Fatalf("midpoint cued packet at cycle %d time = %d, want %d", cycle, packet.TimeNS, wantTimeNS)
			}
		}
	}
	if wantPacketIndex && !demuxer.packetIndexBuilt {
		t.Fatal("packet index was not built for large cue-free corpus")
	}
}

func assertLargeWebRTCCorpusTrackPacket(t testing.TB, demuxer *Demuxer, codec Codec, timeNS int64, wantData []byte, capacity int) {
	t.Helper()
	trackID := benchmarkTrackIDForCodec(t, demuxer, codec)
	packet := Packet{Data: make([]byte, 0, capacity)}
	if err := demuxer.ReadTrackPacketAtTime(trackID, timeNS, &packet); err != nil {
		t.Fatalf("read %v track packet at %d: %v", codec, timeNS, err)
	}
	if packet.TrackID != trackID ||
		packet.TimeNS != timeNS ||
		packet.DurationNS != benchmarkWebRTCCorpusFrameDurationNS ||
		!packet.Keyframe ||
		!bytes.Equal(packet.Data, wantData) {
		t.Fatalf("%v packet at %d = %+v data=%x, want data=%x", codec, timeNS, packet, packet.Data, wantData)
	}
}

func BenchmarkReadXiphLacedSimpleBlock(b *testing.B) {
	data := makeRepeatedLacedMatroskaData(b, simpleBlockLacingXiph, [][]byte{{1, 2}, {3, 4}, {5, 6}}, b.N/3+1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	b.ReportAllocs()
	b.SetBytes(2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := demuxer.ReadPacket(&packet); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteWebRTCCorpus(b *testing.B) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	tracks := addBenchmarkWebRTCTracks(b, muxer)
	payloads := benchmarkWebRTCPayloads()
	writeBenchmarkWebRTCCorpus(b, muxer, tracks, payloads, 0)
	b.ReportAllocs()
	b.SetBytes(payloads.totalBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeBenchmarkWebRTCCorpus(b, muxer, tracks, payloads, i+1)
	}
}

func BenchmarkReadWebRTCCorpus(b *testing.B) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusMatroskaData(b, benchmarkWebRTCCorpusCycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	cyclesRead := 0
	b.ReportAllocs()
	b.SetBytes(payloads.totalBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cyclesRead == benchmarkWebRTCCorpusCycles {
			reader.Reset(data)
			if err := demuxer.init(&reader, DemuxerOptions{}); err != nil {
				b.Fatal(err)
			}
			cyclesRead = 0
		}
		for frame := 0; frame < benchmarkWebRTCTrackCount; frame++ {
			if err := demuxer.ReadPacket(&packet); err != nil {
				b.Fatal(err)
			}
		}
		cyclesRead++
	}
}

func BenchmarkWriteSeekableWebRTCCorpus(b *testing.B) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusSeekableMatroskaData(b, benchmarkWebRTCCorpusCycles, payloads)
	var writer memoryWriteSeeker
	var muxer Muxer
	b.ReportAllocs()
	b.SetBytes(payloads.totalBytes * int64(benchmarkWebRTCCorpusCycles))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cap(writer.bytes) < len(data) {
			writer.bytes = make([]byte, 0, len(data))
		} else {
			writer.bytes = writer.bytes[:0]
		}
		writer.pos = 0
		writeBenchmarkWebRTCCorpusFile(b, &muxer, &writer, benchmarkWebRTCCorpusCycles, payloads)
	}
}

func BenchmarkReadSeekableWebRTCCorpus(b *testing.B) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusSeekableMatroskaData(b, benchmarkWebRTCCorpusCycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	cyclesRead := 0
	b.ReportAllocs()
	b.SetBytes(payloads.totalBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cyclesRead == benchmarkWebRTCCorpusCycles {
			reader.Reset(data)
			if err := demuxer.init(&reader, DemuxerOptions{}); err != nil {
				b.Fatal(err)
			}
			cyclesRead = 0
		}
		for frame := 0; frame < benchmarkWebRTCTrackCount; frame++ {
			if err := demuxer.ReadPacket(&packet); err != nil {
				b.Fatal(err)
			}
		}
		cyclesRead++
	}
}

func BenchmarkReadPacketAtTimeWebRTCCorpus(b *testing.B) {
	benchmarkReadPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCCorpusCycles)
}

func BenchmarkReadPacketAtTimeLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles)
}

func BenchmarkReadPacketAtTimeNoCuesWebRTCCorpus(b *testing.B) {
	benchmarkReadPacketAtTimeNoCuesWebRTCCorpus(b, benchmarkWebRTCCorpusCycles)
}

func BenchmarkReadPacketAtTimeNoCuesLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadPacketAtTimeNoCuesWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles)
}

func BenchmarkReadTrackPacketAtTimeWebRTCCorpus(b *testing.B) {
	benchmarkReadTrackPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCCorpusCycles, false)
}

func BenchmarkReadTrackPacketAtTimeLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadTrackPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles, false)
}

func BenchmarkReadTrackPacketAtTimeNoCuesWebRTCCorpus(b *testing.B) {
	benchmarkReadTrackPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCCorpusCycles, true)
}

func BenchmarkReadTrackPacketAtTimeNoCuesLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadTrackPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles, true)
}

func BenchmarkReadCuedPacketAtTimeWebRTCCorpus(b *testing.B) {
	benchmarkReadCuedPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCCorpusCycles)
}

func BenchmarkReadCuedPacketAtTimeLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadCuedPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles)
}

func BenchmarkReadClusterOnlyCuedPacketAtTimeWebRTCCorpus(b *testing.B) {
	benchmarkReadClusterOnlyCuedPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCCorpusCycles)
}

func BenchmarkReadClusterOnlyCuedPacketAtTimeLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadClusterOnlyCuedPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles)
}

func benchmarkReadPacketAtTimeWebRTCCorpus(b *testing.B, cycles int) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusSeekableMatroskaData(b, cycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	if err := demuxer.ReadPacketAtTime(0, &packet); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetNS := int64(i%cycles) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadPacketAtTime(targetNS, &packet); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkReadPacketAtTimeNoCuesWebRTCCorpus(b *testing.B, cycles int) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusNoCueMatroskaData(b, cycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetNS := int64(i%cycles) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadPacketAtTime(targetNS, &packet); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkReadTrackPacketAtTimeWebRTCCorpus(b *testing.B, cycles int, noCues bool) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusSeekableMatroskaData(b, cycles, payloads)
	if noCues {
		data = makeBenchmarkWebRTCCorpusNoCueMatroskaData(b, cycles, payloads)
	}
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	trackID := benchmarkTrackIDForCodec(b, demuxer, CodecVP8)
	if err := demuxer.SeekToTrackTime(trackID, 0); err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	if err := demuxer.ReadTrackPacketAtTime(trackID, 0, &packet); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetNS := int64(i%cycles) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadTrackPacketAtTime(trackID, targetNS, &packet); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkReadCuedPacketAtTimeWebRTCCorpus(b *testing.B, cycles int) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusSeekableMatroskaData(b, cycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetNS := int64(i%cycles) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadCuedPacketAtTime(targetNS, &packet); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkReadClusterOnlyCuedPacketAtTimeWebRTCCorpus(b *testing.B, cycles int) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusClusterOnlyCueMatroskaData(b, cycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetNS := int64(i%cycles) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadCuedPacketAtTime(targetNS, &packet); err != nil {
			b.Fatal(err)
		}
	}
}

const benchmarkWebRTCTrackCount = 5
const benchmarkWebRTCCorpusCycles = 256
const benchmarkWebRTCLargeCueCorpusCycles = 4096
const largeWebRTCCorpusRegressionCycles = 1024
const benchmarkWebRTCCorpusFrameDurationNS = 20_000_000
const benchmarkWebRTCCorpusClusterDurationNS = 1_000_000_000

type benchmarkWebRTCTracks struct {
	opus uint32
	av1  uint32
	h264 uint32
	vp9  uint32
	vp8  uint32
}

type benchmarkWebRTCPayloadSet struct {
	opus       []byte
	av1        []byte
	h264       []byte
	vp9        []byte
	vp8        []byte
	totalBytes int64
	maxPayload int
}

func benchmarkWebRTCPayloads() benchmarkWebRTCPayloadSet {
	payloads := benchmarkWebRTCPayloadSet{
		opus: []byte{0xf8, 0xff, 0xfe},
		av1:  av1SequenceHeaderOBU(),
		h264: h264AnnexBAccessUnit(),
		vp9:  repeatedBenchmarkPayload(1200, 0x83),
		vp8:  repeatedBenchmarkPayload(1200, 0x10),
	}
	payloads.vp8[3] = 0x9d
	payloads.vp8[4] = 0x01
	payloads.vp8[5] = 0x2a
	for _, payload := range [][]byte{payloads.opus, payloads.av1, payloads.h264, payloads.vp9, payloads.vp8} {
		payloads.totalBytes += int64(len(payload))
		if len(payload) > payloads.maxPayload {
			payloads.maxPayload = len(payload)
		}
	}
	return payloads
}

func repeatedBenchmarkPayload(size int, seed byte) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = seed + byte(i)
	}
	return payload
}

func benchmarkTrackIDForCodec(tb testing.TB, demuxer *Demuxer, codec Codec) uint32 {
	tb.Helper()
	for i := range demuxer.tracks {
		if demuxer.tracks[i].Codec == codec {
			return demuxer.tracks[i].ID
		}
	}
	tb.Fatalf("benchmark track with codec %v not found", codec)
	return 0
}

func addBenchmarkWebRTCTracks(tb testing.TB, muxer *Muxer) benchmarkWebRTCTracks {
	tb.Helper()
	var tracks benchmarkWebRTCTracks
	var err error
	tracks.opus, err = muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: benchmarkWebRTCCorpusFrameDurationNS,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		tb.Fatal(err)
	}
	tracks.av1, err = muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecAV1,
		CodecPrivate: av1CodecConfig(),
		Video:        VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		tb.Fatal(err)
	}
	tracks.h264, err = muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH264,
		CodecPrivate: h264AVCDecoderConfigWithLengthSize(2),
		Video:        VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		tb.Fatal(err)
	}
	tracks.vp9, err = muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP9,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		tb.Fatal(err)
	}
	tracks.vp8, err = muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return tracks
}

func writeBenchmarkWebRTCCorpus(tb testing.TB, muxer *Muxer, tracks benchmarkWebRTCTracks, payloads benchmarkWebRTCPayloadSet, cycle int) {
	tb.Helper()
	timeNS := int64(cycle) * benchmarkWebRTCCorpusFrameDurationNS
	packets := []Packet{
		{TrackID: tracks.opus, TimeNS: timeNS, DurationNS: benchmarkWebRTCCorpusFrameDurationNS, Keyframe: true, Data: payloads.opus},
		{TrackID: tracks.av1, TimeNS: timeNS, DurationNS: benchmarkWebRTCCorpusFrameDurationNS, Keyframe: true, Data: payloads.av1},
		{TrackID: tracks.h264, TimeNS: timeNS, DurationNS: benchmarkWebRTCCorpusFrameDurationNS, Keyframe: true, Data: payloads.h264},
		{TrackID: tracks.vp9, TimeNS: timeNS, DurationNS: benchmarkWebRTCCorpusFrameDurationNS, Keyframe: true, Data: payloads.vp9},
		{TrackID: tracks.vp8, TimeNS: timeNS, DurationNS: benchmarkWebRTCCorpusFrameDurationNS, Keyframe: true, Data: payloads.vp8},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			tb.Fatalf("write corpus packet %d: %v", i, err)
		}
	}
}

func makeBenchmarkWebRTCCorpusMatroskaData(tb testing.TB, cycles int, payloads benchmarkWebRTCPayloadSet) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	buffer.Grow(benchmarkWebRTCCorpusCapacity(cycles, payloads))
	writeBenchmarkWebRTCCorpusFile(tb, &Muxer{}, &buffer, cycles, payloads)
	return buffer.Bytes()
}

func makeBenchmarkWebRTCCorpusSeekableMatroskaData(tb testing.TB, cycles int, payloads benchmarkWebRTCPayloadSet) []byte {
	tb.Helper()
	writer := memoryWriteSeeker{bytes: make([]byte, 0, benchmarkWebRTCCorpusCapacity(cycles, payloads))}
	writeBenchmarkWebRTCCorpusFile(tb, &Muxer{}, &writer, cycles, payloads)
	return writer.bytes
}

func makeBenchmarkWebRTCCorpusClusterOnlyCueMatroskaData(tb testing.TB, cycles int, payloads benchmarkWebRTCPayloadSet) []byte {
	tb.Helper()
	writer := memoryWriteSeeker{bytes: make([]byte, 0, benchmarkWebRTCCorpusCapacity(cycles, payloads))}
	writeBenchmarkWebRTCCorpusFileWithCueRewrite(tb, &Muxer{}, &writer, cycles, payloads, rewriteClusterOnlyBenchmarkCues)
	return writer.bytes
}

func makeBenchmarkWebRTCCorpusNoCueMatroskaData(tb testing.TB, cycles int, payloads benchmarkWebRTCPayloadSet) []byte {
	tb.Helper()
	writer := memoryWriteSeeker{bytes: make([]byte, 0, benchmarkWebRTCCorpusCapacity(cycles, payloads))}
	writeBenchmarkWebRTCCorpusFileWithOptionsAndCueRewrite(tb, &Muxer{}, &writer, cycles, payloads, MuxerOptions{
		ClusterMaxDurationNS: benchmarkWebRTCCorpusClusterDurationNS,
		CuePolicy:            CuePolicyNone,
	}, nil)
	return writer.bytes
}

func benchmarkWebRTCCorpusCapacity(cycles int, payloads benchmarkWebRTCPayloadSet) int {
	payloadBytes := payloads.totalBytes * int64(cycles)
	metadataBytes := int64(cycles*benchmarkWebRTCTrackCount*256 + 256*1024)
	return int(payloadBytes + metadataBytes)
}

func writeBenchmarkWebRTCCorpusFile(tb testing.TB, muxer *Muxer, writer io.Writer, cycles int, payloads benchmarkWebRTCPayloadSet) {
	tb.Helper()
	writeBenchmarkWebRTCCorpusFileWithCueRewrite(tb, muxer, writer, cycles, payloads, nil)
}

func writeBenchmarkWebRTCCorpusFileWithCueRewrite(tb testing.TB, muxer *Muxer, writer io.Writer, cycles int, payloads benchmarkWebRTCPayloadSet, rewrite func([]CuePoint) []CuePoint) {
	tb.Helper()
	writeBenchmarkWebRTCCorpusFileWithOptionsAndCueRewrite(tb, muxer, writer, cycles, payloads, MuxerOptions{
		ClusterMaxDurationNS: benchmarkWebRTCCorpusClusterDurationNS,
	}, rewrite)
}

func writeBenchmarkWebRTCCorpusFileWithOptionsAndCueRewrite(tb testing.TB, muxer *Muxer, writer io.Writer, cycles int, payloads benchmarkWebRTCPayloadSet, opts MuxerOptions, rewrite func([]CuePoint) []CuePoint) {
	tb.Helper()
	muxer.init(writer, opts)
	tracks := addBenchmarkWebRTCTracks(tb, muxer)
	for i := 0; i < cycles; i++ {
		writeBenchmarkWebRTCCorpus(tb, muxer, tracks, payloads, i)
	}
	if rewrite != nil {
		muxer.cues = rewrite(muxer.cues)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
}

func rewriteClusterOnlyBenchmarkCues(cues []CuePoint) []CuePoint {
	out := make([]CuePoint, 0, len(cues))
	for i := range cues {
		position, ok := firstCueTrackPosition(cues[i])
		if !ok {
			continue
		}
		cue := CuePoint{
			TrackID:         position.TrackID,
			TimeNS:          cues[i].TimeNS,
			ClusterPosition: position.ClusterPosition,
			Positions: []CueTrackPosition{{
				TrackID:         position.TrackID,
				ClusterPosition: position.ClusterPosition,
				DurationNS:      position.DurationNS,
				DurationSet:     position.DurationSet,
			}},
		}
		applyCuePosition(&cue, cue.Positions[0])
		out = append(out, cue)
	}
	return out
}

func makeMatroskaData(tb testing.TB, packets int) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{DocType: "webm"})
	if err != nil {
		tb.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < packets; i++ {
		if err := muxer.WritePacket(Packet{
			TrackID:  id,
			TimeNS:   int64(i) * 20_000_000,
			Keyframe: i == 0,
			Data:     []byte{byte(i), 2, 3, 4},
		}); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeH264AVCMatroskaData(tb testing.TB, packets int) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH264,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: h264AVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		tb.Fatal(err)
	}
	data := h264AnnexBAccessUnit()
	for i := 0; i < packets; i++ {
		if err := muxer.WritePacket(Packet{
			TrackID:  id,
			TimeNS:   int64(i) * 20_000_000,
			Keyframe: i == 0,
			Data:     data,
		}); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeH265HEVCMatroskaData(tb testing.TB, packets int) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH265,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: h265HEVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		tb.Fatal(err)
	}
	data := h265AnnexBAccessUnit()
	for i := 0; i < packets; i++ {
		if err := muxer.WritePacket(Packet{
			TrackID:  id,
			TimeNS:   int64(i) * 20_000_000,
			Keyframe: i == 0,
			Data:     data,
		}); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeRepeatedLacedMatroskaData(tb testing.TB, lacing byte, frames [][]byte, blocks int) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
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
	for i := 0; i < blocks; i++ {
		if err := writeLacedSimpleBlock(muxer.ebml, trackID, lacing, frames); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeLaceFrames(count int, frame []byte) [][]byte {
	frames := make([][]byte, count)
	for i := range frames {
		frames[i] = frame
	}
	return frames
}

func equalInt64s(left []int64, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func equalBlockAdditions(left []BlockAddition, right []BlockAddition) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].ID != right[i].ID || !bytes.Equal(left[i].Data, right[i].Data) {
			return false
		}
	}
	return true
}

func equalBlockAdditionMapping(left BlockAdditionMapping, right BlockAdditionMapping) bool {
	return left.IDValue == right.IDValue &&
		left.Name == right.Name &&
		left.Type == right.Type &&
		bytes.Equal(left.ExtraData, right.ExtraData)
}

func equalTrackTranslate(left TrackTranslate, right TrackTranslate) bool {
	return left.Codec == right.Codec &&
		bytes.Equal(left.TrackID, right.TrackID) &&
		reflect.DeepEqual(left.EditionUIDs, right.EditionUIDs)
}

func equalCuePoint(left CuePoint, right CuePoint) bool {
	if left.TrackID != right.TrackID ||
		left.TimeNS != right.TimeNS ||
		left.ClusterPosition != right.ClusterPosition ||
		left.RelativePosition != right.RelativePosition ||
		left.RelativePositionSet != right.RelativePositionSet ||
		left.DurationNS != right.DurationNS ||
		left.DurationSet != right.DurationSet ||
		left.BlockNumber != right.BlockNumber ||
		left.BlockNumberSet != right.BlockNumberSet ||
		left.CodecStatePosition != right.CodecStatePosition ||
		left.CodecStateSet != right.CodecStateSet ||
		!equalCueReferences(left.References, right.References) ||
		len(left.Positions) != len(right.Positions) {
		return false
	}
	for i := range left.Positions {
		if !equalCueTrackPosition(left.Positions[i], right.Positions[i]) {
			return false
		}
	}
	return true
}

func equalCueTrackPosition(left CueTrackPosition, right CueTrackPosition) bool {
	return left.TrackID == right.TrackID &&
		left.ClusterPosition == right.ClusterPosition &&
		left.RelativePosition == right.RelativePosition &&
		left.RelativePositionSet == right.RelativePositionSet &&
		left.DurationNS == right.DurationNS &&
		left.DurationSet == right.DurationSet &&
		left.BlockNumber == right.BlockNumber &&
		left.BlockNumberSet == right.BlockNumberSet &&
		left.CodecStatePosition == right.CodecStatePosition &&
		left.CodecStateSet == right.CodecStateSet &&
		equalCueReferences(left.References, right.References)
}

func equalCueReferences(left []CueReference, right []CueReference) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func requirePacketTrackIndex(t *testing.T, demuxer *Demuxer, trackID uint32, times []int64) {
	t.Helper()
	entries, ok := demuxer.packetIndexEntriesForTrack(trackID)
	if !ok {
		t.Fatalf("packet track index for track %d was not built", trackID)
	}
	if len(entries) != len(times) {
		t.Fatalf("packet track index for track %d has %d entries, want %d", trackID, len(entries), len(times))
	}
	for i, entryIndex := range entries {
		if entryIndex < 0 || entryIndex >= len(demuxer.packetIndex) {
			t.Fatalf("packet track index %d for track %d points outside packet index: %d", i, trackID, entryIndex)
		}
		entry := demuxer.packetIndex[entryIndex]
		if entry.TrackID != trackID || entry.TimeNS != times[i] {
			t.Fatalf("packet track index %d for track %d = %+v, want time %d", i, trackID, entry, times[i])
		}
	}
}

func expectedOpusHead(channels int, sampleRate int) []byte {
	if channels == 0 {
		channels = 2
	}
	if sampleRate == 0 {
		sampleRate = 48000
	}
	header := make([]byte, 19)
	copy(header, "OpusHead")
	header[8] = 1
	header[9] = byte(channels)
	binary.LittleEndian.PutUint32(header[12:16], uint32(sampleRate))
	return header
}

func expectedOpusHeadWithPreSkip(channels int, sampleRate int, preSkip int) []byte {
	header := expectedOpusHead(channels, sampleRate)
	binary.LittleEndian.PutUint16(header[10:12], uint16(preSkip))
	return header
}

func expectedMSACMWaveFormat(codec Codec, channels int, sampleRate int) []byte {
	tag := waveFormatMuLaw
	if codec == CodecPCMA {
		tag = waveFormatALaw
	}
	return msACMWaveFormatBytes(tag, channels, sampleRate, uint32(sampleRate*channels), channels, 8, nil)
}

func msACMWaveFormatBytes(tag uint16, channels int, sampleRate int, avgBytesPerSec uint32, blockAlign int, bitsPerSample int, extra []byte) []byte {
	private := make([]byte, waveFormatExBaseSize+len(extra))
	binary.LittleEndian.PutUint16(private[0:2], tag)
	binary.LittleEndian.PutUint16(private[2:4], uint16(channels))
	binary.LittleEndian.PutUint32(private[4:8], uint32(sampleRate))
	binary.LittleEndian.PutUint32(private[8:12], avgBytesPerSec)
	binary.LittleEndian.PutUint16(private[12:14], uint16(blockAlign))
	binary.LittleEndian.PutUint16(private[14:16], uint16(bitsPerSample))
	binary.LittleEndian.PutUint16(private[16:18], uint16(len(extra)))
	copy(private[waveFormatExBaseSize:], extra)
	return private
}

func validFLACCodecPrivate() []byte {
	private := make([]byte, 4+4+34)
	copy(private, "fLaC")
	private[4] = 0x80
	private[7] = 34
	streamInfo := private[8:]
	binary.BigEndian.PutUint16(streamInfo[0:2], 4096)
	binary.BigEndian.PutUint16(streamInfo[2:4], 4096)
	writeFLACStreamInfoAudio(streamInfo, 48000, 2, 16)
	return private
}

func flacCodecPrivateWithHeaderByte(offset int, value byte) []byte {
	private := validFLACCodecPrivate()
	private[offset] = value
	return private
}

func flacCodecPrivateWithStreamInfoBytes(offset int, values ...byte) []byte {
	private := validFLACCodecPrivate()
	copy(private[8+offset:], values)
	return private
}

func writeFLACStreamInfoAudio(streamInfo []byte, sampleRate int, channels int, bitsPerSample int) {
	channelsMinusOne := channels - 1
	bitsMinusOne := bitsPerSample - 1
	streamInfo[10] = byte(sampleRate >> 12)
	streamInfo[11] = byte(sampleRate >> 4)
	streamInfo[12] = byte((sampleRate&0x0f)<<4 | (channelsMinusOne&0x07)<<1 | (bitsMinusOne>>4)&0x01)
	streamInfo[13] = byte(bitsMinusOne << 4)
}

func makeAACAudioSpecificConfig(sampleRate int, channels int) []byte {
	index := aacSamplingFrequencyIndex(sampleRate)
	if index < 0 || channels < 1 || channels > 7 {
		panic("invalid AAC test config")
	}
	channelConfig := channels
	if channels == 8 {
		channelConfig = 7
	}
	value := uint16(2<<11 | index<<7 | channelConfig<<3)
	return []byte{byte(value >> 8), byte(value)}
}

func aacSamplingFrequencyIndex(sampleRate int) int {
	for i, rate := range aacSamplingFrequencies {
		if rate == sampleRate {
			return i
		}
	}
	return -1
}

func validVorbisCodecPrivate() []byte {
	identification := make([]byte, 30)
	identification[0] = 1
	copy(identification[1:], "vorbis")
	identification[11] = 2
	binary.LittleEndian.PutUint32(identification[12:16], 48000)
	identification[28] = 0xb6
	identification[29] = 1
	comment := []byte{3, 'v', 'o', 'r', 'b', 'i', 's'}
	setup := []byte{5, 'v', 'o', 'r', 'b', 'i', 's', 0}
	return vorbisCodecPrivateWithHeaders(identification, comment, setup)
}

func vorbisCodecPrivateWithIdentificationByte(offset int, value byte) []byte {
	private := validVorbisCodecPrivate()
	idStart := 3
	private[idStart+offset] = value
	return private
}

func vorbisCodecPrivateWithIdentificationBytes(offset int, values ...byte) []byte {
	private := validVorbisCodecPrivate()
	idStart := 3
	copy(private[idStart+offset:], values)
	return private
}

func vorbisCodecPrivateWithHeaders(headers ...[]byte) []byte {
	if len(headers) == 0 {
		return nil
	}
	private := []byte{byte(len(headers) - 1)}
	for i := 0; i < len(headers)-1; i++ {
		size := len(headers[i])
		for size >= 255 {
			private = append(private, 255)
			size -= 255
		}
		private = append(private, byte(size))
	}
	for i := range headers {
		private = append(private, headers[i]...)
	}
	return private
}

func av1CodecConfig() []byte {
	private := []byte{0x81, 0x05, 0x10, 0x00}
	return append(private, av1SequenceHeaderOBU()...)
}

func av1CodecConfigWithByte(index int, value byte) []byte {
	private := av1CodecConfig()
	private[index] = value
	return private
}

func av1CodecConfigWithPrefixOBU(obu []byte) []byte {
	private := []byte{0x81, 0x05, 0x10, 0x00}
	private = append(private, obu...)
	private = append(private, av1SequenceHeaderOBU()...)
	return private
}

func av1SequenceHeaderOBU() []byte {
	return []byte{0x0a, 0x06, 0x19, 0x5d, 0xc3, 0xc3, 0xda, 0x44}
}

func av1TemporalDelimiterOBU() []byte {
	return []byte{0x12, 0x00}
}

func h264AVCDecoderConfig() []byte {
	sps := h264SPS()
	pps := h264PPS()
	private := make([]byte, 0, 11+len(sps)+len(pps))
	private = append(private, 0x01, sps[1], sps[2], sps[3], 0xff, 0xe1)
	private = binary.BigEndian.AppendUint16(private, uint16(len(sps)))
	private = append(private, sps...)
	private = append(private, 0x01)
	private = binary.BigEndian.AppendUint16(private, uint16(len(pps)))
	private = append(private, pps...)
	return private
}

func h264SPS() []byte {
	return []byte{0x67, 0x42, 0x00, 0x0a, 0xf8, 0x41, 0xa2}
}

func h264PPS() []byte {
	return []byte{0x68, 0xce, 0x0f, 0x2c, 0x80}
}

func h264AVCDecoderConfigWithLengthSize(lengthSize int) []byte {
	private := h264AVCDecoderConfig()
	private[4] = 0xfc | byte(lengthSize-1)
	return private
}

func h264AVCDecoderConfigWithByte(index int, value byte) []byte {
	private := h264AVCDecoderConfig()
	private[index] = value
	return private
}

func h264AnnexBAccessUnit() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x99, 0x00, 0x00, 0x00, 0x01, 0x41, 0x9a}
}

func h264AnnexBParameterAccessUnit() []byte {
	var data []byte
	data = append(data, h264StartCode[:]...)
	data = append(data, h264SPS()...)
	data = append(data, h264StartCode[:]...)
	data = append(data, h264PPS()...)
	data = append(data, h264StartCode[:]...)
	data = append(data, 0x65, 0x88, 0x99)
	return data
}

func h264AnnexBInterFrame() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x41, 0xab, 0xcd}
}

func h264AVCSampleWithLengthSize2() []byte {
	return []byte{0x00, 0x03, 0x65, 0x88, 0x99, 0x00, 0x02, 0x41, 0x9a}
}

func h264AVCSampleWithLengthSize1() []byte {
	return []byte{0x03, 0x65, 0x88, 0x99, 0x02, 0x41, 0x9a}
}

func h264AVCSampleWithParameterSetsLengthSize4() []byte {
	var data []byte
	for _, nalu := range [][]byte{h264SPS(), h264PPS(), []byte{0x65, 0x88, 0x99}} {
		data = binary.BigEndian.AppendUint32(data, uint32(len(nalu)))
		data = append(data, nalu...)
	}
	return data
}

func h264AVCInterFrameWithLengthSize2() []byte {
	return []byte{0x00, 0x03, 0x41, 0xab, 0xcd}
}

func h265HEVCDecoderConfig() []byte {
	vps := h265VPS()
	sps := h265SPS()
	pps := h265PPS()
	private := []byte{
		0x01,
		0x01,
		0x60, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x78,
		0xf0, 0x00,
		0xfc,
		0xfd,
		0xf8,
		0xf8,
		0x00, 0x00,
		0x0f,
		0x03,
	}
	private = appendHEVCConfigArray(private, h265NALUVPS, vps)
	private = appendHEVCConfigArray(private, h265NALUSPS, sps)
	private = appendHEVCConfigArray(private, h265NALUPPS, pps)
	return private
}

func appendHEVCConfigArray(private []byte, naluType int, nalu []byte) []byte {
	private = append(private, 0x80|byte(naluType))
	private = binary.BigEndian.AppendUint16(private, 1)
	private = binary.BigEndian.AppendUint16(private, uint16(len(nalu)))
	return append(private, nalu...)
}

func h265HEVCDecoderConfigWithLengthSize(lengthSize int) []byte {
	private := h265HEVCDecoderConfig()
	private[21] = (private[21] & 0xfc) | byte(lengthSize-1)
	return private
}

func h265HEVCDecoderConfigWithByte(index int, value byte) []byte {
	private := h265HEVCDecoderConfig()
	private[index] = value
	return private
}

func h265VPS() []byte {
	return []byte{0x40, 0x01, 0x0c, 0x01, 0xff}
}

func h265SPS() []byte {
	return []byte{
		0x42, 0x01,
		0x01,
		0x01,
		0x60, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x78,
	}
}

func h265PPS() []byte {
	return []byte{0x44, 0x01, 0xc0}
}

func h265AnnexBAccessUnit() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x26, 0x01, 0x88, 0x99, 0x00, 0x00, 0x00, 0x01, 0x02, 0x01, 0xab}
}

func h265AnnexBParameterAccessUnit() []byte {
	var data []byte
	for _, nalu := range [][]byte{h265VPS(), h265SPS(), h265PPS(), []byte{0x26, 0x01, 0x88, 0x99}} {
		data = append(data, h264StartCode[:]...)
		data = append(data, nalu...)
	}
	return data
}

func h265AnnexBInterFrame() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x02, 0x01, 0xcd}
}

func h265LengthPrefixedSample2() []byte {
	return []byte{0x00, 0x04, 0x26, 0x01, 0x88, 0x99, 0x00, 0x03, 0x02, 0x01, 0xab}
}

func h265LengthPrefixedInterFrame2() []byte {
	return []byte{0x00, 0x03, 0x02, 0x01, 0xcd}
}

func h265LengthPrefixedParameterSample4() []byte {
	var data []byte
	for _, nalu := range [][]byte{h265VPS(), h265SPS(), h265PPS(), []byte{0x26, 0x01, 0x88, 0x99}} {
		data = binary.BigEndian.AppendUint32(data, uint32(len(nalu)))
		data = append(data, nalu...)
	}
	return data
}

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
	for attempt := 0; attempt < 8; attempt++ {
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
	for attempt := 0; attempt < 8; attempt++ {
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

func writeTracksWithTrackNumber(writer *ebml.Writer, trackNumber uint64) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, trackNumber); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithFlagValue(writer *ebml.Writer, flagID ebml.ID, value uint64) error {
	return writeTracksWithTrackMetadata(writer, trackUIntElement{flagID, value})
}

type trackUIntElement struct {
	id    ebml.ID
	value uint64
}

func writeTracksWithTrackMetadata(writer *ebml.Writer, elements ...trackUIntElement) error {
	return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
		for i := range elements {
			if err := ew.WriteUInt(elements[i].id, elements[i].value); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeTracksWithTrackExtra(writer *ebml.Writer, writeExtra func(*ebml.Writer) error) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if writeExtra != nil {
		if err := writeExtra(ew); err != nil {
			return err
		}
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

type trackEntryFixture struct {
	Number           uint64
	UID              uint64
	Type             uint64
	CodecID          string
	NumberSet        bool
	UIDSet           bool
	TypeSet          bool
	CodecIDSet       bool
	MediaSet         bool
	DuplicateNumber  bool
	DuplicateUID     bool
	DuplicateType    bool
	DuplicateCodecID bool
	DuplicateMedia   bool
}

func defaultTrackEntryFixture() trackEntryFixture {
	return trackEntryFixture{
		Number:     1,
		UID:        1,
		Type:       matroskaTrackVideo,
		CodecID:    codecIDVP8,
		NumberSet:  true,
		UIDSet:     true,
		TypeSet:    true,
		CodecIDSet: true,
		MediaSet:   true,
	}
}

func writeTracksWithTrackEntryFixtures(writer *ebml.Writer, entries ...trackEntryFixture) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	for i := range entries {
		var entry bytes.Buffer
		ew := ebml.NewWriter(&entry)
		if err := writeTrackEntryFixture(ew, entries[i]); err != nil {
			return err
		}
		if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
			return err
		}
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTrackEntryFixture(w *ebml.Writer, entry trackEntryFixture) error {
	if entry.NumberSet {
		if err := w.WriteUInt(idTrackNumber, entry.Number); err != nil {
			return err
		}
		if entry.DuplicateNumber {
			if err := w.WriteUInt(idTrackNumber, entry.Number); err != nil {
				return err
			}
		}
	}
	if entry.UIDSet {
		if err := w.WriteUInt(idTrackUID, entry.UID); err != nil {
			return err
		}
		if entry.DuplicateUID {
			if err := w.WriteUInt(idTrackUID, entry.UID); err != nil {
				return err
			}
		}
	}
	if entry.TypeSet {
		if err := w.WriteUInt(idTrackType, entry.Type); err != nil {
			return err
		}
		if entry.DuplicateType {
			if err := w.WriteUInt(idTrackType, entry.Type); err != nil {
				return err
			}
		}
	}
	if entry.CodecIDSet {
		if err := w.WriteString(idCodecID, entry.CodecID); err != nil {
			return err
		}
		if entry.DuplicateCodecID {
			if err := w.WriteString(idCodecID, entry.CodecID); err != nil {
				return err
			}
		}
	}
	if entry.MediaSet {
		if err := writeTrackEntryFixtureMedia(w, entry.Type); err != nil {
			return err
		}
		if entry.DuplicateMedia {
			if err := writeTrackEntryFixtureMedia(w, entry.Type); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeTrackEntryFixtureMedia(w *ebml.Writer, trackType uint64) error {
	switch trackType {
	case matroskaTrackAudio:
		return writeAudio(w, AudioConfig{SampleRate: 48000, Channels: 2})
	case matroskaTrackVideo:
		return writeVideo(w, VideoConfig{Width: 16, Height: 16})
	default:
		return nil
	}
}

func trackTranslatePayload(t testing.TB, writeID bool, writeCodec bool) []byte {
	t.Helper()
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if writeID {
		if err := writeBinary(tw, idTrackTranslateTrack, []byte{0x01}); err != nil {
			t.Fatal(err)
		}
	}
	if writeCodec {
		if err := tw.WriteUInt(idTrackTranslateCodec, 1); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func contentEncodingsPayload(t testing.TB, encodings ...[]byte) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	for i := range encodings {
		if err := writer.WriteElement(idContentEncoding, encodings[i]); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func contentEncodingPayload(t testing.TB, write func(*ebml.Writer) error) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if write != nil {
		if err := write(writer); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func contentCompressionPayload(t testing.TB, algorithm uint64, settings []byte) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idContentCompAlgo, algorithm); err != nil {
		t.Fatal(err)
	}
	if settings != nil {
		if err := writeBinary(writer, idContentCompSettings, settings); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

const contentAESCipherUnset = ^uint64(0)

func contentEncryptionPayload(t testing.TB, algorithm uint64, keyID []byte, cipherMode uint64) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idContentEncAlgo, algorithm); err != nil {
		t.Fatal(err)
	}
	if keyID != nil {
		if err := writeBinary(writer, idContentEncKeyID, keyID); err != nil {
			t.Fatal(err)
		}
	}
	if cipherMode != contentAESCipherUnset {
		var aesPayload bytes.Buffer
		aesWriter := ebml.NewWriter(&aesPayload)
		if err := aesWriter.WriteUInt(idContentEncAESCipher, cipherMode); err != nil {
			t.Fatal(err)
		}
		if err := writer.WriteElement(idContentEncAES, aesPayload.Bytes()); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := writer.WriteElement(idContentEncAES, nil); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func blockAdditionMappingPayload(t testing.TB, id uint64, writeID bool, typ uint64, extraData []byte) []byte {
	t.Helper()
	var payload bytes.Buffer
	mw := ebml.NewWriter(&payload)
	if writeID {
		if err := mw.WriteUInt(idBlockAddIDValue, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := mw.WriteUInt(idBlockAddIDType, typ); err != nil {
		t.Fatal(err)
	}
	if extraData != nil {
		if err := writeBinary(mw, idBlockAddIDExtraData, extraData); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func writeTracksWithVideoDimensions(writer *ebml.Writer, width uint64, height uint64) error {
	return writeTracksWithVideoElements(writer,
		videoUIntElement{idPixelWidth, width},
		videoUIntElement{idPixelHeight, height},
	)
}

type videoUIntElement struct {
	id    ebml.ID
	value uint64
}

func writeTracksWithVideoElements(writer *ebml.Writer, elements ...videoUIntElement) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	var video bytes.Buffer
	vw := ebml.NewWriter(&video)
	for i := range elements {
		if err := vw.WriteUInt(elements[i].id, elements[i].value); err != nil {
			return err
		}
	}
	if err := ew.WriteElement(idVideo, video.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

type colourUIntElement struct {
	id    ebml.ID
	value uint64
}

type colourFloatElement struct {
	id    ebml.ID
	value float64
}

func writeTracksWithVideoColourElements(writer *ebml.Writer, elements ...colourUIntElement) error {
	return writeTracksWithVideoColourMetadata(writer, elements, nil)
}

func writeTracksWithVideoMasteringFloatElements(writer *ebml.Writer, elements ...colourFloatElement) error {
	return writeTracksWithVideoColourMetadata(writer, nil, elements)
}

func writeTracksWithVideoColourMetadata(writer *ebml.Writer, uints []colourUIntElement, floats []colourFloatElement) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	var video bytes.Buffer
	vw := ebml.NewWriter(&video)
	if err := vw.WriteUInt(idPixelWidth, 16); err != nil {
		return err
	}
	if err := vw.WriteUInt(idPixelHeight, 16); err != nil {
		return err
	}
	var colour bytes.Buffer
	cw := ebml.NewWriter(&colour)
	for i := range uints {
		if err := cw.WriteUInt(uints[i].id, uints[i].value); err != nil {
			return err
		}
	}
	if len(floats) != 0 {
		var mastering bytes.Buffer
		mw := ebml.NewWriter(&mastering)
		for i := range floats {
			if err := mw.WriteFloat64(floats[i].id, floats[i].value); err != nil {
				return err
			}
		}
		if err := cw.WriteElement(idMasteringMetadata, mastering.Bytes()); err != nil {
			return err
		}
	}
	if err := vw.WriteElement(idColour, colour.Bytes()); err != nil {
		return err
	}
	if err := ew.WriteElement(idVideo, video.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

type projectionUIntElement struct {
	id    ebml.ID
	value uint64
}

type projectionFloatElement struct {
	id    ebml.ID
	value float64
}

func writeTracksWithVideoProjectionMetadata(writer *ebml.Writer, uints []projectionUIntElement, floats []projectionFloatElement, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	var video bytes.Buffer
	vw := ebml.NewWriter(&video)
	if err := vw.WriteUInt(idPixelWidth, 16); err != nil {
		return err
	}
	if err := vw.WriteUInt(idPixelHeight, 16); err != nil {
		return err
	}
	var projection bytes.Buffer
	pw := ebml.NewWriter(&projection)
	for i := range uints {
		if err := pw.WriteUInt(uints[i].id, uints[i].value); err != nil {
			return err
		}
	}
	for i := range floats {
		if err := pw.WriteFloat64(floats[i].id, floats[i].value); err != nil {
			return err
		}
	}
	if len(private) != 0 {
		if err := writeBinary(pw, idProjectionPrivate, private); err != nil {
			return err
		}
	}
	if err := vw.WriteElement(idProjection, projection.Bytes()); err != nil {
		return err
	}
	if err := ew.WriteElement(idVideo, video.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithAudioMetadata(writer *ebml.Writer, sampleRate float64, channels uint64, bitDepth uint64) error {
	return writeTracksWithAudioMetadataValues(writer, sampleRate, 0, false, channels, bitDepth)
}

func writeTracksWithAudioOutputMetadata(writer *ebml.Writer, sampleRate float64, outputSampleRate float64, channels uint64, bitDepth uint64) error {
	return writeTracksWithAudioMetadataValues(writer, sampleRate, outputSampleRate, true, channels, bitDepth)
}

func writeTracksWithAudioMetadataValues(writer *ebml.Writer, sampleRate float64, outputSampleRate float64, writeOutputSampleRate bool, channels uint64, bitDepth uint64) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "A_OPUS"); err != nil {
		return err
	}
	var audio bytes.Buffer
	aw := ebml.NewWriter(&audio)
	if err := aw.WriteFloat64(idSamplingFreq, sampleRate); err != nil {
		return err
	}
	if writeOutputSampleRate {
		if err := aw.WriteFloat64(idOutputFreq, outputSampleRate); err != nil {
			return err
		}
	}
	if err := aw.WriteUInt(idChannels, channels); err != nil {
		return err
	}
	if err := aw.WriteUInt(idBitDepth, bitDepth); err != nil {
		return err
	}
	if err := ew.WriteElement(idAudio, audio.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithAudioElements(writer *ebml.Writer, writeAudioElements func(*ebml.Writer) error) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDOpus); err != nil {
		return err
	}
	var audio bytes.Buffer
	aw := ebml.NewWriter(&audio)
	if writeAudioElements != nil {
		if err := writeAudioElements(aw); err != nil {
			return err
		}
	}
	if err := ew.WriteElement(idAudio, audio.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithOpusPrivate(writer *ebml.Writer, private []byte) error {
	return writeTracksWithOpusPrivateAndTiming(writer, private, 0, 0)
}

func writeTracksWithMSACMPrivate(writer *ebml.Writer, private []byte, audioConfig AudioConfig) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDMS); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeAudio(ew, audioConfig); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithOpusPrivateAndTiming(writer *ebml.Writer, private []byte, codecDelayNS uint64, seekPreRollNS uint64) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "A_OPUS"); err != nil {
		return err
	}
	if codecDelayNS != 0 {
		if err := ew.WriteUInt(idCodecDelay, codecDelayNS); err != nil {
			return err
		}
	}
	if seekPreRollNS != 0 {
		if err := ew.WriteUInt(idSeekPreRoll, seekPreRollNS); err != nil {
			return err
		}
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeAudio(ew, AudioConfig{SampleRate: 48000, Channels: 2}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithAV1Private(writer *ebml.Writer, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDAV1); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithH264Private(writer *ebml.Writer, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDH264); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithH265Private(writer *ebml.Writer, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDH265); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeCuesWithTrackNumber(writer *ebml.Writer, trackNumber uint64) error {
	var cues bytes.Buffer
	cw := ebml.NewWriter(&cues)
	var point bytes.Buffer
	pw := ebml.NewWriter(&point)
	if err := pw.WriteUInt(idCueTime, 0); err != nil {
		return err
	}
	var positions bytes.Buffer
	tw := ebml.NewWriter(&positions)
	if err := tw.WriteUInt(idCueTrack, trackNumber); err != nil {
		return err
	}
	if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
		return err
	}
	if err := pw.WriteElement(idCueTrackPositions, positions.Bytes()); err != nil {
		return err
	}
	if err := cw.WriteElement(idCuePoint, point.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idCues, cues.Bytes())
}

func writeSimpleBlockWithTrackNumber(writer *ebml.Writer, trackNumber uint64, frame []byte) error {
	payload, err := blockPayloadWithTrackNumber(trackNumber, simpleBlockKeyframe, frame)
	if err != nil {
		return err
	}
	return writer.WriteElement(idSimpleBlock, payload)
}

func writeBlockWithTrackNumber(writer *ebml.Writer, trackNumber uint64, frame []byte) error {
	payload, err := blockPayloadWithTrackNumber(trackNumber, 0, frame)
	if err != nil {
		return err
	}
	return writer.WriteElement(idBlock, payload)
}

func blockPayloadWithTrackNumber(trackNumber uint64, flags byte, frame []byte) ([]byte, error) {
	var payload bytes.Buffer
	var scratch [ebml.MaxSizeWidth]byte
	n, err := ebml.EncodeUnsignedVINT(scratch[:], trackNumber)
	if err != nil {
		return nil, err
	}
	payload.Write(scratch[:n])
	var blockHeader [3]byte
	blockHeader[2] = flags
	payload.Write(blockHeader[:])
	payload.Write(frame)
	return payload.Bytes(), nil
}

func writeLacedSimpleBlock(writer *ebml.Writer, trackID uint32, lacing byte, frames [][]byte) error {
	return writeLacedBlockElement(writer, idSimpleBlock, trackID, simpleBlockKeyframe|lacing, frames)
}

func writeLacedBlockElement(writer *ebml.Writer, id ebml.ID, trackID uint32, flags byte, frames [][]byte) error {
	var payload bytes.Buffer
	var scratch [ebml.MaxSizeWidth]byte
	n, err := ebml.EncodeUnsignedVINT(scratch[:], uint64(trackID))
	if err != nil {
		return err
	}
	payload.Write(scratch[:n])
	var blockHeader [3]byte
	binary.BigEndian.PutUint16(blockHeader[:2], 0)
	blockHeader[2] = flags
	payload.Write(blockHeader[:])
	if len(frames) < 2 || len(frames) > 256 {
		return ErrInvalidData
	}
	payload.WriteByte(byte(len(frames) - 1))
	lacing := flags & simpleBlockLacingMask
	switch lacing {
	case simpleBlockLacingXiph:
		for i := 0; i < len(frames)-1; i++ {
			size := len(frames[i])
			for size >= 255 {
				payload.WriteByte(255)
				size -= 255
			}
			payload.WriteByte(byte(size))
		}
	case simpleBlockLacingFixed:
		size := len(frames[0])
		for i := range frames {
			if len(frames[i]) != size {
				return ErrInvalidData
			}
		}
	case simpleBlockLacingEBML:
		n, err := ebml.EncodeUnsignedVINT(scratch[:], uint64(len(frames[0])))
		if err != nil {
			return err
		}
		payload.Write(scratch[:n])
	default:
		return ErrUnsupportedLacing
	}
	if lacing == simpleBlockLacingEBML {
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
	}
	for i := range frames {
		payload.Write(frames[i])
	}
	return writer.WriteElement(id, payload.Bytes())
}

func encodeSignedLaceVINT(dst []byte, value int64) (int, error) {
	for width := 1; width <= ebml.MaxSizeWidth; width++ {
		bias := int64((uint64(1) << uint(7*width-1)) - 1)
		encoded := value + bias
		if encoded < 0 {
			continue
		}
		if uint64(encoded) <= (uint64(1)<<uint(7*width))-2 {
			return ebml.EncodeUnsignedVINTWidth(dst, uint64(encoded), width)
		}
	}
	return 0, ErrInvalidData
}

func ebmlReader(tb testing.TB, data []byte) *ebml.Reader {
	tb.Helper()
	return ebml.NewReader(bytes.NewReader(data), ebml.ReaderOptions{})
}

func countClusters(tb testing.TB, data []byte) int {
	tb.Helper()
	reader := ebmlReader(tb, data)
	header, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if header.ID != idEBML {
		tb.Fatalf("first id = 0x%x, want EBML", uint64(header.ID))
	}
	if err := reader.Skip(header.Size.Value); err != nil {
		tb.Fatal(err)
	}
	segment, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if segment.ID != idSegment {
		tb.Fatalf("second id = 0x%x, want Segment", uint64(segment.ID))
	}
	clusters := 0
	for {
		child, err := reader.ReadHeader()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			tb.Fatal(err)
		}
		if child.ID == idCluster {
			clusters++
			if child.Size.Unknown {
				continue
			}
		}
		if child.Size.Unknown {
			break
		}
		if err := reader.Skip(child.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return clusters
}

func collectTopLevelPositions(tb testing.TB, data []byte) map[ebml.ID]uint64 {
	tb.Helper()
	reader := ebmlReader(tb, data)
	header, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if header.ID != idEBML {
		tb.Fatalf("first id = 0x%x, want EBML", uint64(header.ID))
	}
	if err := reader.Skip(header.Size.Value); err != nil {
		tb.Fatal(err)
	}
	segment, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if segment.ID != idSegment || segment.Size.Unknown {
		tb.Fatalf("segment = %+v, want known-size Segment", segment)
	}
	positions := make(map[ebml.ID]uint64)
	segmentEnd := segment.DataOffset + int64(segment.Size.Value)
	for reader.Offset() < segmentEnd {
		child, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		positions[child.ID] = uint64(child.Offset - segment.DataOffset)
		if child.Size.Unknown {
			tb.Fatalf("unexpected unknown top-level element %+v", child)
		}
		if err := reader.Skip(child.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return positions
}

func assertTopLevelMasterStartsWithCRC32(tb testing.TB, data []byte, id ebml.ID) {
	tb.Helper()
	payload := topLevelElementPayload(tb, data, id)
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	child, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if child.ID != idCRC32 || child.Size.Unknown || child.Size.Value != 4 {
		tb.Fatalf("top-level 0x%x first child = %+v, want CRC32", uint64(id), child)
	}
}

func topLevelElementPayload(tb testing.TB, data []byte, id ebml.ID) []byte {
	tb.Helper()
	reader := ebmlReader(tb, data)
	header, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if header.ID != idEBML {
		tb.Fatalf("first id = 0x%x, want EBML", uint64(header.ID))
	}
	if err := reader.Skip(header.Size.Value); err != nil {
		tb.Fatal(err)
	}
	segment, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if segment.ID != idSegment {
		tb.Fatalf("segment = %+v, want Segment", segment)
	}
	var segmentEnd int64
	if !segment.Size.Unknown {
		segmentEnd = segment.DataOffset + int64(segment.Size.Value)
	}
	for segment.Size.Unknown || reader.Offset() < segmentEnd {
		child, err := reader.ReadHeader()
		if err != nil {
			if segment.Size.Unknown && errors.Is(err, io.EOF) {
				break
			}
			tb.Fatal(err)
		}
		if child.ID == id {
			if child.Size.Unknown || child.Size.Value > uint64(math.MaxInt) {
				tb.Fatalf("top-level 0x%x has invalid size %+v", uint64(id), child.Size)
			}
			payload := make([]byte, int(child.Size.Value))
			if err := reader.ReadFull(payload); err != nil {
				tb.Fatal(err)
			}
			return payload
		}
		if child.Size.Unknown {
			tb.Fatalf("unexpected unknown top-level child %+v while looking for 0x%x", child, uint64(id))
		}
		if err := reader.Skip(child.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	tb.Fatalf("missing top-level element 0x%x", uint64(id))
	return nil
}

func assertSeekEntry(tb testing.TB, entries []SeekEntry, id ebml.ID, position uint64) {
	tb.Helper()
	for i := range entries {
		if entries[i].ID == uint64(id) {
			if entries[i].Position != position {
				tb.Fatalf("seek entry 0x%x position = %d, want %d", uint64(id), entries[i].Position, position)
			}
			return
		}
	}
	tb.Fatalf("missing seek entry 0x%x in %+v", uint64(id), entries)
}

func readInfoDuration(tb testing.TB, payload []byte) (float64, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	for reader.Offset() < int64(len(payload)) {
		header, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == idDuration {
			if header.Size.Value != 8 {
				tb.Fatalf("duration size = %d, want 8", header.Size.Value)
			}
			var scratch [8]byte
			if err := reader.ReadFull(scratch[:]); err != nil {
				tb.Fatal(err)
			}
			return math.Float64frombits(binary.BigEndian.Uint64(scratch[:])), true
		}
		if header.Size.Unknown {
			tb.Fatalf("unexpected unknown Info child: %+v", header)
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return 0, false
}

func firstBlockFlags(tb testing.TB, data []byte, id ebml.ID) byte {
	tb.Helper()
	flags, ok := firstBlockFlagsInPayload(tb, data, id)
	if !ok {
		tb.Fatalf("missing block 0x%x", uint64(id))
	}
	return flags
}

func firstBlockFlagsInPayload(tb testing.TB, payload []byte, id ebml.ID) (byte, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	for reader.Offset() < int64(len(payload)) {
		header, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == id {
			if header.Size.Unknown || header.Size.Value > uint64(len(payload)) {
				tb.Fatalf("invalid block size %+v", header.Size)
			}
			blockPayload := make([]byte, int(header.Size.Value))
			if err := reader.ReadFull(blockPayload); err != nil {
				tb.Fatal(err)
			}
			return rawBlockFlags(tb, blockPayload), true
		}
		if header.Size.Unknown {
			if isBlockSearchMaster(header.ID) {
				flags, ok := firstBlockFlagsInPayload(tb, payload[reader.Offset():], id)
				if ok {
					return flags, true
				}
			}
			return 0, false
		}
		if header.Size.Value > uint64(len(payload)) {
			tb.Fatalf("invalid element size %+v", header.Size)
		}
		if isBlockSearchMaster(header.ID) {
			childPayload := make([]byte, int(header.Size.Value))
			if err := reader.ReadFull(childPayload); err != nil {
				tb.Fatal(err)
			}
			flags, ok := firstBlockFlagsInPayload(tb, childPayload, id)
			if ok {
				return flags, true
			}
			continue
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return 0, false
}

func isBlockSearchMaster(id ebml.ID) bool {
	switch id {
	case idSegment, idCluster, idBlockGroup:
		return true
	default:
		return false
	}
}

func rawBlockFlags(tb testing.TB, payload []byte) byte {
	tb.Helper()
	reader := bytes.NewReader(payload)
	var scratch [ebml.MaxSizeWidth]byte
	if _, _, err := ebml.ReadUnsignedVINT(reader, &scratch); err != nil {
		tb.Fatal(err)
	}
	var header [3]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		tb.Fatal(err)
	}
	return header[2]
}

func readFirstDiscardPadding(tb testing.TB, data []byte) (int64, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(data), ebml.ReaderOptions{})
	for {
		header, err := reader.ReadHeader()
		if errors.Is(err, io.EOF) {
			return 0, false
		}
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == idBlockGroup {
			return readDiscardPaddingFromBlockGroup(tb, data[header.DataOffset:header.DataOffset+int64(header.Size.Value)])
		}
		if header.Size.Unknown {
			continue
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
}

func readDiscardPaddingFromBlockGroup(tb testing.TB, payload []byte) (int64, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	for reader.Offset() < int64(len(payload)) {
		header, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == idDiscardPad {
			value, err := readIntPayload(reader, header.Size.Value)
			if err != nil {
				tb.Fatal(err)
			}
			return value, true
		}
		if header.Size.Unknown {
			tb.Fatalf("unexpected unknown BlockGroup child: %+v", header)
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return 0, false
}

func unknownElementBytes(tb testing.TB, id ebml.ID, payload []byte) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writer.WriteElement(id, payload); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func assertUnknownElement(tb testing.TB, name string, elements []UnknownElement, id ebml.ID, raw []byte) {
	tb.Helper()
	if len(elements) != 1 {
		tb.Fatalf("%s unknown elements = %d, want 1", name, len(elements))
	}
	if elements[0].ID != uint64(id) || !bytes.Equal(elements[0].Raw, raw) {
		tb.Fatalf("%s unknown element = %+v raw=%x, want id=0x%x raw=%x", name, elements[0], elements[0].Raw, uint64(id), raw)
	}
}

type discardWriter struct{}

func (discardWriter) Write(payload []byte) (int, error) {
	return len(payload), nil
}

type memoryWriteSeeker struct {
	bytes []byte
	pos   int64
}

func (m *memoryWriteSeeker) Write(p []byte) (int, error) {
	end := m.pos + int64(len(p))
	if end > int64(len(m.bytes)) {
		oldLen := len(m.bytes)
		endLen := int(end)
		if endLen <= cap(m.bytes) {
			m.bytes = m.bytes[:endLen]
			clear(m.bytes[oldLen:])
		} else {
			nextCap := cap(m.bytes) * 2
			if nextCap < endLen {
				nextCap = endLen
			}
			next := make([]byte, endLen, nextCap)
			copy(next, m.bytes)
			m.bytes = next
		}
	}
	copy(m.bytes[m.pos:end], p)
	m.pos = end
	return len(p), nil
}

func (m *memoryWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += m.pos
	case io.SeekEnd:
		offset += int64(len(m.bytes))
	default:
		return 0, errors.New("invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("negative offset")
	}
	m.pos = offset
	return offset, nil
}

var errFailingWriter = errors.New("failing writer")

type failToggleWriteSeeker struct {
	memoryWriteSeeker
	fail bool
}

func (m *failToggleWriteSeeker) Write(payload []byte) (int, error) {
	if m.fail {
		return 0, errFailingWriter
	}
	return m.memoryWriteSeeker.Write(payload)
}

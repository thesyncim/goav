package matroska

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/ebml"
	"github.com/thesyncim/goav/format"
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

func TestDemuxerRejectsDuplicateTopLevelSegmentMetadata(t *testing.T) {
	tests := []struct {
		name          string
		writeMetadata func(*ebml.Writer) error
	}{
		{
			name: "third seekhead",
			writeMetadata: func(w *ebml.Writer) error {
				for range 3 {
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

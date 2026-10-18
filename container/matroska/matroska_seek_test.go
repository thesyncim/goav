package matroska

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/container/ebml"
)

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

package matroska

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/goav/container/ebml"
)

type failingWriter struct {
	buf       bytes.Buffer
	limit     int
	err       error
	failAfter bool
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.failAfter && w.buf.Len() >= w.limit {
		return 0, w.err
	}
	if w.failAfter && w.buf.Len()+len(p) > w.limit {
		allow := w.limit - w.buf.Len()
		if allow > 0 {
			w.buf.Write(p[:allow])
		}
		return allow, w.err
	}
	return w.buf.Write(p)
}

func TestMuxerAddTrackAfterCloseAndFailed(t *testing.T) {
	var buf bytes.Buffer
	m, err := NewMuxer(&buf, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	}); !errors.Is(err, ErrClosed) {
		t.Fatalf("AddTrack after Close = %v, want ErrClosed", err)
	}

	wantErr := errors.New("write boom")
	fw := &failingWriter{limit: 10, err: wantErr, failAfter: true}
	m2, err := NewMuxer(fw, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tid, err := m2.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.WritePacket(Packet{TrackID: tid, TimeNS: 0, Keyframe: true, Data: []byte{1, 2, 3}}); !errors.Is(err, wantErr) {
		t.Fatalf("WritePacket = %v, want %v", err, wantErr)
	}
	if _, err := m2.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	}); !errors.Is(err, wantErr) {
		t.Fatalf("AddTrack after failure = %v, want %v", err, wantErr)
	}
}

func TestMuxerFailedWriteHeaderDoesNotDuplicateEBMLHeader(t *testing.T) {
	wantErr := errors.New("header write failed")
	// Allow enough bytes for EBML header (~35 bytes) but fail mid-header before Tracks completes.
	fw := &failingWriter{limit: 40, err: wantErr, failAfter: true}
	m, err := NewMuxer(fw, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tid, err := m.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.WritePacket(Packet{TrackID: tid, TimeNS: 0, Keyframe: true, Data: []byte{1, 2, 3}}); !errors.Is(err, wantErr) {
		t.Fatalf("first WritePacket = %v, want %v", err, wantErr)
	}
	writtenAfterFail := fw.buf.Len()
	// Lift failure; sticky error must still prevent duplicate EBML header or Close succeeding.
	fw.failAfter = false
	if err := m.WritePacket(Packet{TrackID: tid, TimeNS: 40_000_000, Keyframe: true, Data: []byte{4, 5, 6}}); !errors.Is(err, wantErr) {
		t.Fatalf("second WritePacket = %v, want sticky %v", err, wantErr)
	}
	if err := m.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close after failed header = %v, want sticky %v", err, wantErr)
	}
	if fw.buf.Len() != writtenAfterFail {
		t.Fatalf("bytes written grew from %d to %d after failure", writtenAfterFail, fw.buf.Len())
	}
}

func TestMuxerFailedStartClusterPreventsOrphanBlocks(t *testing.T) {
	// First measure exact header length on a clean buffer.
	var probe bytes.Buffer
	pm, err := NewMuxer(&probe, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pm.AddTrack(Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 16, Height: 16}}); err != nil {
		t.Fatal(err)
	}
	if err := pm.Close(); err != nil {
		t.Fatal(err)
	}
	headerLen := probe.Len()

	wantErr := errors.New("cluster start failed")
	// Fail right after header + partial cluster header.
	fw := &failingWriter{limit: headerLen + 4, err: wantErr, failAfter: true}
	m, err := NewMuxer(fw, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tid, err := m.AddTrack(Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 16, Height: 16}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.WritePacket(Packet{TrackID: tid, TimeNS: 0, Keyframe: true, Data: []byte{1, 2, 3}}); !errors.Is(err, wantErr) {
		t.Fatalf("WritePacket = %v, want %v", err, wantErr)
	}
	writtenAfterFail := fw.buf.Len()
	fw.failAfter = false
	if err := m.WritePacket(Packet{TrackID: tid, TimeNS: 40_000_000, Keyframe: true, Data: []byte{4, 5, 6}}); !errors.Is(err, wantErr) {
		t.Fatalf("second WritePacket = %v, want sticky %v", err, wantErr)
	}
	if fw.buf.Len() != writtenAfterFail {
		t.Fatalf("orphan bytes written after failed startCluster (%d -> %d)", writtenAfterFail, fw.buf.Len())
	}
}

func TestMuxerFailedCloseIsSticky(t *testing.T) {
	wantErr := errors.New("close failed")
	fw := &failingWriter{limit: 0, err: wantErr, failAfter: false}
	m, err := NewMuxer(fw, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.AddTrack(Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 16, Height: 16}}); err != nil {
		t.Fatal(err)
	}
	// Fail during Close's writeHeader.
	fw.limit = 10
	fw.failAfter = true
	if err := m.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("first Close = %v, want %v", err, wantErr)
	}
	fw.failAfter = false
	if err := m.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("second Close = %v, want sticky %v", err, wantErr)
	}
}

func TestDemuxerUnknownSizedClusterExitsOnTerminator(t *testing.T) {
	// Build a streaming-mode Matroska stream (unknown-sized Segment + unknown-sized Cluster),
	// then append top-level Cues and Tags directly after the unknown-sized Cluster.
	var buf bytes.Buffer
	m, err := NewMuxer(&buf, MuxerOptions{Streaming: true})
	if err != nil {
		t.Fatal(err)
	}
	tid, err := m.AddTrack(Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 16, Height: 16}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.WritePacket(Packet{TrackID: tid, TimeNS: 0, Keyframe: true, Data: []byte{0x10, 0x20}}); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// Append top-level Cues and Tags elements after the unknown-sized Cluster.
	w := ebml.NewWriter(&buf)
	var cuesPayload bytes.Buffer
	cw := ebml.NewWriter(&cuesPayload)
	if err := writeCuePoint(cw, CuePoint{
		TimeNS: 0,
		Positions: []CueTrackPosition{{
			TrackID:         tid,
			ClusterPosition: 0,
		}},
	}, defaultTimecodeScaleNS); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteElement(idCues, cuesPayload.Bytes()); err != nil {
		t.Fatal(err)
	}

	var tagsPayload bytes.Buffer
	tw := ebml.NewWriter(&tagsPayload)
	if err := writeTag(tw, Tag{
		Target: TagTarget{TypeValue: 50},
		Simple: []SimpleTag{{Name: "TITLE", String: "live", StringSet: true, Language: "und", Default: true, DefaultSet: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteElement(idTags, tagsPayload.Bytes()); err != nil {
		t.Fatal(err)
	}

	d, err := NewDemuxer(bytes.NewReader(buf.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pkt := Packet{Data: make([]byte, 0, 64)}
	if err := d.ReadPacket(&pkt); err != nil {
		t.Fatalf("ReadPacket(0) = %v", err)
	}
	if len(pkt.UnknownClusterElements) != 0 {
		t.Fatalf("unexpected UnknownClusterElements: %+v", pkt.UnknownClusterElements)
	}
	if err := d.ReadPacket(&pkt); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadPacket(1) = %v, want io.EOF", err)
	}
	if len(d.Cues()) != 1 {
		t.Fatalf("Cues() count = %d, want 1 (should not be swallowed by unknown-sized cluster)", len(d.Cues()))
	}
	if len(d.Tags()) != 1 || d.Tags()[0].Simple[0].String != "live" {
		t.Fatalf("Tags() = %+v, want TITLE=live", d.Tags())
	}
}

func TestDemuxerEnsureIndexesRestoreSeekerOnLookupMiss(t *testing.T) {
	ws := &memoryWriteSeeker{}
	m, err := NewMuxer(ws, MuxerOptions{CuePolicy: CuePolicyNone})
	if err != nil {
		t.Fatal(err)
	}
	tid, err := m.AddTrack(Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 16, Height: 16}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.WritePacket(Packet{TrackID: tid, TimeNS: 0, Keyframe: true, Data: []byte{1, 2}}); err != nil {
		t.Fatal(err)
	}
	if err := m.WritePacket(Packet{TrackID: tid, TimeNS: 40_000_000, Keyframe: false, Data: []byte{3, 4}}); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	d, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pkt := Packet{Data: make([]byte, 0, 64)}
	if err := d.ReadPacket(&pkt); err != nil {
		t.Fatal(err)
	}
	savedOffset := d.reader.Offset()

	// Trigger ensurePacketIndex with a timestamp beyond EOF so index build succeeds
	// while the subsequent lookup fails.
	if _, err := d.packetAtOrAfterTime(1_000_000_000); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("packetAtOrAfterTime(1s) = %v, want ErrInvalidData", err)
	}
	pos, err := d.seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if pos != savedOffset {
		t.Fatalf("seeker offset = %d, want restored %d", pos, savedOffset)
	}

	// Sequential ReadPacket must still read packet 1 cleanly (not EOF).
	if err := d.ReadPacket(&pkt); err != nil {
		t.Fatalf("ReadPacket after missed index lookup = %v, want nil", err)
	}
	if pkt.TimeNS != 40_000_000 || !bytes.Equal(pkt.Data, []byte{3, 4}) {
		t.Fatalf("got packet time=%d data=%x, want 40ms [3,4]", pkt.TimeNS, pkt.Data)
	}
}

func TestDemuxerSeekToClusterPositionInvalidHeaderPreservesCursor(t *testing.T) {
	ws := &memoryWriteSeeker{}
	m, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 10_000_000})
	if err != nil {
		t.Fatal(err)
	}
	tid, err := m.AddTrack(Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 16, Height: 16}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.WritePacket(Packet{TrackID: tid, TimeNS: 0, Keyframe: true, Data: []byte{0xaa}}); err != nil {
		t.Fatal(err)
	}
	if err := m.WritePacket(Packet{TrackID: tid, TimeNS: 100_000_000, Keyframe: true, Data: []byte{0xbb}}); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	d, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pkt := Packet{Data: make([]byte, 0, 64)}
	if err := d.ReadPacket(&pkt); err != nil {
		t.Fatal(err)
	}
	wantOffset := d.reader.Offset()
	wantCluster := d.cluster
	wantTimecode := d.clusterTimecode

	// Position 0 in Segment is SeekHead/Info, not a Cluster.
	if _, err := d.seekToClusterPosition(0); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("seekToClusterPosition(0) = %v, want ErrInvalidData", err)
	}
	if d.reader.Offset() != wantOffset || d.cluster != wantCluster || d.clusterTimecode != wantTimecode {
		t.Fatalf("cursor mutated on failed seekToClusterPosition: offset=%d (want %d), cluster=%d (want %d), tc=%d (want %d)",
			d.reader.Offset(), wantOffset, d.cluster, wantCluster, d.clusterTimecode, wantTimecode)
	}
	if err := d.ReadPacket(&pkt); err != nil {
		t.Fatalf("ReadPacket after failed seekToClusterPosition = %v", err)
	}
	if pkt.TimeNS != 100_000_000 || !bytes.Equal(pkt.Data, []byte{0xbb}) {
		t.Fatalf("got packet time=%d data=%x, want 100ms [bb]", pkt.TimeNS, pkt.Data)
	}
}

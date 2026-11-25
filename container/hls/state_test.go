package hls_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/container/hls"
	"github.com/thesyncim/goav/provider"
)

// box builds a minimal ISO BMFF box with the given 4-byte type and payload.
func box(typ string, payload ...byte) []byte {
	b := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(b[0:4], uint32(len(b)))
	copy(b[4:8], typ)
	copy(b[8:], payload)
	return b
}

// fullBox builds a FullBox (4-byte version+flags followed by payload).
func fullBox(typ string, version byte, flags uint32, payload ...byte) []byte {
	p := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(p[0:4], uint32(version)<<24|(flags&0xffffff))
	copy(p[4:], payload)
	return box(typ, p...)
}

// testMoov builds a minimal moov with a single video track (id=1, timescale=1000).
func testMoov() []byte {
	tkhdPayload := make([]byte, 20)
	binary.BigEndian.PutUint32(tkhdPayload[8:12], 1) // track ID = 1 (v0 offset 12 from payload start = 8 after ver+flags)
	tkhd := fullBox("tkhd", 0, 3, tkhdPayload...)

	mdhdPayload := make([]byte, 20)
	binary.BigEndian.PutUint32(mdhdPayload[8:12], 1000) // timescale = 1000
	mdhd := fullBox("mdhd", 0, 0, mdhdPayload...)

	hdlrPayload := make([]byte, 12)
	copy(hdlrPayload[4:8], "vide")
	hdlr := fullBox("hdlr", 0, 0, hdlrPayload...)

	mdia := box("mdia", append(mdhd, hdlr...)...)
	trak := box("trak", append(tkhd, mdia...)...)
	return box("moov", trak...)
}

// testMoof builds a minimal moof for track 1 with base decode time and single sample duration (in ms).
func testMoof(baseMS, durMS uint32, sync bool) []byte {
	tfhd := fullBox("tfhd", 0, 0, 0, 0, 0, 1) // track ID = 1
	tfdtPayload := make([]byte, 4)
	binary.BigEndian.PutUint32(tfdtPayload, baseMS)
	tfdt := fullBox("tfdt", 0, 0, tfdtPayload...)

	// trun flags: 0x4 (first-sample-flags) | 0x100 (sample-duration)
	trunPayload := make([]byte, 12)
	binary.BigEndian.PutUint32(trunPayload[0:4], 1) // sample count = 1
	if !sync {
		binary.BigEndian.PutUint32(trunPayload[4:8], 0x00010000) // non-sync
	}
	binary.BigEndian.PutUint32(trunPayload[8:12], durMS)
	trun := fullBox("trun", 0, 0x104, trunPayload...)

	traf := box("traf", append(append(tfhd, tfdt...), trun...)...)
	return box("moof", traf...)
}

func testFtyp() []byte {
	return box("ftyp", 'i', 's', 'o', '6', 0, 0, 0, 0)
}

func testMdat(n int) []byte {
	return box("mdat", make([]byte, n)...)
}

type failStore struct {
	mu      sync.Mutex
	files   map[string][]byte
	creates map[string]int
	failOn  string
	failErr error
}

func newFailStore() *failStore {
	return &failStore{files: map[string][]byte{}, creates: map[string]int{}}
}

func (s *failStore) Create(name string) (io.WriteCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creates[name]++
	if s.failOn != "" && name == s.failOn {
		return nil, s.failErr
	}
	return &failStoreFile{store: s, name: name}, nil
}

func (s *failStore) Remove(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, name)
	return nil
}

type failStoreFile struct {
	store *failStore
	name  string
	buf   []byte
}

func (f *failStoreFile) Write(p []byte) (int, error) {
	f.buf = append(f.buf, p...)
	return len(p), nil
}

func (f *failStoreFile) Close() error {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	f.store.files[f.name] = append([]byte(nil), f.buf...)
	return nil
}

func TestWriterWriteAfterCloseFailsAndDoubleCloseIdempotent(t *testing.T) {
	store := newFailStore()
	dest := hls.New("vod.m3u8", store, hls.SegmentDuration(2*time.Second))
	w, err := dest.Open(context.Background(), provider.Info{})
	if err != nil {
		t.Fatal(err)
	}
	var stream []byte
	stream = append(stream, testFtyp()...)
	stream = append(stream, testMoov()...)
	stream = append(stream, testMoof(0, 1000, true)...)
	stream = append(stream, testMdat(16)...)
	if _, err := w.Write(stream); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	playlistBefore := string(store.files["vod.m3u8"])
	if !strings.Contains(playlistBefore, "#EXT-X-ENDLIST\n") || !strings.Contains(playlistBefore, "#EXT-X-PLAYLIST-TYPE:VOD\n") {
		t.Fatalf("expected finalized VOD playlist, got:\n%s", playlistBefore)
	}
	createsBefore := store.creates["vod.m3u8"]

	// Double Close must be an idempotent no-op.
	if err := w.Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}
	if store.creates["vod.m3u8"] != createsBefore {
		t.Fatalf("second Close() republished playlist (%d -> %d)", createsBefore, store.creates["vod.m3u8"])
	}

	// Write after Close must fail and leave the VOD playlist intact.
	more := append(testMoof(1000, 1000, true), testMdat(16)...)
	if _, err := w.Write(more); err == nil {
		t.Fatalf("Write() after Close() succeeded, want error")
	}
	if got := string(store.files["vod.m3u8"]); got != playlistBefore {
		t.Fatalf("Write() after Close() mutated playlist:\ngot:\n%s\nwant:\n%s", got, playlistBefore)
	}
}

func TestWriterCloseAfterFtypWithoutMoovNoPlaylist(t *testing.T) {
	store := newFailStore()
	dest := hls.New("vod.m3u8", store, hls.SegmentDuration(2*time.Second))
	w, err := dest.Open(context.Background(), provider.Info{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(testFtyp()); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	if _, ok := store.files["vod.m3u8"]; ok {
		t.Fatalf("playlist was published without an init segment:\n%s", store.files["vod.m3u8"])
	}
	if _, ok := store.files["vod-init.mp4"]; ok {
		t.Fatalf("init segment should not exist without moov")
	}
}

func TestWriterCloseFinishSegmentErrorIsSticky(t *testing.T) {
	store := newFailStore()
	dest := hls.New("vod.m3u8", store, hls.SegmentDuration(2*time.Second))
	w, err := dest.Open(context.Background(), provider.Info{})
	if err != nil {
		t.Fatal(err)
	}
	var stream []byte
	stream = append(stream, testFtyp()...)
	stream = append(stream, testMoov()...)
	stream = append(stream, testMoof(0, 1000, true)...)
	stream = append(stream, testMdat(16)...)
	if _, err := w.Write(stream); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("disk full")
	store.failOn = "vod-00000.m4s"
	store.failErr = wantErr

	if err := w.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("first Close() = %v, want %v", err, wantErr)
	}
	// Clear injected error and retry Close: error must remain sticky.
	store.failOn = ""
	if err := w.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("retry Close() = %v, want sticky %v", err, wantErr)
	}
}

func TestWriterUnpairedMoofDoesNotAdvanceSegmentEnd(t *testing.T) {
	store := newFailStore()
	dest := hls.New("vod.m3u8", store, hls.SegmentDuration(4*time.Second))
	w, err := dest.Open(context.Background(), provider.Info{})
	if err != nil {
		t.Fatal(err)
	}
	var stream []byte
	stream = append(stream, testFtyp()...)
	stream = append(stream, testMoov()...)
	// Committed fragment: 0..1000ms.
	stream = append(stream, testMoof(0, 1000, true)...)
	stream = append(stream, testMdat(16)...)
	// Unpaired moof: 1000..3000ms with no mdat.
	stream = append(stream, testMoof(1000, 2000, false)...)
	if _, err := w.Write(stream); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	playlist := string(store.files["vod.m3u8"])
	segments := extinf.FindAllStringSubmatch(playlist, -1)
	if len(segments) != 1 || segments[0][1] != "1.000" {
		t.Fatalf("segments = %q, want single segment of 1.000s (playlist:\n%s)", segments, playlist)
	}
}

func TestWriterInitialUnpairedMoofProducesNoEmptySegment(t *testing.T) {
	store := newFailStore()
	dest := hls.New("vod.m3u8", store, hls.SegmentDuration(4*time.Second))
	w, err := dest.Open(context.Background(), provider.Info{})
	if err != nil {
		t.Fatal(err)
	}
	var stream []byte
	stream = append(stream, testFtyp()...)
	stream = append(stream, testMoov()...)
	stream = append(stream, testMoof(0, 1000, true)...) // no mdat
	if _, err := w.Write(stream); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.files["vod-00000.m4s"]; ok {
		t.Fatalf("unexpected empty segment published for unpaired moof")
	}
}

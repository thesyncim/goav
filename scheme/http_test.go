package scheme_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/provider"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/scheme"
)

// indexAtEndMP4 is the MP4 fixture remuxed progressively (ftyp, mdat, moov)
// with a pad-byte free box before the moov, so reading the index means
// seeking far past the samples and then back.
func indexAtEndMP4(t *testing.T, pad int) []byte {
	t.Helper()
	src, err := os.ReadFile("../container/mp4/testdata/h264_aac.mp4")
	if err != nil {
		t.Fatal(err)
	}
	var out seekBuffer
	if err := bundle.Run(context.Background(), goav.FileInput("in.mp4", bytes.NewReader(src)).To(goav.Write("out.mp4", &out))); err != nil {
		t.Fatal(err)
	}
	data := out.buf
	moov := -1
	for off := 0; off+8 <= len(data); {
		size := int(binary.BigEndian.Uint32(data[off:]))
		if size == 1 {
			size = int(binary.BigEndian.Uint64(data[off+8:]))
		}
		if string(data[off+4:off+8]) == "moov" {
			moov = off
			break
		}
		if size < 8 {
			t.Fatalf("bad box at %d", off)
		}
		off += size
	}
	if moov < 0 || !bytes.Contains(data[:moov], []byte("mdat")) {
		t.Fatal("remuxed fixture does not end with its moov")
	}
	free := make([]byte, pad)
	binary.BigEndian.PutUint32(free, uint32(pad))
	copy(free[4:], "free")
	return append(append(append([]byte(nil), data[:moov]...), free...), data[moov:]...)
}

type seekBuffer struct {
	buf []byte
	pos int
}

func (b *seekBuffer) Write(p []byte) (int, error) {
	if need := b.pos + len(p); need > len(b.buf) {
		b.buf = append(b.buf, make([]byte, need-len(b.buf))...)
	}
	copy(b.buf[b.pos:], p)
	b.pos += len(p)
	return len(p), nil
}

func (b *seekBuffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		offset += int64(b.pos)
	case io.SeekEnd:
		offset += int64(len(b.buf))
	}
	b.pos = int(offset)
	return offset, nil
}

// digest demuxes an input's video and audio and fingerprints every packet.
func digest(t *testing.T, rt *goav.Runtime, in goav.InputSpec) (string, error) {
	t.Helper()
	video, audio := goavtest.NewCollector(), goavtest.NewCollector()
	if err := rt.Run(context.Background(), in.Video().To(video.Sink()), in.Audio().To(audio.Sink())); err != nil {
		return "", err
	}
	sum := sha256.New()
	for _, collector := range []*goavtest.Collector{video, audio} {
		for _, packet := range collector.Packets() {
			fmt.Fprintf(sum, "%s/%d/%d/", packet.StreamID, packet.PTS.Value, len(packet.Payload.Bytes))
			sum.Write(packet.Payload.Bytes)
		}
	}
	if len(video.Packets()) == 0 || len(audio.Packets()) == 0 {
		t.Fatalf("video=%d audio=%d packets, want both", len(video.Packets()), len(audio.Packets()))
	}
	return fmt.Sprintf("%x", sum.Sum(nil)), nil
}

// fileServer serves one resource and records every request.
type fileServer struct {
	mu       sync.Mutex
	data     []byte
	etag     string
	ranges   bool
	requests []*http.Request
	served   int64
}

func (s *fileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests = append(s.requests, r.Clone(context.Background()))
	data, etag, ranges := s.data, s.etag, s.ranges
	s.mu.Unlock()
	counted := &countingWriter{ResponseWriter: w, server: s}
	if !ranges {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("ETag", etag)
		counted.Write(data)
		return
	}
	w.Header().Set("ETag", etag)
	http.ServeContent(counted, r, "movie.mp4", time.Unix(1700000000, 0), bytes.NewReader(data))
}

func (s *fileServer) log() (requests []*http.Request, served int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*http.Request(nil), s.requests...), s.served
}

type countingWriter struct {
	http.ResponseWriter
	server *fileServer
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.server.mu.Lock()
	w.server.served += int64(n)
	w.server.mu.Unlock()
	return n, err
}

// TestHTTPRangedReadSeeksAnIndexAtTheEnd pins the ranged path: an MP4 whose
// index sits past a 1 MiB gap demuxes exactly like the local file, through
// range requests that skip the gap, each after the first carrying If-Range.
func TestHTTPRangedReadSeeksAnIndexAtTheEnd(t *testing.T) {
	data := indexAtEndMP4(t, 1<<20)
	want, err := digest(t, bundle.Default(), goav.FileInput("local.mp4", bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	server := &fileServer{data: data, etag: `"v1"`, ranges: true}
	srv := httptest.NewServer(server)
	defer srv.Close()

	got, err := digest(t, bundle.Default(), goav.URIInput(srv.URL+"/movie.mp4?token=SECRET"))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatal("ranged HTTP read demuxed different packets than the local file")
	}
	requests, served := server.log()
	if served >= int64(len(data)) {
		t.Fatalf("served %d of %d bytes: the gap was downloaded", served, len(data))
	}
	far := false
	for i, req := range requests {
		rng := req.Header.Get("Range")
		if !strings.HasPrefix(rng, "bytes=") {
			t.Fatalf("request %d has no Range: %v", i, req.Header)
		}
		if i > 0 && req.Header.Get("If-Range") != `"v1"` {
			t.Fatalf("request %d If-Range = %q, want the first ETag", i, req.Header.Get("If-Range"))
		}
		start, _ := strconv.Atoi(strings.TrimPrefix(strings.Split(rng, "-")[0], "bytes="))
		far = far || start > 1<<20
	}
	if !far {
		t.Fatalf("no range reached the index: %d requests", len(requests))
	}
}

// TestHTTPProgressiveReadWithoutRanges pins the fallback: a server that
// ignores Range still serves the index-at-end MP4: read forward to the index
// and again from the start, since the samples fell out of the window.
func TestHTTPProgressiveReadWithoutRanges(t *testing.T) {
	data := indexAtEndMP4(t, 3<<20)
	want, err := digest(t, bundle.Default(), goav.FileInput("local.mp4", bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	server := &fileServer{data: data, etag: `"v1"`}
	srv := httptest.NewServer(server)
	defer srv.Close()
	got, err := digest(t, bundle.Default(), goav.URIInput(srv.URL+"/movie.mp4"))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatal("progressive HTTP read demuxed different packets than the local file")
	}
	if requests, _ := server.log(); len(requests) < 2 {
		t.Fatalf("%d requests, want the first read and a restart", len(requests))
	}
}

// TestHTTPRefusesAResourceThatChanges pins If-Range: when the ETag changes
// between ranges the read fails with ErrChanged instead of mixing files.
func TestHTTPRefusesAResourceThatChanges(t *testing.T) {
	data := indexAtEndMP4(t, 1<<20)
	server := &fileServer{data: data, etag: `"v1"`, ranges: true}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.ServeHTTP(w, r)
		server.mu.Lock()
		server.etag = `"v2"` // the file is replaced after the first request
		server.mu.Unlock()
	}))
	defer srv.Close()
	_, err := digest(t, bundle.Default(), goav.URIInput(srv.URL+"/movie.mp4"))
	if !errors.Is(err, scheme.ErrChanged) {
		t.Fatalf("err = %v, want scheme.ErrChanged", err)
	}
}

// TestHTTPStatusAndRedirects pins a 404 (reported without the URL's
// secrets), following redirects once to the final URL, the redirect cap,
// and header credentials dropped when a redirect changes host.
func TestHTTPStatusAndRedirects(t *testing.T) {
	data := indexAtEndMP4(t, 1<<20)
	server := &fileServer{data: data, etag: `"v1"`, ranges: true}
	var redirects, loops int
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.Handle("/movie.mp4", server)
	mux.HandleFunc("/old.mp4", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		redirects++
		mu.Unlock()
		http.Redirect(w, r, "/movie.mp4", http.StatusFound)
	})
	mux.HandleFunc("/loop.mp4", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		loops++
		mu.Unlock()
		http.Redirect(w, r, "/loop.mp4", http.StatusFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	sink := goavtest.NewCollector()
	err := bundle.Run(context.Background(), goav.URIInput(srv.URL+"/missing.mp4?token=SECRET").Video().To(sink.Sink()))
	if err == nil || !strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("err = %v, want a redacted 404", err)
	}

	if _, err := digest(t, bundle.Default(), goav.URIInput(srv.URL+"/old.mp4")); err != nil {
		t.Fatal(err)
	}
	requests, _ := server.log()
	if redirects != 1 || len(requests) < 2 {
		t.Fatalf("redirects = %d, ranged requests = %d: want one redirect, then the final URL", redirects, len(requests))
	}

	err = bundle.Run(context.Background(), goav.URIInput(srv.URL+"/loop.mp4").Video().To(sink.Sink()))
	if err == nil || !strings.Contains(err.Error(), "stopped after 10 redirects") || loops != 10 {
		t.Fatalf("loop err = %v after %d requests, want the redirect cap", err, loops)
	}

	// A header credential reaches the URL's host, never a redirect target.
	var leaked string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked = r.Header.Get("X-Api-Key")
		server.ServeHTTP(w, r)
	}))
	defer other.Close()
	var sent string
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent = r.Header.Get("X-Api-Key")
		target := strings.Replace(other.URL, "127.0.0.1", "localhost", 1) + "/movie.mp4"
		http.Redirect(w, r, target, http.StatusFound)
	}))
	defer front.Close()
	rt := bundle.MustNew(runconfig.Scheme("http", scheme.HTTP(scheme.Header("X-Api-Key", "k1"))))
	if _, err := digest(t, rt, goav.URIInput(front.URL+"/movie.mp4")); err != nil {
		t.Fatal(err)
	}
	if sent != "k1" || leaked != "" {
		t.Fatalf("X-Api-Key sent=%q leaked=%q, want it on the URL's host only", sent, leaked)
	}
}

// TestHTTPReaderHonorsClose pins that Close ends a read blocked on a server
// that stopped sending.
func TestHTTPReaderHonorsClose(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		w.Write(make([]byte, 10))
		w.(http.Flusher).Flush()
		<-release
	}))
	defer srv.Close()
	defer close(release)
	u, _ := url.Parse(srv.URL + "/stream.ts")
	in, err := scheme.HTTP().Input(u)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := in.(provider.ByteSource).OpenBytes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(reader)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	reader.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a closed read reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not end the blocked read")
	}
}

// TestHTTPSeekAwayFromAStalledRange pins that abandoning a range whose server
// stalled costs the connection, not a stall: the drain is bounded.
func TestHTTPSeekAwayFromAStalledRange(t *testing.T) {
	data := bytes.Repeat([]byte{7}, 4<<20)
	stall := make(chan struct{})
	var first sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stalled := false
		first.Do(func() { stalled = true })
		if !stalled {
			http.ServeContent(w, r, "big.bin", time.Unix(1700000000, 0), bytes.NewReader(data))
			return
		}
		// The first range promises 256 KiB, sends all but 16 KiB, and stalls.
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", 256<<10-1, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(256<<10))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[:240<<10])
		w.(http.Flusher).Flush()
		<-stall
	}))
	defer func() { close(stall); srv.Close() }()
	u, _ := url.Parse(srv.URL + "/big.bin")
	in, _ := scheme.HTTP().Input(u)
	reader, err := in.(provider.ByteSource).OpenBytes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	at := reader.(io.ReaderAt)
	buf := make([]byte, 230<<10)
	if _, err := at.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := at.ReadAt(buf[:1024], 2<<20); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("seeking away from the stalled range took %s", waited)
	}
}

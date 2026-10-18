package hls

import (
	"bytes"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Server keeps packagers' files in memory and serves them over HTTP, the
// origin a CDN or a player reads from. It is a Store, and it holds what
// LL-HLS asks a server to hold: a playlist request with _HLS_msn (and
// _HLS_part) waits until the playlist lists that segment (or part), and a
// request for the part a playlist hints waits until it is published. A
// request waits at most three target durations, then gets 503.
//
//	srv := hls.NewServer()
//	live := goav.Custom("live", hls.New("live.m3u8", srv, hls.Parts(250*time.Millisecond), hls.Window(6)))
//	http.Handle("/live/", http.StripPrefix("/live/", srv))
type Server struct {
	mu    sync.Mutex
	files map[string][]byte
	edges map[string]edge
	// waiting holds, per name, the channel the requests waiting on it
	// block on; publishing or announcing the name closes it, so a part
	// wakes only the players waiting for that part.
	waiting map[string]chan struct{}
}

// edge is where a packager's playlist stands: msn is its newest segment,
// part the newest part of that segment (-1 for none), complete the newest
// finished segment, hint the part it publishes next.
type edge struct {
	msn, part, complete int
	hint                string
	ended               bool
	target              time.Duration
	// delta is the playlist's delta update, for _HLS_skip, when it has
	// segments past the skip boundary.
	delta []byte
}

// announcer is a Store that hears where each playlist stands, so it can
// answer blocking requests.
type announcer interface {
	announce(playlist string, e edge)
}

// NewServer returns an empty server.
func NewServer() *Server {
	return &Server{files: map[string][]byte{}, edges: map[string]edge{}, waiting: map[string]chan struct{}{}}
}

// Create returns a writer whose Close publishes name.
func (s *Server) Create(name string) (io.WriteCloser, error) {
	return &serverFile{server: s, name: name}, nil
}

// Remove deletes name.
func (s *Server) Remove(name string) error {
	s.mu.Lock()
	delete(s.files, name)
	s.mu.Unlock()
	return nil
}

type serverFile struct {
	bytes.Buffer
	server *Server
	name   string
}

func (f *serverFile) Close() error {
	f.server.mu.Lock()
	f.server.files[f.name] = f.Bytes()
	f.server.wakeLocked(f.name)
	f.server.mu.Unlock()
	return nil
}

func (s *Server) announce(playlist string, e edge) {
	s.mu.Lock()
	if old, ok := s.edges[playlist]; ok && old.hint != e.hint {
		// Requests for the old hint learn it will not come.
		s.wakeLocked(old.hint)
	}
	s.edges[playlist] = e
	s.wakeLocked(playlist)
	s.mu.Unlock()
}

func (s *Server) wakeLocked(name string) {
	if ch, ok := s.waiting[name]; ok {
		close(ch)
		delete(s.waiting, name)
	}
}

// waitLocked is the channel a request for name blocks on.
func (s *Server) waitLocked(name string) chan struct{} {
	ch, ok := s.waiting[name]
	if !ok {
		ch = make(chan struct{})
		s.waiting[name] = ch
	}
	return ch
}

// ServeHTTP serves the file the path names, relative to where the server
// is mounted.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "GET a playlist or a segment", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	query := r.URL.Query()
	ready, err := s.blockingRequest(name, query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	limit := 3 * s.targetFor(name)
	timer := time.NewTimer(limit)
	defer timer.Stop()
	for {
		s.mu.Lock()
		data, have := s.files[name]
		ok := have && ready()
		if e := s.edges[name]; ok && wantsDelta(query) && e.delta != nil {
			data = e.delta
		}
		pending := !ok && s.pendingLocked(name)
		var wait chan struct{}
		if pending {
			wait = s.waitLocked(name)
		}
		s.mu.Unlock()
		if ok {
			s.write(w, r, name, data, query.Has("_HLS_msn"))
			return
		}
		if !pending {
			http.NotFound(w, r)
			return
		}
		select {
		case <-wait:
		case <-timer.C:
			http.Error(w, "not published in time", http.StatusServiceUnavailable)
			return
		case <-r.Context().Done():
			return
		}
	}
}

// blockingRequest reads a playlist request's _HLS_msn and _HLS_part: the
// returned condition, called with the server locked, reports whether the
// playlist lists what was asked for.
func (s *Server) blockingRequest(name string, query map[string][]string) (func() bool, error) {
	always := func() bool { return true }
	msnText, part := first(query["_HLS_msn"]), first(query["_HLS_part"])
	if msnText == "" {
		if part != "" {
			return nil, errBadRequest("_HLS_part needs _HLS_msn")
		}
		return always, nil
	}
	msn, err := strconv.Atoi(msnText)
	if err != nil || msn < 0 {
		return nil, errBadRequest("_HLS_msn is a segment number")
	}
	s.mu.Lock()
	e, known := s.edges[name]
	s.mu.Unlock()
	if known && !e.ended && msn > e.complete+2 {
		return nil, errBadRequest("_HLS_msn " + msnText + " is more than two segments past the playlist's last")
	}
	wantPart := -1
	if part != "" {
		if wantPart, err = strconv.Atoi(part); err != nil || wantPart < 0 {
			return nil, errBadRequest("_HLS_part is a part number")
		}
	}
	return func() bool {
		e, ok := s.edges[name]
		switch {
		case !ok:
			return false
		case e.ended || e.complete >= msn:
			// A finished segment satisfies any part of it: a part number
			// past its last will never come.
			return true
		case wantPart < 0:
			return false
		default:
			return e.msn > msn || e.msn == msn && e.part >= wantPart
		}
	}, nil
}

// pendingLocked reports whether a request for name may yet be answered: a
// playlist still being written, or the part a playlist hints.
func (s *Server) pendingLocked(name string) bool {
	if e, ok := s.edges[name]; ok {
		return !e.ended
	}
	for _, e := range s.edges {
		if e.hint == name && !e.ended {
			return true
		}
	}
	return false
}

// targetFor is the target duration of the playlist name is, or hints.
func (s *Server) targetFor(name string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.edges[name]; ok {
		return e.target
	}
	for _, e := range s.edges {
		if e.hint == name {
			return e.target
		}
	}
	return 4 * time.Second
}

func (s *Server) write(w http.ResponseWriter, r *http.Request, name string, data []byte, blocking bool) {
	switch path.Ext(name) {
	case ".m3u8":
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		if blocking {
			// The answer to a blocking reload is keyed by its query: a CDN
			// can serve it to every player that asks the same.
			w.Header().Set("Cache-Control", "max-age="+strconv.Itoa(int(6*s.targetFor(name).Seconds())))
		} else {
			// A plain playlist changes every part.
			w.Header().Set("Cache-Control", "no-cache")
		}
	case ".mp4", ".m4s":
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Cache-Control", "max-age=3600")
	}
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

type errBadRequest string

func (e errBadRequest) Error() string { return string(e) }

// wantsDelta reports whether a request asks for a delta update.
func wantsDelta(query map[string][]string) bool {
	skip := first(query["_HLS_skip"])
	return skip == "YES" || skip == "v2"
}

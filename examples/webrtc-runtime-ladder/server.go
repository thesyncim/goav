package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/goav"
)

type server struct {
	runtime  goav.Runtime
	mu       sync.Mutex
	sessions map[string]*session
}

func newServer(runtime goav.Runtime) *server {
	return &server{
		runtime:  runtime,
		sessions: make(map[string]*session),
	}
}

func (s *server) routes(mux *http.ServeMux) {
	mux.HandleFunc("/", serveStatic)
	mux.HandleFunc("/api/offer", s.handleInitialOffer)
	mux.HandleFunc("/api/sessions/", s.handleSession)
}

func (s *server) handleInitialOffer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var offer signalRequest
	if err := decodeJSON(r, &offer); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	session, err := newSession(r.Context(), s.runtime)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	answer, err := session.answer(r.Context(), offer)
	if err != nil {
		session.Close()
		writeError(w, http.StatusBadRequest, err)
		return
	}

	s.mu.Lock()
	s.sessions[session.id] = session
	s.mu.Unlock()

	writeJSON(w, signalResponse{ID: session.id, Type: answer.Type.String(), SDP: answer.SDP})
}

func (s *server) handleSession(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/api/sessions/")
	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	session := s.lookup(parts[0])
	if session == nil {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 2 && parts[1] == "offer" && r.Method == http.MethodPost {
		session.handleOffer(w, r)
		return
	}
	if len(parts) == 2 && parts[1] == "state" && r.Method == http.MethodGet {
		writeJSON(w, session.State())
		return
	}
	if len(parts) == 2 && parts[1] == "events" && r.Method == http.MethodGet {
		session.handleEvents(w, r)
		return
	}
	if len(parts) == 2 && parts[1] == "renditions" && r.Method == http.MethodPost {
		session.handleAddRendition(w, r)
		return
	}
	if len(parts) == 3 && parts[1] == "renditions" && r.Method == http.MethodPut {
		session.handleUpdateRendition(w, r, parts[2])
		return
	}
	if len(parts) == 3 && parts[1] == "renditions" && r.Method == http.MethodDelete {
		session.handleDeleteRendition(w, r, parts[2])
		return
	}
	http.NotFound(w, r)
}

func (s *server) lookup(id string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

func (s *session) handleOffer(w http.ResponseWriter, r *http.Request) {
	var offer signalRequest
	if err := decodeJSON(r, &offer); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	answer, err := s.answer(r.Context(), offer)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, signalResponse{Type: answer.Type.String(), SDP: answer.SDP})
}

func (s *session) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unavailable", http.StatusInternalServerError)
		return
	}
	events, unsubscribe := s.subscribe()
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case state := <-events:
			data, err := json.Marshal(state)
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "event: state\ndata: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *session) handleAddRendition(w http.ResponseWriter, r *http.Request) {
	var spec renditionSpec
	if err := decodeJSON(r, &spec); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	rendition, err := s.addRendition(r.Context(), spec)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, addRenditionResponse{Rendition: rendition.Spec, NeedsRenegotiate: true})
}

func (s *session) handleUpdateRendition(w http.ResponseWriter, r *http.Request, id string) {
	var spec renditionSpec
	if err := decodeJSON(r, &spec); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	spec.ID = id
	updated, err := s.updateRendition(r.Context(), spec)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, addRenditionResponse{Rendition: updated.Spec})
}

func (s *session) handleDeleteRendition(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.deleteRendition(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "needsRenegotiate": true})
}

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
	runtime    goav.Runtime
	browserURL string
	mu         sync.Mutex
	sessions   map[string]*session
}

func newServer(runtime goav.Runtime, browserURL string) *server {
	return &server{
		runtime:    runtime,
		browserURL: browserURL,
		sessions:   make(map[string]*session),
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
	session, err := newSession(r.Context(), s.runtime, s.browserURL)
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
	if len(parts) == 2 && parts[1] == "branches" && r.Method == http.MethodPost {
		session.handleAddBranch(w, r)
		return
	}
	if len(parts) == 3 && parts[1] == "branches" && r.Method == http.MethodPut {
		session.handleUpdateBranch(w, r, parts[2])
		return
	}
	if len(parts) == 3 && parts[1] == "branches" && r.Method == http.MethodDelete {
		session.handleDeleteBranch(w, r, parts[2])
		return
	}
	if len(parts) == 4 && parts[1] == "branches" && parts[3] == "pause" && r.Method == http.MethodPost {
		session.handlePauseBranch(w, r, parts[2])
		return
	}
	if len(parts) == 4 && parts[1] == "branches" && parts[3] == "resume" && r.Method == http.MethodPost {
		session.handleResumeBranch(w, r, parts[2])
		return
	}
	if len(parts) == 4 && parts[1] == "branches" && parts[3] == "rebranch" && r.Method == http.MethodPost {
		session.handleRebranch(w, r, parts[2])
		return
	}
	if len(parts) == 4 && parts[1] == "branches" && parts[3] == "bitrate" && r.Method == http.MethodPost {
		session.handleSetBitrate(w, r, parts[2])
		return
	}
	if len(parts) == 3 && parts[1] == "control" && parts[2] == "keyframe" && r.Method == http.MethodPost {
		session.handleKeyframe(w, r)
		return
	}
	if len(parts) == 2 && parts[1] == "scenarios" && r.Method == http.MethodPost {
		session.handleScenarios(w, r)
		return
	}
	http.NotFound(w, r)
}

func (s *server) lookup(id string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

func (s *server) latestSession() *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest *session
	for _, session := range s.sessions {
		if latest == nil || session.created.After(latest.created) {
			latest = session
		}
	}
	return latest
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

func (s *session) handleAddBranch(w http.ResponseWriter, r *http.Request) {
	var spec branchSpec
	if err := decodeJSON(r, &spec); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	branch, err := s.addBranch(r.Context(), spec)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, addBranchResponse{Branch: branch.Spec, NeedsRenegotiate: true})
}

func (s *session) handleUpdateBranch(w http.ResponseWriter, r *http.Request, id string) {
	var spec branchSpec
	if err := decodeJSON(r, &spec); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	spec.ID = id
	updated, err := s.updateBranch(r.Context(), spec)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, addBranchResponse{Branch: updated.Spec})
}

func (s *session) handleDeleteBranch(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.deleteBranch(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "needsRenegotiate": true})
}

func (s *session) handlePauseBranch(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.pauseBranch(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, controlResponse{OK: true, Message: "paused"})
}

func (s *session) handleResumeBranch(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.resumeBranch(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, controlResponse{OK: true, Message: "resumed"})
}

func (s *session) handleRebranch(w http.ResponseWriter, r *http.Request, id string) {
	var spec branchSpec
	if err := decodeJSON(r, &spec); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	spec.ID = id
	branch, err := s.rebranch(r.Context(), id, spec)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, addBranchResponse{Branch: branch.Spec})
}

func (s *session) handleSetBitrate(w http.ResponseWriter, r *http.Request, id string) {
	var req bitrateRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.setBranchBitrate(r.Context(), id, req.Bitrate); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, controlResponse{OK: true, Message: "bitrate retargeted"})
}

func (s *session) handleKeyframe(w http.ResponseWriter, r *http.Request) {
	var req keyframeRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Kind == "" {
		req.Kind = "video"
	}
	if err := s.requestKeyframe(r.Context(), req.Kind); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, controlResponse{OK: true, Message: "keyframe requested"})
}

func (s *session) handleScenarios(w http.ResponseWriter, r *http.Request) {
	results := runPlannerScenarios(r.Context())
	s.setScenarios(results)
	writeJSON(w, results)
}

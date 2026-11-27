package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/thesyncim/goav/examples/gio-webrtc-showcase/screenav"
)

type studioServer struct {
	studio *studio
}

func newStudioServer(st *studio) *studioServer {
	return &studioServer{studio: st}
}

func (s *studioServer) routes(mux *http.ServeMux) {
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(sub))

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		data, err := fs.ReadFile(sub, "index.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(data)
	})

	mux.HandleFunc("GET /watch", func(w http.ResponseWriter, r *http.Request) {
		data, err := fs.ReadFile(sub, "watch.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(data)
	})

	mux.Handle("GET /static/", http.StripPrefix("/static/", noStore(fileServer)))
	mux.Handle("GET /hls/", http.StripPrefix("/hls/", withCORS(s.studio.hlsSrv)))

	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("POST /api/config", s.handleUpdateConfig)
	mux.HandleFunc("POST /api/permissions/camera", s.handleCameraPermission)
	mux.HandleFunc("POST /api/permissions/screen", s.handleScreenPermission)

	mux.HandleFunc("POST /api/branches", s.handleAddBranch)
	mux.HandleFunc("POST /api/branches/{id}/pause", s.handlePauseBranch)
	mux.HandleFunc("POST /api/branches/{id}/resume", s.handleResumeBranch)
	mux.HandleFunc("POST /api/branches/{id}/bitrate", s.handleBranchBitrate)
	mux.HandleFunc("POST /api/branches/{id}/rebranch", s.handleRebranch)
	mux.HandleFunc("DELETE /api/branches/{id}", s.handleDeleteBranch)

	mux.HandleFunc("POST /api/record/start", s.handleRecordStart)
	mux.HandleFunc("POST /api/record/stop", s.handleRecordStop)
	mux.HandleFunc("POST /api/hls", s.handleHLS)
	mux.HandleFunc("POST /api/whip/start", s.handleWHIPStart)
	mux.HandleFunc("POST /api/whip/stop", s.handleWHIPStop)

	mux.HandleFunc("POST /api/webrtc/offer", s.handleWebRTCOffer)
	mux.HandleFunc("POST /api/webrtc/answer", s.handleWebRTCAnswer)
	mux.HandleFunc("POST /api/open-watch", s.handleOpenWatch)
}

func noStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Cache-Control", "no-cache")
		next.ServeHTTP(w, r)
	})
}

func (s *studioServer) handleState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.studio.Snapshot())
}

func (s *studioServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch, unsub := s.studio.subscribeState()
	defer unsub()

	ticker := time.NewTicker(900 * time.Millisecond)
	defer ticker.Stop()

	send := func(st studioState) bool {
		payload, err := json.Marshal(st)
		if err != nil {
			return false
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case st := <-ch:
			if !send(st) {
				return
			}
		case <-ticker.C:
			if !send(s.studio.Snapshot()) {
				return
			}
		}
	}
}

func (s *studioServer) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	var cfg studioConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.studio.UpdateConfig(r.Context(), cfg); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.studio.Snapshot())
}

func (s *studioServer) handleCameraPermission(w http.ResponseWriter, r *http.Request) {
	if err := s.studio.RequestCameraAccess(r.Context()); err != nil {
		if runtime.GOOS == "darwin" {
			_ = exec.Command("open", "x-apple.systempreferences:com.apple.preference.security?Privacy_Camera").Start()
		}
		writeErr(w, http.StatusForbidden, fmt.Errorf("%w (opened System Settings → Privacy & Security → Camera)", err))
		return
	}
	writeJSON(w, http.StatusOK, s.studio.Snapshot())
}

func (s *studioServer) handleScreenPermission(w http.ResponseWriter, _ *http.Request) {
	granted := screenav.RequestAccess()
	s.studio.refreshDevices()
	if !granted && runtime.GOOS == "darwin" {
		_ = exec.Command("open", "x-apple.systempreferences:com.apple.preference.security?Privacy_ScreenCapture").Start()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"granted": granted,
		"state":   s.studio.Snapshot(),
	})
}

func (s *studioServer) handleAddBranch(w http.ResponseWriter, r *http.Request) {
	var spec branchSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	created, err := s.studio.AddBranch(r.Context(), spec)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"branch": created, "state": s.studio.Snapshot()})
}

func (s *studioServer) handlePauseBranch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.studio.PauseBranch(r.Context(), id, true); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.studio.Snapshot())
}

func (s *studioServer) handleResumeBranch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.studio.PauseBranch(r.Context(), id, false); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.studio.Snapshot())
}

func (s *studioServer) handleBranchBitrate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req struct {
		Bitrate int `json:"bitrate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.studio.SetBitrate(r.Context(), id, req.Bitrate); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.studio.Snapshot())
}

func (s *studioServer) handleRebranch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req struct {
		Width   int `json:"width"`
		Height  int `json:"height"`
		Bitrate int `json:"bitrate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.studio.Rebranch(r.Context(), id, req.Width, req.Height, req.Bitrate); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.studio.Snapshot())
}

func (s *studioServer) handleDeleteBranch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.studio.RemoveBranch(r.Context(), id); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.studio.Snapshot())
}

func (s *studioServer) handleRecordStart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Format  string `json:"format"`
		Codec   string `json:"codec"`
		Bitrate int    `json:"bitrate"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	st, err := s.studio.StartRecording(r.Context(), req.Format, req.Codec, req.Bitrate)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"recording": st, "state": s.studio.Snapshot()})
}

func (s *studioServer) handleRecordStop(w http.ResponseWriter, r *http.Request) {
	st, err := s.studio.StopRecording(r.Context())
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"recording": st, "state": s.studio.Snapshot()})
}

func (s *studioServer) handleHLS(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enable  bool `json:"enable"`
		Bitrate int  `json:"bitrate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.studio.ToggleHLS(r.Context(), req.Enable, req.Bitrate); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.studio.Snapshot())
}

func (s *studioServer) handleWHIPStart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL     string `json:"url"`
		Token   string `json:"token"`
		Codec   string `json:"codec"`
		Bitrate int    `json:"bitrate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.studio.StartWHIP(r.Context(), req.URL, req.Token, req.Codec, req.Bitrate); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.studio.Snapshot())
}

func (s *studioServer) handleWHIPStop(w http.ResponseWriter, r *http.Request) {
	if err := s.studio.StopWHIP(r.Context()); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.studio.Snapshot())
}

func (s *studioServer) handleWebRTCOffer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ViewerID string `json:"viewerId"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	viewerID, sdp, err := s.studio.OfferViewer(strings.TrimSpace(req.ViewerID))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"viewerId": viewerID,
		"type":     "offer",
		"sdp":      sdp,
	})
}

func (s *studioServer) handleWebRTCAnswer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ViewerID string `json:"viewerId"`
		SDP      string `json:"sdp"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.studio.AnswerViewer(req.ViewerID, req.SDP); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *studioServer) handleOpenWatch(w http.ResponseWriter, _ *http.Request) {
	url := s.studio.baseURL + "/watch"
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux":
		cmd = exec.Command("xdg-open", url)
	}
	if cmd != nil {
		if err := cmd.Start(); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"url": url})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

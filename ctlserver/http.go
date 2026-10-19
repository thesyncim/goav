package ctlserver

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/thesyncim/goav"

	"github.com/thesyncim/goav/av"
)

// maxRequestBytes bounds one HTTP request body.
const maxRequestBytes = 1 << 20

// ServeHTTP serves the methods over HTTP, for mounting the control plane in
// an existing service (behind its own authentication): POST a JSON-RPC
// request and receive its response; GET .../events streams the task's events
// as server-sent events, filtered by ?types=a,b and ?stream=id.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost:
		raw, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply, ok := s.handleMessage(r.Context(), nil, raw)
		if !ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events"):
		s.serveEvents(w, r)
	default:
		w.Header().Set("Allow", "POST, GET")
		http.Error(w, "POST a JSON-RPC request, or GET .../events", http.StatusMethodNotAllowed)
	}
}

func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	var filters []goav.EventFilter
	if types := r.URL.Query().Get("types"); types != "" {
		var ts []av.EventType
		for _, t := range strings.Split(types, ",") {
			ts = append(ts, av.EventType(strings.TrimSpace(t)))
		}
		filters = append(filters, goav.WatchTypes(ts...))
	}
	if stream := r.URL.Query().Get("stream"); stream != "" {
		filters = append(filters, goav.WatchStream(av.StreamID(stream)))
	}
	sub := s.task.Watch(filters...)
	defer sub.Close()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-sub.Events():
			if !ok {
				return
			}
			data, err := json.Marshal(wireEvent(event))
			if err != nil {
				continue
			}
			if _, err := io.WriteString(w, "data: "+string(data)+"\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

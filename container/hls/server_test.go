package hls

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestServerBlocking pins the waits LL-HLS asks for: a reload for a segment
// waits until it is complete, a reload for a part until it is listed, and a
// request that is never satisfied gets 503 after three target durations.
func TestServerBlocking(t *testing.T) {
	srv := NewServer()
	web := httptest.NewServer(srv)
	defer web.Close()
	publish := func(name, body string) {
		f, _ := srv.Create(name)
		io.WriteString(f, body)
		f.Close()
	}
	target := 50 * time.Millisecond
	publish("p.m3u8", "v0")
	srv.announce("p.m3u8", edge{msn: 0, part: 1, complete: -1, hint: "p-00000.2.m4s", target: target})

	type result struct {
		code int
		body string
	}
	get := func(path string) chan result {
		out := make(chan result, 1)
		go func() {
			resp, err := http.Get(web.URL + "/" + path)
			if err != nil {
				out <- result{}
				return
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			out <- result{resp.StatusCode, string(b)}
		}()
		return out
	}
	part := get("p.m3u8?_HLS_msn=0&_HLS_part=2")
	segment := get("p.m3u8?_HLS_msn=0")
	hinted := get("p-00000.2.m4s")
	time.Sleep(20 * time.Millisecond)
	publish("p-00000.2.m4s", "moof")
	publish("p.m3u8", "v1")
	srv.announce("p.m3u8", edge{msn: 0, part: 2, complete: -1, hint: "p-00000.3.m4s", target: target})
	if r := <-part; r.code != http.StatusOK || r.body != "v1" {
		t.Fatalf("part reload = %+v, want the playlist listing part 2", r)
	}
	if r := <-hinted; r.code != http.StatusOK || r.body != "moof" {
		t.Fatalf("hinted part = %+v", r)
	}
	// Segment 0 never completes: the reload gives up after three targets.
	if r := <-segment; r.code != http.StatusServiceUnavailable {
		t.Fatalf("segment reload = %+v, want 503", r)
	}
	if r := <-get("p.m3u8?_HLS_part=1"); r.code != http.StatusBadRequest {
		t.Fatalf("_HLS_part alone = %+v, want 400", r)
	}
}

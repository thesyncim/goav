package hls

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestServerDeltaAndBounds(t *testing.T) {
	srv := NewServer()
	web := httptest.NewServer(srv)
	defer web.Close()
	f, _ := srv.Create("p.m3u8")
	io.WriteString(f, "full")
	f.Close()
	srv.announce("p.m3u8", edge{msn: 5, part: 1, complete: 4, target: time.Second, delta: []byte("delta")})
	get := func(path string) (int, string, string) {
		resp, err := http.Get(web.URL + "/" + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), resp.Header.Get("Cache-Control")
	}
	if code, body, _ := get("p.m3u8?_HLS_skip=YES"); code != http.StatusOK || body != "delta" {
		t.Fatalf("delta request = %d %q", code, body)
	}
	if code, body, cache := get("p.m3u8?_HLS_msn=5&_HLS_part=1&_HLS_skip=YES"); code != http.StatusOK || body != "delta" || cache != "max-age=6" {
		t.Fatalf("blocking delta = %d %q %q, want the delta cached for six targets", code, body, cache)
	}
	if code, body, cache := get("p.m3u8"); body != "full" || cache != "no-cache" || code != http.StatusOK {
		t.Fatalf("plain = %d %q %q", code, body, cache)
	}
	if code, _, _ := get("p.m3u8?_HLS_msn=7"); code != http.StatusBadRequest {
		t.Fatalf("a reload three segments past the last = %d, want 400", code)
	}
}

// TestServerForgetsUnknownNames: a request for a name nothing will publish
// leaves nothing behind.
func TestServerForgetsUnknownNames(t *testing.T) {
	srv := NewServer()
	web := httptest.NewServer(srv)
	defer web.Close()
	for i := range 100 {
		resp, err := http.Get(web.URL + "/nope-" + strconv.Itoa(i) + ".m4s")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown name = %d", resp.StatusCode)
		}
	}
	if n := len(srv.waiting); n != 0 {
		t.Fatalf("%d wait channels left for names nothing publishes", n)
	}
}

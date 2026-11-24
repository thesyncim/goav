package scheme

import (
	"math"
	"net/http"
	"net/url"
	"strconv"
	"testing"
)

func TestContentRangeOverflowAndBoundsRejected(t *testing.T) {
	bad := []string{
		"bytes 0-" + strconv.FormatInt(math.MaxInt64, 10) + "/*",
		"bytes -1-10/20",
		"bytes 10-5/20",
		"bytes 0-20/20",
		"bytes 0-20/10",
		"bytes 0-10/0",
	}
	for _, hdr := range bad {
		if _, _, _, ok := contentRange(hdr); ok {
			t.Errorf("contentRange(%q) = ok, want rejected", hdr)
		}
	}
	start, end, total, ok := contentRange("bytes 0-99/100")
	if !ok || start != 0 || end != 99 || total != 100 {
		t.Fatalf("valid contentRange = (%d,%d,%d,%v), want (0,99,100,true)", start, end, total, ok)
	}
}

func TestHTTPReadAtRejectsNegativeOffset(t *testing.T) {
	u, _ := url.Parse("http://example.invalid/media.mp4")
	r := newHTTPReader(httpScheme{}, u)
	defer r.Close()
	buf := make([]byte, 4)
	if _, err := r.ReadAt(buf, -1); err == nil {
		t.Fatal("ReadAt(-1) succeeded, want error")
	}
}

func TestRedirectCappedDropsHeadersOnHTTPSDowngrade(t *testing.T) {
	hdrs := http.Header{"X-Api-Key": {"secret"}}
	client := redirectCapped(nil, hdrs)
	orig, _ := http.NewRequest(http.MethodGet, "https://example.com/a", nil)
	next, _ := http.NewRequest(http.MethodGet, "http://example.com/b", nil)
	next.Header.Set("X-Api-Key", "secret")
	if err := client.CheckRedirect(next, []*http.Request{orig}); err != nil {
		t.Fatalf("CheckRedirect: %v", err)
	}
	if got := next.Header.Get("X-Api-Key"); got != "" {
		t.Fatalf("X-Api-Key leaked across https->http downgrade: %q", got)
	}
}

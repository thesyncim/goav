package rtmpav

import (
	"net/url"
	"os/exec"
	"strings"
	"testing"
)

func TestParseTarget(t *testing.T) {
	for raw, want := range map[string]target{
		"rtmp://a.rtmp.youtube.com/live2/abcd-efgh":         {address: "a.rtmp.youtube.com:1935", app: "live2", key: "abcd-efgh", tcURL: "rtmp://a.rtmp.youtube.com/live2", name: "rtmp://a.rtmp.youtube.com/live2/REDACTED"},
		"rtmps://live-api-s.facebook.com:443/rtmp/FB-1?a=b": {address: "live-api-s.facebook.com:443", tls: true, app: "rtmp", key: "FB-1?a=b", tcURL: "rtmps://live-api-s.facebook.com:443/rtmp", name: "rtmps://live-api-s.facebook.com:443/rtmp/REDACTED"},
		"rtmps://host/app/inst/key":                         {address: "host:443", tls: true, app: "app/inst", key: "key", tcURL: "rtmps://host/app/inst", name: "rtmps://host/app/inst/REDACTED"},
	} {
		u, _ := url.Parse(raw)
		got, err := parseTarget(u)
		want.host = u.Hostname()
		if err != nil || got != want {
			t.Errorf("parseTarget(%q) = %+v, %v\nwant %+v", raw, got, err, want)
		}
	}
	for raw, fix := range map[string]string{
		"rtmp://host/live":      "rtmp://host/app/key",
		"rtmp://u:p@host/app/k": "stream key or its query",
		"rtmpe://host/app/k":    "not rtmp or rtmps",
		"rtmp:///app/k":         "names no server",
	} {
		u, _ := url.Parse(raw)
		if _, err := parseTarget(u); err == nil || !strings.Contains(err.Error(), fix) {
			t.Errorf("parseTarget(%q) = %v, want an error naming %q", raw, err, fix)
		}
	}
}

// A server's text loses the stream key wherever it repeats it as a word,
// and keeps words the key only appears inside.
func TestScrub(t *testing.T) {
	tg := target{key: "k?sig=1"}
	if got := tg.scrub("no input takes live/k (k?sig=1)"); got != "no input takes live/REDACTED (REDACTED)" {
		t.Fatalf("scrub = %q", got)
	}
}

// rtmpav is written against goav's public packages only, as a transport
// in any other module would be.
func TestUsesOnlyPublicAPI(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", `{{range .Imports}}{{.}} {{end}}`, ".").Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range strings.Fields(string(out)) {
		if strings.Contains(path, "/internal") {
			t.Errorf("rtmpav imports %s", path)
		}
	}
}

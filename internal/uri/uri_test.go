package uri

import (
	"net/url"
	"testing"
)

func TestURLAndMask(t *testing.T) {
	u, _ := url.Parse("https://alice:pw@cdn.example/a.mp4?api_key=k&q=1&Signature=s#frag")
	name, full := Redact(u)
	if name != "https://cdn.example/a.mp4" || full != "https://REDACTED@cdn.example/a.mp4?api_key=REDACTED&q=1&Signature=REDACTED" {
		t.Fatalf("name=%q full=%q", name, full)
	}
	for in, want := range map[string]string{
		`uri "srt://host:9000?passphrase=xyz&latency=1"`: `uri "srt://host:9000?passphrase=***&latency=1"`,
		`"https://u@h/p" ! x`:                            `"https://*@h/p" ! x`,
		`file a.mp4 ! video`:                             `file a.mp4 ! video`,
		`"https://h/p?token=a#t=1"`:                      `"https://h/p?token=*#t=1"`,
	} {
		if got := Mask(in); got != want {
			t.Errorf("Mask(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestScheme(t *testing.T) {
	for raw, want := range map[string]string{
		"https://h/p": "https", "WHEP+HTTP://h": "whep+http", "file:a.mp4": "file",
		"100%.mp4": "", `C:\clips\a.mp4`: "", "dir/a:b.mp4": "", "a?b#c.mp4": "", "": "", "1x:y": "",
	} {
		if got := Scheme(raw); got != want {
			t.Errorf("Scheme(%q) = %q, want %q", raw, got, want)
		}
	}
}

// An RTMP URL's stream key, its last path segment, never shows.
func TestRTMPStreamKeyRedacted(t *testing.T) {
	for raw, want := range map[string][2]string{
		"rtmp://a.rtmp.youtube.com/live2/abcd-efgh-ijkl":                      {"rtmp://a.rtmp.youtube.com/live2/REDACTED", "rtmp://a.rtmp.youtube.com/live2/REDACTED"},
		"rtmps://u:p@live.twitch.tv:443/app/live_1?token=t":                   {"rtmps://live.twitch.tv:443/app/REDACTED", "rtmps://REDACTED@live.twitch.tv:443/app/REDACTED?REDACTED"},
		"rtmps://live-api-s.facebook.com:443/rtmp/FB-123?s_bl=1&a=AbCdSecret": {"rtmps://live-api-s.facebook.com:443/rtmp/REDACTED", "rtmps://live-api-s.facebook.com:443/rtmp/REDACTED?REDACTED"},
		"RTMP://host/app/inst/key%2Fwith%2Fslashes":                           {"rtmp://host/app/inst/REDACTED", "rtmp://host/app/inst/REDACTED"},
		"rtmp://host/live":           {"rtmp://host/live", "rtmp://host/live"},
		"https://host/app/not-a-key": {"https://host/app/not-a-key", "https://host/app/not-a-key"},
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if name, full := Redact(u); name != want[0] || full != want[1] {
			t.Errorf("Redact(%q) = %q, %q; want %q, %q", raw, name, full, want[0], want[1])
		}
	}
	for in, want := range map[string]string{
		`file a.flv ! uri "rtmp://a.rtmp.youtube.com/live2/abcd-efgh"`: `file a.flv ! uri "rtmp://a.rtmp.youtube.com/live2/*********"`,
		`uri "rtmps://h/app/key?x=1&a=Secret" ! x`:                     `uri "rtmps://h/app/***?************" ! x`,
		`"rtmp://h/live"`: `"rtmp://h/live"`,
	} {
		if got := Mask(in); got != want {
			t.Errorf("Mask(%q) = %q, want %q", in, got, want)
		}
	}
}

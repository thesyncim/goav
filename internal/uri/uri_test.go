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

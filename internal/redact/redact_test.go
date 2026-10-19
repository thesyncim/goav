package redact

import (
	"net/url"
	"testing"
)

func TestURLAndMask(t *testing.T) {
	u, _ := url.Parse("https://alice:pw@cdn.example/a.mp4?api_key=k&q=1&Signature=s#frag")
	name, full := URL(u)
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

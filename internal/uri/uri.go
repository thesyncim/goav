// Package uri is goav's one URL handling for what it reports and reads:
// the scheme a value names, names without credentials, URLs with their
// secrets replaced, and source text with its secrets masked in place.
package uri

import (
	"net/url"
	"strings"
)

// Marker replaces a redacted secret.
const Marker = "REDACTED"

// Redact renders u for reports: name is scheme://host/path with no userinfo,
// query, or fragment; full keeps the query with secret values replaced and
// userinfo as Marker.
func Redact(u *url.URL) (name, full string) {
	shown := *u
	shown.User = nil
	shown.RawQuery, shown.ForceQuery = "", false
	shown.Fragment, shown.RawFragment = "", ""
	name = shown.String()
	if u.User != nil {
		shown.User = url.User(Marker)
	}
	shown.RawQuery = query(u.RawQuery)
	return name, shown.String()
}

func query(raw string) string {
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, "&")
	for i, part := range parts {
		key, _, hasValue := strings.Cut(part, "=")
		if hasValue && Secret(key) {
			parts[i] = key + "=" + Marker
		}
	}
	return strings.Join(parts, "&")
}

// Secret reports a query parameter that may carry a credential. Matching
// errs wide: a redacted harmless value costs nothing.
func Secret(name string) bool {
	if unescaped, err := url.QueryUnescape(name); err == nil {
		name = unescaped
	}
	name = strings.ToLower(name)
	for _, marker := range []string{"token", "key", "secret", "pass", "pwd", "sig", "credential", "auth", "jwt"} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return false
}

// Mask overwrites, byte for byte, the userinfo and secret query values of
// every URL in text, so a source excerpt keeps its columns.
func Mask(text string) string {
	out := []byte(text)
	for from := 0; ; {
		i := strings.Index(text[from:], "://")
		if i < 0 {
			return string(out)
		}
		start := from + i + 3
		end := start
		for end < len(text) && !strings.ContainsRune(" \t\n\"'", rune(text[end])) {
			end++
		}
		maskURL(out[start:end])
		from = end
	}
}

// maskURL masks the authority's userinfo and the query's secret values of
// one URL after its "://".
func maskURL(u []byte) {
	authority := len(u)
	for i, c := range u {
		if c == '/' || c == '?' || c == '#' {
			authority = i
			break
		}
	}
	if at := strings.LastIndexByte(string(u[:authority]), '@'); at >= 0 {
		fill(u[:at])
	}
	q := strings.IndexByte(string(u), '?')
	if q < 0 {
		return
	}
	for i := q + 1; i < len(u); {
		end := i
		for end < len(u) && u[end] != '&' && u[end] != '#' {
			end++
		}
		if eq := strings.IndexByte(string(u[i:end]), '='); eq >= 0 && Secret(string(u[i:i+eq])) {
			fill(u[i+eq+1 : end])
		}
		if end < len(u) && u[end] == '#' {
			return
		}
		i = end + 1
	}
}

func fill(b []byte) {
	for i := range b {
		b[i] = '*'
	}
}

// Scheme returns the lower-case scheme raw names, or "" for a path: a value
// whose prefix before ":" is not an RFC 3986 scheme of two or more letters (a
// Windows drive letter is one) names a file, whatever else it contains.
func Scheme(raw string) string {
	colon := strings.IndexByte(raw, ':')
	if colon < 2 {
		return ""
	}
	for i := 0; i < colon; i++ {
		c := raw[i]
		switch {
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
		case i > 0 && (c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.'):
		default:
			return ""
		}
	}
	return strings.ToLower(raw[:colon])
}

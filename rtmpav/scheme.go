package rtmpav

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/thesyncim/goav/provider"
)

// Option configures RTMP and Listen.
type Option func(*config)

type config struct {
	tls *tls.Config
}

func newConfig(options []Option) config {
	var c config
	for _, option := range options {
		if option != nil {
			option(&c)
		}
	}
	return c
}

// WithTLSConfig sets the TLS configuration: the roots and server name an
// rtmps client trusts (the system's by default), or the certificates that
// make Listen serve rtmps.
func WithTLSConfig(c *tls.Config) Option {
	return func(config *config) { config.tls = c }
}

// RTMP is the rtmp and rtmps scheme, an RTMP client for goav.URIInput and
// goav.URI. Register it under both names:
//
//	rtmp := rtmpav.RTMP()
//	runconfig.Scheme("rtmp", rtmp), runconfig.Scheme("rtmps", rtmp)
//
// A URL names the server, the application, and the stream key:
// rtmp://host[:port]/app/key, the port 1935 by default and 443 for rtmps,
// which runs over TLS. A query belongs to the key, as the services that
// sign keys with one expect.
//
// goav.URI publishes. The destination takes H.264, HEVC, AV1, and VP9
// video and AAC, Opus, and FLAC audio packets, and encodes frames to H.264
// and AAC, which every server reads. When the run opens it, it connects,
// offers the Enhanced RTMP codecs, and publishes; a server that answers
// with the codecs it takes refuses the others with ErrCodecRefused, and
// one that answers with none is sent what the streams carry. It publishes
// live: a file routed to it plays at its media time.
//
// goav.URIInput plays a live stream: FLV over RTMP, which the runtime
// demuxes, Enhanced RTMP included. A publisher that stops ends it with
// ErrStreamEnded, which goav.Reconnect recovers from, and a read that waits
// 30s for media fails. A refused connect, publish, or play is a
// *StatusError.
//
// goav reports the URL with its stream key redacted, as it does userinfo
// and secret query values; only this scheme sees it.
func RTMP(options ...Option) provider.Scheme {
	return scheme{config: newConfig(options)}
}

type scheme struct{ config config }

var (
	_ provider.InputScheme       = scheme{}
	_ provider.DestinationScheme = scheme{}
)

func (s scheme) Input(u *url.URL) (provider.Input, error) {
	t, err := parseTarget(u)
	if err != nil {
		return nil, err
	}
	return &playInput{target: t, config: s.config}, nil
}

func (s scheme) Destination(u *url.URL) (provider.Output, error) {
	t, err := parseTarget(u)
	if err != nil {
		return nil, err
	}
	return &publisher{target: t, config: s.config}, nil
}

// redacted stands for the stream key, and its query, in what goav reports.
const redacted = "REDACTED"

// target is where a URL points: the server address, the application and
// stream key the commands name, and the tcUrl connect sends.
type target struct {
	address string
	host    string
	tls     bool
	app     string
	key     string
	tcURL   string
	// name is the URL as goav reports it, the key redacted.
	name string
}

func parseTarget(u *url.URL) (target, error) {
	var t target
	switch strings.ToLower(u.Scheme) {
	case "rtmp":
	case "rtmps":
		t.tls = true
	default:
		return t, fmt.Errorf("rtmpav: the %s scheme is not rtmp or rtmps", u.Scheme)
	}
	if u.Host == "" {
		return t, errors.New("rtmpav: the URL names no server: rtmp://host/app/key")
	}
	if u.User != nil {
		return t, errors.New("rtmpav: the URL carries userinfo; RTMP servers take credentials in the stream key or its query")
	}
	t.host = u.Hostname()
	port := u.Port()
	if port == "" {
		port = "1935"
		if t.tls {
			port = "443"
		}
	}
	t.address = net.JoinHostPort(t.host, port)
	path := strings.Trim(u.EscapedPath(), "/")
	slash := strings.LastIndexByte(path, '/')
	if slash < 0 {
		return t, errors.New("rtmpav: the URL names no stream key: rtmp://host/app/key")
	}
	app, err := url.PathUnescape(path[:slash])
	if err != nil {
		return t, fmt.Errorf("rtmpav: the URL's application: %w", err)
	}
	key, err := url.PathUnescape(path[slash+1:])
	if err != nil {
		return t, errors.New("rtmpav: the URL's stream key is not a valid path segment")
	}
	t.app, t.key = app, key
	if u.RawQuery != "" {
		t.key += "?" + u.RawQuery
	}
	t.tcURL = (&url.URL{Scheme: strings.ToLower(u.Scheme), Host: u.Host, Path: "/" + app}).String()
	t.name = (&url.URL{Scheme: strings.ToLower(u.Scheme), Host: u.Host, Path: "/" + app + "/" + redacted}).String()
	return t, nil
}

// scrub replaces the stream key, with its query and without, wherever a
// server's text repeats it.
func (t target) scrub(text string) string {
	for _, secret := range []string{t.key, strings.SplitN(t.key, "?", 2)[0]} {
		text = replaceToken(text, secret, redacted)
	}
	return text
}

// replaceToken replaces old in s where it stands alone, not inside a
// longer word.
func replaceToken(s, old, with string) string {
	if old == "" {
		return s
	}
	var b strings.Builder
	for {
		i := strings.Index(s, old)
		if i < 0 {
			b.WriteString(s)
			return b.String()
		}
		end := i + len(old)
		if i > 0 && wordByte(s[i-1]) || end < len(s) && wordByte(s[end]) {
			b.WriteString(s[:i+1])
			s = s[i+1:]
			continue
		}
		b.WriteString(s[:i])
		b.WriteString(with)
		s = s[end:]
	}
}

func wordByte(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c == '-'
}

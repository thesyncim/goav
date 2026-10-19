package webrtcav

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

// Option configures the WHIP and WHEP clients, which share the HTTP
// mechanics of RFC 9725.
type Option func(*signalConfig)

type signalConfig struct {
	token   string
	client  *http.Client
	session SessionConfig
	// gather bounds the wait for ICE candidates before the offer is sent;
	// candidates found later are trickled when the server accepts them.
	gather time.Duration
	// connect bounds the wait for media to start: a WHEP track's first
	// packet, a WHIP peer connection's connected state.
	connect time.Duration
}

func newSignalConfig(options []Option) signalConfig {
	config := signalConfig{gather: 2 * time.Second, connect: 10 * time.Second}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.client == nil {
		config.client = &http.Client{Timeout: 15 * time.Second}
	}
	return config
}

// Token sends token as the Bearer credential on every request (POST, PATCH,
// DELETE), so the URL carries none.
func Token(token string) Option {
	return func(c *signalConfig) { c.token = token }
}

// HTTPClient sends the requests through client; the default bounds each
// request to 15s.
func HTTPClient(client *http.Client) Option {
	return func(c *signalConfig) { c.client = client }
}

// PeerConfig configures the Pion peer connection: its API (media engine,
// interceptors, setting engine) and Configuration (ICE servers).
func PeerConfig(config SessionConfig) Option {
	return func(c *signalConfig) { c.session = config }
}

// StatusError reports a WHIP or WHEP request the server refused: the
// protocol, the request method, and the HTTP status. It names no URL, which
// may carry credentials.
type StatusError struct {
	Protocol   string
	Method     string
	StatusCode int
	Status     string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s: %s answered %s", e.Protocol, e.Method, e.Status)
}

// iceServers remembers, per endpoint host, the STUN/TURN servers its
// answers named (Link rel="ice-server"), so later sessions configure them
// before their offer.
type iceServers struct {
	mu     sync.Mutex
	byHost map[string][]webrtc.ICEServer
}

func newICEServers() *iceServers {
	return &iceServers{byHost: map[string][]webrtc.ICEServer{}}
}

func (c *iceServers) get(host string) []webrtc.ICEServer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byHost[host]
}

func (c *iceServers) put(host string, servers []webrtc.ICEServer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byHost[host] = servers
}

// negotiated is a session a WHIP or WHEP offer opened.
type negotiated interface {
	linkedServers() []webrtc.ICEServer
	Close() error
}

// negotiate opens a session with open. The ICE servers an answer names
// (Link rel="ice-server", RFC 9725 section 4.6) only help candidates
// gathered after them, so a session that learns new ones is replaced, once,
// by one configured with them before its offer; later sessions to the host
// start with them. Servers the caller configured (PeerConfig) take
// precedence.
func negotiate[S negotiated](ctx context.Context, servers *iceServers, host string, own webrtc.Configuration, open func(context.Context, webrtc.Configuration) (S, error)) (S, error) {
	config := own
	configured := len(config.ICEServers) != 0
	if !configured {
		config.ICEServers = servers.get(host)
	}
	s, err := open(ctx, config)
	if err != nil {
		return s, err
	}
	if learned := s.linkedServers(); !configured && len(learned) != 0 && !slices.EqualFunc(learned, config.ICEServers, sameICEServer) {
		servers.put(host, learned)
		s.Close()
		config.ICEServers = learned
		return open(ctx, config)
	}
	return s, nil
}

func sameICEServer(a, b webrtc.ICEServer) bool {
	return slices.Equal(a.URLs, b.URLs) && a.Username == b.Username && a.Credential == b.Credential
}

// endpointURL is the https (or, for a "+http" scheme, http) URL a whip or
// whep URL names.
func endpointURL(u *url.URL, protocol string) (*url.URL, error) {
	if u.Host == "" {
		return nil, errors.New(protocol + " URL has no host")
	}
	endpoint := *u
	endpoint.Scheme = "https"
	if strings.HasSuffix(strings.ToLower(u.Scheme), "+http") {
		endpoint.Scheme = "http"
	}
	return &endpoint, nil
}

// signaling is the HTTP side of one WHIP or WHEP session (RFC 9725): POST
// the offer, keep the session resource, trickle late candidates by PATCH,
// and DELETE the resource on close.
type signaling struct {
	protocol string
	config   signalConfig
	pc       *webrtc.PeerConnection
	ctx      context.Context
	wake     chan struct{}

	mu       sync.Mutex
	resource *url.URL
	etag     string
	// trickle holds candidates gathered after the offer went out, and
	// trickleEnd the end of gathering, until the trickle loop PATCHes them.
	trickle    []webrtc.ICECandidateInit
	trickleEnd bool
	patches    bool
	posted     bool
	// linked is the ICE servers the answer named (Link rel="ice-server").
	linked    []webrtc.ICEServer
	closeOnce sync.Once
}

// newSignaling signals for pc until ctx ends.
func newSignaling(ctx context.Context, protocol string, config signalConfig, pc *webrtc.PeerConnection) *signaling {
	s := &signaling{protocol: protocol, config: config, pc: pc, ctx: ctx, wake: make(chan struct{}, 1)}
	pc.OnICECandidate(s.candidate)
	return s
}

// exchange POSTs the peer connection's offer, with the candidates gathered
// within the gather bound, and applies the answer it returns once check, if
// any, accepts it.
func (s *signaling) exchange(ctx context.Context, endpoint *url.URL, check func(answer string) error) (string, error) {
	offer, err := s.pc.CreateOffer(nil)
	if err != nil {
		return "", err
	}
	gathered := webrtc.GatheringCompletePromise(s.pc)
	if err := s.pc.SetLocalDescription(offer); err != nil {
		return "", err
	}
	// Half trickle: wait briefly for candidates, then send what there is.
	wait := time.NewTimer(s.config.gather)
	select {
	case <-gathered:
	case <-wait.C:
	case <-ctx.Done():
		wait.Stop()
		return "", ctx.Err()
	}
	wait.Stop()
	// The offer carries every candidate found so far; only later ones trickle.
	s.mu.Lock()
	local := s.pc.LocalDescription().SDP
	s.trickle, s.trickleEnd = nil, false
	s.mu.Unlock()

	resp, err := s.request(ctx, http.MethodPost, endpoint, "application/sdp", []byte(local), nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	answer, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusCreated {
		return "", &StatusError{Protocol: s.protocol, Method: http.MethodPost, StatusCode: resp.StatusCode, Status: resp.Status}
	}
	location, err := url.Parse(resp.Header.Get("Location"))
	if err != nil || resp.Header.Get("Location") == "" {
		return "", errors.New(s.protocol + ": the answer names no session resource (Location)")
	}
	s.mu.Lock()
	s.resource = resp.Request.URL.ResolveReference(location)
	s.etag = resp.Header.Get("ETag")
	s.patches = strings.Contains(resp.Header.Get("Accept-Patch"), "application/trickle-ice-sdpfrag")
	s.linked = linkedICEServers(resp.Header.Values("Link"))
	s.posted = true
	s.mu.Unlock()
	if check != nil {
		if err := check(string(answer)); err != nil {
			return "", err
		}
	}
	if err := s.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(answer)}); err != nil {
		return "", err
	}
	go s.trickleLoop()
	s.signal()
	return string(answer), nil
}

func (s *signaling) linkedServers() []webrtc.ICEServer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.linked
}

// close DELETEs the session resource, bounded, since the run's context may
// be gone.
func (s *signaling) close() error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		resource := s.resource
		s.mu.Unlock()
		if resource == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, deleteErr := s.request(ctx, http.MethodDelete, resource, "", nil, nil)
		if deleteErr != nil {
			err = deleteErr
			return
		}
		resp.Body.Close()
	})
	return err
}

// candidate queues an ICE candidate (nil: gathering ended) for the trickle
// loop, which runs once the session resource exists.
func (s *signaling) candidate(candidate *webrtc.ICECandidate) {
	s.mu.Lock()
	if candidate == nil {
		s.trickleEnd = true
	} else {
		s.trickle = append(s.trickle, candidate.ToJSON())
	}
	posted := s.posted
	s.mu.Unlock()
	if posted {
		s.signal()
	}
}

func (s *signaling) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// trickleLoop PATCHes queued candidates in order until gathering ends, the
// server refuses PATCH, or the session closes.
func (s *signaling) trickleLoop() {
	for {
		select {
		case <-s.wake:
		case <-s.ctx.Done():
			return
		}
		s.mu.Lock()
		batch, end, patches := s.trickle, s.trickleEnd, s.patches
		s.trickle, s.trickleEnd = nil, false
		s.mu.Unlock()
		if !patches {
			return
		}
		if len(batch) != 0 || end {
			s.patch(batch, end)
		}
		if end {
			return
		}
	}
}

// patch trickles candidates (and the end of candidates) as an
// application/trickle-ice-sdpfrag PATCH; a server that refuses PATCH (405,
// 501) stops the trickle and gets the candidates through ICE itself.
func (s *signaling) patch(candidates []webrtc.ICECandidateInit, end bool) {
	s.mu.Lock()
	resource, etag := s.resource, s.etag
	s.mu.Unlock()
	frag, err := sdpFragment(s.pc.LocalDescription(), candidates, end)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	header := http.Header{}
	if etag != "" {
		header.Set("If-Match", etag)
	}
	resp, err := s.request(ctx, http.MethodPatch, resource, "application/trickle-ice-sdpfrag", frag, header)
	if err != nil {
		return
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented {
		s.mu.Lock()
		s.patches = false
		s.mu.Unlock()
	}
}

func (s *signaling) request(ctx context.Context, method string, target *url.URL, contentType string, body []byte, header http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, s.plainError(err)
	}
	for key, values := range header {
		req.Header[key] = values
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if s.config.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.token)
	}
	resp, err := s.config.client.Do(req)
	if err != nil {
		return nil, s.plainError(err)
	}
	return resp, nil
}

// plainError drops the URL a *url.Error quotes: it may carry credentials.
func (s *signaling) plainError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Errorf("%s: %s: %w", s.protocol, strings.ToUpper(urlErr.Op), urlErr.Err)
	}
	return err
}

// sdpFragment renders candidates as RFC 8840 SDP fragment lines for the
// bundled first media section.
func sdpFragment(local *webrtc.SessionDescription, candidates []webrtc.ICECandidateInit, end bool) ([]byte, error) {
	if local == nil {
		return nil, errors.New("no local description")
	}
	parsed, err := (&webrtc.SessionDescription{Type: local.Type, SDP: local.SDP}).Unmarshal()
	if err != nil || len(parsed.MediaDescriptions) == 0 {
		return nil, errors.New("no media section")
	}
	media := parsed.MediaDescriptions[0]
	ufrag, _ := media.Attribute("ice-ufrag")
	pwd, _ := media.Attribute("ice-pwd")
	mid, _ := media.Attribute("mid")
	var b bytes.Buffer
	fmt.Fprintf(&b, "a=ice-ufrag:%s\r\na=ice-pwd:%s\r\n", ufrag, pwd)
	fmt.Fprintf(&b, "m=%s 9 %s %s\r\na=mid:%s\r\n", media.MediaName.Media, strings.Join(media.MediaName.Protos, "/"), strings.Join(media.MediaName.Formats, " "), mid)
	for _, candidate := range candidates {
		fmt.Fprintf(&b, "a=%s\r\n", candidate.Candidate)
	}
	if end {
		b.WriteString("a=end-of-candidates\r\n")
	}
	return b.Bytes(), nil
}

// linkedICEServers reads the STUN/TURN servers of Link header values of
// rel "ice-server" (RFC 9725 section 4.6):
//
//	<turn:turn.example.net?transport=udp>; rel="ice-server"; username="u"; credential="p"
func linkedICEServers(values []string) []webrtc.ICEServer {
	var servers []webrtc.ICEServer
	for _, value := range values {
		for _, link := range splitOutsideQuotes(value, ',') {
			target, params, ok := strings.Cut(strings.TrimSpace(link), ";")
			target = strings.TrimSpace(target)
			if !ok || !strings.HasPrefix(target, "<") || !strings.HasSuffix(target, ">") {
				continue
			}
			server := webrtc.ICEServer{URLs: []string{target[1 : len(target)-1]}}
			ice := false
			for _, param := range splitOutsideQuotes(params, ';') {
				key, val, _ := strings.Cut(strings.TrimSpace(param), "=")
				val = strings.Trim(strings.TrimSpace(val), `"`)
				switch strings.ToLower(strings.TrimSpace(key)) {
				case "rel":
					ice = slices.Contains(strings.Fields(val), "ice-server")
				case "username":
					server.Username = val
				case "credential":
					server.Credential = val
				}
			}
			if ice {
				servers = append(servers, server)
			}
		}
	}
	return servers
}

// splitOutsideQuotes splits s at sep where sep is not inside quotes or <>.
func splitOutsideQuotes(s string, sep byte) []string {
	var parts []string
	quoted, angled, start := false, false, 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"':
			quoted = !quoted
		case c == '<' && !quoted:
			angled = true
		case c == '>' && !quoted:
			angled = false
		case c == sep && !quoted && !angled:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

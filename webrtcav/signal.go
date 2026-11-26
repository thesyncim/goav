package webrtcav

import (
	"bytes"
	"cmp"
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

	"github.com/pion/sdp/v3"
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
	config.client = keepMethodOnRedirect(config.client)
	return config
}

// keepMethodOnRedirect returns a copy of client that follows only the
// redirects that keep the method and body, 307 and 308 (RFC 9725 section 4.5): Go turns a POST
// redirected by 301, 302, or 303 into a GET, which no WHIP or WHEP endpoint
// answers with a session, so those return as they are, refused. A redirect
// policy the caller set still applies to the rest.
func keepMethodOnRedirect(client *http.Client) *http.Client {
	clone := *client
	check := client.CheckRedirect
	clone.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if req.Response != nil && req.Response.StatusCode != http.StatusTemporaryRedirect && req.Response.StatusCode != http.StatusPermanentRedirect {
			return http.ErrUseLastResponse
		}
		if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
			return errors.New("redirect to unsupported scheme " + req.URL.Scheme)
		}
		if len(via) > 0 && via[0].URL.Scheme == "https" && req.URL.Scheme != "https" {
			return errors.New("redirect downgrades https to " + req.URL.Scheme)
		}
		if check != nil {
			return check(req, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	return &clone
}

// WithToken sends token as the Bearer credential on every request (POST,
// PATCH, DELETE), so the URL carries none.
func WithToken(token string) Option {
	return func(c *signalConfig) { c.token = token }
}

// WithHTTPClient sends the requests through client; the default bounds each
// request to 15s.
func WithHTTPClient(client *http.Client) Option {
	return func(c *signalConfig) { c.client = client }
}

// WithSession configures the Pion peer connection the way a receive Session
// is: its API (media engine, interceptors, setting engine) and Configuration
// (ICE servers).
func WithSession(config SessionConfig) Option {
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
// start with them. Servers the caller configured (WithSession) take
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

type signalState uint8

const (
	sigGathering signalState = iota
	sigTrickling
	sigTrickleStopped
	sigClosed
)

// signaling is the HTTP side of one WHIP or WHEP session (RFC 9725): POST
// the offer, keep the session resource, trickle late candidates by PATCH,
// and DELETE the resource on close.
type signaling struct {
	protocol string
	config   signalConfig
	pc       *webrtc.PeerConnection
	ctx      context.Context
	wake     chan struct{}

	mu           sync.Mutex
	state        signalState
	originHost   string
	originScheme string
	resource     *url.URL
	etag         string
	// trickle holds candidates gathered after the offer went out, and
	// trickleEnd the end of gathering, until the trickle loop PATCHes them.
	trickle    []webrtc.ICECandidateInit
	trickleEnd bool
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
	s.originHost = endpoint.Host
	s.originScheme = endpoint.Scheme
	local := s.pc.LocalDescription().SDP
	s.trickle, s.trickleEnd = nil, false
	s.mu.Unlock()

	resp, err := s.request(ctx, http.MethodPost, endpoint, "application/sdp", []byte(local), nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", s.plainError(err)
	}
	if resp.StatusCode != http.StatusCreated {
		return "", &StatusError{Protocol: s.protocol, Method: http.MethodPost, StatusCode: resp.StatusCode, Status: resp.Status}
	}
	location, err := url.Parse(resp.Header.Get("Location"))
	if err != nil || resp.Header.Get("Location") == "" {
		return "", errors.New(s.protocol + ": the answer names no session resource (Location)")
	}
	resolved := resp.Request.URL.ResolveReference(location)
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return "", errors.New(s.protocol + ": session resource Location has unsupported scheme")
	}
	if endpoint.Scheme == "https" && resolved.Scheme != "https" {
		return "", errors.New(s.protocol + ": session resource Location downgrades https to " + resolved.Scheme)
	}
	s.mu.Lock()
	s.resource = resolved
	s.etag = resp.Header.Get("ETag")
	s.linked = linkedICEServers(resp.Header.Values("Link"))
	trickling := false
	if s.state == sigGathering {
		if acceptsTricklePatch(resp.Header.Values("Accept-Patch")) {
			s.state = sigTrickling
			trickling = true
		} else {
			s.state = sigTrickleStopped
			s.trickle, s.trickleEnd = nil, false
		}
	}
	s.mu.Unlock()
	if check != nil {
		if err := check(string(answer)); err != nil {
			return "", err
		}
	}
	if err := s.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(answer)}); err != nil {
		return "", err
	}
	if trickling {
		go s.trickleLoop()
		s.signal()
	}
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
		s.state = sigClosed
		s.trickle, s.trickleEnd = nil, false
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
	if s.state >= sigTrickleStopped {
		s.mu.Unlock()
		return
	}
	if candidate == nil {
		s.trickleEnd = true
	} else {
		s.trickle = append(s.trickle, candidate.ToJSON())
	}
	trickling := s.state == sigTrickling
	s.mu.Unlock()
	if trickling {
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
		if s.state != sigTrickling {
			s.mu.Unlock()
			return
		}
		batch, end := s.trickle, s.trickleEnd
		s.trickle, s.trickleEnd = nil, false
		if end {
			s.state = sigTrickleStopped
		}
		s.mu.Unlock()
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
	// A trickle PATCH names its ICE session by the entity-tag the 201 gave
	// (RFC 9725 sections 4.3.1, 4.3.2; If-Match: * is how an ICE restart
	// asks, 4.3.3). With none, * satisfies a server that requires a
	// precondition (428) without naming a session.
	header := http.Header{"If-Match": {cmp.Or(etag, "*")}}
	resp, err := s.request(ctx, http.MethodPatch, resource, "application/trickle-ice-sdpfrag", frag, header)
	if err != nil {
		return
	}
	resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if newETag := resp.Header.Get("ETag"); newETag != "" {
			s.mu.Lock()
			s.etag = newETag
			s.mu.Unlock()
		}
		return
	}
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented {
		s.mu.Lock()
		if s.state < sigTrickleStopped {
			s.state = sigTrickleStopped
			s.trickle, s.trickleEnd = nil, false
		}
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
		s.mu.Lock()
		originHost, originScheme := s.originHost, s.originScheme
		s.mu.Unlock()
		if originHost == "" || (target.Host == originHost && target.Scheme == originScheme) {
			req.Header.Set("Authorization", "Bearer "+s.config.token)
		}
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

func acceptsTricklePatch(values []string) bool {
	for _, v := range values {
		if strings.Contains(strings.ToLower(v), "application/trickle-ice-sdpfrag") {
			return true
		}
	}
	return false
}

// sdpFragment renders candidates as RFC 8840 SDP fragment lines grouped by
// their target media section (SDPMid / SDPMLineIndex), skipping rejected
// sections and empty candidates.
func sdpFragment(local *webrtc.SessionDescription, candidates []webrtc.ICECandidateInit, end bool) ([]byte, error) {
	if local == nil {
		return nil, errors.New("no local description")
	}
	parsed, err := (&webrtc.SessionDescription{Type: local.Type, SDP: local.SDP}).Unmarshal()
	if err != nil || len(parsed.MediaDescriptions) == 0 {
		return nil, errors.New("no media section")
	}
	sessionUfrag, _ := parsed.Attribute("ice-ufrag")
	sessionPwd, _ := parsed.Attribute("ice-pwd")

	defaultIdx := 0
	for i, md := range parsed.MediaDescriptions {
		if md.MediaName.Port.Value != 0 {
			defaultIdx = i
			break
		}
	}
	grouped := make([][]string, len(parsed.MediaDescriptions))
	used := make([]bool, len(parsed.MediaDescriptions))
	for _, candidate := range candidates {
		if candidate.Candidate == "" {
			continue
		}
		idx := matchCandidateSection(parsed.MediaDescriptions, candidate, defaultIdx)
		if parsed.MediaDescriptions[idx].MediaName.Port.Value == 0 {
			continue
		}
		grouped[idx] = append(grouped[idx], candidate.Candidate)
		used[idx] = true
	}
	anyUsed := false
	for _, u := range used {
		if u {
			anyUsed = true
			break
		}
	}
	if !anyUsed {
		if !end {
			return nil, errors.New("no candidates")
		}
		used[defaultIdx] = true
	}

	var b bytes.Buffer
	wroteHeader := false
	for i, md := range parsed.MediaDescriptions {
		if !used[i] {
			continue
		}
		if err := writeFragmentSection(&b, md, sessionUfrag, sessionPwd, grouped[i], end, &wroteHeader); err != nil {
			return nil, err
		}
	}
	return b.Bytes(), nil
}

func matchCandidateSection(mds []*sdp.MediaDescription, candidate webrtc.ICECandidateInit, defaultIdx int) int {
	if candidate.SDPMid != nil && *candidate.SDPMid != "" {
		for i, md := range mds {
			if mid, _ := md.Attribute("mid"); mid == *candidate.SDPMid {
				return i
			}
		}
	}
	if candidate.SDPMLineIndex != nil {
		if i := int(*candidate.SDPMLineIndex); i >= 0 && i < len(mds) {
			return i
		}
	}
	return defaultIdx
}

func writeFragmentSection(b *bytes.Buffer, md *sdp.MediaDescription, sessionUfrag, sessionPwd string, cands []string, end bool, wroteHeader *bool) error {
	ufrag, _ := md.Attribute("ice-ufrag")
	if ufrag == "" {
		ufrag = sessionUfrag
	}
	pwd, _ := md.Attribute("ice-pwd")
	if pwd == "" {
		pwd = sessionPwd
	}
	if ufrag == "" || pwd == "" {
		return errors.New("missing ice credentials")
	}
	mid, _ := md.Attribute("mid")
	if !*wroteHeader {
		fmt.Fprintf(b, "a=ice-ufrag:%s\r\na=ice-pwd:%s\r\n", ufrag, pwd)
		*wroteHeader = true
	}
	fmt.Fprintf(b, "m=%s 9 %s %s\r\na=mid:%s\r\n", md.MediaName.Media, strings.Join(md.MediaName.Protos, "/"), strings.Join(md.MediaName.Formats, " "), mid)
	for _, cand := range cands {
		fmt.Fprintf(b, "a=%s\r\n", cand)
	}
	if end {
		b.WriteString("a=end-of-candidates\r\n")
	}
	return nil
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
					ice = slices.Contains(strings.Fields(strings.ToLower(val)), "ice-server")
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
		case c == '\\' && quoted && i+1 < len(s):
			i++
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

package scheme

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

// ErrChanged reports an HTTP resource that changed while it was read: a
// ranged request's validator (ETag or Last-Modified) no longer matched, so
// the bytes read so far and the bytes to come belong to different files.
var ErrChanged = errors.New("scheme: http resource changed while it was read")

// HTTPOption configures HTTP.
type HTTPOption func(*httpConfig)

type httpConfig struct {
	client  *http.Client
	headers http.Header
}

// Client makes HTTP send its requests through c instead of the default
// client, whose transport bounds dialing (10s), the TLS handshake (10s), and
// the wait for response headers (30s), and which follows at most 10
// redirects. c keeps its own timeouts and redirect policy; credentials set
// with Header are still dropped when a redirect leaves the URL's host.
func Client(c *http.Client) HTTPOption {
	return func(config *httpConfig) { config.client = c }
}

// Header adds a header to every request HTTP sends: the place for
// credentials (Authorization, an API key) a URL should not carry. goav
// redacts the URLs it reports, and never reports headers.
func Header(key, value string) HTTPOption {
	return func(config *httpConfig) { config.headers.Add(key, value) }
}

// HTTP is the http and https scheme for inputs. The resource is container
// bytes read through ranged GETs, so a random-access container (MP4 with its
// index at the end) seeks instead of downloading everything first. Requests
// after the first carry If-Range with its strong ETag or Last-Modified, and
// a server that answers with a different representation fails the read with
// ErrChanged instead of splicing two files. Redirects are followed once:
// later requests go to the final URL. A server without range support is read
// progressively, keeping a window of recent bytes for short backward reads
// and reading again from the start for longer ones.
//
// A read that waits 30s for data fails, and the run's context ends reads.
// HTTP is an input scheme: it does not write.
func HTTP(options ...HTTPOption) provider.InputScheme {
	config := httpConfig{headers: http.Header{}}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	return httpScheme{client: redirectCapped(config.client, config.headers), headers: config.headers}
}

const maxRedirects = 10

// defaultTransport bounds every phase before the body: an unbounded dial or
// header wait would hang a run on a dead server.
var defaultTransport = &http.Transport{
	Proxy:                 http.ProxyFromEnvironment,
	DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	TLSHandshakeTimeout:   10 * time.Second,
	ResponseHeaderTimeout: 30 * time.Second,
	ExpectContinueTimeout: time.Second,
	IdleConnTimeout:       90 * time.Second,
	MaxIdleConnsPerHost:   4,
	ForceAttemptHTTP2:     true,
}

// redirectCapped returns a client that stops after maxRedirects (keeping a
// caller's own redirect policy when it has one) and drops the configured
// header credentials when a redirect leaves the original host: net/http
// drops Authorization and Cookie itself, but not custom headers.
func redirectCapped(client *http.Client, headers http.Header) *http.Client {
	var capped http.Client
	if client != nil {
		capped = *client
	} else {
		capped = http.Client{Transport: defaultTransport}
	}
	own := capped.CheckRedirect
	capped.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > 0 && req.URL.Host != via[0].URL.Host {
			for key := range headers {
				req.Header.Del(key)
			}
		}
		if own != nil {
			return own(req, via)
		}
		if len(via) >= maxRedirects {
			return fmt.Errorf("stopped after %d redirects", maxRedirects)
		}
		return nil
	}
	return &capped
}

type httpScheme struct {
	client  *http.Client
	headers http.Header
}

func (s httpScheme) Input(u *url.URL) (provider.Input, error) {
	if u.Host == "" {
		return nil, errors.New("http URL has no host")
	}
	return httpInput{scheme: s, url: u}, nil
}

type httpInput struct {
	scheme httpScheme
	url    *url.URL
}

func (httpInput) SourceShape() shape.Spec { return shape.Spec{} }

func (in httpInput) OpenBytes(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := newHTTPReader(in.scheme, in.url)
	// The open honors ctx; reads after it honor Close, which the runtime
	// calls when the run's context ends.
	stop := context.AfterFunc(ctx, r.cancel)
	defer stop()
	if err := r.open(); err != nil {
		r.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	return r, nil
}

const (
	minChunk = 256 << 10
	maxChunk = 8 << 20
	// skipLimit is how far ahead a read may land and still be served by
	// discarding from the open body rather than by a new request.
	skipLimit = 256 << 10
	// drainLimit is how much of an abandoned body is drained so its
	// connection is reused.
	drainLimit = 64 << 10
	// drainWait bounds that drain.
	drainWait = 100 * time.Millisecond
	// window is how many recent bytes a progressive read keeps for backward
	// reads.
	window = 1 << 20
	// readIdle bounds a read waiting for data.
	readIdle = 30 * time.Second
)

var errStalled = fmt.Errorf("http: no data for %s", readIdle)

// httpReader is an io.ReadSeeker and io.ReaderAt over an HTTP resource.
// Calls serialize, so ReadAt is safe for concurrent use.
type httpReader struct {
	client  *http.Client
	headers http.Header
	ctx     context.Context
	cancel  context.CancelFunc

	mu  sync.Mutex
	url *url.URL
	// origin is the host the caller named, the one header credentials go to.
	origin string
	// size is the resource length, or -1 when the server does not say.
	size   int64
	ranges bool
	// validator is the If-Range value: a strong ETag, else Last-Modified.
	validator string
	etag      string

	body *httpBody
	pos  int64 // Read and Seek position
	// nextEnd and chunk grow ranges while reads stay sequential.
	nextEnd int64
	chunk   int64
	// win holds the most recent bytes a progressive read passed, from
	// offset winStart.
	win      []byte
	winStart int64
	scratch  []byte
	closed   bool
}

// httpBody is one response body positioned at offset at and ending at end
// (exclusive), or at the resource's end when end is -1.
type httpBody struct {
	resp    *http.Response
	reader  *bufio.Reader
	guard   *stallGuard
	at, end int64
}

func newHTTPReader(s httpScheme, u *url.URL) *httpReader {
	ctx, cancel := context.WithCancel(context.Background())
	return &httpReader{client: s.client, headers: s.headers, ctx: ctx, cancel: cancel, url: u, origin: u.Host, size: -1, chunk: minChunk}
}

// open probes the resource with a first ranged GET: a 206 means ranges, a
// 200 a progressive read of the body it already started.
func (r *httpReader) open() error {
	resp, err := r.get(0, minChunk, false)
	if err != nil {
		return err
	}
	r.url = resp.Request.URL
	r.etag = resp.Header.Get("ETag")
	switch {
	case r.etag != "" && !strings.HasPrefix(r.etag, "W/"):
		r.validator = r.etag
	case resp.Header.Get("Last-Modified") != "":
		r.validator = resp.Header.Get("Last-Modified")
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		start, end, total, ok := contentRange(resp.Header.Get("Content-Range"))
		if !ok || start != 0 {
			resp.Body.Close()
			return fmt.Errorf("http: malformed Content-Range %q", resp.Header.Get("Content-Range"))
		}
		r.ranges, r.size, r.nextEnd = true, total, end+1
		r.attach(resp, 0, end+1)
	case http.StatusOK:
		r.size = resp.ContentLength
		r.attach(resp, 0, -1)
	case http.StatusRequestedRangeNotSatisfiable:
		// Only an empty resource refuses bytes=0-.
		resp.Body.Close()
		r.ranges, r.size = true, 0
	default:
		resp.Body.Close()
		return statusError(resp)
	}
	return nil
}

// get sends a GET for length bytes at off (the whole resource when length
// is 0), with If-Range when ifRange.
func (r *httpReader) get(off, length int64, ifRange bool) (*http.Response, error) {
	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, r.url.String(), nil)
	if err != nil {
		return nil, plainError(err)
	}
	if r.url.Host == r.origin {
		// Credentials go to the URL's host only, not to where it redirected.
		for key, values := range r.headers {
			req.Header[key] = append([]string(nil), values...)
		}
	}
	if length > 0 {
		end := off + length - 1
		if r.size >= 0 && end >= r.size {
			end = r.size - 1
		}
		req.Header.Set("Range", "bytes="+strconv.FormatInt(off, 10)+"-"+strconv.FormatInt(end, 10))
		if ifRange && r.validator != "" {
			req.Header.Set("If-Range", r.validator)
		}
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, plainError(err)
	}
	return resp, nil
}

func (r *httpReader) attach(resp *http.Response, at, end int64) {
	guard := &stallGuard{body: resp.Body}
	guard.timer = time.AfterFunc(readIdle, guard.expire)
	guard.timer.Stop()
	r.body = &httpBody{resp: resp, reader: bufio.NewReaderSize(guard, 64<<10), guard: guard, at: at, end: end}
}

// release lets go of the open body, draining a short rest so its
// connection is reused. The drain never waits on a slow server: after
// drainWait the body closes, costing only the connection.
func (r *httpReader) release() {
	b := r.body
	if b == nil {
		return
	}
	r.body = nil
	if b.end >= 0 && b.end-b.at <= drainLimit && !b.guard.stalled.Load() {
		deadline := time.AfterFunc(drainWait, func() { b.resp.Body.Close() })
		_, _ = io.Copy(io.Discard, b.reader)
		deadline.Stop()
	}
	b.guard.timer.Stop()
	b.resp.Body.Close()
}

// Read reads at the reader's position and advances it.
func (r *httpReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, err := r.readAt(p, r.pos)
	r.pos += int64(n)
	return n, err
}

// ReadAt reads len(p) bytes at off without moving the position.
func (r *httpReader) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for total < len(p) {
		n, err := r.readAt(p[total:], off+int64(total))
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// Seek moves the position; SeekEnd needs a known size.
func (r *httpReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.pos
	case io.SeekEnd:
		if r.size < 0 {
			return 0, errors.New("http: seek from the end of a resource of unknown size")
		}
		offset += r.size
	default:
		return 0, errors.New("http: invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("http: negative position")
	}
	r.pos = offset
	return offset, nil
}

// Size reports the resource length, or 0 when the server does not say.
func (r *httpReader) Size() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return max(r.size, 0)
}

// Close ends every request, unblocking a pending read.
func (r *httpReader) Close() error {
	r.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		if b := r.body; b != nil {
			r.body = nil
			b.guard.timer.Stop()
			b.resp.Body.Close()
		}
	}
	return nil
}

// readAt reads some bytes at off.
func (r *httpReader) readAt(p []byte, off int64) (int, error) {
	if r.closed {
		return 0, errors.New("http: read after close")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.size >= 0 && off >= r.size {
		return 0, io.EOF
	}
	if !r.ranges {
		return r.readProgressive(p, off)
	}
	return r.readRanged(p, off)
}

// readRanged serves a read from the open ranged body when off is in it or
// just ahead, else from a new range at off. A body that breaks mid-range (a
// dropped connection, a stall) reopens once at the same offset.
func (r *httpReader) readRanged(p []byte, off int64) (int, error) {
	for attempt := 0; ; attempt++ {
		b := r.body
		if b == nil || off < b.at || off-b.at > skipLimit || off >= b.end {
			if err := r.openRange(off); err != nil {
				return 0, err
			}
			b = r.body
		}
		skipped, err := b.reader.Discard(int(off - b.at))
		b.at += int64(skipped)
		n := 0
		if err == nil {
			n, err = b.reader.Read(p[:min(int64(len(p)), b.end-off)])
			b.at += int64(n)
		}
		if b.at >= b.end {
			// Fully read: closing it returns its connection to the pool.
			r.release()
		}
		if n > 0 {
			return n, nil
		}
		if err == nil {
			continue
		}
		err = r.bodyFailure(b, err)
		r.release()
		if attempt > 0 || r.ctx.Err() != nil {
			return 0, err
		}
	}
}

// bodyFailure names why a body read failed: a stall, the reader closing, or
// the connection.
func (r *httpReader) bodyFailure(b *httpBody, err error) error {
	switch {
	case b.guard.stalled.Load():
		return errStalled
	case r.ctx.Err() != nil:
		return r.ctx.Err()
	case err == io.EOF:
		return io.ErrUnexpectedEOF
	default:
		return plainError(err)
	}
}

// openRange replaces the open body with a ranged one at off, doubling the
// range while reads stay sequential.
func (r *httpReader) openRange(off int64) error {
	r.release()
	if off == r.nextEnd {
		r.chunk = min(r.chunk*2, maxChunk)
	} else {
		r.chunk = minChunk
	}
	resp, err := r.get(off, r.chunk, true)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		start, end, total, ok := contentRange(resp.Header.Get("Content-Range"))
		etag := resp.Header.Get("ETag")
		switch {
		case !ok || start != off:
			resp.Body.Close()
			return fmt.Errorf("http: Content-Range %q does not start at %d", resp.Header.Get("Content-Range"), off)
		case total != r.size || r.etag != "" && etag != "" && etag != r.etag:
			resp.Body.Close()
			return ErrChanged
		}
		r.nextEnd = end + 1
		r.attach(resp, off, end+1)
		return nil
	case http.StatusOK:
		// If-Range failed: the server sent the whole file, a different one.
		resp.Body.Close()
		if r.validator != "" {
			return ErrChanged
		}
		return errors.New("http: server stopped honoring ranges")
	case http.StatusRequestedRangeNotSatisfiable:
		resp.Body.Close()
		return io.EOF
	default:
		resp.Body.Close()
		return statusError(resp)
	}
}

// readProgressive serves a read from a server without ranges: from the
// window of recent bytes, from the open body read forward, or, further
// back, from the resource read again from its start.
func (r *httpReader) readProgressive(p []byte, off int64) (int, error) {
	if n, ok := r.fromWindow(p, off); ok {
		return n, nil
	}
	if r.body == nil || off < r.body.at {
		if err := r.restart(); err != nil {
			return 0, err
		}
	}
	if r.scratch == nil {
		r.scratch = make([]byte, 32<<10)
	}
	for r.body.at < off {
		n, err := r.body.reader.Read(r.scratch[:min(int64(len(r.scratch)), off-r.body.at)])
		r.keep(r.scratch[:n])
		if err != nil {
			return 0, r.progressiveFailure(err)
		}
	}
	n, err := r.body.reader.Read(p)
	r.keep(p[:n])
	if n > 0 {
		return n, nil
	}
	return 0, r.progressiveFailure(err)
}

func (r *httpReader) fromWindow(p []byte, off int64) (int, bool) {
	if off < r.winStart || off >= r.winStart+int64(len(r.win)) {
		return 0, false
	}
	return copy(p, r.win[off-r.winStart:]), true
}

func (r *httpReader) progressiveFailure(err error) error {
	if err == io.EOF && r.ctx.Err() == nil && !r.body.guard.stalled.Load() {
		if r.size < 0 {
			r.size = r.body.at
		}
		if r.body.at < r.size {
			return io.ErrUnexpectedEOF
		}
		return io.EOF
	}
	return r.bodyFailure(r.body, err)
}

// keep records bytes the progressive body passed in the window.
func (r *httpReader) keep(b []byte) {
	r.body.at += int64(len(b))
	r.win = append(r.win, b...)
	if len(r.win) > 2*window {
		drop := len(r.win) - window
		r.win = append(r.win[:0], r.win[drop:]...)
		r.winStart += int64(drop)
	}
}

// restart reads a progressive resource again from its start, refusing a
// different representation.
func (r *httpReader) restart() error {
	r.release()
	resp, err := r.get(0, 0, false)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return statusError(resp)
	}
	if etag := resp.Header.Get("ETag"); etag != r.etag ||
		r.size >= 0 && resp.ContentLength >= 0 && resp.ContentLength != r.size {
		resp.Body.Close()
		return ErrChanged
	}
	r.attach(resp, 0, -1)
	r.win, r.winStart = r.win[:0], 0
	return nil
}

// stallGuard fails a body read that waits readIdle for data. Its timer runs
// only while a read is blocked, so a paused task never trips it.
type stallGuard struct {
	body    io.ReadCloser
	timer   *time.Timer
	stalled atomic.Bool
}

func (g *stallGuard) Read(p []byte) (int, error) {
	g.timer.Reset(readIdle)
	n, err := g.body.Read(p)
	g.timer.Stop()
	return n, err
}

func (g *stallGuard) expire() {
	g.stalled.Store(true)
	g.body.Close()
}

// contentRange parses "bytes start-end/total"; total is -1 for "*".
func contentRange(value string) (start, end, total int64, ok bool) {
	rest, found := strings.CutPrefix(value, "bytes ")
	if !found {
		return 0, 0, 0, false
	}
	span, size, found := strings.Cut(rest, "/")
	if !found {
		return 0, 0, 0, false
	}
	first, last, found := strings.Cut(span, "-")
	if !found {
		return 0, 0, 0, false
	}
	var err error
	if start, err = strconv.ParseInt(first, 10, 64); err != nil {
		return 0, 0, 0, false
	}
	if end, err = strconv.ParseInt(last, 10, 64); err != nil || end < start {
		return 0, 0, 0, false
	}
	total = -1
	if size != "*" {
		if total, err = strconv.ParseInt(size, 10, 64); err != nil {
			return 0, 0, 0, false
		}
	}
	return start, end, total, true
}

func statusError(resp *http.Response) error {
	return fmt.Errorf("http: GET answered %s", resp.Status)
}

// plainError drops the URL a *url.Error quotes, which may carry credentials.
func plainError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Errorf("http: %s: %w", strings.ToUpper(urlErr.Op), urlErr.Err)
	}
	return err
}

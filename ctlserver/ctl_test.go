package ctlserver_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/ctlserver"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/pipeline"
)

// counter is a sink element that counts the frames an attached program
// delivers.
func counter(name string, n *atomic.Int64) launch.Element {
	return launch.Sink(name, "count frames", func(struct{}) (goav.Destination, error) {
		return goav.Sink(component.SinkFunc(name, func(_ context.Context, m component.Message) error {
			if m.Frame != nil || m.Packet != nil {
				n.Add(1)
			}
			return nil
		})), nil
	})
}

type harness struct {
	task    goav.LiveTask
	server  *ctlserver.Server
	client  *ctlserver.Client
	address string
}

// serve runs a live program and serves it on a unix socket.
func serve(t *testing.T, program string, options ...ctlserver.Option) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	outs, err := launch.Compile(program)
	if err != nil {
		t.Fatal(err)
	}
	task, err := bundle.Build(ctx, outs...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = task.Close() })
	go func() { _ = task.Run(ctx) }()
	server, err := ctlserver.New(task, options...)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "ctl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	address := "unix://" + filepath.Join(dir, "s.sock")
	go func() { _ = server.ListenAndServe(ctx, address) }()
	var client *ctlserver.Client
	deadline := time.Now().Add(5 * time.Second)
	for client == nil {
		if c, err := ctlserver.Dial(ctx, address); err == nil {
			client = c
		} else if time.Now().After(deadline) {
			t.Fatalf("dial: %v", err)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Cleanup(func() { _ = client.Close() })
	return &harness{task: task, server: server, client: client, address: address}
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

const liveProgram = `testsrc video size=32x32 live ! tap live ! encode vp8 ! as cam ! null`

func TestInspectMethods(t *testing.T) {
	h := serve(t, liveProgram)
	ctx := context.Background()
	taps, err := h.client.Call[[]goav.TapSnapshot](ctx, "taps", nil)
	if err != nil || len(taps) != 1 || taps[0].Name != "live" {
		t.Fatalf("taps = %+v, %v", taps, err)
	}
	if _, err := h.client.Call[pipeline.GraphStats](ctx, "stats", nil); err != nil {
		t.Fatalf("stats: %v", err)
	}
	if _, err := h.client.Call[goav.TaskSnapshot](ctx, "snapshot", nil); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	text, err := h.client.Call[string](ctx, "describe", map[string]string{"format": "text"})
	if err != nil || !strings.Contains(text, "encode-cam") {
		t.Fatalf("describe text = %q, %v", text, err)
	}
	if _, err := h.client.Call[pipeline.Spec](ctx, "describe", nil); err != nil {
		t.Fatalf("describe json: %v", err)
	}
	caps, err := h.client.Call[ctlserver.Capabilities](ctx, "capabilities", nil)
	if err != nil {
		t.Fatal(err)
	}
	var attach, testsrc bool
	for _, m := range caps.Methods {
		attach = attach || m.Name == "attach" && len(m.Params) == 1 && m.Params[0].Required
	}
	for _, e := range caps.Elements {
		testsrc = testsrc || e.Name == "testsrc"
	}
	if !attach || !testsrc {
		t.Fatalf("capabilities lack attach(program) or the testsrc element: %+v", caps)
	}
}

func TestAttachRebranchDetach(t *testing.T) {
	var first, second atomic.Int64
	h := serve(t, liveProgram, ctlserver.Elements(counter("first", &first), counter("second", &second)))
	ctx := context.Background()
	att, err := h.client.Call[ctlserver.Attached](ctx, "attach", map[string]string{"program": "tap live ! resize 16x16 ! as preview ! first"})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	eventually(t, "the attached program to receive frames", func() bool { return first.Load() > 0 })
	next, err := h.client.Call[ctlserver.Attached](ctx, "rebranch", map[string]string{"attachment": att.Attachment, "program": "tap live ! as preview2 ! second", "switch": "next-frame"})
	if err != nil {
		t.Fatalf("rebranch: %v", err)
	}
	eventually(t, "the replacement to receive frames", func() bool { return second.Load() > 0 })
	if _, err := h.client.Call[struct{}](ctx, "detach", map[string]string{"attachment": next.Attachment, "outcome": "drain"}); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if _, err := h.client.Call[struct{}](ctx, "detach", map[string]string{"attachment": att.Attachment}); err == nil {
		t.Fatal("detaching a rebranched-away attachment succeeded")
	}
}

func TestControlMethods(t *testing.T) {
	h := serve(t, liveProgram)
	ctx := context.Background()
	for _, call := range []struct {
		method string
		params map[string]string
	}{
		{"bitrate", map[string]string{"stream": "cam", "bitrate": "500k"}},
		{"keyframe", map[string]string{"stream": "cam"}},
		{"seek", map[string]string{"position": "100ms", "source": "video"}},
		{"rate", map[string]string{"value": "2"}},
		{"pause", nil},
		{"resume", nil},
	} {
		if _, err := h.client.Call[struct{}](ctx, call.method, call.params); err != nil {
			t.Fatalf("%s: %v", call.method, err)
		}
	}
}

func TestRefusals(t *testing.T) {
	h := serve(t, liveProgram)
	ctx := context.Background()
	refused := func(method string, params any) *ctlserver.Error {
		t.Helper()
		_, err := h.client.Call[json.RawMessage](ctx, method, params)
		var e *ctlserver.Error
		if !errors.As(err, &e) {
			t.Fatalf("%s: err = %v, want a *ctlserver.Error", method, err)
		}
		return e
	}
	if e := refused("statz", nil); e.Code != ctlserver.CodeMethodNotFound || !strings.Contains(e.Message, `did you mean "stats"?`) {
		t.Fatalf("unknown method: %+v", e)
	}
	if e := refused("bitrate", map[string]string{"stream": "cam"}); e.Code != ctlserver.CodeInvalidParams || !strings.Contains(e.Message, `missing param "bitrate"`) {
		t.Fatalf("missing param: %+v", e)
	}
	if e := refused("bitrate", map[string]string{"stream": "cam", "bitrat": "1M"}); e.Code != ctlserver.CodeInvalidParams || !strings.Contains(e.Message, `did you mean "bitrate"?`) {
		t.Fatalf("misspelled param: %+v", e)
	}
	e := refused("attach", map[string]string{"program": "tap live ! resise 16x16 ! null"})
	var detail struct {
		Kind   string `json:"kind"`
		Line   int    `json:"line"`
		Column int    `json:"column"`
		Hint   string `json:"hint"`
	}
	if e.Code != ctlserver.CodeRefused || json.Unmarshal(e.Data, &detail) != nil || detail.Kind != "program" || detail.Column != 12 || detail.Hint != `did you mean "resize"?` {
		t.Fatalf("program refusal: %+v / %s", e, e.Data)
	}
	if e := refused("attach", map[string]string{"program": "tap live ! encode vp8 ! file out.webm"}); !strings.Contains(e.Message, "may not write files") {
		t.Fatalf("file write without Files: %+v", e)
	}
}

func TestWatchStreamsEvents(t *testing.T) {
	var n atomic.Int64
	h := serve(t, liveProgram, ctlserver.Elements(counter("sink", &n)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := h.client.Watch(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.Call[ctlserver.Attached](ctx, "attach", map[string]string{"program": "tap live ! as watched ! sink"}); err != nil {
		t.Fatal(err)
	}
	select {
	case e, ok := <-events:
		if !ok || e.Type == "" {
			t.Fatalf("event = %+v, open = %v", e, ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no event after an attach")
	}
	cancel()
	eventually(t, "the watch to close", func() bool {
		select {
		case _, ok := <-events:
			return !ok
		default:
			return false
		}
	})
}

func TestHostMethod(t *testing.T) {
	type greetParams struct {
		Name string `json:"name" ctl:"required" help:"who to greet"`
	}
	h := serve(t, liveProgram, ctlserver.Method("greet", "say hello", func(_ context.Context, task goav.LiveTask, p greetParams) (any, error) {
		return "hello " + p.Name + " from " + task.Taps()[0].Name, nil
	}))
	got, err := h.client.Call[string](context.Background(), "greet", map[string]string{"name": "ops"})
	if err != nil || got != "hello ops from live" {
		t.Fatalf("greet = %q, %v", got, err)
	}
}

func TestHTTPTransport(t *testing.T) {
	h := serve(t, liveProgram)
	web := httptest.NewServer(h.server)
	defer web.Close()
	resp, err := http.Post(web.URL, "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"taps"}`))
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		ID     int                `json:"id"`
		Result []goav.TapSnapshot `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.ID != 7 || len(out.Result) != 1 {
		t.Fatalf("POST taps = %+v, %v", out, err)
	}
	resp.Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, web.URL+"/events", nil)
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	go func() {
		body := `{"jsonrpc":"2.0","id":1,"method":"keyframe","params":{"stream":"cam"}}`
		for ctx.Err() == nil {
			if r, err := http.Post(web.URL, "application/json", strings.NewReader(body)); err == nil {
				r.Body.Close()
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	scanner := bufio.NewScanner(stream.Body)
	for scanner.Scan() {
		if line := scanner.Text(); strings.HasPrefix(line, "data: {") {
			return
		}
	}
	t.Fatalf("no server-sent event: %v", scanner.Err())
}

// TestServingCostsTheDataPlaneNothing pins the control plane's cost: the
// allocations each frame makes are the same whether or not a server, a
// connected client, and its watch are attached.
func TestServingCostsTheDataPlaneNothing(t *testing.T) {
	perFrame := func(served bool) float64 {
		run := func(frames int) uint64 {
			outs, err := launch.Compile("testsrc video size=16x16 frames=" + strconv.Itoa(frames) + " ! null")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			task, err := bundle.Build(ctx, outs...)
			if err != nil {
				t.Fatal(err)
			}
			defer task.Close()
			if served {
				server, err := ctlserver.New(task)
				if err != nil {
					t.Fatal(err)
				}
				dir, _ := os.MkdirTemp("", "ctl")
				defer os.RemoveAll(dir)
				address := "unix://" + filepath.Join(dir, "s.sock")
				go func() { _ = server.ListenAndServe(ctx, address) }()
				var client *ctlserver.Client
				for client == nil {
					client, _ = ctlserver.Dial(ctx, address)
					time.Sleep(5 * time.Millisecond)
				}
				defer client.Close()
				if _, err := client.Watch(ctx, map[string][]string{"types": {"never"}}); err != nil {
					t.Fatal(err)
				}
			}
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			if err := task.Run(ctx); err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&after)
			return after.Mallocs - before.Mallocs
		}
		return float64(run(400)-run(200)) / 200
	}
	unserved, served := perFrame(false), perFrame(true)
	if served > unserved+0.5 {
		t.Fatalf("allocations per frame: %.2f served vs %.2f unserved; serving must cost the data plane nothing", served, unserved)
	}
	t.Logf("allocations per frame: %.2f served, %.2f unserved", served, unserved)
}

func TestBatchAndMalformedRequests(t *testing.T) {
	h := serve(t, liveProgram)
	web := httptest.NewServer(h.server)
	defer web.Close()
	post := func(body string) string {
		t.Helper()
		resp, err := http.Post(web.URL, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out bytes.Buffer
		_, _ = out.ReadFrom(resp.Body)
		return out.String()
	}
	// A batch runs in order and answers every request but the notification.
	got := post(`[{"jsonrpc":"2.0","id":1,"method":"pause"},{"jsonrpc":"2.0","method":"keyframe","params":{"stream":"cam"}},{"jsonrpc":"2.0","id":2,"method":"resume"}]`)
	var replies []struct {
		ID    int             `json:"id"`
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal([]byte(got), &replies); err != nil || len(replies) != 2 || replies[0].ID != 1 || replies[1].ID != 2 || replies[0].Error != nil || replies[1].Error != nil {
		t.Fatalf("batch = %s", got)
	}
	// A request goav cannot read is answered with a null id.
	for _, body := range []string{`{"id":3,"method":"stats"}`, `[]`} {
		var reply struct {
			ID    json.RawMessage  `json:"id"`
			Error *ctlserver.Error `json:"error"`
		}
		if got := post(body); json.Unmarshal([]byte(got), &reply) != nil || reply.Error == nil || reply.Error.Code != ctlserver.CodeInvalidRequest {
			t.Fatalf("%s answered %s", body, got)
		} else if body == "[]" && string(reply.ID) != "null" {
			t.Fatalf("%s answered id %s, want null", body, reply.ID)
		}
	}
}

func TestUnixSocketSafety(t *testing.T) {
	h := serve(t, liveProgram)
	path := strings.TrimPrefix(h.address, "unix://")
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %v (%v), want owner-only", info.Mode(), err)
	}
	if err := h.server.ListenAndServe(context.Background(), h.address); err == nil || !strings.Contains(err.Error(), "already being served") {
		t.Fatalf("serving a live socket again: %v", err)
	}
	plain := filepath.Join(filepath.Dir(path), "notes.txt")
	if err := os.WriteFile(plain, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := h.server.ListenAndServe(context.Background(), "unix://"+plain); err == nil || !strings.Contains(err.Error(), "not a socket") {
		t.Fatalf("serving over a regular file: %v", err)
	}
	if data, _ := os.ReadFile(plain); string(data) != "keep me" {
		t.Fatal("ListenAndServe replaced a regular file")
	}
}

func TestMethodNamesAreUnique(t *testing.T) {
	outs, err := launch.Compile("testsrc video frames=1 ! null")
	if err != nil {
		t.Fatal(err)
	}
	task, err := bundle.Build(context.Background(), outs...)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	noop := func(context.Context, goav.LiveTask, struct{}) (any, error) { return nil, nil }
	if _, err := ctlserver.New(task, ctlserver.Method("stats", "shadow", noop)); err == nil {
		t.Fatal("a host method replaced the built-in stats")
	}
	if _, err := ctlserver.New(task, ctlserver.Method("a", "", noop), ctlserver.Method("a", "", noop)); err == nil {
		t.Fatal("two host methods share a name")
	}
}

// TestStopAnswersBeforeShutdown serves the way goav run --control does: the
// task's end shuts the server down, and the stop call that ended it still
// gets its answer.
func TestStopAnswersBeforeShutdown(t *testing.T) {
	for range 10 {
		outs, err := launch.Compile(liveProgram)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		task, err := bundle.Build(ctx, outs...)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = task.Run(ctx); cancel() }()
		server, err := ctlserver.New(task)
		if err != nil {
			t.Fatal(err)
		}
		dir, _ := os.MkdirTemp("", "ctl")
		address := "unix://" + filepath.Join(dir, "s.sock")
		served := make(chan error, 1)
		go func() { served <- server.ListenAndServe(ctx, address) }()
		var client *ctlserver.Client
		for client == nil {
			client, _ = ctlserver.Dial(context.Background(), address)
			time.Sleep(5 * time.Millisecond)
		}
		if _, err := client.Call[struct{}](context.Background(), "stop", nil); err != nil {
			t.Fatalf("stop: %v", err)
		}
		if err := <-served; err != nil {
			t.Fatalf("serve: %v", err)
		}
		client.Close()
		os.RemoveAll(dir)
	}
}

func TestParamSpellings(t *testing.T) {
	h := serve(t, liveProgram)
	ctx := context.Background()
	if _, err := h.client.Call[struct{}](ctx, "bitrate", map[string]any{"stream": "cam", "bitrate": 900_000}); err != nil {
		t.Fatalf("a JSON number: %v", err)
	}
	for _, bad := range []string{"1e30", "Inf", "fast"} {
		if _, err := h.client.Call[struct{}](ctx, "bitrate", map[string]string{"stream": "cam", "bitrate": bad}); err == nil {
			t.Fatalf("bitrate=%s accepted", bad)
		}
	}
}

// TestCancelReachesTheServer: a call whose context ends is canceled on the
// server too, not left running.
func TestCancelReachesTheServer(t *testing.T) {
	canceled := make(chan error, 1)
	h := serve(t, liveProgram, ctlserver.Method("slow", "wait for a cancel", func(ctx context.Context, _ goav.LiveTask, _ struct{}) (any, error) {
		<-ctx.Done()
		canceled <- ctx.Err()
		return nil, ctx.Err()
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := h.client.Call[struct{}](ctx, "slow", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("slow: %v", err)
	}
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("server saw %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the server never saw the cancel")
	}
	// The connection still serves.
	if caps, err := h.client.Call[ctlserver.Capabilities](context.Background(), "capabilities", nil); err != nil || caps.Protocol != 1 {
		t.Fatalf("capabilities after a cancel = %+v, %v", caps.Protocol, err)
	}
}

// TestRefusalKeepsTheBuildError: a program refusal caused by a BuildError,
// such as an element's, carries both the program position and the
// BuildError's code.
func TestRefusalKeepsTheBuildError(t *testing.T) {
	refuse := launch.Sink("refuse", "always refuses", func(struct{}) (goav.Destination, error) {
		return goav.Destination{}, &goav.BuildError{Code: "element_refused", Reason: "this sink is closed"}
	})
	h := serve(t, liveProgram, ctlserver.Elements(refuse))
	_, err := h.client.Call[ctlserver.Attached](context.Background(), "attach", map[string]string{"program": "tap live ! refuse"})
	var e *ctlserver.Error
	if !errors.As(err, &e) {
		t.Fatalf("attach: %v", err)
	}
	var detail struct {
		Kind  string `json:"kind"`
		Build struct {
			Code string `json:"code"`
		} `json:"build"`
	}
	if json.Unmarshal(e.Data, &detail) != nil || detail.Kind != "program" || detail.Build.Code != "element_refused" {
		t.Fatalf("refusal data = %s", e.Data)
	}
}

// TestParseErrorArrives: a client that sends malformed JSON followed by more
// bytes still reads the parse error; the server drains before it closes.
func TestParseErrorArrives(t *testing.T) {
	h := serve(t, liveProgram)
	conn, err := net.Dial("unix", strings.TrimPrefix(h.address, "unix://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write(append([]byte(`{"jsonrpc": nope`), bytes.Repeat([]byte("x"), 64<<10)...)); err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Error *ctlserver.Error `json:"error"`
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := json.NewDecoder(conn).Decode(&reply); err != nil || reply.Error == nil || reply.Error.Code != ctlserver.CodeParse {
		t.Fatalf("reply = %+v, %v; want the parse error", reply.Error, err)
	}
}

// TestTokenGuardsEveryTransport pins the control plane's authentication: a
// tcp server needs a Token, and with one every connection and HTTP request
// must present it before any method runs.
func TestTokenGuardsEveryTransport(t *testing.T) {
	const token = "0123456789abcdef0123"
	if _, err := ctlserver.New(serve(t, liveProgram).task, ctlserver.Token("short")); err == nil {
		t.Fatal("a short token was accepted")
	}
	open := serve(t, liveProgram)
	if err := open.server.ListenAndServe(context.Background(), "tcp://127.0.0.1:0"); err == nil || !strings.Contains(err.Error(), "ctlserver.Token") {
		t.Fatalf("tcp without a token: err = %v, want a refusal naming ctlserver.Token", err)
	}

	h := serve(t, liveProgram, ctlserver.Token(token))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var refusal *ctlserver.Error
	if _, err := h.client.Call[json.RawMessage](ctx, "taps", nil); !errors.As(err, &refusal) || refusal.Code != ctlserver.CodeUnauthorized {
		t.Fatalf("unauthenticated call: err = %v, want CodeUnauthorized", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = h.server.Serve(ctx, l) }()
	if _, err := ctlserver.Dial(ctx, "tcp://wrong-token-0123456789@"+l.Addr().String()); !errors.As(err, &refusal) || refusal.Code != ctlserver.CodeUnauthorized {
		t.Fatalf("wrong token: err = %v, want CodeUnauthorized", err)
	}
	client, err := ctlserver.Dial(ctx, "tcp://"+token+"@"+l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if taps, err := client.Call[[]goav.TapSnapshot](ctx, "taps", nil); err != nil || len(taps) != 1 {
		t.Fatalf("authenticated taps = %v, %v", taps, err)
	}

	web := httptest.NewServer(h.server)
	defer web.Close()
	for auth, want := range map[string]int{"": http.StatusUnauthorized, "Bearer nope": http.StatusUnauthorized, "Bearer " + token: http.StatusOK} {
		req, _ := http.NewRequest(http.MethodPost, web.URL, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"taps"}`))
		req.Header.Set("Authorization", auth)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("HTTP with %q = %d, want %d", auth, resp.StatusCode, want)
		}
	}
}

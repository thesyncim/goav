package goav_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/metrics"
	"github.com/thesyncim/goav/provider"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
)

// TestURIFileRoundTrip reads and writes files through the file scheme, the
// one resolver both ends share, and Describe names the scheme.
func TestURIFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.ivf")
	if err := os.WriteFile(in, tinyIVF(), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "nested", "out.ivf")
	recipe := goav.URIInput("file://" + filepath.ToSlash(in)).To(goav.URI("file://" + filepath.ToSlash(out)))
	spec, err := bundle.Describe(recipe)
	if err != nil {
		t.Fatal(err)
	}
	if text := describeText(spec); !strings.Contains(text, "protocol=file") {
		t.Fatalf("describe does not name the scheme:\n%s", text)
	}
	if err := bundle.Run(context.Background(), recipe); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:4], []byte("DKIF")) || len(got) < len(tinyIVF()) {
		t.Fatalf("output = %d bytes %q, want a remuxed IVF", len(got), got[:min(4, len(got))])
	}

	// A bare path is a file too.
	bare := filepath.Join(dir, "bare.ivf")
	if err := bundle.Run(context.Background(), goav.URIInput(in).To(goav.URI(bare))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(bare); err != nil {
		t.Fatal(err)
	}
}

func describeText(spec interface{}) string {
	data, _ := json.Marshal(spec)
	return string(data)
}

// TestURIUnknownSchemeNamesTheProvider pins the refusal for a scheme the
// runtime does not register: the registered schemes, and the module that
// provides the missing one.
func TestURIUnknownSchemeNamesTheProvider(t *testing.T) {
	sink := goavtest.NewCollector()
	_, err := bundle.Describe(goav.URIInput("whep://media.example/live/cam").Video().To(sink.Sink()))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "input_scheme_unsupported" || buildErr.Family != errcode.FamilyInput {
		t.Fatalf("err = %v, want input_scheme_unsupported", err)
	}
	registered, _ := buildErr.Detail[string]("registered_schemes")
	if !strings.Contains(registered, "file") || !strings.Contains(registered, "https") {
		t.Fatalf("registered_schemes = %q", registered)
	}
	if fixes := strings.Join(buildErr.FixLines(), "\n"); !strings.Contains(fixes, `runconfig.Scheme("whep", webrtcav.WHEP())`) ||
		!strings.Contains(fixes, "github.com/thesyncim/goav/webrtcav") {
		t.Fatalf("fixes do not name the whep provider:\n%s", fixes)
	}

	// A bare runtime registers no schemes at all.
	rt, err := goav.New()
	if err != nil {
		t.Fatal(err)
	}
	_, err = rt.Describe(goav.FileInput("in.ivf", bytes.NewReader(tinyIVF())).To(goav.URI("out.ivf")))
	if !errors.As(err, &buildErr) || buildErr.Code != "output_scheme_unsupported" || buildErr.Family != errcode.FamilyDestination ||
		!strings.Contains(strings.Join(buildErr.FixLines(), "\n"), "scheme.File()") {
		t.Fatalf("err = %v, want output_scheme_unsupported naming scheme.File()", err)
	}
}

// TestURISchemeDirectionRefusal pins a one-direction scheme: the other end
// refuses with the scheme's own reason.
func TestURISchemeDirectionRefusal(t *testing.T) {
	rt := bundle.MustNew(runconfig.Scheme("tape", &testScheme{}))
	_, err := rt.Describe(goav.FileInput("in.ivf", bytes.NewReader(tinyIVF())).To(goav.URI("tape://deck/out.ivf")))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_scheme_unsupported" ||
		!errors.Is(err, errors.ErrUnsupported) || !strings.Contains(buildErr.Reason, "tape plays media in only") {
		t.Fatalf("err = %v, want the scheme's direction refusal", err)
	}
}

// TestURISchemeLastRegistrationWins replaces a bundled scheme.
func TestURISchemeLastRegistrationWins(t *testing.T) {
	custom := &testScheme{data: tinyIVF()}
	rt := bundle.MustNew(runconfig.Scheme("FILE", custom))
	sink := goavtest.NewCollector()
	if err := rt.Run(context.Background(), goav.URIInput("file:///nowhere/clip.ivf").Video().To(sink.Sink())); err != nil {
		t.Fatal(err)
	}
	if custom.opened != 1 || len(sink.Packets()) != 1 {
		t.Fatalf("opened = %d packets = %d, want the replacement scheme", custom.opened, len(sink.Packets()))
	}
	if _, err := goav.New(runconfig.Scheme("not a scheme", custom)); err == nil {
		t.Fatal("an invalid scheme name registered")
	}
}

// TestURIRedactsCredentials pins that a URL's userinfo and secret query
// values never reach Describe, Explain, errors, events, or snapshots, while
// the scheme sees the URL verbatim and probes the container from its path.
func TestURIRedactsCredentials(t *testing.T) {
	custom := &testScheme{data: tinyIVF(), live: true, breakFirst: true}
	rt := bundle.MustNew(runconfig.Scheme("vault", custom))
	const raw = "vault://alice:hunter2@media.example/clips/movie.ivf?token=SECRET1&quality=hd&X-Amz-Signature=SECRET2"
	sink := goavtest.NewCollector()
	out := "file://" + filepath.ToSlash(filepath.Join(t.TempDir(), "out.ivf")) + "?sig=SECRET3"
	in := goav.URIInput(raw, goav.Reconnect(1, time.Millisecond))
	recipe := []goav.Output{in.Video().To(sink.Sink()), in.Video().To(goav.URI(out))}

	spec, err := rt.Describe(recipe...)
	if err != nil {
		t.Fatal(err)
	}
	report, err := rt.Explain(context.Background(), recipe...)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Inputs) != 1 || !report.Inputs[0].Realtime || report.Inputs[0].Protocol != "vault" ||
		report.Inputs[0].Format != av.FormatIVF {
		t.Fatalf("explain input = %+v, want a live vault IVF input", report.Inputs)
	}
	if report.Inputs[0].URI != "vault://REDACTED@media.example/clips/movie.ivf?token=REDACTED&quality=hd&X-Amz-Signature=REDACTED" {
		t.Fatalf("explain uri = %q", report.Inputs[0].URI)
	}
	task, err := rt.Build(context.Background(), recipe...)
	if err != nil {
		t.Fatal(err)
	}
	watch := task.Watch()
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := task.Snapshot()
	scrape := httptest.NewRecorder()
	metrics.Handler(task).ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	task.Close()
	var events []string
	reconnects := 0
	for event := range watch.Events() {
		events = append(events, fmt.Sprintf("%s %s %v %v", event.Type, event.Reason, event.Cause, event.Metadata))
		if event.Type == av.EventReconnect {
			reconnects++
		}
	}
	if reconnects != 2 || !strings.Contains(scrape.Body.String(), "media.example") {
		t.Fatalf("reconnect events = %d, metrics:\n%s", reconnects, scrape.Body.String())
	}
	if custom.seen != raw {
		t.Fatalf("scheme saw %q, want the URL verbatim", custom.seen)
	}

	// A refusal names the input redacted too.
	_, refusal := rt.Describe(goav.URIInput(strings.Replace(raw, "vault:", "vaulted:", 1)).Video().To(sink.Sink()))
	for name, rendered := range map[string]string{
		"describe": describeText(spec),
		"explain":  describeText(report),
		"snapshot": describeText(snapshot),
		"metrics":  scrape.Body.String(),
		"events":   strings.Join(events, "\n"),
		"refusal":  refusal.Error(),
	} {
		for _, secret := range []string{"SECRET1", "SECRET2", "SECRET3", "hunter2", "alice"} {
			if strings.Contains(rendered, secret) {
				t.Errorf("%s leaks %q:\n%s", name, secret, rendered)
			}
		}
	}
}

// testScheme is an in-memory scheme: inputs read data as container bytes,
// destinations refuse.
type testScheme struct {
	data []byte
	live bool
	// breakFirst makes the first reader fail at its end instead of ending.
	breakFirst bool
	seen       string
	opened     int
}

func (s *testScheme) Input(u *url.URL) (provider.Input, error) {
	s.seen = u.String()
	return testBytes{scheme: s}, nil
}

func (s *testScheme) Destination(*url.URL) (provider.Destination, error) {
	return nil, errUnsupportedTape
}

var errUnsupportedTape = &tapeError{}

type tapeError struct{}

func (*tapeError) Error() string        { return "tape plays media in only; write with file://" }
func (*tapeError) Is(target error) bool { return target == errors.ErrUnsupported }

type testBytes struct{ scheme *testScheme }

func (b testBytes) SourceShape() shape.Spec { return shape.Spec{Realtime: b.scheme.live} }

func (b testBytes) OpenBytes(context.Context) (io.ReadCloser, error) {
	b.scheme.opened++
	var reader io.Reader = bytes.NewReader(b.scheme.data)
	if b.scheme.breakFirst && b.scheme.opened == 1 {
		reader = io.MultiReader(reader, iotest.ErrReader(io.ErrUnexpectedEOF))
	}
	return io.NopCloser(reader), nil
}

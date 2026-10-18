package metrics

import (
	"bufio"
	"expvar"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/snapshot"
)

type fakeTask struct {
	goav.LiveTask
}

func (fakeTask) Stats() pipeline.GraphStats {
	return pipeline.GraphStats{
		Messages: 9, Packets: 6, Frames: 3, Dropped: 2,
		Nodes: map[string]pipeline.NodeStats{
			"decode": {InMessages: 5, InPackets: 5, OutMessages: 4, OutFrames: 4,
				Dropped: 2, DropReasons: map[pipeline.DropPolicy]uint64{"drop-oldest": 2}},
			`we"ird`: {InMessages: 1},
		},
	}
}

func (fakeTask) Snapshot() snapshot.Task {
	return snapshot.Task{
		State:        lifecycle.TaskRunning,
		Branches:     []snapshot.Branch{{Name: "preview", State: lifecycle.BranchAttached}},
		Destinations: []snapshot.Destination{{Name: "out.webm", State: lifecycle.DestinationOpen}},
	}
}

// parse reads exposition text into "name{labels}" -> value, failing on any
// malformed line or sample without HELP/TYPE.
func parse(t *testing.T, text string) map[string]float64 {
	t.Helper()
	got := map[string]float64{}
	typed := map[string]bool{}
	sc := bufio.NewScanner(strings.NewReader(text))
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "# HELP "):
		case strings.HasPrefix(line, "# TYPE "):
			typed[strings.Fields(line)[2]] = true
		case line == "":
			t.Fatal("blank line in exposition")
		default:
			i := strings.LastIndexByte(line, ' ')
			v, err := strconv.ParseFloat(line[i+1:], 64)
			if err != nil {
				t.Fatalf("bad sample %q: %v", line, err)
			}
			key := line[:i]
			name, _, _ := strings.Cut(key, "{")
			if !strings.HasPrefix(name, "goav_") || !typed[name] {
				t.Fatalf("sample %q lacks goav_ prefix or TYPE", line)
			}
			got[key] = v
		}
	}
	return got
}

func TestHandlerServesPrometheusText(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler(fakeTask{}).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Fatalf("content type %q", ct)
	}
	got := parse(t, rec.Body.String())
	want := map[string]float64{
		`goav_task_state{state="running"}`:                              1,
		`goav_task_state{state="failed"}`:                               0,
		`goav_messages_total`:                                           9,
		`goav_dropped_total`:                                            2,
		`goav_node_in_packets_total{node="decode"}`:                     5,
		`goav_node_out_frames_total{node="decode"}`:                     4,
		`goav_node_dropped_total{node="decode",reason="drop-oldest"}`:   2,
		`goav_node_in_messages_total{node="we\"ird"}`:                   1,
		`goav_branch_state{branch="preview",state="attached"}`:          1,
		`goav_branch_state{branch="preview",state="detached"}`:          0,
		`goav_destination_state{destination="out.webm",state="open"}`:   1,
		`goav_destination_state{destination="out.webm",state="closed"}`: 0,
	}
	for k, v := range want {
		if g, ok := got[k]; !ok || g != v {
			t.Errorf("%s = %v (present %v), want %v", k, g, ok, v)
		}
	}
}

func TestPublishExposesExpvar(t *testing.T) {
	Publish("goav_metrics_test", fakeTask{})
	v := expvar.Get("goav_metrics_test")
	if v == nil {
		t.Fatal("not published")
	}
	s := v.String()
	for _, frag := range []string{`"state":"running"`, `"messages":9`, `"preview"`, `"out.webm"`} {
		if !strings.Contains(s, frag) {
			t.Errorf("expvar %s lacks %s", s, frag)
		}
	}
}

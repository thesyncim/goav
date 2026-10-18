// Package metrics bridges a running goav task to monitoring: Handler serves
// its counters in the Prometheus text exposition format, and Publish exposes
// the same data through expvar. It reads Stats and Snapshot, which are
// lock-free atomic reads, so scraping never slows the data plane.
//
//	http.Handle("/metrics", metrics.Handler(task))
//	metrics.Publish("goav", task) // visible at /debug/vars
//
// Metrics carry the goav_ prefix. Per-node counters are labelled node;
// drops add reason; branch and destination states are one-hot gauges
// labelled by name and state (1 for the current state, 0 for the others).
package metrics

import (
	"bytes"
	"expvar"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
)

// Handler serves the task's counters and states as Prometheus text.
func Handler(task goav.LiveTask) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		write(&buf, collect(task))
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write(buf.Bytes())
	})
}

// Publish exposes the task's stats and snapshot (state, branches,
// destinations) as the expvar variable name, rendered as JSON on demand. Like
// expvar.Publish it panics if name is already taken.
func Publish(name string, task goav.LiveTask) {
	expvar.Publish(name, expvar.Func(func() any {
		snap := task.Snapshot()
		return struct {
			State        any `json:"state"`
			Paused       any `json:"paused"`
			Stats        any `json:"stats"`
			Branches     any `json:"branches"`
			Destinations any `json:"destinations"`
		}{snap.State, snap.Paused, task.Stats(), snap.Branches, snap.Destinations}
	}))
}

type sample struct {
	labels string // rendered {k="v",...} or ""
	value  float64
}

type family struct {
	name, help, kind string
	samples          []sample
}

func collect(task goav.LiveTask) []*family {
	var fams []*family
	add := func(name, help, kind string) *family {
		f := &family{name: "goav_" + name, help: help, kind: kind}
		fams = append(fams, f)
		return f
	}
	put := func(f *family, v float64, kv ...string) {
		f.samples = append(f.samples, sample{labels: labels(kv), value: v})
	}
	stats := task.Stats()
	snap := task.Snapshot()

	ts := add("task_state", "Task lifecycle state, one-hot.", "gauge")
	for _, s := range []lifecycle.TaskState{lifecycle.TaskBuilt, lifecycle.TaskRunning, lifecycle.TaskClosed, lifecycle.TaskFailed} {
		put(ts, b2f(snap.State == s), "state", string(s))
	}
	put(add("task_paused", "1 while the task timeline is paused.", "gauge"), b2f(snap.Paused))
	put(add("messages_total", "Messages through the graph.", "counter"), float64(stats.Messages))
	put(add("packets_total", "Packets through the graph.", "counter"), float64(stats.Packets))
	put(add("frames_total", "Frames through the graph.", "counter"), float64(stats.Frames))
	put(add("events_total", "Events through the graph.", "counter"), float64(stats.Events))
	put(add("dropped_total", "Messages deliberately dropped by the graph.", "counter"), float64(stats.Dropped))

	nodeNames := make([]string, 0, len(stats.Nodes))
	for n := range stats.Nodes {
		nodeNames = append(nodeNames, n)
	}
	sort.Strings(nodeNames)
	type col struct {
		name, help string
		get        func(n string) float64
	}
	cols := []col{
		{"node_in_messages_total", "Messages received by the node.", func(n string) float64 { return float64(stats.Nodes[n].InMessages) }},
		{"node_in_packets_total", "Packets received by the node.", func(n string) float64 { return float64(stats.Nodes[n].InPackets) }},
		{"node_in_frames_total", "Frames received by the node.", func(n string) float64 { return float64(stats.Nodes[n].InFrames) }},
		{"node_out_messages_total", "Messages emitted by the node.", func(n string) float64 { return float64(stats.Nodes[n].OutMessages) }},
		{"node_out_packets_total", "Packets emitted by the node.", func(n string) float64 { return float64(stats.Nodes[n].OutPackets) }},
		{"node_out_frames_total", "Frames emitted by the node.", func(n string) float64 { return float64(stats.Nodes[n].OutFrames) }},
	}
	for _, c := range cols {
		f := add(c.name, c.help, "counter")
		for _, n := range nodeNames {
			put(f, c.get(n), "node", n)
		}
	}
	drops := add("node_dropped_total", "Messages dropped by the node, by reason.", "counter")
	for _, n := range nodeNames {
		dr := stats.Nodes[n].DropReasons
		reasons := make([]string, 0, len(dr))
		for r := range dr {
			reasons = append(reasons, string(r))
		}
		sort.Strings(reasons)
		for _, r := range reasons {
			put(drops, float64(dr[pipeline.DropPolicy(r)]), "node", n, "reason", r)
		}
	}

	bs := add("branch_state", "Runtime branch state, one-hot.", "gauge")
	for _, b := range snap.Branches {
		for _, s := range []lifecycle.BranchState{lifecycle.BranchAttached, lifecycle.BranchDetached} {
			put(bs, b2f(b.State == s), "branch", b.Name, "state", string(s))
		}
	}
	ds := add("destination_state", "Destination state, one-hot.", "gauge")
	for _, d := range snap.Destinations {
		for _, s := range []lifecycle.DestinationState{lifecycle.DestinationOpen, lifecycle.DestinationCommitted, lifecycle.DestinationAborted, lifecycle.DestinationClosed} {
			put(ds, b2f(d.State == s), "destination", d.Name, "state", string(s))
		}
	}
	return fams
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// labels renders alternating key, value pairs as a Prometheus label set.
func labels(kv []string) string {
	if len(kv) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(kv[i])
		sb.WriteString(`="`)
		sb.WriteString(escape(kv[i+1]))
		sb.WriteByte('"')
	}
	sb.WriteByte('}')
	return sb.String()
}

var escaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func escape(s string) string { return escaper.Replace(s) }

// write renders families, merging families that share a name, in the text
// exposition format.
func write(buf *bytes.Buffer, fams []*family) {
	for _, f := range fams {
		if len(f.samples) == 0 {
			continue
		}
		fmt.Fprintf(buf, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.kind)
		for _, s := range f.samples {
			fmt.Fprintf(buf, "%s%s %s\n", f.name, s.labels, strconv.FormatFloat(s.value, 'g', -1, 64))
		}
	}
}

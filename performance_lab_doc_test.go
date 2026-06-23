package goav_test

import (
	"os"
	"strings"
	"testing"
)

func TestPerformanceLabIsDocumentedAndGated(t *testing.T) {
	perf, err := os.ReadFile("docs/PERFORMANCE.md")
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile("scripts/bench/perf-lab.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(perf)
	for _, required := range []string{
		"scripts/bench/perf-lab.sh",
		"scripts/bench/ci-compare.sh",
		"bench-results/README.md",
		"bench-results/baseline/<timestamp>/<machine>.txt",
		"bench-results/latency/<scenario>-<timestamp>.json",
		"bench-results/rss/<scenario>-<timestamp>.json",
		"bench-results/pressure/<scenario>-<timestamp>.json",
		"bench-results/control/<scenario>-<timestamp>.json",
		"bench-results/fanout/<scenario>-<timestamp>.json",
		"bench-results/live-sync/<scenario>-<timestamp>.json",
		"bench-results/container/<scenario>-<timestamp>.json",
		"bench-results/pprof/<scenario>-<timestamp>/",
		"cpu.out",
		"mem.out",
		"BenchmarkLatencyRecordPackets",
		"BenchmarkSustainedRecordMemory",
		"BenchmarkRealOpusEncode",
		"BenchmarkRealOpusDecode",
		"BenchmarkLiveRoomSync",
		"BenchmarkSourcePush",
		"BenchmarkAttachDetachUnderLoad",
		"BenchmarkDirectFanout",
		"BenchmarkBufferedFanout",
		"BenchmarkReadWebRTCCorpus",
		"BenchmarkReadWebMCorpus",
		"p50/p95/p99",
		"max RSS",
		"benchstat-pr-vs-base.txt",
		"committed smoke numbers",
		"not production",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("docs/PERFORMANCE.md missing %q", required)
		}
	}
	if !strings.Contains(string(workflow), "scripts/bench/perf-lab.sh") ||
		!strings.Contains(string(workflow), "bench-results/perf-lab-*.txt") ||
		!strings.Contains(string(workflow), "bench-results/baseline/**/*.txt") ||
		!strings.Contains(string(workflow), "bench-results/latency/*.json") ||
		!strings.Contains(string(workflow), "bench-results/rss/*.json") ||
		!strings.Contains(string(workflow), "bench-results/pressure/*.json") ||
		!strings.Contains(string(workflow), "bench-results/control/*.json") ||
		!strings.Contains(string(workflow), "bench-results/fanout/*.json") ||
		!strings.Contains(string(workflow), "bench-results/live-sync/*.json") ||
		!strings.Contains(string(workflow), "bench-results/container/*.json") ||
		!strings.Contains(string(workflow), "bench-results/pprof/**") ||
		!strings.Contains(string(workflow), "scripts/bench/ci-compare.sh") ||
		!strings.Contains(string(workflow), "bench-results/benchstat-pr-vs-base.txt") {
		t.Fatalf(".github/workflows/ci.yml should run and upload performance artifacts")
	}
	for _, required := range []string{
		"-cpuprofile",
		"${pprof_dir}/cpu.out",
		"-memprofile",
		"${pprof_dir}/mem.out",
		"pressure_dir",
		"control_dir",
		"fanout_dir",
		"live_sync_dir",
		"container_dir",
		"BenchmarkLatencyRecordPackets|BenchmarkLiveRoomSync|BenchmarkSustainedRecordMemory",
		"Benchmark(SourcePush|AttachDetachUnderLoad)",
		"Benchmark(DirectFanout|DirectFanoutParallel|BufferedFanout)",
		"Benchmark(Read|Write).*Corpus|BenchmarkExternal.*FieldCorpusScan",
	} {
		if !strings.Contains(string(script), required) {
			t.Fatalf("scripts/bench/perf-lab.sh missing profile flag %q", required)
		}
	}
}

func TestCIWorkflowCoversTrustGates(t *testing.T) {
	workflow, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(workflow)
	for _, required := range []string{
		"os: ubuntu-latest",
		"os: macos-latest",
		"go: '1.26.4'",
		"go: stable",
		"Changelog hygiene",
		"skip-changelog",
		"CHANGELOG.md must be updated",
		"Upload coverage artifact",
		"Fuzz smoke",
		"Staticcheck",
		"Govulncheck",
		"Package documentation smoke",
		"go list -f '{{if not .Doc}}{{.ImportPath}}{{end}}'",
		"Release tag validation",
		"git verify-tag",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf(".github/workflows/ci.yml missing trust gate %q", required)
		}
	}
}

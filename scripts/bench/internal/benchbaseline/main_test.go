package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestParseBenchOutputNormalizesRows(t *testing.T) {
	path := writeBenchOutput(t, `
goos: darwin
BenchmarkBeta-8       100  20 ns/op  7 B/op  1 allocs/op  12 MB/s
BenchmarkAlpha/N=2-8  100  10 ns/op  0 B/op  0 allocs/op
PASS
`)
	records, err := parseBenchOutput(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
	if records[0].Name != "BenchmarkAlpha/N=2" || records[1].Name != "BenchmarkBeta" {
		t.Fatalf("records sorted/named as %#v", records)
	}
	if got := records[1].Metrics["MB/s"]; got != 0 {
		t.Fatalf("MB/s metric should be ignored, got %v", got)
	}
	if got := records[1].Metrics["allocs/op"]; got != 1 {
		t.Fatalf("allocs/op = %v, want 1", got)
	}
}

func TestCollectRecordsStableSourceLabelAndCeilings(t *testing.T) {
	source := writeBenchOutput(t, `BenchmarkHot-8 100 10 ns/op 0 B/op 0 allocs/op
BenchmarkCold-8 100 10 ns/op 2000 B/op 200 allocs/op
`)
	out := filepath.Join(t.TempDir(), "baseline.json")

	collect([]string{
		"-source", source,
		"-source-label", "stable-suite-label",
		"-out", out,
		"-kind", "test",
		"-package", ".",
		"-bench-regex", "Benchmark(Hot|Cold)$",
		"-benchtime", "100x",
		"-count", "1",
	})

	var file baselineFile
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	if file.Source != "stable-suite-label" {
		t.Fatalf("source = %q, want stable label", file.Source)
	}
	records := map[string]benchmarkRecord{}
	for _, record := range file.Benchmarks {
		records[record.Name] = record
	}
	if got := records["BenchmarkHot"].Ceilings["allocs/op"]; got != 0 {
		t.Fatalf("hot alloc ceiling = %v, want 0", got)
	}
	if got := records["BenchmarkHot"].Ceilings["B/op"]; got != 1024 {
		t.Fatalf("hot byte ceiling = %v, want 1024", got)
	}
	if got := records["BenchmarkCold"].Ceilings["allocs/op"]; got != 300 {
		t.Fatalf("cold alloc ceiling = %v, want 300 (200 + 200*0.5 cold slack)", got)
	}
	if got := records["BenchmarkCold"].Ceilings["B/op"]; got != 3024 {
		t.Fatalf("cold byte ceiling = %v, want 3024 (2000 + max(1024, 2000*0.25))", got)
	}
}

func writeBenchOutput(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bench.txt")
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

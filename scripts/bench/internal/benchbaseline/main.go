package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

type baselineFile struct {
	Schema     int               `json:"schema"`
	Kind       string            `json:"kind"`
	Generated  string            `json:"generated_at"`
	GoVersion  string            `json:"go_version"`
	GOOS       string            `json:"goos"`
	GOARCH     string            `json:"goarch"`
	Package    string            `json:"package"`
	BenchRegex string            `json:"bench_regex"`
	Benchtime  string            `json:"benchtime"`
	Count      string            `json:"count"`
	Source     string            `json:"source"`
	Thresholds thresholds        `json:"thresholds"`
	Benchmarks []benchmarkRecord `json:"benchmarks"`
}

type thresholds struct {
	BytesSlackRatio  float64 `json:"bytes_per_op_slack_ratio"`
	BytesSlackMin    float64 `json:"bytes_per_op_slack_min"`
	AllocsSlack      float64 `json:"allocs_per_op_slack"`
	AllocsSlackRatio float64 `json:"allocs_per_op_slack_ratio"`
	AllocsSlackMin   float64 `json:"allocs_per_op_slack_min"`
	AllocsColdAt     float64 `json:"allocs_per_op_cold_threshold"`
	AllocsColdRatio  float64 `json:"allocs_per_op_cold_slack_ratio"`
	AllocsColdMin    float64 `json:"allocs_per_op_cold_slack_min"`
}

type benchmarkRecord struct {
	Name     string             `json:"name"`
	Metrics  map[string]float64 `json:"metrics"`
	Ceilings map[string]float64 `json:"ceilings,omitempty"`
}

var benchmarkName = regexp.MustCompile(`^(Benchmark\S+?)(?:-\d+)?$`)

func main() {
	if len(os.Args) < 2 {
		fail("usage: benchbaseline collect|check ...")
	}
	switch os.Args[1] {
	case "collect":
		collect(os.Args[2:])
	case "check":
		check(os.Args[2:])
	default:
		fail("unknown command %q", os.Args[1])
	}
}

func collect(args []string) {
	fs := flag.NewFlagSet("collect", flag.ExitOnError)
	source := fs.String("source", "", "benchmark output to parse")
	sourceLabel := fs.String("source-label", "", "stable source label recorded in the baseline JSON")
	out := fs.String("out", "", "baseline JSON to write")
	kind := fs.String("kind", "", "baseline kind")
	pkg := fs.String("package", "", "go test package")
	benchRegex := fs.String("bench-regex", "", "benchmark regex")
	benchtime := fs.String("benchtime", "", "benchmark benchtime")
	count := fs.String("count", "", "benchmark count")
	bytesSlackRatio := fs.Float64("bytes-slack-ratio", 0.05, "B/op ceiling ratio slack")
	bytesSlackMin := fs.Float64("bytes-slack-min", 1024, "minimum B/op ceiling slack")
	allocsSlack := fs.Float64("allocs-slack", 0, "allocs/op ceiling slack")
	allocsSlackRatio := fs.Float64("allocs-slack-ratio", 0.05, "allocs/op ceiling ratio slack for cold paths")
	allocsSlackMin := fs.Float64("allocs-slack-min", 4, "minimum allocs/op ceiling slack for cold paths")
	allocsColdAt := fs.Float64("allocs-cold-at", 100, "allocs/op value treated as noisy cold-path work")
	allocsColdRatio := fs.Float64("allocs-cold-ratio", 0.15, "allocs/op ceiling ratio slack for noisy cold paths")
	allocsColdMin := fs.Float64("allocs-cold-min", 64, "minimum allocs/op ceiling slack for noisy cold paths")
	_ = fs.Parse(args)
	if *source == "" || *out == "" || *kind == "" {
		fail("collect requires -source, -out, and -kind")
	}
	if *sourceLabel == "" {
		*sourceLabel = *source
	}
	records, err := parseBenchOutput(*source)
	if err != nil {
		fail("%v", err)
	}
	for i := range records {
		records[i].Ceilings = ceilings(records[i].Metrics, *bytesSlackRatio, *bytesSlackMin, *allocsSlack, *allocsSlackRatio, *allocsSlackMin, *allocsColdAt, *allocsColdRatio, *allocsColdMin)
	}
	goVersion := strings.TrimSpace(commandOutput("go", "version"))
	if goVersion == "" {
		goVersion = runtime.Version()
	}
	file := baselineFile{
		Schema:     1,
		Kind:       *kind,
		Generated:  time.Now().UTC().Format(time.RFC3339),
		GoVersion:  goVersion,
		GOOS:       runtime.GOOS,
		GOARCH:     runtime.GOARCH,
		Package:    *pkg,
		BenchRegex: *benchRegex,
		Benchtime:  *benchtime,
		Count:      *count,
		Source:     *sourceLabel,
		Thresholds: thresholds{
			BytesSlackRatio:  *bytesSlackRatio,
			BytesSlackMin:    *bytesSlackMin,
			AllocsSlack:      *allocsSlack,
			AllocsSlackRatio: *allocsSlackRatio,
			AllocsSlackMin:   *allocsSlackMin,
			AllocsColdAt:     *allocsColdAt,
			AllocsColdRatio:  *allocsColdRatio,
			AllocsColdMin:    *allocsColdMin,
		},
		Benchmarks: records,
	}
	data, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		fail("%v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		fail("%v", err)
	}
}

func check(args []string) {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	baselinePath := fs.String("baseline", "", "baseline JSON")
	currentPath := fs.String("current", "", "current benchmark output")
	_ = fs.Parse(args)
	if *baselinePath == "" || *currentPath == "" {
		fail("check requires -baseline and -current")
	}
	var baseline baselineFile
	data, err := os.ReadFile(*baselinePath)
	if err != nil {
		fail("%v", err)
	}
	if err := json.Unmarshal(data, &baseline); err != nil {
		fail("%v", err)
	}
	currentRecords, err := parseBenchOutput(*currentPath)
	if err != nil {
		fail("%v", err)
	}
	current := make(map[string]benchmarkRecord, len(currentRecords))
	for _, record := range currentRecords {
		current[record.Name] = record
	}
	var failures []string
	for _, want := range baseline.Benchmarks {
		got, ok := current[want.Name]
		if !ok {
			failures = append(failures, fmt.Sprintf("%s: missing from current benchmark output", want.Name))
			continue
		}
		for _, metric := range []string{"allocs/op", "B/op"} {
			ceiling, ok := want.Ceilings[metric]
			if !ok {
				continue
			}
			value, ok := got.Metrics[metric]
			if !ok {
				failures = append(failures, fmt.Sprintf("%s: missing %s in current benchmark output", want.Name, metric))
				continue
			}
			if value > ceiling {
				failures = append(failures, fmt.Sprintf("%s: %s %.4g > ceiling %.4g", want.Name, metric, value, ceiling))
			}
		}
	}
	if len(failures) != 0 {
		sort.Strings(failures)
		for _, failure := range failures {
			fmt.Fprintln(os.Stderr, failure)
		}
		os.Exit(1)
	}
	fmt.Printf("%s OK: %d benchmark allocation/byte ceilings hold\n", *baselinePath, len(baseline.Benchmarks))
}

func parseBenchOutput(path string) ([]benchmarkRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var records []benchmarkRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 {
			continue
		}
		match := benchmarkName.FindStringSubmatch(fields[0])
		if match == nil {
			continue
		}
		metrics := make(map[string]float64)
		for i := 1; i+1 < len(fields); i++ {
			value, err := strconv.ParseFloat(fields[i], 64)
			if err != nil {
				continue
			}
			unit := fields[i+1]
			if unit == "MB/s" {
				continue
			}
			if strings.Contains(unit, "/") || strings.HasSuffix(unit, "_ns") ||
				strings.HasSuffix(unit, "_B") || strings.HasSuffix(unit, "_drops") ||
				unit == "delivered" || unit == "source_drops" || unit == "sync_drops" ||
				unit == "gc_cycles" || unit == "gc_pause_total_ns" || unit == "max_drift_ns" {
				metrics[unit] = value
			}
		}
		if len(metrics) == 0 {
			continue
		}
		records = append(records, benchmarkRecord{Name: match[1], Metrics: metrics})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Name < records[j].Name })
	return records, nil
}

func ceilings(metrics map[string]float64, bytesSlackRatio float64, bytesSlackMin float64, allocsSlack float64, allocsSlackRatio float64, allocsSlackMin float64, allocsColdAt float64, allocsColdRatio float64, allocsColdMin float64) map[string]float64 {
	out := make(map[string]float64, 2)
	if value, ok := metrics["B/op"]; ok {
		slack := math.Max(bytesSlackMin, value*bytesSlackRatio)
		out["B/op"] = math.Ceil(value + slack)
	}
	if value, ok := metrics["allocs/op"]; ok {
		slack := allocsSlack
		if value >= allocsColdAt {
			slack = math.Max(slack, math.Max(allocsColdMin, value*allocsColdRatio))
		} else if value > 2 {
			slack = math.Max(slack, math.Max(allocsSlackMin, value*allocsSlackRatio))
		}
		out["allocs/op"] = math.Ceil(value + slack)
	}
	return out
}

func commandOutput(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return ""
	}
	return string(out)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}

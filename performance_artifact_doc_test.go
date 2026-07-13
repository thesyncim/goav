package goav

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPerformanceLabReleaseManifestDocs(t *testing.T) {
	for _, tt := range []struct {
		path      string
		fragments []string
	}{
		{
			path: "scripts/bench/perf-lab.sh",
			fragments: []string{
				"PERF_RELEASE_QUALITY",
				"PERF_RELEASE_QUALITY=true requires PERF_SOAK_BENCHTIME",
				"PERF_SOAK_BENCHTIME",
				"PERF_LIVE_ROOM_CHURN_BENCHTIME",
				"PERF_LIVE_ROOM_CHURN_INTERVAL",
				"PERF_GO_TEST_TIMEOUT",
				"GOAV_PERF_RECORD_DRIFT_SOAK_DURATION",
				"GOAV_PERF_LIVE_ROOM_CHURN_SOAK_DURATION",
				"GOAV_PERF_LIVE_ROOM_CHURN_INTERVAL",
				"TestPerformanceLabRecordDriftSoak",
				"TestPerformanceLabLiveRoomAttachDetachSoak",
				"bench-results/manifest",
				"bench-results/soak",
				"BenchmarkLiveRoomAttachDetachSoak",
				"live_room_churn_soak",
				"max_rss_B",
				"goav-perf-lab-manifest",
				"git_dirty",
			},
		},
		{
			path: filepath.Join("bench-results", "README.md"),
			fragments: []string{
				"manifest/perf-lab-<timestamp>.json",
				"soak/<scenario>-<timestamp>.json",
				"live-room-churn-<timestamp>.json",
				"max_rss_B",
				"churn_interval",
				"host/toolchain/git provenance",
				"PERF_RELEASE_QUALITY=true",
				"PERF_SOAK_BENCHTIME",
				"PERF_GO_TEST_TIMEOUT",
				"wall-clock",
			},
		},
		{
			path: filepath.Join("docs", "PERFORMANCE.md"),
			fragments: []string{
				"bench-results/manifest/perf-lab-<timestamp>.json",
				"bench-results/soak/<scenario>-<timestamp>.json",
				"BenchmarkLiveRoomAttachDetachSoak",
				"max_rss_B",
				"churn_interval",
				"release_quality=false",
				"PERF_RELEASE_QUALITY=true",
				"PERF_SOAK_BENCHTIME",
				"PERF_GO_TEST_TIMEOUT",
			},
		},
		{
			path: filepath.Join("docs", "RELEASING.md"),
			fragments: []string{
				"PERF_RELEASE_QUALITY=true PERF_BENCHTIME=2000x PERF_SOAK_BENCHTIME=1h PERF_LIVE_ROOM_CHURN_BENCHTIME=1h PERF_LIVE_ROOM_CHURN_INTERVAL=100ms scripts/bench/perf-lab.sh",
				"PERF_GO_TEST_TIMEOUT",
				"perf-lab manifest",
			},
		},
		{
			path: filepath.Join(".github", "workflows", "ci.yml"),
			fragments: []string{
				"bench-results/soak/*.json",
				"bench-results/manifest/*.json",
			},
		},
	} {
		t.Run(tt.path, func(t *testing.T) {
			data, err := os.ReadFile(tt.path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(data)
			for _, fragment := range tt.fragments {
				if !strings.Contains(text, fragment) {
					t.Fatalf("%s missing %q", tt.path, fragment)
				}
			}
		})
	}
}

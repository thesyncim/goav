package goav

import (
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

func TestBranchComposeTargetHasMuxDestinationContracts(t *testing.T) {
	sink := SinkFunc("frames", func(context.Context, Message) error { return nil })
	if branchComposeTargetHasMuxDestination(branchComposeTarget{Sink: sink}) {
		t.Fatal("sink-only target reported mux destination")
	}
	if branchComposeTargetHasMuxDestination(branchComposeTarget{Destination: Sink(sink).spec}) {
		t.Fatal("sink destination reported mux destination")
	}

	tests := []struct {
		name   string
		target branchComposeTarget
	}{
		{name: "file destination", target: branchComposeTarget{Destination: File("archive.webm", io.Discard).spec}},
		{name: "target name", target: branchComposeTarget{Target: format.Output{Name: "archive.webm"}}},
		{name: "target uri", target: branchComposeTarget{Target: format.Output{URI: "s3://bucket/archive.webm"}}},
		{name: "target protocol", target: branchComposeTarget{Target: format.Output{Protocol: av.ProtocolFile}}},
		{name: "target writer", target: branchComposeTarget{Target: format.Output{Writer: io.Discard}}},
		{name: "target mime", target: branchComposeTarget{Target: format.Output{MIMEType: "video/webm"}}},
		{name: "explicit format", target: branchComposeTarget{Format: av.FormatWebM}},
		{name: "resolved format", target: resolveBranchComposeTargetFormat(branchComposeTarget{}, av.FormatOgg)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !branchComposeTargetHasMuxDestination(tt.target) {
				t.Fatalf("branchComposeTargetHasMuxDestination(%+v) = false, want true", tt.target)
			}
		})
	}
}

func TestBranchComposeTargetMatchesContracts(t *testing.T) {
	branches := []branchComposeRoute{
		{
			name: "route-preview",
			branch: branchComposeBranch{
				Name:   "preview",
				Labels: []string{"mobile", "shared"},
			},
		},
		{
			name: "route-archive",
			branch: branchComposeBranch{
				Name:   "archive",
				Labels: []string{"backup"},
			},
		},
		{name: "route-private"},
	}

	tests := []struct {
		name   string
		output branchComposeTarget
		want   []int
	}{
		{name: "all when no filters", output: branchComposeTarget{}, want: []int{0, 1, 2}},
		{name: "internal route name", output: branchComposeTarget{Branches: []string{"route-preview"}}, want: []int{0}},
		{name: "public branch name", output: branchComposeTarget{Branches: []string{"archive"}}, want: []int{1}},
		{name: "labels across routes", output: branchComposeTarget{Branches: []string{"mobile", "backup"}}, want: []int{0, 1}},
		{name: "private route name", output: branchComposeTarget{Branches: []string{"route-private"}}, want: []int{2}},
		{name: "unmatched", output: branchComposeTarget{Branches: []string{"missing"}}, want: []int{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := branchComposeTargetMatches(tt.output, branches); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("matches = %v, want %v", got, tt.want)
			}
		})
	}

	output := branchComposeTarget{Branches: []string{"shared"}}
	if !branchComposeTargetSelectsRoute(output, branches[0]) {
		t.Fatal("label selector did not match route")
	}
	if branchComposeTargetSelectsRoute(output, branches[1]) {
		t.Fatal("label selector matched wrong route")
	}
}

func TestBranchComposeTargetRouteNameContracts(t *testing.T) {
	sink := SinkFunc("frames", func(context.Context, Message) error { return nil })
	unnamedSink := SinkFunc("", func(context.Context, Message) error { return nil })

	tests := []struct {
		name   string
		route  branchComposeTargetRoute
		expect string
	}{
		{
			name: "declared output name wins",
			route: branchComposeTargetRoute{
				output: branchComposeTarget{Name: "declared"},
				target: format.Output{Name: "target", URI: "file.webm"},
				sink:   sink,
			},
			expect: "declared",
		},
		{
			name: "target name",
			route: branchComposeTargetRoute{
				target: format.Output{Name: "archive"},
			},
			expect: "archive",
		},
		{
			name: "target URI",
			route: branchComposeTargetRoute{
				target: format.Output{URI: "s3://bucket/archive.webm"},
			},
			expect: "s3://bucket/archive.webm",
		},
		{
			name: "sink fallback",
			route: branchComposeTargetRoute{
				sink: sink,
			},
			expect: "frames",
		},
		{
			name: "unnamed sink uses default sink name",
			route: branchComposeTargetRoute{
				sink: unnamedSink,
			},
			expect: "sink",
		},
		{
			name: "empty route",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := branchComposeTargetRouteName(tt.route); got != tt.expect {
				t.Fatalf("route name = %q, want %q", got, tt.expect)
			}
		})
	}

	routes := []branchComposeTargetRoute{
		{output: branchComposeTarget{Name: "preview"}},
		{target: format.Output{URI: "file:///archive.webm"}},
		{sink: sink},
	}
	if got := branchComposeTargetRouteIndex(routes, "preview"); got != 0 {
		t.Fatalf("preview route index = %d, want 0", got)
	}
	if got := branchComposeTargetRouteIndex(routes, "file:///archive.webm"); got != 1 {
		t.Fatalf("archive route index = %d, want 1", got)
	}
	if got := branchComposeTargetRouteIndex(routes, "frames"); got != 2 {
		t.Fatalf("sink route index = %d, want 2", got)
	}
	if got := branchComposeTargetRouteIndex(routes, "missing"); got != -1 {
		t.Fatalf("missing route index = %d, want -1", got)
	}
}

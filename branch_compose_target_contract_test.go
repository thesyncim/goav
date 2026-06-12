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

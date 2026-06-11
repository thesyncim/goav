package launchctl

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
)

func TestControlManifestContainsBuiltInVerbs(t *testing.T) {
	want := []string{"keyframe", "bitrate", "seek", "rate", "segment", "select", "deliver"}
	for _, name := range want {
		spec, ok := LookupControlCommand(name)
		if !ok {
			t.Fatalf("manifest missing %q", name)
		}
		if spec.ArgsType.Kind() != reflect.Struct {
			t.Fatalf("%s ArgsType = %v, want struct", name, spec.ArgsType)
		}
		if spec.Apply == nil {
			t.Fatalf("%s has nil Apply", name)
		}
	}
}

func TestBindArgsParsesControlFields(t *testing.T) {
	bitrateSpec, _ := LookupControlCommand("bitrate")
	bitrateArgs, err := BindArgs(bitrateSpec, []string{"stream=video", "value=1200k", "at=raw_video"})
	if err != nil {
		t.Fatal(err)
	}
	bitrate := bitrateArgs.(BitrateCommand)
	if bitrate.Stream != "video" || bitrate.Value != 1_200_000 || bitrate.At != "raw_video" {
		t.Fatalf("bitrate = %+v", bitrate)
	}

	seekSpec, _ := LookupControlCommand("seek")
	seekArgs, err := BindArgs(seekSpec, []string{"position=12.5s"})
	if err != nil {
		t.Fatal(err)
	}
	if got := seekArgs.(SeekCommand).Position; got != 12500*time.Millisecond {
		t.Fatalf("seek position = %v", got)
	}

	segmentSpec, _ := LookupControlCommand("segment")
	segmentArgs, err := BindArgs(segmentSpec, []string{"start=10s", "end=20s"})
	if err != nil {
		t.Fatal(err)
	}
	segment := segmentArgs.(SegmentCommand)
	if segment.Start != 10*time.Second || segment.End != 20*time.Second {
		t.Fatalf("segment = %+v", segment)
	}

	selectSpec, _ := LookupControlCommand("select")
	selectArgs, err := BindArgs(selectSpec, []string{"active=camera_b"})
	if err != nil {
		t.Fatal(err)
	}
	if got := selectArgs.(SelectCommand).Active; got != "camera_b" {
		t.Fatalf("active = %q", got)
	}

	deliverSpec, _ := LookupControlCommand("deliver")
	deliverArgs, err := BindArgs(deliverSpec, []string{"type=vendor.force_idr", "stream=video", "at=raw_video", "reason=manual", "metadata.foo=bar"})
	if err != nil {
		t.Fatal(err)
	}
	deliver := deliverArgs.(DeliverCommand)
	if deliver.Type != "vendor.force_idr" || deliver.Stream != "video" || deliver.At != "raw_video" || deliver.Reason != "manual" || deliver.Metadata["foo"] != "bar" {
		t.Fatalf("deliver = %+v", deliver)
	}
}

func TestBindArgsMissingRequiredIncludesUsage(t *testing.T) {
	spec, _ := LookupControlCommand("bitrate")
	_, err := BindArgs(spec, []string{"stream=video"})
	var ctlErr *Error
	if !errors.As(err, &ctlErr) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if ctlErr.Code != "missing_required" || ctlErr.Node != "value" {
		t.Fatalf("err = %+v, want missing value", ctlErr)
	}
	if !detailsContain(ctlErr.Details, "usage=goav ctl --control unix://PATH control bitrate") {
		t.Fatalf("details = %v, want generated usage", ctlErr.Details)
	}
}

func TestBindArgsUnknownFieldSuggestsKnownField(t *testing.T) {
	spec, _ := LookupControlCommand("keyframe")
	_, err := BindArgs(spec, []string{"stram=video"})
	var ctlErr *Error
	if !errors.As(err, &ctlErr) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if ctlErr.Code != "unknown_field" || !suggestionsContain(ctlErr.Suggestions, "stream=") {
		t.Fatalf("err = %+v, want stream suggestion", ctlErr)
	}
}

func TestExecuteRawControlCallsTaskControl(t *testing.T) {
	task := newFakeTask()
	_, err := Execute(context.Background(), task, []string{"control", "--json", `{"type":"bitrate","stream_id":"video","bitrate":1200000,"tap":"main_encoded"}`})
	if err != nil {
		t.Fatal(err)
	}
	if len(task.controls) != 1 {
		t.Fatalf("controls = %d, want 1", len(task.controls))
	}
	control := task.controls[0]
	if control.Type != goav.ControlBitrate || control.StreamID != "video" || control.Bitrate != 1_200_000 || control.Tap != "main_encoded" {
		t.Fatalf("control = %+v", control)
	}
}

func TestExecuteRawEventCallsTaskControlDeliver(t *testing.T) {
	task := newFakeTask()
	_, err := Execute(context.Background(), task, []string{"control", "deliver", "--json", `{"type":"vendor.force_idr","stream_id":"video","reason":"manual","metadata":{"source":"cli","count":2,"ok":true}}`, "at=raw_video"})
	if err != nil {
		t.Fatal(err)
	}
	if len(task.controls) != 1 {
		t.Fatalf("controls = %d, want 1", len(task.controls))
	}
	control := task.controls[0]
	if control.Type != goav.ControlEvent || control.Tap != "raw_video" {
		t.Fatalf("control = %+v", control)
	}
	event := control.Event
	if event.Type != "vendor.force_idr" || event.StreamID != "video" || event.Reason != "manual" ||
		event.Metadata["source"] != "cli" || event.Metadata["count"] != "2" || event.Metadata["ok"] != "true" {
		t.Fatalf("event = %+v", event)
	}
}

func TestUnknownTapErrorListsAvailableTaps(t *testing.T) {
	task := newFakeTask()
	_, err := Execute(context.Background(), task, []string{"control", "deliver", "type=vendor.force_idr", "at=raw_vdieo"})
	var ctlErr *Error
	if !errors.As(err, &ctlErr) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if ctlErr.Code != "unknown_tap" || !detailsContain(ctlErr.Details, "raw_video") || !suggestionsContain(ctlErr.Suggestions, "at=raw_video") {
		t.Fatalf("err = %+v, want available tap details", ctlErr)
	}
}

func TestUnknownBranchErrorListsAvailableBranches(t *testing.T) {
	task := newFakeTask()
	_, err := Execute(context.Background(), task, []string{"detach", "preveiw"})
	var ctlErr *Error
	if !errors.As(err, &ctlErr) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if ctlErr.Code != "unknown_branch" || !detailsContain(ctlErr.Details, "preview") || !suggestionsContain(ctlErr.Suggestions, "preview") {
		t.Fatalf("err = %+v, want available branch details", ctlErr)
	}
}

func TestHelpGeneratedFromManifestMetadata(t *testing.T) {
	spec, _ := LookupControlCommand("bitrate")
	help := CommandHelp(spec)
	for _, fragment := range []string{
		"control bitrate",
		"goav ctl --control unix://PATH control bitrate stream=<stream-id> value=<rate> [at=<tap>]",
		"stream    required",
		"value     required",
		"bits per second, accepts 1200k, 2M, or integer",
	} {
		if !strings.Contains(help, fragment) {
			t.Fatalf("help missing %q:\n%s", fragment, help)
		}
	}
}

func TestReflectionConfinedToLaunchctlProductionFiles(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	var offenders []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		slash := filepath.ToSlash(path)
		if strings.Contains(slash, "internal/launchctl/") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), `"reflect"`) {
			offenders = append(offenders, slash)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) != 0 {
		t.Fatalf("reflect imports outside internal/launchctl: %v", offenders)
	}
}

func TestTestOnlyCommandExtensionPath(t *testing.T) {
	type fakeCommand struct {
		Name string `goavctl:"name,required" usage:"name=<value>" help:"test value"`
	}
	var applied string
	spec := CommandSpec{
		Name:     "fake",
		Summary:  "fake extension",
		ArgsType: reflect.TypeOf(fakeCommand{}),
		Apply: func(_ context.Context, _ goav.Task, args any) (ControlResponse, error) {
			applied = args.(fakeCommand).Name
			return ControlResponse{Operation: "control fake", Result: applied}, nil
		},
	}
	response, err := Invoke(context.Background(), newFakeTask(), spec, []string{"name=added"})
	if err != nil {
		t.Fatal(err)
	}
	if applied != "added" || response.Result != "added" {
		t.Fatalf("applied=%q response=%+v", applied, response)
	}
	if !strings.Contains(CommandHelp(spec), "name=<value>") {
		t.Fatalf("fake help was not generated from tags")
	}
}

func suggestionsContain(suggestions []string, fragment string) bool {
	for _, suggestion := range suggestions {
		if strings.Contains(suggestion, fragment) {
			return true
		}
	}
	return false
}

func detailsContain(details []string, fragment string) bool {
	for _, detail := range details {
		if strings.Contains(detail, fragment) {
			return true
		}
	}
	return false
}

type fakeTask struct {
	controls []goav.Control
	taps     []snapshot.Tap
	spec     pipeline.Spec
	snapshot snapshot.Task
	events   []av.Event
	closed   bool
}

func newFakeTask() *fakeTask {
	taps := []snapshot.Tap{
		{Name: "raw_video", MediaKind: av.MediaVideo, Domain: shape.DomainFrame, Shape: shape.Frame(av.MediaVideo, shape.Stream("video")), Node: "raw-node"},
		{Name: "main_encoded", MediaKind: av.MediaVideo, Domain: shape.DomainPacket, Shape: shape.Packet(av.MediaVideo, av.CodecVP8, shape.Stream("video")), Node: "enc-node"},
	}
	spec := pipeline.Spec{
		Name: "fake",
		Nodes: []pipeline.NodeSpec{
			{Name: "source", Kind: pipeline.NodeSource},
			{Name: "raw-node", Kind: pipeline.NodeStage},
			{Name: "enc-node", Kind: pipeline.NodeStage},
			{Name: "sink", Kind: pipeline.NodeSink},
		},
		Edges: []pipeline.EdgeSpec{
			{From: "source", To: "raw-node", Policy: pipeline.RouteAll},
			{From: "raw-node", To: "enc-node", Policy: pipeline.RouteAll},
			{From: "enc-node", To: "sink", Policy: pipeline.RouteAll},
		},
	}
	snap := snapshot.Task{
		Spec: spec,
		Taps: taps,
		Branches: []snapshot.Branch{
			{Name: "preview"},
			{Name: "archive"},
		},
	}
	return &fakeTask{taps: taps, spec: spec, snapshot: snap}
}

func (t *fakeTask) Describe() pipeline.Spec { return t.spec }

func (t *fakeTask) Explain(context.Context) (plan.Report, error) {
	return plan.Report{Graph: t.spec}, nil
}

func (t *fakeTask) Attach(context.Context, ...goav.BranchSpec) (goav.Attachment, error) {
	return nil, errors.New("not implemented")
}

func (t *fakeTask) Detach(context.Context, goav.Attachment) error { return nil }

func (t *fakeTask) Taps() []snapshot.Tap { return append([]snapshot.Tap(nil), t.taps...) }

func (t *fakeTask) Snapshot() snapshot.Task { return t.snapshot }

func (t *fakeTask) Control(_ context.Context, control goav.Control) error {
	t.controls = append(t.controls, control)
	return nil
}

func (t *fakeTask) Run(context.Context) error { return nil }

func (t *fakeTask) Events() <-chan av.Event {
	ch := make(chan av.Event, len(t.events))
	for _, event := range t.events {
		ch <- event
	}
	close(ch)
	return ch
}

func (t *fakeTask) Watch(filters ...goav.EventFilter) <-chan av.Event {
	ch := make(chan av.Event, len(t.events))
	for _, event := range t.events {
		if eventMatches(event, filters) {
			ch <- event
		}
	}
	close(ch)
	return ch
}

func (t *fakeTask) Stats() pipeline.GraphStats { return pipeline.GraphStats{} }

func (t *fakeTask) Close() error {
	t.closed = true
	return nil
}

func eventMatches(event av.Event, filters []goav.EventFilter) bool {
	for _, filter := range filters {
		if filter != nil && !filter(event) {
			return false
		}
	}
	return true
}

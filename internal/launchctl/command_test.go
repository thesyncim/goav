package launchctl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/inspect"
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

func TestNewErrorClonesAndUnwrapsCustomControlFailures(t *testing.T) {
	cause := errors.New("native device refused command")
	details := []string{"field=gain", "value=hot"}
	suggestions := []string{"lower gain", "retry"}
	err := NewError("vendor_refused", "control vendor", "gain", "vendor command failed", details, suggestions, cause)
	details[0] = "mutated"
	suggestions[0] = "mutated"

	if err.Code != "vendor_refused" ||
		err.Operation != "control vendor" ||
		err.Node != "gain" ||
		err.Details[0] != "field=gain" ||
		err.Suggestions[0] != "lower gain" ||
		!errors.Is(err, cause) {
		t.Fatalf("NewError() = %+v", err)
	}
	text := err.Error()
	for _, fragment := range []string{"vendor command failed", "field=gain", "lower gain"} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("Error() = %q, want fragment %q", text, fragment)
		}
	}
}

func TestInvokeCustomCommandContracts(t *testing.T) {
	type vendorCommand struct {
		Stream av.StreamID `goavctl:"stream,required"`
		Gain   int         `goavctl:"gain,required"`
	}

	var got vendorCommand
	spec := CommandSpec{
		Name:     "vendor",
		ArgsType: reflect.TypeOf(vendorCommand{}),
		Apply: func(_ context.Context, _ goav.LiveTask, args any) (ControlResponse, error) {
			got = args.(vendorCommand)
			return ControlResponse{
				Operation: "control vendor",
				Result:    map[string]any{"stream": got.Stream, "gain": got.Gain},
			}, nil
		},
	}
	response, err := Invoke(context.Background(), newFakeTask(), spec, []string{"stream=audio", "gain=7"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Stream != "audio" || got.Gain != 7 || response.Operation != "control vendor" {
		t.Fatalf("Invoke() got=%+v response=%+v", got, response)
	}

	response, err = Invoke(context.Background(), newFakeTask(), CommandSpec{
		Name:     "vendor",
		ArgsType: reflect.TypeOf(vendorCommand{}),
	}, []string{"stream=audio", "gain=7"})
	var structured *Error
	if response.Operation != "" ||
		!errors.As(err, &structured) ||
		structured.Code != "unsupported" ||
		structured.Operation != "control vendor" {
		t.Fatalf("nil Apply response=%+v err=%+v", response, structured)
	}

	cause := errors.New("native command failed")
	customErr := NewError("vendor_failed", "control vendor", "gain", "vendor failed", []string{"gain=7"}, []string{"try gain=3"}, cause)
	spec.Apply = func(context.Context, goav.LiveTask, any) (ControlResponse, error) {
		return ControlResponse{}, customErr
	}
	_, err = Invoke(context.Background(), newFakeTask(), spec, []string{"stream=audio", "gain=7"})
	if !errors.As(err, &structured) || structured.Code != "vendor_failed" || !errors.Is(err, cause) {
		t.Fatalf("custom Invoke error = %+v", err)
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
	segmentArgs, err = BindArgs(segmentSpec, []string{"10s..20s"})
	if err != nil {
		t.Fatal(err)
	}
	segment = segmentArgs.(SegmentCommand)
	if segment.Start != 10*time.Second || segment.End != 20*time.Second {
		t.Fatalf("range segment = %+v", segment)
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

func TestBindArgsRejectsDuplicateFields(t *testing.T) {
	bitrateSpec, _ := LookupControlCommand("bitrate")
	segmentSpec, _ := LookupControlCommand("segment")
	deliverSpec, _ := LookupControlCommand("deliver")
	type boolCommand struct {
		Enabled bool `goavctl:"enabled" usage:"[enabled=<bool>]" help:"feature flag"`
	}
	boolSpec := CommandSpec{Name: "vendor.bool", ArgsType: reflect.TypeOf(boolCommand{})}
	for _, tc := range []struct {
		name string
		spec CommandSpec
		args []string
		node string
	}{
		{name: "canonical", spec: bitrateSpec, args: []string{"stream=video", "value=1200k", "value=900k"}, node: "value"},
		{name: "bool flag", spec: boolSpec, args: []string{"--enabled", "enabled=false"}, node: "enabled"},
		{name: "metadata key", spec: deliverSpec, args: []string{"type=vendor.force_idr", "metadata.foo=bar", "metadata.foo=baz"}, node: "metadata.foo"},
		{name: "metadata empty key", spec: deliverSpec, args: []string{"type=vendor.force_idr", "metadata.=bar"}, node: "metadata."},
		{name: "range then field", spec: segmentSpec, args: []string{"10s..20s", "start=11s"}, node: "start"},
		{name: "field then range", spec: segmentSpec, args: []string{"start=10s", "11s..20s"}, node: "start"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BindArgs(tc.spec, tc.args)
			var structured *Error
			if !errors.As(err, &structured) ||
				structured.Code != "invalid_argument" ||
				structured.Node != tc.node {
				t.Fatalf("err = %+v, want invalid_argument node=%s", structured, tc.node)
			}
		})
	}
}

func TestBindJSONParsesBoolFields(t *testing.T) {
	type boolCommand struct {
		Enabled bool `goavctl:"enabled,required" usage:"enabled=<bool>" help:"feature flag"`
	}
	spec := CommandSpec{Name: "vendor.bool", ArgsType: reflect.TypeOf(boolCommand{})}
	args, err := BindJSON(spec, []byte(`{"enabled":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if !args.(boolCommand).Enabled {
		t.Fatalf("args = %+v", args)
	}

	_, err = BindJSON(spec, []byte(`{"enabled":"yes"}`))
	var structured *Error
	if !errors.As(err, &structured) || structured.Code != "invalid_value" {
		t.Fatalf("err = %v, want invalid_value", err)
	}
}

func TestBindJSONParsesNumericFieldsAndMetadata(t *testing.T) {
	bitrateSpec, _ := LookupControlCommand("bitrate")
	args, err := BindJSON(bitrateSpec, []byte(`{"stream":"video","value":1200000,"at":"main_encoded"}`))
	if err != nil {
		t.Fatal(err)
	}
	bitrate := args.(BitrateCommand)
	if bitrate.Stream != "video" || bitrate.Value != 1_200_000 || bitrate.At != "main_encoded" {
		t.Fatalf("bitrate = %+v", bitrate)
	}

	rateSpec, _ := LookupControlCommand("rate")
	args, err = BindJSON(rateSpec, []byte(`{"value":0.5,"source":"fixture"}`))
	if err != nil {
		t.Fatal(err)
	}
	rate := args.(RateCommand)
	if rate.Value != 0.5 || rate.Source != "fixture" {
		t.Fatalf("rate = %+v", rate)
	}

	deliverSpec, _ := LookupControlCommand("deliver")
	args, err = BindJSON(deliverSpec, []byte(`{"type":"vendor.force_idr","stream":"video","at":"raw_video","metadata":{"count":2,"ok":true,"empty":null}}`))
	if err != nil {
		t.Fatal(err)
	}
	deliver := args.(DeliverCommand)
	if deliver.Metadata["count"] != "2" ||
		deliver.Metadata["ok"] != "true" ||
		deliver.Metadata["empty"] != "" {
		t.Fatalf("metadata = %+v", deliver.Metadata)
	}

	_, err = BindJSON(rateSpec, []byte(`{"value":0.5} {"value":1}`))
	var structured *Error
	if !errors.As(err, &structured) || structured.Code != "invalid_json" {
		t.Fatalf("err = %v, want invalid_json", err)
	}

	_, err = BindJSON(rateSpec, []byte(`{"value":0.5,"value":1}`))
	if !errors.As(err, &structured) || structured.Code != "invalid_json" {
		t.Fatalf("duplicate field err = %v, want invalid_json", err)
	}

	_, err = BindJSON(deliverSpec, []byte(`{"type":"vendor.force_idr","metadata":{"source":"a","source":"b"}}`))
	if !errors.As(err, &structured) || structured.Code != "invalid_json" {
		t.Fatalf("duplicate metadata err = %v, want invalid_json", err)
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
	ctrl := task.controls[0]
	if ctrl.Type != control.BitrateType || ctrl.StreamID != "video" || ctrl.Bitrate != 1_200_000 || ctrl.Tap != "main_encoded" {
		t.Fatalf("control = %+v", ctrl)
	}

	_, err = Execute(context.Background(), task, []string{"control", "--json", `{"type":"keyframe","stream_id":"video"}`})
	if err != nil {
		t.Fatal(err)
	}
	if len(task.controls) != 2 {
		t.Fatalf("controls = %d, want 2", len(task.controls))
	}
	ctrl = task.controls[1]
	if ctrl.Type != control.KeyframeType || ctrl.StreamID != "video" || ctrl.Tap != "" {
		t.Fatalf("keyframe control = %+v", ctrl)
	}
}

func TestDecodeRawControlCanonicalFieldsAndRefusals(t *testing.T) {
	keyframe, err := DecodeRawControl([]byte(`{"type":"keyframe","stream_id":"video","tap":"raw_video"}`))
	if err != nil {
		t.Fatal(err)
	}
	if keyframe.Type != control.KeyframeType || keyframe.StreamID != "video" || keyframe.Tap != "raw_video" {
		t.Fatalf("keyframe = %+v", keyframe)
	}

	bitrate, err := DecodeRawControl([]byte(`{"type":"bitrate","stream_id":"audio","bitrate":"2M"}`))
	if err != nil {
		t.Fatal(err)
	}
	if bitrate.Type != control.BitrateType || bitrate.StreamID != "audio" || bitrate.Bitrate != 2_000_000 {
		t.Fatalf("bitrate = %+v", bitrate)
	}

	segment, err := DecodeRawControl([]byte(`{"type":"segment","start":"10s","end":"20s"}`))
	if err != nil {
		t.Fatal(err)
	}
	if segment.Type != control.SegmentType || segment.Position != 10*time.Second || segment.End != 20*time.Second {
		t.Fatalf("segment = %+v", segment)
	}

	selectControl, err := DecodeRawControl([]byte(`{"type":"select","active":"camera_b"}`))
	if err != nil {
		t.Fatal(err)
	}
	if selectControl.Type != control.SelectType || selectControl.StreamID != "camera_b" {
		t.Fatalf("select = %+v", selectControl)
	}

	deliver, err := DecodeRawControl([]byte(`{"type":"event","event":{"type":"vendor.force_idr","stream_id":"video","metadata":{"source":null}},"tap":"raw_video","reason":"manual"}`))
	if err != nil {
		t.Fatal(err)
	}
	if deliver.Type != control.EventType ||
		deliver.Tap != "raw_video" ||
		deliver.Reason != "manual" ||
		deliver.Event.Type != "vendor.force_idr" ||
		deliver.Event.StreamID != "video" ||
		deliver.Event.Metadata["source"] != "" {
		t.Fatalf("deliver = %+v event=%+v", deliver, deliver.Event)
	}

	for _, tc := range []struct {
		name string
		json string
		code string
		node string
	}{
		{name: "missing type", json: `{}`, code: "missing_required", node: "type"},
		{name: "missing bitrate", json: `{"type":"bitrate","stream_id":"video"}`, code: "missing_required", node: "bitrate"},
		{name: "missing seek", json: `{"type":"seek"}`, code: "missing_required", node: "position"},
		{name: "missing rate", json: `{"type":"rate"}`, code: "missing_required", node: "rate"},
		{name: "missing segment end", json: `{"type":"segment","start":"10s"}`, code: "missing_required", node: "segment"},
		{name: "missing event object", json: `{"type":"event"}`, code: "missing_required", node: "event"},
		{name: "deliver control type", json: `{"type":"deliver","event":{"type":"vendor.force_idr"}}`, code: "invalid_value", node: "type"},
		{name: "unknown type", json: `{"type":"warp"}`, code: "invalid_value", node: "type"},
		{name: "stream alias", json: `{"type":"keyframe","stream":"video"}`, code: "unknown_field", node: "stream"},
		{name: "value alias", json: `{"type":"bitrate","stream_id":"audio","value":"2M"}`, code: "unknown_field", node: "value"},
		{name: "position alias", json: `{"type":"segment","position":"10s","end":"20s"}`, code: "unknown_field", node: "position"},
		{name: "event stream alias", json: `{"type":"event","event":{"type":"vendor.force_idr","stream":"video"}}`, code: "unknown_field", node: "stream"},
		{name: "duplicate raw field", json: `{"type":"rate","rate":0.5,"rate":1}`, code: "invalid_json"},
		{name: "duplicate nested field", json: `{"type":"event","event":{"type":"vendor.force_idr","stream_id":"video","stream_id":"audio"}}`, code: "invalid_json"},
		{name: "extra object", json: `{"type":"keyframe"} {"type":"rate"}`, code: "invalid_json"},
		{name: "null object", json: `null`, code: "invalid_json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeRawControl([]byte(tc.json))
			var structured *Error
			if !errors.As(err, &structured) || structured.Code != tc.code || (tc.node != "" && structured.Node != tc.node) {
				t.Fatalf("err = %+v, want code=%s node=%s", structured, tc.code, tc.node)
			}
		})
	}
}

func TestDecodeRawControlParsesFloatAndDurationFields(t *testing.T) {
	rate, err := DecodeRawControl([]byte(`{"type":"rate","rate":"1.5","node":"source"}`))
	if err != nil {
		t.Fatal(err)
	}
	if rate.Type != control.RateType || rate.Rate != 1.5 || rate.Node != "source" {
		t.Fatalf("rate = %+v", rate)
	}

	seek, err := DecodeRawControl([]byte(`{"type":"seek","position":"250ms"}`))
	if err != nil {
		t.Fatal(err)
	}
	if seek.Type != control.SeekType || seek.Position != 250*time.Millisecond {
		t.Fatalf("seek = %+v", seek)
	}

	segment, err := DecodeRawControl([]byte(`{"type":"segment","start":1000000,"end":"2ms"}`))
	if err != nil {
		t.Fatal(err)
	}
	if segment.Type != control.SegmentType || segment.Position != time.Millisecond || segment.End != 2*time.Millisecond {
		t.Fatalf("segment = %+v", segment)
	}

	_, err = DecodeRawControl([]byte(`{"type":"seek","position":{"bad":true}}`))
	var structured *Error
	if !errors.As(err, &structured) || structured.Code != "invalid_value" {
		t.Fatalf("err = %v, want invalid_value", err)
	}
}

func TestRawEventRejectsLossyMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		json string
		code string
		node string
	}{
		{name: "metadata array", json: `{"type":"vendor.force_idr","metadata":[]}`, code: "invalid_value", node: "metadata"},
		{name: "nested metadata", json: `{"type":"vendor.force_idr","metadata":{"nested":{"bad":true}}}`, code: "invalid_value", node: "metadata.nested"},
		{name: "missing type", json: `{"metadata":{"source":"cli"}}`, code: "missing_required", node: "type"},
		{name: "stream alias", json: `{"type":"vendor.force_idr","stream":"video"}`, code: "unknown_field", node: "stream"},
		{name: "duplicate stream", json: `{"type":"vendor.force_idr","stream_id":"video","stream_id":"audio"}`, code: "invalid_json"},
		{name: "duplicate metadata", json: `{"type":"vendor.force_idr","metadata":{"source":"a","source":"b"}}`, code: "invalid_json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeRawEvent([]byte(tc.json))
			var structured *Error
			if !errors.As(err, &structured) || structured.Code != tc.code || structured.Node != tc.node {
				t.Fatalf("err = %+v, want code=%s node=%s", structured, tc.code, tc.node)
			}
		})
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
	ctrl := task.controls[0]
	if ctrl.Type != control.EventType || ctrl.Tap != "raw_video" {
		t.Fatalf("control = %+v", ctrl)
	}
	event := ctrl.Event
	if event.Type != "vendor.force_idr" || event.StreamID != "video" || event.Reason != "manual" ||
		event.Metadata["source"] != "cli" || event.Metadata["count"] != "2" || event.Metadata["ok"] != "true" {
		t.Fatalf("event = %+v", event)
	}

	_, err = Execute(context.Background(), task, []string{"control", "deliver", "--json", `{"type":"vendor.force_idr","stream_id":"video"}`, "reason=ignored", "at=raw_video"})
	var structured *Error
	if !errors.As(err, &structured) ||
		structured.Code != "unknown_field" ||
		structured.Node != "reason" {
		t.Fatalf("raw event extra field err = %+v", structured)
	}
}

func TestControlTargetingVariantsAndRefusals(t *testing.T) {
	task := newFakeTask()
	commands := [][]string{
		{"control", "keyframe", "stream=video", "at=raw_video"},
		{"control", "bitrate", "stream=video", "value=1200k", "at=main_encoded"},
		{"control", "seek", "position=1s", "node=program"},
		{"control", "select", "active=camera_b", "at=raw_video"},
	}
	for _, command := range commands {
		if _, err := Execute(context.Background(), task, command); err != nil {
			t.Fatalf("%v: %v", command, err)
		}
	}
	if task.controls[0].Tap != "raw_video" ||
		task.controls[1].Tap != "main_encoded" ||
		task.controls[2].Node != "program" ||
		task.controls[3].Tap != "raw_video" {
		t.Fatalf("controls = %+v", task.controls)
	}

	for _, tc := range []struct {
		name string
		args []string
		code string
		node string
	}{
		{name: "selector at conflict", args: []string{"control", "select", "active=camera_b", "selector=program", "at=raw_video"}, code: "target_conflict", node: "program,raw_video"},
		{name: "source node conflict", args: []string{"control", "rate", "value=1", "source=source", "node=program"}, code: "target_conflict", node: "source,program"},
		{name: "wrong source kind", args: []string{"control", "rate", "value=1", "source=raw-node"}, code: "wrong_target_kind", node: "raw-node"},
		{name: "unknown source", args: []string{"control", "seek", "position=1s", "source=soruce"}, code: "unknown_node", node: "soruce"},
		{name: "unknown node", args: []string{"control", "seek", "position=1s", "node=progra"}, code: "unknown_node", node: "progra"},
		{name: "deliver missing at", args: []string{"control", "deliver", "type=vendor.force_idr"}, code: "missing_target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Execute(context.Background(), task, tc.args)
			var structured *Error
			if !errors.As(err, &structured) || structured.Code != tc.code || (tc.node != "" && structured.Node != tc.node) {
				t.Fatalf("err = %+v, want code=%s node=%s", structured, tc.code, tc.node)
			}
		})
	}
}

func TestRequestFromCLIUsesCanonicalControlProtocol(t *testing.T) {
	request, err := RequestFromCLI([]string{"control", "bitrate", "stream=video", "value=1200k", "at=main_encoded"})
	if err != nil {
		t.Fatal(err)
	}
	if request.Op != "control" || request.Verb != "bitrate" ||
		request.Args["stream"] != "video" || request.Args["value"] != "1200k" || request.Args["at"] != "main_encoded" {
		t.Fatalf("request = %+v", request)
	}

	raw, err := RequestFromCLI([]string{"control", "--json", `{"type":"keyframe","stream_id":"video"}`})
	if err != nil {
		t.Fatal(err)
	}
	if raw.Op != "control_raw" || string(raw.Control) != `{"type":"keyframe","stream_id":"video"}` {
		t.Fatalf("raw request = %+v", raw)
	}
}

func TestRequestFromCLIRejectsMalformedCommands(t *testing.T) {
	for _, tc := range []struct {
		name string
		argv []string
		code string
	}{
		{name: "empty", argv: nil, code: "missing_command"},
		{name: "missing control verb", argv: []string{"control"}, code: "missing_command"},
		{name: "raw too many args", argv: []string{"control", "--json", `{}`, `{}`}, code: "invalid_argument"},
		{name: "duplicate control arg", argv: []string{"control", "bitrate", "stream=video", "stream=audio", "value=1200k"}, code: "invalid_argument"},
		{name: "duplicate raw event arg", argv: []string{"control", "deliver", "--json", `{"type":"vendor.force_idr"}`, "at=raw_video", "at=frames"}, code: "invalid_argument"},
		{name: "duplicate watch arg", argv: []string{"watch", "--follow", "follow=false"}, code: "invalid_argument"},
		{name: "bad graph args", argv: []string{"graph", "mermaid", "dot"}, code: "invalid_argument"},
		{name: "bad attach", argv: []string{"attach", "tap", "branch", "copy"}, code: "invalid_argument"},
		{name: "bad detach", argv: []string{"detach"}, code: "invalid_argument"},
		{name: "unknown", argv: []string{"warp"}, code: "unknown_command"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RequestFromCLI(tc.argv)
			var structured *Error
			if !errors.As(err, &structured) || structured.Code != tc.code {
				t.Fatalf("err = %+v, want code=%s", structured, tc.code)
			}
		})
	}
}

func TestExecuteRequestAppliesControlRequest(t *testing.T) {
	task := newFakeTask()
	response := ExecuteRequest(context.Background(), task, Request{
		Op:   "control",
		Verb: "bitrate",
		Args: map[string]string{"stream": "video", "value": "1200k", "at": "main_encoded"},
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("response = %+v", response)
	}
	if len(task.controls) != 1 || task.controls[0].Type != control.BitrateType || task.controls[0].Bitrate != 1_200_000 {
		t.Fatalf("controls = %+v", task.controls)
	}
}

func TestExecuteRequestCoversRawAndUtilityOps(t *testing.T) {
	task := newFakeTask()
	response := ExecuteRequest(context.Background(), task, Request{
		Op:      "control_raw",
		Control: json.RawMessage(`{"type":"keyframe","stream_id":"video","tap":"raw_video"}`),
	})
	if !response.OK || response.Error != nil || len(task.controls) != 1 || task.controls[0].Type != control.KeyframeType {
		t.Fatalf("raw response=%+v controls=%+v", response, task.controls)
	}

	response = ExecuteRequest(context.Background(), task, Request{Op: "flowchart", Args: map[string]string{"format": "text"}})
	text, ok := response.Result.(string)
	if !response.OK || response.Error != nil || !ok || !strings.Contains(text, "pipeline fake") {
		t.Fatalf("flowchart response = %+v", response)
	}

	for _, tc := range []struct {
		name    string
		request Request
		code    string
	}{
		{name: "missing control verb", request: Request{Op: "control"}, code: "missing_command"},
		{name: "unknown op", request: Request{Op: "warp"}, code: "unknown_command"},
		{name: "invalid raw", request: Request{Op: "control_raw", Control: json.RawMessage(`{}`)}, code: "missing_required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := ExecuteRequest(context.Background(), task, tc.request)
			if response.OK || response.Error == nil || response.Error.Code != tc.code {
				t.Fatalf("response = %+v, want code=%s", response, tc.code)
			}
		})
	}
}

func TestExecuteRequestAppliesHelpRequest(t *testing.T) {
	response := ExecuteRequest(context.Background(), newFakeTask(), Request{
		Op:   "help",
		Args: map[string]string{"topic": "control", "command": "bitrate"},
	})
	text, ok := response.Result.(string)
	if !response.OK || response.Error != nil || !ok || !strings.Contains(text, "control bitrate") {
		t.Fatalf("help response = %+v", response)
	}
}

func TestExecuteRequestHelpIncludesRuntimeCapabilities(t *testing.T) {
	task := &descriptorTask{
		fakeTask: newFakeTask(),
		encoders: []codec.Descriptor{{
			ID:   "x_request_pcm",
			Name: "Request PCM",
			Type: av.MediaAudio,
		}},
		muxers: []format.Descriptor{{
			Format: "x_request_mux",
			Codecs: []av.CodecID{"x_request_pcm"},
		}},
	}
	response := ExecuteRequest(context.Background(), task, Request{
		Op:   "help",
		Args: map[string]string{"topic": "attach"},
	})
	text, ok := response.Result.(string)
	if !response.OK || response.Error != nil || !ok {
		t.Fatalf("help response = %+v", response)
	}
	for _, fragment := range []string{
		"Runtime encoders:",
		"encode codec=x_request_pcm media=audio",
		"Request PCM",
		"Runtime muxers:",
		"filesink location=<path> [format=x_request_mux]",
		"runtime-registered muxer for codecs x_request_pcm",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("help missing %q:\n%s", fragment, text)
		}
	}
}

func TestRequestFromCLIParsesGraphCommand(t *testing.T) {
	request, err := RequestFromCLI([]string{"graph", "format=dot"})
	if err != nil {
		t.Fatal(err)
	}
	if request.Op != "graph" || request.Args["format"] != "dot" {
		t.Fatalf("request = %+v", request)
	}

	shorthand, err := RequestFromCLI([]string{"flowchart", "mermaid"})
	if err != nil {
		t.Fatal(err)
	}
	if shorthand.Op != "flowchart" || shorthand.Args["format"] != "mermaid" {
		t.Fatalf("shorthand = %+v", shorthand)
	}

	caps, err := RequestFromCLI([]string{"capabilities"})
	if err != nil {
		t.Fatal(err)
	}
	if caps.Op != "capabilities" {
		t.Fatalf("capabilities request = %+v", caps)
	}
	if _, err := RequestFromCLI([]string{"capabilities", "extra"}); err == nil {
		t.Fatal("capabilities with arguments succeeded, want error")
	}
}

func TestRequestFromCLIParsesHelpTopicAndCommand(t *testing.T) {
	request, err := RequestFromCLI([]string{"help", "control", "bitrate"})
	if err != nil {
		t.Fatal(err)
	}
	if request.Op != "help" || request.Args["topic"] != "control" || request.Args["command"] != "bitrate" {
		t.Fatalf("request = %+v", request)
	}
}

func TestRequestFromCLIParsesRebranchCommand(t *testing.T) {
	request, err := RequestFromCLI([]string{
		"rebranch",
		"preview",
		"--switch", "next_keyframe",
		"--keep-old-on-failure",
		"copy ! filesink location=preview.webm",
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.Op != "rebranch" ||
		request.Branch != "preview" ||
		request.Switch != "next_keyframe" ||
		!request.KeepOldOnFailure ||
		request.Pipeline != "copy ! filesink location=preview.webm" {
		t.Fatalf("request = %+v", request)
	}

	for _, argv := range [][]string{
		{"rebranch", "preview"},
		{"rebranch", "preview", "--switch"},
		{"rebranch", "preview", "copy ! filesink location=a.webm", "copy ! filesink location=b.webm"},
	} {
		if _, err := RequestFromCLI(argv); err == nil {
			t.Fatalf("RequestFromCLI(%v) succeeded, want error", argv)
		}
	}
}

func TestExecuteRequestRendersTaskGraph(t *testing.T) {
	response := ExecuteRequest(context.Background(), newFakeTask(), Request{Op: "graph"})
	text, ok := response.Result.(string)
	if !response.OK || response.Error != nil || !ok {
		t.Fatalf("graph response = %+v", response)
	}
	if !strings.Contains(text, "flowchart LR") || !strings.Contains(text, "source") {
		t.Fatalf("graph:\n%s", text)
	}

	response = ExecuteRequest(context.Background(), newFakeTask(), Request{
		Op:   "graph",
		Args: map[string]string{"format": "dot"},
	})
	text, ok = response.Result.(string)
	if !response.OK || response.Error != nil || !ok || !strings.Contains(text, "digraph") {
		t.Fatalf("dot graph response = %+v text=%q", response, text)
	}

	direct, err := Execute(context.Background(), newFakeTask(), []string{"graph", "text"})
	if err != nil {
		t.Fatal(err)
	}
	text, ok = direct.Result.(string)
	if direct.Operation != "graph" || !ok || !strings.Contains(text, "pipeline fake") {
		t.Fatalf("direct graph = %+v text=%q", direct, text)
	}
}

func TestExecuteGraphRejectsInvalidFormat(t *testing.T) {
	_, err := Execute(context.Background(), newFakeTask(), []string{"graph", "format=json"})
	if err == nil {
		t.Fatal("expected invalid graph format")
	}
	var structured *Error
	if !errors.As(err, &structured) || structured.Code != "invalid_value" {
		t.Fatalf("err = %v", err)
	}
}

func TestExecuteStreamsAndWatchCommands(t *testing.T) {
	task := newFakeTask()
	task.events = []av.Event{
		{Type: av.EventStats, StreamID: "video"},
		{Type: av.EventBitrateChanged, StreamID: "audio"},
	}

	streams, err := Execute(context.Background(), task, []string{"streams"})
	if err != nil {
		t.Fatal(err)
	}
	infos, ok := streams.Result.([]StreamInfo)
	if streams.Operation != "streams" || !ok || len(infos) != 1 || infos[0].ID != "video" || infos[0].Tap != "raw_video" {
		t.Fatalf("streams = %+v", streams)
	}

	watch, err := Execute(context.Background(), task, []string{"watch", "type=stats", "stream=video"})
	if err != nil {
		t.Fatal(err)
	}
	events, ok := watch.Result.([]av.Event)
	if watch.Operation != "watch" || !ok || len(events) != 1 || events[0].Type != av.EventStats {
		t.Fatalf("watch = %+v", watch)
	}

	all, err := Execute(context.Background(), task, []string{"events"})
	if err != nil {
		t.Fatal(err)
	}
	allEvents, ok := all.Result.([]av.Event)
	if all.Operation != "events" || !ok || len(allEvents) != 2 {
		t.Fatalf("events = %+v", all)
	}

	_, err = Execute(context.Background(), task, []string{"watch", "--follow"})
	var structured *Error
	if !errors.As(err, &structured) || structured.Code != "unsupported_streaming_response" {
		t.Fatalf("err = %v, want unsupported_streaming_response", err)
	}

	_, err = Execute(context.Background(), task, []string{"watch", "type=stats", "type=bitrate_changed"})
	if !errors.As(err, &structured) || structured.Code != "invalid_argument" || structured.Node != "type" {
		t.Fatalf("err = %+v, want duplicate type", structured)
	}
}

func TestExecuteUnsupportedRebranchReportsKnownBranches(t *testing.T) {
	task := newFakeTask()
	_, err := Execute(context.Background(), task, []string{"rebranch", "preview"})
	var structured *Error
	if !errors.As(err, &structured) || structured.Code != "unsupported" {
		t.Fatalf("err = %v, want unsupported", err)
	}

	_, err = Execute(context.Background(), task, []string{"rebranch", "preveiw"})
	if !errors.As(err, &structured) ||
		structured.Code != "unknown_branch" ||
		!detailsContain(structured.Details, "preview") ||
		!suggestionsContain(structured.Suggestions, "preview") {
		t.Fatalf("err = %+v", structured)
	}
}

func TestBranchPipelineExtensionHandleCallsConfiguredHooks(t *testing.T) {
	var calls []string
	handle := &BranchPipeline{
		copyFn:   func() { calls = append(calls, "copy") },
		decodeFn: func() { calls = append(calls, "decode") },
		resizeFn: func(width int, height int) { calls = append(calls, fmt.Sprintf("resize:%dx%d", width, height)) },
		resampleFn: func(sampleRate int, channels int) {
			calls = append(calls, fmt.Sprintf("resample:%d:%d", sampleRate, channels))
		},
		doFn: func(...pipeline.Stage) { calls = append(calls, "do") },
		encodeFn: func(spec codec.CodecSpec) {
			calls = append(calls, "encode:"+string(spec.ID))
		},
		destinationFn: func(goav.Destination) { calls = append(calls, "destination") },
		finishFn: func() goav.BranchSpec {
			calls = append(calls, "finish")
			return goav.Branch("custom").To()
		},
	}

	handle.Copy()
	handle.Decode()
	handle.Resize(320, 180)
	handle.Resample(16000, 1)
	handle.Do()
	handle.Encode(codec.Opus())
	handle.Destination(goav.Destination{})
	_ = handle.finish()

	want := []string{
		"copy",
		"decode",
		"resize:320x180",
		"resample:16000:1",
		"do",
		"encode:opus",
		"destination",
		"finish",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}

	var nilHandle *BranchPipeline
	nilHandle.Copy()
	nilHandle.Decode()
	nilHandle.Resize(1, 1)
	nilHandle.Resample(1, 1)
	nilHandle.Do()
	nilHandle.Encode(codec.CodecSpec{})
	nilHandle.Destination(goav.Destination{})
	_ = nilHandle.finish()
}

func TestPipelineParserHelpersCoverCommonFormsAndErrors(t *testing.T) {
	width, height, err := parseResizeArgs([]string{"width=854", "height=480"})
	if err != nil {
		t.Fatal(err)
	}
	if width != 854 || height != 480 {
		t.Fatalf("resize = %dx%d", width, height)
	}

	rate, channels, err := parseResampleArgs([]string{"sample_rate=48000", "channels=2"})
	if err != nil {
		t.Fatal(err)
	}
	if rate != 48000 || channels != 2 {
		t.Fatalf("resample = %d/%d", rate, channels)
	}

	bitrate, err := parseRate("2mbps")
	if err != nil {
		t.Fatal(err)
	}
	if bitrate != 2_000_000 {
		t.Fatalf("bitrate = %d, want 2000000", bitrate)
	}

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "resize positional", run: func() error {
			_, _, err := parseResizeArgs([]string{"854x480"})
			return err
		}},
		{name: "resize short width", run: func() error {
			_, _, err := parseResizeArgs([]string{"w=854", "height=480"})
			return err
		}},
		{name: "resize zero height", run: func() error {
			_, _, err := parseResizeArgs([]string{"width=854", "height=0"})
			return err
		}},
		{name: "resample positional", run: func() error {
			_, _, err := parseResampleArgs([]string{"48000", "2"})
			return err
		}},
		{name: "resample rate alias", run: func() error {
			_, _, err := parseResampleArgs([]string{"rate=48000", "channels=2"})
			return err
		}},
		{name: "resample zero channels", run: func() error {
			_, _, err := parseResampleArgs([]string{"sample_rate=48000", "channels=0"})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			var structured *Error
			if !errors.As(err, &structured) || structured.Code != "invalid_value" {
				t.Fatalf("err = %v, want invalid_value", err)
			}
		})
	}
}

func TestBranchPipelineLexingSupportsQuotedValues(t *testing.T) {
	steps, err := splitPipeline(`meter label="left ! right" note='two words' raw="a=b" ! filesink location="/tmp/a b=1.ogg" title="say \"hi\"" format=ogg`)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 {
		t.Fatalf("steps = %v", steps)
	}

	fields, err := pipelineFields(steps[0])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fields, []string{"meter", "label=left ! right", "note=two words", "raw=a=b"}) {
		t.Fatalf("fields[0] = %v", fields)
	}

	fields, err = pipelineFields(steps[1])
	if err != nil {
		t.Fatal(err)
	}
	args, err := stepArgs(fields[1:])
	if err != nil {
		t.Fatal(err)
	}
	if fields[0] != "filesink" ||
		args["location"] != "/tmp/a b=1.ogg" ||
		args["title"] != `say "hi"` ||
		args["format"] != "ogg" {
		t.Fatalf("fields[1]=%v args=%v", fields, args)
	}

	for _, tc := range []struct {
		name       string
		run        func() error
		wantOffset bool
	}{
		{name: "empty step", run: func() error {
			_, err := splitPipeline(`copy ! ! filesink location=out.ivf`)
			return err
		}, wantOffset: true},
		{name: "split quote", run: func() error {
			_, err := splitPipeline(`copy ! filesink location="unterminated`)
			return err
		}, wantOffset: true},
		{name: "field quote", run: func() error {
			_, err := pipelineFields(`meter label="unterminated`)
			return err
		}, wantOffset: true},
		{name: "field escape", run: func() error {
			_, err := pipelineFields(`meter label="dangling\`)
			return err
		}, wantOffset: true},
		{name: "duplicate step arg", run: func() error {
			_, err := stepArgs([]string{"label=left", "label=right"})
			return err
		}},
		{name: "empty step arg name", run: func() error {
			_, err := stepArgs([]string{"=value"})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			var structured *Error
			if !errors.As(err, &structured) ||
				structured.Code != "invalid_value" ||
				(tc.wantOffset && !detailsContain(structured.Details, "offset=")) {
				t.Fatalf("err = %+v, want invalid_value", err)
			}
		})
	}
}

func TestPipelineStepNamesIncludeCustomAliases(t *testing.T) {
	names := pipelineStepNames(PipelineRegistry{
		Steps: []BranchPipelineStepSpec{{
			Name:    "VendorFilter",
			Aliases: []string{"vf"},
		}},
		Encoders: []EncoderSpec{{
			Name:    "VendorEnc",
			Aliases: []string{"venc"},
		}},
	})
	for _, want := range []string{"copy", "encode", "vendorfilter", "vf", "vendorenc", "venc"} {
		if !stringSliceContains(names, want) {
			t.Fatalf("names = %v, want %q", names, want)
		}
	}
}

func TestPipelineRegistryRejectsReservedAndDuplicateNames(t *testing.T) {
	tests := []struct {
		name     string
		registry PipelineRegistry
		node     string
		first    string
		second   string
	}{
		{
			name: "step alias shadows built-in",
			registry: PipelineRegistry{Steps: []BranchPipelineStepSpec{{
				Name:    "Meter",
				Aliases: []string{"copy"},
			}}},
			node:   "copy",
			first:  "built-in branch-pipeline step:copy",
			second: "custom branch-pipeline step alias:Meter",
		},
		{
			name: "encoder alias shadows built-in",
			registry: PipelineRegistry{Encoders: []EncoderSpec{{
				Name:    "AcmeEnc",
				Aliases: []string{"encode"},
			}}},
			node:   "encode",
			first:  "built-in branch-pipeline step:encode",
			second: "custom encoder alias:AcmeEnc",
		},
		{
			name: "step and encoder collide",
			registry: PipelineRegistry{
				Steps: []BranchPipelineStepSpec{{Name: "meter"}},
				Encoders: []EncoderSpec{{
					Name:    "acmeenc",
					Aliases: []string{"meter"},
				}},
			},
			node:   "meter",
			first:  "custom branch-pipeline step:meter",
			second: "custom encoder alias:acmeenc",
		},
		{
			name: "duplicate aliases collide",
			registry: PipelineRegistry{Steps: []BranchPipelineStepSpec{
				{Name: "meter", Aliases: []string{"levels"}},
				{Name: "probe", Aliases: []string{"levels"}},
			}},
			node:   "levels",
			first:  "custom branch-pipeline step alias:meter",
			second: "custom branch-pipeline step alias:probe",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePipelineRegistry(tt.registry)
			assertRegistryError(t, err, tt.node, tt.first, tt.second)

			_, err = HelpWithRegistry([]string{"attach"}, nil, tt.registry)
			assertRegistryError(t, err, tt.node, tt.first, tt.second)

			_, err = parseBranchPipelineWithRegistry(newFakeTask(), "raw_video", "archive", "copy", tt.registry)
			assertRegistryError(t, err, tt.node, tt.first, tt.second)
		})
	}
}

func TestRegistryValidationRejectsMalformedCapabilityRows(t *testing.T) {
	structType := reflect.TypeOf(struct{}{})
	pointerType := reflect.TypeOf(&struct{}{})

	tests := []struct {
		name     string
		validate func() error
		node     string
		message  string
	}{
		{
			name: "command empty name",
			validate: func() error {
				return validateCommandManifest(append(ControlManifest(), CommandSpec{
					Name:     "  ",
					ArgsType: structType,
				}))
			},
			node:    "name",
			message: "control command needs a non-empty name",
		},
		{
			name: "command missing args type",
			validate: func() error {
				return validateCommandManifest(append(ControlManifest(), CommandSpec{Name: "vendor.missing"}))
			},
			node:    "vendor.missing",
			message: `control command "vendor.missing" needs a struct ArgsType`,
		},
		{
			name: "command pointer args type",
			validate: func() error {
				return validateCommandManifest(append(ControlManifest(), CommandSpec{
					Name:     "vendor.pointer",
					ArgsType: pointerType,
				}))
			},
			node:    "vendor.pointer",
			message: `control command "vendor.pointer" ArgsType must be a struct`,
		},
		{
			name: "branch step empty alias",
			validate: func() error {
				return validatePipelineRegistry(PipelineRegistry{
					Steps: []BranchPipelineStepSpec{{Name: "meter", Aliases: []string{"  "}}},
				})
			},
			node:    "alias",
			message: "custom branch-pipeline step alias needs a non-empty name",
		},
		{
			name: "branch step pointer args type",
			validate: func() error {
				return validatePipelineRegistry(PipelineRegistry{
					Steps: []BranchPipelineStepSpec{{Name: "meter", ArgsType: pointerType}},
				})
			},
			node:    "meter",
			message: `custom branch-pipeline step "meter" ArgsType must be a struct`,
		},
		{
			name: "encoder empty name",
			validate: func() error {
				return validatePipelineRegistry(PipelineRegistry{
					Encoders: []EncoderSpec{{Name: "", ArgsType: structType}},
				})
			},
			node:    "name",
			message: "custom encoder needs a non-empty name",
		},
		{
			name: "encoder pointer args type",
			validate: func() error {
				return validatePipelineRegistry(PipelineRegistry{
					Encoders: []EncoderSpec{{Name: "acmeenc", ArgsType: pointerType}},
				})
			},
			node:    "acmeenc",
			message: `custom encoder "acmeenc" ArgsType must be a struct`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.validate()
			var structured *Error
			if !errors.As(err, &structured) ||
				structured.Code != "invalid_registry" ||
				structured.Node != tt.node ||
				!strings.Contains(structured.Message, tt.message) ||
				!suggestionsContain(structured.Suggestions, "struct settings types") {
				t.Fatalf("err = %+v, want invalid_registry node=%s message containing %q", structured, tt.node, tt.message)
			}
		})
	}

	server := &Server{
		Task:     newFakeTask(),
		Commands: []CommandSpec{{Name: "vendor.pointer", ArgsType: pointerType}},
	}
	response := server.Handle(context.Background(), Request{
		Op:   "help",
		Args: map[string]string{"topic": "control"},
	})
	if response.OK || response.Error == nil ||
		response.Error.Code != "invalid_registry" ||
		response.Error.Node != "vendor.pointer" {
		t.Fatalf("server response = %+v, want invalid registry before help", response)
	}

	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goavctl-malformed-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	err := ServeUnixWithOptions(context.Background(), newFakeTask(), "unix://"+socket, WithCommands(CommandSpec{Name: "vendor.pointer", ArgsType: pointerType}))
	var structured *Error
	if !errors.As(err, &structured) ||
		structured.Code != "invalid_registry" ||
		structured.Node != "vendor.pointer" {
		t.Fatalf("ServeUnixWithOptions err = %+v, want invalid registry", err)
	}
}

func TestParseBranchPipelineWithBuiltInStepsAndEncoders(t *testing.T) {
	out := filepath.Join(t.TempDir(), "builtins.ogg")
	_, err := parseBranchPipelineWithRegistry(newFakeTask(), "raw_video", "builtins",
		`decode ! resize width=320 height=180 ! resample sample_rate=48000 channels=2 ! `+
			`encode codec=vp8 media=video bitrate=1k ! encode codec=vp9 media=video profile=screen ! encode codec=h264 media=video level=3 ! encode codec=av1 media=video fps=30000/1001 ! `+
			`encode codec=opus media=audio sample_rate=48000 channels=2 ! encode codec=vendor_custom media=audio clock_rate=48000 ! `+
			`filesink location="`+out+`" format=ogg`,
		PipelineRegistry{},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestParseBranchPipelineWithRegistryUsesCustomAliases(t *testing.T) {
	task := newFakeTask()
	var calls []string
	registry := PipelineRegistry{
		Steps: []BranchPipelineStepSpec{
			{
				Name:    "Meter",
				Aliases: []string{"levelmeter"},
				Apply: func(branch *BranchPipeline, args StepArgs) error {
					calls = append(calls, "meter:"+args["label"])
					branch.Do(goav.FrameFunc("meter", func(_ context.Context, frame *av.Frame, emit goav.Emit) error {
						return emit.Frame(frame)
					}))
					return nil
				},
			},
			{
				Name:    "Out",
				Aliases: []string{"outsink"},
				Apply: func(branch *BranchPipeline, args StepArgs) error {
					calls = append(calls, "sink:"+args["name"])
					branch.Destination(goav.Sink(goav.SinkFunc("memory", func(context.Context, goav.Message) error {
						return nil
					})))
					return nil
				},
			},
		},
		Encoders: []EncoderSpec{{
			Name:    "AcmeEnc",
			Aliases: []string{"acme"},
			Apply: func(args StepArgs) (codec.CodecSpec, error) {
				calls = append(calls, "encoder:"+args["quality"])
				bitrate, err := parseRate(args["bitrate"])
				if err != nil {
					return codec.CodecSpec{}, err
				}
				return codec.Codec("vendor_audio", av.MediaAudio, codec.Bitrate(bitrate), codec.Profile(args["quality"])), nil
			},
		}},
	}

	if _, err := parseBranchPipelineWithRegistry(task, "raw_video", "archive", `levelmeter label="left ! right" ! acme bitrate=123k quality=voice ! outsink name=memory`, registry); err != nil {
		t.Fatal(err)
	}
	want := []string{"meter:left ! right", "encoder:voice", "sink:memory"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
}

func TestParseBranchPipelineWithRegistryStructuredErrors(t *testing.T) {
	applyErr := errors.New("stage failed")
	encoderErr := errors.New("encoder failed")
	tests := []struct {
		name     string
		tap      string
		branch   string
		pipeline string
		registry PipelineRegistry
		code     string
		node     string
	}{
		{name: "missing branch", tap: "raw_video", pipeline: "copy", code: "missing_required", node: "branch"},
		{name: "missing tap", branch: "archive", pipeline: "copy", code: "missing_required", node: "tap"},
		{name: "unknown tap", tap: "raw_vdieo", branch: "archive", pipeline: "copy", code: "unknown_tap", node: "raw_vdieo"},
		{name: "empty pipeline", tap: "raw_video", branch: "archive", pipeline: "   ", code: "missing_required", node: "pipeline"},
		{
			name:     "nil custom apply",
			tap:      "raw_video",
			branch:   "archive",
			pipeline: "meter",
			registry: PipelineRegistry{Steps: []BranchPipelineStepSpec{{Name: "meter"}}},
			code:     "invalid_pipeline_step",
			node:     "meter",
		},
		{
			name:     "custom apply error",
			tap:      "raw_video",
			branch:   "archive",
			pipeline: "meter",
			registry: PipelineRegistry{Steps: []BranchPipelineStepSpec{{Name: "meter", Apply: func(*BranchPipeline, StepArgs) error { return applyErr }}}},
			code:     "control_failed",
		},
		{
			name:     "custom encoder error",
			tap:      "raw_video",
			branch:   "archive",
			pipeline: "badenc",
			registry: PipelineRegistry{Encoders: []EncoderSpec{{Name: "badenc", Apply: func(StepArgs) (codec.CodecSpec, error) { return codec.CodecSpec{}, encoderErr }}}},
			code:     "control_failed",
		},
		{name: "unsupported step", tap: "raw_video", branch: "archive", pipeline: "bogus", code: "unsupported_pipeline_step", node: "bogus"},
		{name: "file step alias", tap: "raw_video", branch: "archive", pipeline: "copy ! file location=out.ogg", code: "unsupported_pipeline_step", node: "file"},
		{name: "old encoder step", tap: "raw_video", branch: "archive", pipeline: "av1enc bitrate=900k", code: "unsupported_pipeline_step", node: "av1enc"},
		{name: "old codec step", tap: "raw_video", branch: "archive", pipeline: "av1 bitrate=900k", code: "unsupported_pipeline_step", node: "av1"},
		{name: "encoder alias step", tap: "raw_video", branch: "archive", pipeline: "encoder codec=av1 media=video", code: "unsupported_pipeline_step", node: "encoder"},
		{name: "encoder id alias", tap: "raw_video", branch: "archive", pipeline: "encode id=av1 media=video", code: "invalid_value", node: "id"},
		{name: "encoder type alias", tap: "raw_video", branch: "archive", pipeline: "encode codec=av1 type=video", code: "invalid_value", node: "type"},
		{name: "encoder invalid media", tap: "raw_video", branch: "archive", pipeline: "encode codec=av1 media=image", code: "invalid_value", node: "media"},
		{name: "encoder duplicate codec", tap: "raw_video", branch: "archive", pipeline: "encode codec=av1 codec=vp8 media=video", code: "invalid_value", node: "codec"},
		{name: "missing destination", tap: "raw_video", branch: "archive", pipeline: "copy", code: "missing_required", node: "filesink"},
		{name: "custom duplicate field", tap: "raw_video", branch: "archive", pipeline: "meter label=left label=right", registry: PipelineRegistry{Steps: []BranchPipelineStepSpec{{Name: "meter", Apply: func(*BranchPipeline, StepArgs) error { return nil }}}}, code: "invalid_value", node: "label"},
		{name: "file sink duplicate location", tap: "raw_video", branch: "archive", pipeline: "copy ! filesink location=one.ogg location=two.ogg", code: "invalid_value", node: "location"},
		{name: "file sink path alias", tap: "raw_video", branch: "archive", pipeline: "copy ! filesink path=out.ogg", code: "invalid_value", node: "path"},
		{name: "file sink file alias", tap: "raw_video", branch: "archive", pipeline: "copy ! filesink file=out.ogg", code: "invalid_value", node: "file"},
		{name: "file sink container alias", tap: "raw_video", branch: "archive", pipeline: "copy ! filesink location=out.ogg container=ogg", code: "invalid_value", node: "container"},
		{name: "file sink unknown field", tap: "raw_video", branch: "archive", pipeline: "copy ! filesink location=out.ogg mode=fast", code: "invalid_value", node: "mode"},
		{name: "file sink open", tap: "raw_video", branch: "archive", pipeline: "copy ! filesink location=" + filepath.Join(t.TempDir(), "missing", "out.ogg"), code: "open_destination"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseBranchPipelineWithRegistry(newFakeTask(), tt.tap, tt.branch, tt.pipeline, tt.registry)
			var structured *Error
			if !errors.As(err, &structured) || structured.Code != tt.code || (tt.node != "" && structured.Node != tt.node) {
				t.Fatalf("err = %+v, want code=%s node=%s", structured, tt.code, tt.node)
			}
		})
	}
}

func TestBranchPipelineParserHelperEdges(t *testing.T) {
	args, err := stepArgs([]string{"flag", "Key=Value"})
	if err != nil {
		t.Fatal(err)
	}
	if args["flag"] != "" || args["key"] != "Value" {
		t.Fatalf("args = %+v", args)
	}

	steps := pipelineStepMap([]BranchPipelineStepSpec{{Name: "Meter", Aliases: []string{"M", ""}}, {Aliases: []string{"aliasOnly"}}})
	if _, ok := steps["meter"]; !ok {
		t.Fatalf("steps = %+v, want meter", steps)
	}
	if _, ok := steps["m"]; !ok {
		t.Fatalf("steps = %+v, want alias m", steps)
	}
	if _, ok := steps["aliasonly"]; !ok {
		t.Fatalf("steps = %+v, want aliasonly", steps)
	}

	encoders := encoderMap([]EncoderSpec{{Name: "VendorEnc", Aliases: []string{"VEnc", ""}}, {Aliases: []string{"aliasEnc"}}})
	if _, ok := encoders["vendorenc"]; !ok {
		t.Fatalf("encoders = %+v, want vendorenc", encoders)
	}
	if _, ok := encoders["venc"]; !ok {
		t.Fatalf("encoders = %+v, want alias venc", encoders)
	}
	if _, ok := encoders["aliasenc"]; !ok {
		t.Fatalf("encoders = %+v, want aliasenc", encoders)
	}

	for _, tc := range []struct {
		id    av.CodecID
		media av.MediaType
	}{
		{id: av.CodecOpus, media: av.MediaAudio},
		{id: av.CodecVP8, media: av.MediaVideo},
		{id: av.CodecVP9, media: av.MediaVideo},
		{id: av.CodecH264, media: av.MediaVideo},
		{id: av.CodecAV1, media: av.MediaVideo},
	} {
		spec, err := parseEncoder(tc.id, tc.media, map[string]string{"bitrate": "1k"})
		if err != nil {
			t.Fatalf("%s: %v", tc.id, err)
		}
		if spec.ID != tc.id || spec.Type != tc.media {
			t.Fatalf("spec = %+v, want id=%s media=%s", spec, tc.id, tc.media)
		}
	}
	if _, err := parseEncoder("vendor_custom", "", nil); err == nil {
		t.Fatal("expected encoder without media to fail")
	}
	spec, err := parseEncoder("vendor_pcm", av.MediaAudio, map[string]string{
		"bitrate":           "128k",
		"profile":           "low-delay",
		"level":             "1",
		"sample_rate":       "16000",
		"channels":          "1",
		"clock_rate":        "16000",
		"keyframe_interval": "100",
		"fps":               "30000/1001",
		"lookahead":         "deep",
		"aq-mode":           "cyclic",
	})
	if err != nil {
		t.Fatal(err)
	}
	if spec.ID != "vendor_pcm" ||
		spec.Type != av.MediaAudio ||
		spec.Parameters.SampleRate != 16000 ||
		spec.Parameters.Channels != 1 ||
		spec.Parameters.ChannelLayout != "mono" ||
		spec.Parameters.ClockRate != 16000 ||
		spec.Settings.Bitrate != 128_000 ||
		spec.Settings.Profile != "low-delay" ||
		spec.Settings.Level != "1" ||
		spec.Settings.KeyframeInterval != 100 ||
		spec.Settings.Framerate != (av.Duration{Value: 1001, Base: av.TimeBase{Num: 1, Den: 30000}}) ||
		spec.Settings.Custom["lookahead"] != "deep" ||
		spec.Settings.Custom["aq-mode"] != "cyclic" {
		t.Fatalf("custom encoder spec = %+v", spec)
	}
	av1Spec, err := parseEncoder(av.CodecAV1, av.MediaVideo, map[string]string{
		"bitrate":         "1M",
		"min_qindex":      "20",
		"max_qindex":      "180",
		"temporal_layers": "2",
		"tune":            "zerolatency",
	})
	if err != nil {
		t.Fatal(err)
	}
	if av1Spec.Settings.Bitrate != 1_000_000 ||
		av1Spec.Settings.Custom["min_qindex"] != "20" ||
		av1Spec.Settings.Custom["max_qindex"] != "180" ||
		av1Spec.Settings.Custom["temporal_layers"] != "2" ||
		av1Spec.Settings.Custom["tune"] != "zerolatency" {
		t.Fatalf("av1 custom settings = %+v", av1Spec.Settings)
	}
	_, err = parseEncoder("vendor_pcm", "", map[string]string{"bitrate": "128k"})
	var structured *Error
	if !errors.As(err, &structured) || structured.Code != "missing_required" || structured.Node != "media" {
		t.Fatalf("custom encoder missing media error = %+v", err)
	}
	for _, tc := range []struct {
		name string
		args map[string]string
		node string
	}{
		{name: "fps", args: map[string]string{"fps": "0"}, node: "fps"},
		{name: "id", args: map[string]string{"id": "av1"}, node: "id"},
		{name: "type", args: map[string]string{"type": "video"}, node: "type"},
		{name: "clock_rate", args: map[string]string{"clock_rate": "0"}, node: "clock_rate"},
		{name: "rate", args: map[string]string{"rate": "48000"}, node: "rate"},
		{name: "bitrate_bps", args: map[string]string{"bitrate_bps": "48000"}, node: "bitrate_bps"},
		{name: "framerate", args: map[string]string{"framerate": "30"}, node: "framerate"},
		{name: "samplerate", args: map[string]string{"samplerate": "48000"}, node: "samplerate"},
		{name: "ch", args: map[string]string{"ch": "2"}, node: "ch"},
		{name: "clockrate", args: map[string]string{"clockrate": "90000"}, node: "clockrate"},
		{name: "keyint", args: map[string]string{"keyint": "60"}, node: "keyint"},
		{name: "gop", args: map[string]string{"gop": "60"}, node: "gop"},
	} {
		t.Run("invalid encoder "+tc.name, func(t *testing.T) {
			_, err := parseEncoder(av.CodecAV1, av.MediaVideo, tc.args)
			var structured *Error
			if !errors.As(err, &structured) ||
				structured.Code != "invalid_value" ||
				structured.Node != tc.node {
				t.Fatalf("err = %+v, want invalid %s", err, tc.node)
			}
		})
	}
	if err := (closeOnceWriter{}).Close(); err != nil {
		t.Fatalf("nil closeOnceWriter close = %v", err)
	}

	task := newFakeTask()
	task.taps = append(task.taps, snapshot.Tap{Name: "events", Domain: shape.DomainEvent})
	tap, err := resolveBranchTap(task, "attach", "events")
	if err != nil {
		t.Fatal(err)
	}
	if tap.Name() != "events" || tap.Domain() != "" {
		t.Fatalf("tap = %s/%s, want inferred events tap", tap.Name(), tap.Domain())
	}
}

func TestExecuteRequestAppliesAttachRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	task, err := goav.From(goavtest.Packets(av.CodecOpus, packet)).
		Audio().Copy().Tap(goav.PacketTap("pkts")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	out := filepath.Join(t.TempDir(), "direct.ogg")
	response := ExecuteRequest(ctx, task, Request{
		Op:       "attach",
		Tap:      "pkts",
		Branch:   "direct",
		Pipeline: "copy ! filesink location=" + out + " format=ogg",
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("attach response = %+v", response)
	}
	branch, ok := response.Result.(snapshot.Branch)
	if !ok || branch.Name != "direct" {
		t.Fatalf("attach result = %#v", response.Result)
	}
}

func TestExecuteAppliesAttachCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	task, err := goav.From(goavtest.Packets(av.CodecOpus, packet)).
		Audio().Copy().Tap(goav.PacketTap("pkts")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	out := filepath.Join(t.TempDir(), "direct.ogg")
	response, err := Execute(ctx, task, []string{"attach", "pkts", "as", "direct", "copy ! filesink location=" + out + " format=ogg"})
	if err != nil {
		t.Fatal(err)
	}
	branch, ok := response.Result.(snapshot.Branch)
	if response.Operation != "attach" || !ok || branch.Name != "direct" {
		t.Fatalf("attach response = %+v", response)
	}
}

func TestCanonicalControlCommandsApplySupportedVerbs(t *testing.T) {
	ctx := context.Background()
	task := newFakeTask()
	commands := [][]string{
		{"control", "keyframe", "stream=video"},
		{"control", "bitrate", "stream=video", "value=1200k"},
		{"control", "seek", "position=12.5s"},
		{"control", "rate", "value=0.5"},
		{"control", "segment", "start=10s", "end=20s"},
		{"control", "select", "active=camera_b", "selector=program"},
		{"control", "deliver", "type=vendor.force_idr", "stream=video", "at=raw_video", "reason=manual"},
	}
	for _, command := range commands {
		if _, err := Execute(ctx, task, command); err != nil {
			t.Fatalf("%v: %v", command, err)
		}
	}
	if got, want := len(task.controls), len(commands); got != want {
		t.Fatalf("controls = %d, want %d: %+v", got, want, task.controls)
	}
	ctrl := task.controls[5]
	if ctrl.Type != control.SelectType || ctrl.StreamID != "camera_b" || ctrl.Node != "program" {
		t.Fatalf("select control = %+v", ctrl)
	}
	ctrl = task.controls[6]
	if ctrl.Type != control.EventType || ctrl.Event.Metadata != nil || ctrl.Event.Reason != "manual" {
		t.Fatalf("deliver control = %+v", ctrl)
	}
}

func TestArgsFromMapPreservesTrueValues(t *testing.T) {
	args := argsFromMap(map[string]string{
		"follow":      "true",
		"metadata.ok": "true",
	})
	if !reflect.DeepEqual(args, []string{"--follow", "metadata.ok=true"}) {
		t.Fatalf("args = %v", args)
	}
}

func TestUnknownControlHelpListsAvailableCommands(t *testing.T) {
	_, err := Execute(context.Background(), newFakeTask(), []string{"control", "bogus"})
	var structured *Error
	if !errors.As(err, &structured) ||
		structured.Code != "unknown_command" ||
		!suggestionsContain(structured.Suggestions, "bitrate") {
		t.Fatalf("err = %+v", structured)
	}

	_, err = HelpWithCommands([]string{"control", "bogus"}, ControlManifest())
	if !errors.As(err, &structured) ||
		structured.Code != "unknown_command" ||
		!suggestionsContain(structured.Suggestions, "bitrate") {
		t.Fatalf("help err = %+v", structured)
	}
}

func TestServeUnixHandlesOneControlRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := newFakeTask()
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goavctl-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ServeUnix(ctx, task, "unix://"+socket)
	}()
	waitForSocket(t, socket, errC)

	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	request := Request{
		Op:    "control",
		Verb:  "deliver",
		Event: json.RawMessage(`{"type":"vendor.force_idr","stream_id":"video"}`),
		Args:  map[string]string{"at": "raw_video"},
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if !response.OK || response.Error != nil {
		t.Fatalf("response = %+v", response)
	}
	if len(task.controls) != 1 || task.controls[0].Type != control.EventType || task.controls[0].Tap != "raw_video" {
		t.Fatalf("controls = %+v", task.controls)
	}
	cancel()
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func TestServeUnixWithOptionsHandlesCustomCommand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type socketCommand struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"socket value"`
	}
	var applied string
	spec := CommandSpec{
		Name:     "vendor.socket",
		Summary:  "socket custom command",
		ArgsType: reflect.TypeOf(socketCommand{}),
		Apply: func(_ context.Context, _ goav.LiveTask, args any) (ControlResponse, error) {
			applied = args.(socketCommand).Value
			return ControlResponse{Operation: "control vendor.socket", Result: applied}, nil
		},
	}

	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goavctl-custom-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ServeUnixWithOptions(ctx, newFakeTask(), "unix://"+socket, WithCommands(spec))
	}()
	waitForSocket(t, socket, errC)

	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Op: "control", Verb: "vendor.socket", Args: map[string]string{"value": "socket-called"}}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if !response.OK || response.Error != nil || applied != "socket-called" {
		t.Fatalf("response=%+v applied=%q", response, applied)
	}
	cancel()
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func TestServerStreamsWatchFollowResponses(t *testing.T) {
	task := newFakeTask()
	task.events = []av.Event{
		{Type: av.EventStats, StreamID: "video"},
		{Type: av.EventBitrateChanged, StreamID: "video"},
	}
	server := &Server{Task: task}
	client, serverConn := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.handleConn(context.Background(), serverConn)
	}()
	request := Request{Op: "watch", Args: map[string]string{"follow": "true", "type": string(av.EventStats)}}
	if err := json.NewEncoder(client).Encode(request); err != nil {
		t.Fatal(err)
	}
	var response struct {
		OK     bool     `json:"ok"`
		Result av.Event `json:"result"`
		Error  *Error   `json:"error,omitempty"`
	}
	if err := json.NewDecoder(client).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.Error != nil || response.Result.Type != av.EventStats {
		t.Fatalf("response = %+v", response)
	}
	<-done
}

func TestServerFollowWithoutTaskReturnsStructuredError(t *testing.T) {
	server := &Server{}
	client, serverConn := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.handleConn(context.Background(), serverConn)
	}()
	request := Request{Op: "watch", Args: map[string]string{"follow": "true"}}
	if err := json.NewEncoder(client).Encode(request); err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.NewDecoder(client).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.OK || response.Error == nil || response.Error.Code != "task_missing" {
		t.Fatalf("response = %+v", response)
	}
	<-done
}

func TestServerFollowValidatesCustomConfiguration(t *testing.T) {
	server := &Server{
		Task: newFakeTask(),
		Commands: []CommandSpec{{
			Name:     "vendor.pointer",
			ArgsType: reflect.TypeOf(&struct{}{}),
		}},
	}
	client, serverConn := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.handleConn(context.Background(), serverConn)
	}()
	request := Request{Op: "watch", Args: map[string]string{"follow": "true"}}
	if err := json.NewEncoder(client).Encode(request); err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.NewDecoder(client).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.OK ||
		response.Error == nil ||
		response.Error.Code != "invalid_registry" ||
		response.Error.Node != "vendor.pointer" {
		t.Fatalf("response = %+v, want invalid registry", response)
	}
	<-done
}

func TestServerAttachRebranchDetachUsesAttachmentTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	task, err := goav.From(goavtest.Packets(av.CodecOpus, packet)).
		Audio().Copy().Tap(goav.PacketTap("pkts")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	server := &Server{Task: task}
	first := filepath.Join(t.TempDir(), "first.ogg")
	response := server.Handle(ctx, Request{
		Op:       "attach",
		Tap:      "pkts",
		Branch:   "preview",
		Pipeline: "copy ! filesink location=" + first + " format=ogg",
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("attach response = %+v", response)
	}

	second := filepath.Join(t.TempDir(), "second.ogg")
	response = server.Handle(ctx, Request{
		Op:       "rebranch",
		Branch:   "preview",
		Pipeline: "copy ! filesink location=" + second + " format=ogg",
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("rebranch response = %+v", response)
	}

	response = server.Handle(ctx, Request{Op: "detach", Branch: "preview"})
	if !response.OK || response.Error != nil {
		t.Fatalf("detach response = %+v", response)
	}
}

func TestServerSupportsCustomControlCommand(t *testing.T) {
	type vendorCommand struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"custom value"`
	}
	var applied string
	server := &Server{
		Task: newFakeTask(),
		Commands: []CommandSpec{{
			Name:     "vendor.custom",
			Summary:  "custom vendor command",
			ArgsType: reflect.TypeOf(vendorCommand{}),
			Apply: func(_ context.Context, _ goav.LiveTask, args any) (ControlResponse, error) {
				applied = args.(vendorCommand).Value
				return ControlResponse{Operation: "control vendor.custom", Result: applied}, nil
			},
		}},
	}
	response := server.Handle(context.Background(), Request{
		Op:   "control",
		Verb: "vendor.custom",
		Args: map[string]string{"value": "called"},
	})
	if !response.OK || response.Error != nil || applied != "called" {
		t.Fatalf("response=%+v applied=%q", response, applied)
	}
	help := server.Handle(context.Background(), Request{
		Op:   "help",
		Args: map[string]string{"topic": "control", "command": "vendor.custom"},
	})
	if !help.OK || !strings.Contains(help.Result.(string), "custom vendor command") {
		t.Fatalf("help = %+v", help)
	}
}

func TestTypedCapabilityHelpersBindAndReport(t *testing.T) {
	type tuneCommand struct {
		Stream av.StreamID `goavctl:"stream,required" usage:"stream=<stream-id>" help:"stream to tune"`
		Value  int         `goavctl:"value,required,rate" usage:"value=<rate>" help:"target rate"`
		At     string      `goavctl:"at" usage:"[at=<tap>]" help:"optional tap target"`
	}
	var tuneSeen tuneCommand
	command := NewCommand[tuneCommand](
		"vendor.tune",
		"typed vendor tuning control",
		func(_ context.Context, _ goav.LiveTask, args tuneCommand) (ControlResponse, error) {
			tuneSeen = args
			return ControlResponse{Operation: "control vendor.tune", Result: map[string]any{"value": args.Value}}, nil
		},
		Aliases("vtune"),
	)
	response, err := Invoke(context.Background(), newFakeTask(), command, []string{"stream=video", "value=1.2M", "at=raw_video"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Operation != "control vendor.tune" || tuneSeen.Stream != "video" || tuneSeen.Value != 1_200_000 || tuneSeen.At != "raw_video" {
		t.Fatalf("response=%+v tuneSeen=%+v", response, tuneSeen)
	}

	type thumbnailStep struct {
		Every   int    `goavctl:"every,required" usage:"every=<positive-int>" help:"keep every Nth frame"`
		Label   string `goavctl:"label" usage:"[label=<text>]" help:"diagnostic label"`
		Enabled bool   `goavctl:"enabled" usage:"[enabled=<bool>]" help:"enable the stage"`
	}
	var stepSeen thumbnailStep
	var stageCalled bool
	step := NewBranchStep[thumbnailStep](
		"thumbnail",
		"typed thumbnail sampler",
		func(branch *BranchPipeline, args thumbnailStep) error {
			stepSeen = args
			branch.Do()
			return nil
		},
		Aliases("thumb"),
	)
	err = step.Apply(&BranchPipeline{doFn: func(...pipeline.Stage) { stageCalled = true }}, StepArgs{
		"every":   "5",
		"label":   "preview",
		"enabled": "",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !stageCalled || stepSeen.Every != 5 || stepSeen.Label != "preview" || !stepSeen.Enabled {
		t.Fatalf("stageCalled=%v stepSeen=%+v", stageCalled, stepSeen)
	}
	err = step.Apply(&BranchPipeline{}, StepArgs{"label": "missing"})
	var structured *Error
	if !errors.As(err, &structured) ||
		structured.Code != "missing_required" ||
		structured.Operation != "parse branch pipeline" ||
		!detailsContain(structured.Details, "usage=thumbnail every=<positive-int>") {
		t.Fatalf("missing step error = %+v", structured)
	}

	type acmeEncoder struct {
		Bitrate   int    `goavctl:"bitrate,required,rate" usage:"bitrate=<rate>" help:"target bitrate"`
		Quality   string `goavctl:"quality" usage:"[quality=<profile>]" help:"native quality profile"`
		Lookahead string `goavctl:"lookahead" usage:"[lookahead=<mode>]" help:"native lookahead mode"`
	}
	var nativeLookahead string
	encoder := NewEncoderSpec[acmeEncoder](
		"acmeenc",
		"typed native encoder settings",
		func(args acmeEncoder) (codec.CodecSpec, error) {
			return codec.Codec("vendor_video", av.MediaVideo,
				codec.Bitrate(args.Bitrate),
				codec.Profile(args.Quality),
				codec.Control(func(any) error {
					nativeLookahead = args.Lookahead
					return nil
				}),
			), nil
		},
		Aliases("acme"),
	)
	spec, err := encoder.Apply(StepArgs{"bitrate": "250k", "quality": "preview", "lookahead": "deep"})
	if err != nil {
		t.Fatal(err)
	}
	if spec.ID != "vendor_video" || spec.Settings.Bitrate != 250_000 || spec.Settings.Profile != "preview" || spec.Settings.Control == nil {
		t.Fatalf("encoder spec = %+v", spec)
	}
	if err := spec.Settings.Control(nil); err != nil || nativeLookahead != "deep" {
		t.Fatalf("native callback err=%v lookahead=%q", err, nativeLookahead)
	}

	server := &Server{Task: &descriptorTask{
		fakeTask: newFakeTask(),
		encoders: []codec.Descriptor{{
			ID:   "vendor_video",
			Name: "Vendor runtime video",
			Type: av.MediaVideo,
		}},
		muxers: []format.Descriptor{{
			Format: "vendor_mux",
			Codecs: []av.CodecID{"vendor_video"},
		}},
	}}
	WithCapabilities(CapabilitySet{
		Commands: []CommandSpec{command},
		Pipeline: PipelineRegistry{
			Steps:    []BranchPipelineStepSpec{step},
			Encoders: []EncoderSpec{encoder},
		},
	})(server)

	reportResponse := server.Handle(context.Background(), Request{Op: "capabilities"})
	report, ok := reportResponse.Result.(CapabilityReport)
	if !reportResponse.OK || reportResponse.Error != nil || !ok {
		t.Fatalf("capabilities response = %+v", reportResponse)
	}
	if entry, ok := capabilityEntryNamed(report.Controls, "vendor.tune"); !ok ||
		!stringSliceContains(entry.Aliases, "vtune") ||
		!capabilityFieldRequired(entry.Fields, "value") {
		t.Fatalf("control capabilities = %+v", report.Controls)
	}
	if entry, ok := capabilityEntryNamed(report.CustomBranchSteps, "thumbnail"); !ok ||
		entry.Usage != "every=<positive-int> [label=<text>] [enabled=<bool>]" ||
		!capabilityFieldRequired(entry.Fields, "every") {
		t.Fatalf("step capabilities = %+v", report.CustomBranchSteps)
	}
	if entry, ok := capabilityEntryNamed(report.CustomEncoders, "acmeenc"); !ok ||
		!strings.Contains(entry.Usage, "bitrate=<rate>") {
		t.Fatalf("encoder capabilities = %+v", report.CustomEncoders)
	}
	if entry, ok := capabilityEntryNamed(report.BuiltInBranchSteps, "encode"); !ok ||
		!capabilityFieldRequired(entry.Fields, "codec") ||
		!capabilityFieldRequired(entry.Fields, "media") ||
		!capabilityFieldNamed(entry.Fields, "channel_layout") ||
		!capabilityFieldNamed(entry.Fields, "custom") {
		t.Fatalf("built-in encode capabilities = %+v", report.BuiltInBranchSteps)
	}
	if len(report.RuntimeEncoders) != 1 || report.RuntimeEncoders[0].ID != "vendor_video" ||
		len(report.RuntimeMuxers) != 1 || report.RuntimeMuxers[0].Format != "vendor_mux" {
		t.Fatalf("runtime capabilities = encoders:%+v muxers:%+v", report.RuntimeEncoders, report.RuntimeMuxers)
	}

	help := server.Handle(context.Background(), Request{Op: "help", Args: map[string]string{"topic": "attach"}})
	text, ok := help.Result.(string)
	if !help.OK || help.Error != nil || !ok ||
		!strings.Contains(text, "thumbnail every=<positive-int> [label=<text>] [enabled=<bool>]") ||
		!strings.Contains(text, "acmeenc bitrate=<rate> [quality=<profile>] [lookahead=<mode>]") {
		t.Fatalf("typed help = %+v\n%s", help, text)
	}
}

func TestServerRejectsCommandRegistryCollisions(t *testing.T) {
	type vendorCommand struct {
		Value string `goavctl:"value" usage:"value=<text>" help:"custom value"`
	}
	argType := reflect.TypeOf(vendorCommand{})
	tests := []struct {
		name     string
		commands []CommandSpec
		node     string
		first    string
		second   string
	}{
		{
			name:     "custom command shadows built-in",
			commands: []CommandSpec{{Name: "rate", ArgsType: argType}},
			node:     "rate",
			first:    "control command:rate",
			second:   "control command:rate",
		},
		{
			name: "custom alias shadows built-in",
			commands: []CommandSpec{{
				Name:     "vendor.rate",
				Aliases:  []string{"bitrate"},
				ArgsType: argType,
			}},
			node:   "bitrate",
			first:  "control command:bitrate",
			second: "control command alias:vendor.rate",
		},
		{
			name: "duplicate custom aliases",
			commands: []CommandSpec{
				{Name: "vendor.one", Aliases: []string{"vendor.tune"}, ArgsType: argType},
				{Name: "vendor.two", Aliases: []string{"vendor.tune"}, ArgsType: argType},
			},
			node:   "vendor.tune",
			first:  "control command alias:vendor.one",
			second: "control command alias:vendor.two",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCommandManifest(append(ControlManifest(), tt.commands...))
			assertRegistryError(t, err, tt.node, tt.first, tt.second)

			server := &Server{Task: newFakeTask(), Commands: tt.commands}
			response := server.Handle(context.Background(), Request{
				Op:   "help",
				Args: map[string]string{"topic": "control"},
			})
			if response.OK || response.Error == nil {
				t.Fatalf("response = %+v, want registry error", response)
			}
			assertRegistryError(t, response.Error, tt.node, tt.first, tt.second)

			socket := filepath.Join(os.TempDir(), fmt.Sprintf("goavctl-collision-%d.sock", time.Now().UnixNano()))
			t.Cleanup(func() { _ = os.Remove(socket) })
			err = ServeUnixWithOptions(context.Background(), newFakeTask(), "unix://"+socket, WithCommands(tt.commands...))
			assertRegistryError(t, err, tt.node, tt.first, tt.second)
		})
	}
}

func TestServerSupportsCustomEncoderSettings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	const customCodec = av.CodecID("vendor_audio")
	factory := &recordingEncoderFactory{descriptor: codec.Descriptor{ID: customCodec, Type: av.MediaAudio}}
	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Tap(goav.FrameTap("frames")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime(goav.WithEncoder(factory.descriptor, factory))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	var controlSeen bool
	server := &Server{
		Task: task,
		Pipeline: PipelineRegistry{
			Encoders: []EncoderSpec{{
				Name: "fancyenc",
				Apply: func(args StepArgs) (codec.CodecSpec, error) {
					bitrate, err := parseRate(args["bitrate"])
					if err != nil {
						return codec.CodecSpec{}, err
					}
					return codec.Codec(customCodec, av.MediaAudio,
						codec.Bitrate(bitrate),
						codec.Profile(args["quality"]),
						codec.Control(func(any) error {
							controlSeen = true
							return nil
						}),
					), nil
				},
			}},
		},
	}
	out := filepath.Join(t.TempDir(), "custom.ogg")
	response := server.Handle(ctx, Request{
		Op:       "attach",
		Tap:      "frames",
		Branch:   "custom",
		Pipeline: "fancyenc bitrate=123k quality=cinema ! filesink location=" + out,
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("attach response = %+v", response)
	}
	if factory.config.Settings.Bitrate != 123000 || factory.config.Settings.Profile != "cinema" || factory.config.Settings.Control == nil {
		t.Fatalf("encoder config = %+v", factory.config.Settings)
	}
	if err := factory.config.Settings.Control(nil); err != nil || !controlSeen {
		t.Fatalf("custom control callback err=%v seen=%v", err, controlSeen)
	}
}

func TestServerHelpListsCustomPipelineRegistry(t *testing.T) {
	server := &Server{
		Task: newFakeTask(),
		Pipeline: PipelineRegistry{
			Steps: []BranchPipelineStepSpec{{
				Name:    "meter",
				Aliases: []string{"levelmeter"},
				Summary: "observe samples before encoding",
				Usage:   "[window=<duration>]",
			}},
			Encoders: []EncoderSpec{{
				Name:    "acmeenc",
				Aliases: []string{"acme"},
				Summary: "ACME native audio encoder",
				Usage:   "bitrate=<bps> quality=<name> lookahead=<mode>",
			}},
		},
	}

	for _, topic := range []string{"attach", "rebranch"} {
		response := server.Handle(context.Background(), Request{
			Op:   "help",
			Args: map[string]string{"topic": topic},
		})
		text, ok := response.Result.(string)
		if !response.OK || response.Error != nil || !ok {
			t.Fatalf("%s response = %+v", topic, response)
		}
		for _, fragment := range []string{
			"Built-in steps:",
			"encode codec=<id>",
			"Custom steps:",
			"meter [window=<duration>]",
			"(aliases: levelmeter)",
			"observe samples before encoding",
			"Custom encoders:",
			"acmeenc bitrate=<bps> quality=<name> lookahead=<mode>",
			"(aliases: acme)",
			"ACME native audio encoder",
			"StepArgs",
		} {
			if !strings.Contains(text, fragment) {
				t.Fatalf("%s help missing %q:\n%s", topic, fragment, text)
			}
		}
	}
}

func TestServerOptionsAndUnknownBranchErrors(t *testing.T) {
	server := &Server{Task: newFakeTask()}
	WithPipelineRegistry(PipelineRegistry{
		Steps: []BranchPipelineStepSpec{{Name: "meter"}},
	})(server)
	if len(server.Pipeline.Steps) != 1 || server.Pipeline.Steps[0].Name != "meter" {
		t.Fatalf("pipeline registry = %+v", server.Pipeline)
	}

	response := server.Handle(context.Background(), Request{Op: "detach", Branch: "preveiw"})
	if response.OK || response.Error == nil ||
		response.Error.Code != "unknown_branch" ||
		!detailsContain(response.Error.Details, "preview") ||
		!suggestionsContain(response.Error.Suggestions, "preview") {
		t.Fatalf("response = %+v", response)
	}
	if got := firstAnchorTap(nil); got != "" {
		t.Fatalf("firstAnchorTap(nil) = %q", got)
	}
}

func TestServerGenericEncodeStepCarriesCommonCodecOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	const customCodec = av.CodecID("vendor_generic_audio")
	factory := &recordingEncoderFactory{descriptor: codec.Descriptor{ID: customCodec, Name: "Vendor generic audio", Type: av.MediaAudio}}
	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Tap(goav.FrameTap("frames")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime(goav.WithEncoder(factory.descriptor, factory))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	server := &Server{Task: task}
	help := server.Handle(ctx, Request{
		Op:   "help",
		Args: map[string]string{"topic": "attach"},
	})
	helpText, ok := help.Result.(string)
	if !help.OK || help.Error != nil || !ok {
		t.Fatalf("help response = %+v", help)
	}
	for _, fragment := range []string{
		"Runtime encoders:",
		"encode codec=vendor_generic_audio media=audio",
		"Vendor generic audio",
		"Any encoder registered on the task runtime is callable",
		"Runtime muxers:",
		"filesink location=<path> [format=ogg]",
		"Any muxer registered on the task runtime is callable",
	} {
		if !strings.Contains(helpText, fragment) {
			t.Fatalf("help missing %q:\n%s", fragment, helpText)
		}
	}

	out := filepath.Join(t.TempDir(), "generic.ogg")
	response := server.Handle(ctx, Request{
		Op:     "attach",
		Tap:    "frames",
		Branch: "generic",
		Pipeline: "encode codec=vendor_generic_audio media=audio bitrate=64k profile=voice level=1 sample_rate=16000 channels=1 clock_rate=48000 keyframe_interval=20 fps=30 lookahead=deep aq-mode=cyclic" +
			" ! filesink location=" + out + " format=ogg",
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("attach response = %+v", response)
	}
	settings := factory.config.Settings
	if settings.Bitrate != 64000 ||
		settings.Profile != "voice" ||
		settings.Level != "1" ||
		settings.SampleRate != 16000 ||
		!settings.SampleRateSet ||
		settings.Channels != 1 ||
		!settings.ChannelsSet ||
		settings.ClockRate != 48000 ||
		settings.KeyframeInterval != 20 ||
		settings.Framerate.Value != 1 ||
		settings.Framerate.Base.Den != 30 ||
		settings.Custom["lookahead"] != "deep" ||
		settings.Custom["aq-mode"] != "cyclic" {
		t.Fatalf("encoder settings = %+v", settings)
	}
}

func TestExecuteControlAgainstRealControllableTestSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	source := goavtest.NewTestSource("fixture",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48000, 1, av.SampleFormatS16)),
	)
	task, err := goav.From(source.Input()).
		Audio().Copy().
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if _, err := Execute(ctx, task, []string{"control", "rate", "value=0.5", "source=fixture"}); err != nil {
		t.Fatal(err)
	}
	event, err := source.WaitControl(ctx, av.EventRate)
	if err != nil {
		t.Fatal(err)
	}
	if rate, ok := av.EventRateValue(&event); !ok || rate != 0.5 {
		t.Fatalf("rate event = %+v, parsed=%v ok=%v", event, rate, ok)
	}

	if _, err := Execute(ctx, task, []string{"control", "seek", "position=12.5s", "source=fixture"}); err != nil {
		t.Fatal(err)
	}
	event, err = source.WaitControl(ctx, av.EventSeek)
	if err != nil {
		t.Fatal(err)
	}
	if position, ok := event.Timestamp.ToDuration(); !ok || position != 12500*time.Millisecond {
		t.Fatalf("seek event = %+v, parsed=%v ok=%v", event, position, ok)
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

	deliver, _ := LookupControlCommand("deliver")
	help = CommandHelp(deliver)
	for _, fragment := range []string{
		"control deliver",
		"goav ctl --control unix://PATH control deliver type=<event-type> [stream=<stream-id>] [reason=<text>] [at=<tap>] [metadata.<key>=<value>...]",
		"Raw event usage:",
		"goav ctl --control unix://PATH control deliver --json '<json-av-event>' at=<tap-name>",
		"metadata",
	} {
		if !strings.Contains(help, fragment) {
			t.Fatalf("deliver help missing %q:\n%s", fragment, help)
		}
	}
}

func TestHelpRendersRootStaticAndCustomControlTopics(t *testing.T) {
	root, err := Help(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"goav ctl", "capabilities", "graph [format=mermaid|dot|text]", "events [--follow]", "watch [type=<event-type>] [stream=<stream-id>] [--follow]", "rebranch <branch-name>"} {
		if !strings.Contains(root, fragment) {
			t.Fatalf("root help missing %q:\n%s", fragment, root)
		}
	}

	for topic, fragments := range map[string][]string{
		"inspect":      {"goav ctl --control unix://PATH inspect", "structural description"},
		"snapshot":     {"goav ctl --control unix://PATH snapshot", "current task snapshot"},
		"stats":        {"goav ctl --control unix://PATH stats", "latest task statistics"},
		"taps":         {"goav ctl --control unix://PATH taps", "named tap points"},
		"streams":      {"goav ctl --control unix://PATH streams", "Lists streams inferred"},
		"branches":     {"goav ctl --control unix://PATH branches", "active runtime branches"},
		"destinations": {"goav ctl --control unix://PATH destinations", "active output destinations"},
		"capabilities": {"goav ctl --control unix://PATH capabilities", "server-aware"},
		"events":       {"goav ctl --control unix://PATH events [--follow]", "buffered events"},
		"watch":        {"goav ctl --control unix://PATH watch [type=<event-type>] [stream=<stream-id>] [--follow]", "optional filters"},
		"stop":         {"goav ctl --control unix://PATH stop", "close cleanly"},
	} {
		text, err := Help([]string{topic})
		if err != nil {
			t.Fatalf("help %s: %v", topic, err)
		}
		for _, fragment := range fragments {
			if !strings.Contains(text, fragment) {
				t.Fatalf("help %s missing %q:\n%s", topic, fragment, text)
			}
		}
	}

	attach, err := Help([]string{"attach"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(attach, "Built-in steps:") || !strings.Contains(attach, "encode codec=<id>") {
		t.Fatalf("attach help:\n%s", attach)
	}

	graph, err := Help([]string{"graph"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(graph, "Mermaid") || !strings.Contains(graph, "Graphviz DOT") {
		t.Fatalf("graph help:\n%s", graph)
	}

	type vendorCommand struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"custom value"`
	}
	manifest := []CommandSpec{{
		Name:     "vendor.tune",
		Summary:  "custom tuning command",
		ArgsType: reflect.TypeOf(vendorCommand{}),
	}}
	control, err := HelpWithCommands([]string{"control"}, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(control, "vendor.tune") || !strings.Contains(control, "custom tuning command") || !strings.Contains(control, "control deliver --json '<json-av-event>' at=<tap-name>") {
		t.Fatalf("control help:\n%s", control)
	}

	specific, err := HelpWithCommands([]string{"control", "vendor.tune"}, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specific, "goav ctl --control unix://PATH control vendor.tune value=<text>") {
		t.Fatalf("specific help:\n%s", specific)
	}

	_, err = Help([]string{"missing"})
	var structured *Error
	if !errors.As(err, &structured) || structured.Code != "unknown_command" {
		t.Fatalf("err = %v, want unknown_command", err)
	}
}

func TestStructuredErrorFormattingAndUnwrap(t *testing.T) {
	cause := fmt.Errorf("root cause")
	err := commandError("bad", "op", "node", "", []string{"detail"}, []string{"suggestion"}, cause)
	if err.Error() == "" ||
		!strings.Contains(err.Error(), "bad") ||
		!strings.Contains(err.Error(), "detail") ||
		!strings.Contains(err.Error(), "suggestion") {
		t.Fatalf("error text = %q", err.Error())
	}
	if !errors.Is(err, cause) {
		t.Fatalf("unwrap failed for %+v", err)
	}
	var nilErr *Error
	if nilErr.Error() != "" || nilErr.Unwrap() != nil {
		t.Fatalf("nil error behavior changed")
	}
}

func TestStructuredErrorPreservesUnderlyingShapes(t *testing.T) {
	if got := structuredError("fallback", nil); got != nil {
		t.Fatalf("structuredError(nil) = %+v", got)
	}

	existing := commandError("already_structured", "op", "node", "message", nil, nil, nil)
	if got := structuredError("fallback", existing); got != existing {
		t.Fatalf("structuredError(existing) = %+v, want same pointer", got)
	}

	cause := errors.New("sentinel")
	buildErr := &goav.BuildError{
		Code:        errcode.RuntimeBranchTapMissing,
		Operation:   "attach runtime branch",
		Node:        "raw_vdieo",
		Reason:      "unknown tap",
		Details:     []string{"available_taps=raw_video"},
		Suggestions: []string{"use at=raw_video"},
		Cause:       cause,
	}
	wrapped := structuredError("fallback", buildErr)
	if wrapped.Code != string(errcode.RuntimeBranchTapMissing) ||
		wrapped.Operation != "attach runtime branch" ||
		wrapped.Node != "raw_vdieo" ||
		wrapped.Message != "unknown tap" ||
		!detailsContain(wrapped.Details, "raw_video") ||
		!suggestionsContain(wrapped.Suggestions, "raw_video") ||
		!errors.Is(wrapped, cause) {
		t.Fatalf("wrapped = %+v", wrapped)
	}

	genericCause := errors.New("plain failure")
	generic := structuredError("control keyframe", genericCause)
	if generic.Code != "control_failed" ||
		generic.Operation != "control keyframe" ||
		generic.Message != "plain failure" ||
		!errors.Is(generic, genericCause) {
		t.Fatalf("generic = %+v", generic)
	}
}

func TestReflectionConfinedToColdPathBinders(t *testing.T) {
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
		if strings.Contains(slash, "internal/launchctl/") || strings.Contains(slash, "internal/argbind/") {
			return nil
		}
		if strings.Contains(slash, "/examples/") || strings.HasPrefix(slash, "../../examples/") {
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
		Apply: func(_ context.Context, _ goav.LiveTask, args any) (ControlResponse, error) {
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

func assertRegistryError(t *testing.T, err error, node string, first string, second string) {
	t.Helper()
	var structured *Error
	if !errors.As(err, &structured) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if structured.Code != "invalid_registry" || structured.Node != node {
		t.Fatalf("err = %+v, want invalid_registry node=%s", structured, node)
	}
	if !detailsContain(structured.Details, "first="+first) || !detailsContain(structured.Details, "second="+second) {
		t.Fatalf("details = %v, want first=%q second=%q", structured.Details, first, second)
	}
	if !suggestionsContain(structured.Suggestions, "unique custom") {
		t.Fatalf("suggestions = %v, want unique-name guidance", structured.Suggestions)
	}
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func capabilityEntryNamed(entries []CapabilityEntry, name string) (CapabilityEntry, bool) {
	for _, entry := range entries {
		if entry.Name == name {
			return entry, true
		}
	}
	return CapabilityEntry{}, false
}

func capabilityFieldRequired(fields []CapabilityField, name string) bool {
	for _, field := range fields {
		if field.Name == name {
			return field.Required
		}
	}
	return false
}

func capabilityFieldNamed(fields []CapabilityField, name string) bool {
	for _, field := range fields {
		if field.Name == name {
			return true
		}
	}
	return false
}

func waitForSocket(t *testing.T, socket string, errC <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.Dial("unix", socket)
		if err == nil {
			_ = conn.Close()
			return
		}
		select {
		case err := <-errC:
			t.Fatalf("server stopped before creating socket: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %s was not created", socket)
		}
		time.Sleep(time.Millisecond)
	}
}

type fakeTask struct {
	controls   []control.Control
	controlErr error
	taps       []snapshot.Tap
	spec       pipeline.Spec
	snapshot   snapshot.Task
	events     []av.Event
	closed     bool
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
			{Name: "program", Kind: pipeline.NodeStage},
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

func (t *fakeTask) Detach(context.Context, goav.Attachment, ...goav.DetachOption) error {
	return nil
}

func (t *fakeTask) Taps() []snapshot.Tap { return append([]snapshot.Tap(nil), t.taps...) }

func (t *fakeTask) Snapshot() snapshot.Task { return t.snapshot }

func (t *fakeTask) Control(_ context.Context, ctrl control.Control) error {
	if t.controlErr != nil {
		return t.controlErr
	}
	t.controls = append(t.controls, ctrl)
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

func (t *fakeTask) Watch(filters ...inspect.EventFilter) <-chan av.Event {
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

type descriptorTask struct {
	*fakeTask
	encoders []codec.Descriptor
	muxers   []format.Descriptor
}

func (t *descriptorTask) EncoderDescriptors() []codec.Descriptor {
	return append([]codec.Descriptor(nil), t.encoders...)
}

func (t *descriptorTask) MuxerDescriptors() []format.Descriptor {
	return append([]format.Descriptor(nil), t.muxers...)
}

func eventMatches(event av.Event, filters []inspect.EventFilter) bool {
	for _, filter := range filters {
		if filter != nil && !filter(event) {
			return false
		}
	}
	return true
}

type recordingEncoderFactory struct {
	descriptor codec.Descriptor
	config     codec.EncodeConfig
}

func (f *recordingEncoderFactory) NewEncoder(_ context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	f.config = config
	return recordingEncoder{descriptor: f.descriptor}, nil
}

type recordingEncoder struct {
	descriptor codec.Descriptor
}

func (e recordingEncoder) Descriptor() codec.Descriptor { return e.descriptor }
func (e recordingEncoder) Open(context.Context, codec.EncodeConfig) error {
	return nil
}
func (e recordingEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if frame == nil || len(out.Packets) == cap(out.Packets) {
		return nil
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	out.Packets[index].Reset()
	out.Packets[index].StreamID = frame.StreamID
	out.Packets[index].Type = frame.Type
	out.Packets[index].Payload = av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}
	return nil
}
func (e recordingEncoder) FlushInto(context.Context, *codec.EncodeResult) error { return nil }
func (e recordingEncoder) HandleEvent(context.Context, *av.Event) error         { return nil }
func (e recordingEncoder) Close() error                                         { return nil }

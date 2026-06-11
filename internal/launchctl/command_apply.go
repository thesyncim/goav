package launchctl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/snapshot"
)

// Error is the JSON-safe control-plane refusal shape. It mirrors goav's
// BuildError vocabulary where possible while also covering CLI parse failures.
type Error struct {
	Code        string   `json:"code,omitempty"`
	Message     string   `json:"message,omitempty"`
	Operation   string   `json:"operation,omitempty"`
	Node        string   `json:"node,omitempty"`
	Details     []string `json:"details,omitempty"`
	Suggestions []string `json:"suggestions,omitempty"`
	Cause       error    `json:"-"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	var out strings.Builder
	if e.Message != "" {
		out.WriteString(e.Message)
	} else if e.Code != "" {
		out.WriteString(e.Code)
	} else {
		out.WriteString("control command failed")
	}
	if len(e.Details) != 0 {
		out.WriteString("\nDetails:")
		for _, detail := range e.Details {
			out.WriteString("\n  - ")
			out.WriteString(detail)
		}
	}
	if len(e.Suggestions) != 0 {
		out.WriteString("\nSuggestions:")
		for _, suggestion := range e.Suggestions {
			out.WriteString("\n  - ")
			out.WriteString(suggestion)
		}
	}
	return out.String()
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func commandError(code, operation, node, message string, details, suggestions []string, cause error) *Error {
	return &Error{
		Code:        code,
		Operation:   operation,
		Node:        node,
		Message:     message,
		Details:     cloneStrings(details),
		Suggestions: cloneStrings(suggestions),
		Cause:       cause,
	}
}

func structuredError(operation string, err error) *Error {
	if err == nil {
		return nil
	}
	var ctlErr *Error
	if errors.As(err, &ctlErr) {
		return ctlErr
	}
	var buildErr *goav.BuildError
	if errors.As(err, &buildErr) {
		return commandError(
			string(buildErr.Code),
			firstNonEmpty(buildErr.Operation, operation),
			buildErr.Node,
			buildErr.Reason,
			buildErr.Details,
			buildErr.Suggestions,
			buildErr.Cause,
		)
	}
	return commandError("control_failed", operation, "", err.Error(), nil, nil, err)
}

// KeyframeCommand is the args struct for control keyframe.
type KeyframeCommand struct {
	Stream av.StreamID `goavctl:"stream,required" usage:"stream=<stream-id>" help:"stream id, for example video"`
	At     string      `goavctl:"at" usage:"[at=<tap>]" help:"tap name reported by goav ctl taps"`
}

// BitrateCommand is the args struct for control bitrate.
type BitrateCommand struct {
	Stream av.StreamID `goavctl:"stream,required" usage:"stream=<stream-id>" help:"stream id, for example video"`
	Value  int         `goavctl:"value,required,rate" usage:"value=<rate>" help:"bits per second, accepts 1200k, 2M, or integer"`
	At     string      `goavctl:"at" usage:"[at=<tap>]" help:"tap name reported by goav ctl taps"`
}

// SeekCommand is the args struct for control seek.
type SeekCommand struct {
	Position time.Duration `goavctl:"position,required,duration" usage:"position=<duration>" help:"media position, for example 12.5s"`
	Source   string        `goavctl:"source" usage:"[source=<source-name>]" help:"source node name reported by inspect"`
	Node     string        `goavctl:"node" usage:"[node=<node-name>]" help:"expert graph node name reported by inspect"`
}

// RateCommand is the args struct for control rate.
type RateCommand struct {
	Value  float64 `goavctl:"value,required" usage:"value=<float>" help:"positive playback rate, for example 0.5 or 2"`
	Source string  `goavctl:"source" usage:"[source=<source-name>]" help:"source node name reported by inspect"`
	Node   string  `goavctl:"node" usage:"[node=<node-name>]" help:"expert graph node name reported by inspect"`
}

// SegmentCommand is the args struct for control segment.
type SegmentCommand struct {
	Start  time.Duration `goavctl:"start,required,duration" usage:"start=<duration>" help:"inclusive segment start, for example 10s"`
	End    time.Duration `goavctl:"end,required,duration" usage:"end=<duration>" help:"exclusive segment end, for example 20s"`
	Source string        `goavctl:"source" usage:"[source=<source-name>]" help:"source node name reported by inspect"`
	Node   string        `goavctl:"node" usage:"[node=<node-name>]" help:"expert graph node name reported by inspect"`
}

// SelectCommand is the args struct for control select.
type SelectCommand struct {
	Active   av.StreamID `goavctl:"active,required" usage:"active=<arm-or-stream-id>" help:"arm or stream id to make active"`
	Selector string      `goavctl:"selector" usage:"[selector=<selector-name>]" help:"selector node/name reported by inspect"`
	At       string      `goavctl:"at" usage:"[at=<tap>]" help:"tap name reported by goav ctl taps"`
}

// DeliverCommand is the args struct for control deliver.
type DeliverCommand struct {
	Type     av.EventType `goavctl:"type,required" usage:"type=<event-type>" help:"event type, for example vendor.force_idr"`
	Stream   av.StreamID  `goavctl:"stream" usage:"[stream=<stream-id>]" help:"stream id to carry on the delivered event"`
	Reason   string       `goavctl:"reason" usage:"[reason=<text>]" help:"human reason carried on the event"`
	At       string       `goavctl:"at" usage:"[at=<tap>]" help:"tap name reported by goav ctl taps"`
	Metadata av.Metadata  `goavctl:"metadata" usage:"[metadata.<key>=<value>...]" help:"string metadata entries to carry on the event"`
}

func applyKeyframe(ctx context.Context, task goav.Task, args any) (ControlResponse, error) {
	cmd := args.(KeyframeCommand)
	if err := ensureTap(task, "control keyframe", cmd.At); err != nil {
		return ControlResponse{}, err
	}
	ctrl := goav.Keyframe(cmd.Stream)
	if cmd.At != "" {
		ctrl = ctrl.AtTap(cmd.At)
	}
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control keyframe", err)
	}
	return ControlResponse{Operation: "control keyframe", Result: map[string]any{"stream": cmd.Stream, "at": cmd.At}}, nil
}

func applyBitrate(ctx context.Context, task goav.Task, args any) (ControlResponse, error) {
	cmd := args.(BitrateCommand)
	if err := ensureTap(task, "control bitrate", cmd.At); err != nil {
		return ControlResponse{}, err
	}
	ctrl := goav.SetBitrate(cmd.Stream, cmd.Value)
	if cmd.At != "" {
		ctrl = ctrl.AtTap(cmd.At)
	}
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control bitrate", err)
	}
	return ControlResponse{Operation: "control bitrate", Result: map[string]any{"stream": cmd.Stream, "value": cmd.Value, "at": cmd.At}}, nil
}

func applySeek(ctx context.Context, task goav.Task, args any) (ControlResponse, error) {
	cmd := args.(SeekCommand)
	ctrl := goav.Seek(cmd.Position)
	var err error
	ctrl, err = applySourceOrNodeTarget(task, "control seek", ctrl, cmd.Source, cmd.Node)
	if err != nil {
		return ControlResponse{}, err
	}
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control seek", err)
	}
	return ControlResponse{Operation: "control seek", Result: map[string]any{"position": cmd.Position.String()}}, nil
}

func applyRate(ctx context.Context, task goav.Task, args any) (ControlResponse, error) {
	cmd := args.(RateCommand)
	ctrl := goav.Rate(cmd.Value)
	var err error
	ctrl, err = applySourceOrNodeTarget(task, "control rate", ctrl, cmd.Source, cmd.Node)
	if err != nil {
		return ControlResponse{}, err
	}
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control rate", err)
	}
	return ControlResponse{Operation: "control rate", Result: map[string]any{"value": cmd.Value}}, nil
}

func applySegment(ctx context.Context, task goav.Task, args any) (ControlResponse, error) {
	cmd := args.(SegmentCommand)
	ctrl := goav.Segment(cmd.Start, cmd.End)
	var err error
	ctrl, err = applySourceOrNodeTarget(task, "control segment", ctrl, cmd.Source, cmd.Node)
	if err != nil {
		return ControlResponse{}, err
	}
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control segment", err)
	}
	return ControlResponse{Operation: "control segment", Result: map[string]any{"start": cmd.Start.String(), "end": cmd.End.String()}}, nil
}

func applySelect(ctx context.Context, task goav.Task, args any) (ControlResponse, error) {
	cmd := args.(SelectCommand)
	if cmd.Selector != "" && cmd.At != "" {
		return ControlResponse{}, commandError(
			"target_conflict",
			"control select",
			cmd.Selector+","+cmd.At,
			"selector and at are mutually exclusive",
			nil,
			[]string{"use selector=<selector-name> or at=<tap-name>, not both"},
			nil,
		)
	}
	ctrl := goav.SelectActive(cmd.Active)
	if cmd.Selector != "" {
		if err := ensureNode(task, "control select", cmd.Selector); err != nil {
			return ControlResponse{}, err
		}
		ctrl = ctrl.At(pipeline.NodeRef(cmd.Selector))
	} else if cmd.At != "" {
		if err := ensureTap(task, "control select", cmd.At); err != nil {
			return ControlResponse{}, err
		}
		ctrl = ctrl.AtTap(cmd.At)
	}
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control select", err)
	}
	return ControlResponse{Operation: "control select", Result: map[string]any{"active": cmd.Active, "selector": cmd.Selector, "at": cmd.At}}, nil
}

func applyDeliver(ctx context.Context, task goav.Task, args any) (ControlResponse, error) {
	cmd := args.(DeliverCommand)
	if cmd.At == "" {
		return ControlResponse{}, commandError(
			"missing_target",
			"control deliver",
			"",
			"deliver controls need a tap target in the current goav API",
			nil,
			[]string{"add at=<tap-name>", "run `goav ctl taps`"},
			nil,
		)
	}
	if err := ensureTap(task, "control deliver", cmd.At); err != nil {
		return ControlResponse{}, err
	}
	event := av.Event{Type: cmd.Type, StreamID: cmd.Stream, Reason: cmd.Reason, Metadata: cloneMetadata(cmd.Metadata)}
	ctrl := goav.Deliver(event).AtTap(cmd.At)
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control deliver", err)
	}
	return ControlResponse{Operation: "control deliver", Result: map[string]any{"type": cmd.Type, "stream": cmd.Stream, "at": cmd.At}}, nil
}

// Invoke binds args for one allowlisted spec and applies it.
func Invoke(ctx context.Context, task goav.Task, spec CommandSpec, args []string) (ControlResponse, error) {
	bound, err := BindArgs(spec, args)
	if err != nil {
		return ControlResponse{}, err
	}
	if spec.Apply == nil {
		return ControlResponse{}, commandError("unsupported", "control "+spec.Name, "", "command has no apply function", nil, nil, nil)
	}
	return spec.Apply(ctx, task, bound)
}

func executeRawControl(ctx context.Context, task goav.Task, data []byte) (ControlResponse, error) {
	ctrl, err := DecodeRawControl(data)
	if err != nil {
		return ControlResponse{}, err
	}
	if err := ensureTap(task, "control --json", ctrl.Tap); err != nil {
		return ControlResponse{}, err
	}
	if ctrl.Type == goav.ControlEvent && ctrl.Tap == "" && ctrl.Node == "" {
		return ControlResponse{}, commandError(
			"missing_target",
			"control --json",
			"",
			"raw event controls need tap or node target in the current goav API",
			nil,
			[]string{"include \"tap\":\"<tap-name>\" in the JSON", "use `goav ctl control deliver --json ... at=<tap-name>`"},
			nil,
		)
	}
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control --json", err)
	}
	return ControlResponse{Operation: "control --json", Result: map[string]any{"type": ctrl.Type, "stream": ctrl.StreamID, "tap": ctrl.Tap}}, nil
}

func executeRawEvent(ctx context.Context, task goav.Task, data []byte, args []string) (ControlResponse, error) {
	event, err := DecodeRawEvent(data)
	if err != nil {
		return ControlResponse{}, err
	}
	spec, _ := LookupControlCommand("deliver")
	bound, err := BindArgs(spec, append([]string{"type=" + string(event.Type), "stream=" + string(event.StreamID), "reason=" + event.Reason}, args...))
	if err != nil {
		return ControlResponse{}, err
	}
	cmd := bound.(DeliverCommand)
	cmd.Type = event.Type
	cmd.Stream = event.StreamID
	cmd.Reason = event.Reason
	cmd.Metadata = event.Metadata
	return applyDeliver(ctx, task, cmd)
}

// DecodeRawControl decodes the explicit raw JSON fallback into the real
// goav.Control shape. It does not introduce a second control model.
func DecodeRawControl(data []byte) (goav.Control, error) {
	obj, err := decodeObject(data)
	if err != nil {
		return goav.Control{}, commandError("invalid_json", "control --json", "", err.Error(), nil, nil, err)
	}
	typ := goav.ControlType(fieldString(obj, "type"))
	if typ == "" {
		return goav.Control{}, commandError("missing_required", "control --json", "type", "raw control JSON needs type", nil, []string{`include "type":"bitrate"`}, nil)
	}
	var ctrl goav.Control
	switch typ {
	case goav.ControlKeyframe:
		stream := av.StreamID(firstNonEmpty(fieldString(obj, "stream_id"), fieldString(obj, "stream")))
		ctrl = goav.Keyframe(stream)
	case goav.ControlBitrate:
		stream := av.StreamID(firstNonEmpty(fieldString(obj, "stream_id"), fieldString(obj, "stream")))
		bitrate, ok := fieldInt(obj, "bitrate")
		if !ok {
			bitrate, ok = fieldInt(obj, "value")
		}
		if !ok {
			return goav.Control{}, commandError("missing_required", "control --json", "bitrate", "raw bitrate control needs bitrate", nil, []string{`include "bitrate":1200000`}, nil)
		}
		ctrl = goav.SetBitrate(stream, bitrate)
	case goav.ControlSeek:
		position, ok, err := fieldDuration(obj, "position")
		if err != nil {
			return goav.Control{}, err
		}
		if !ok {
			return goav.Control{}, commandError("missing_required", "control --json", "position", "raw seek control needs position", nil, []string{`include "position":"12.5s"`}, nil)
		}
		ctrl = goav.Seek(position)
	case goav.ControlRate:
		rate, ok := fieldFloat(obj, "rate")
		if !ok {
			rate, ok = fieldFloat(obj, "value")
		}
		if !ok {
			return goav.Control{}, commandError("missing_required", "control --json", "rate", "raw rate control needs rate", nil, []string{`include "rate":0.5`}, nil)
		}
		ctrl = goav.Rate(rate)
	case goav.ControlSegment:
		start, ok, err := fieldDuration(obj, "start")
		if err != nil {
			return goav.Control{}, err
		}
		if !ok {
			start, ok, err = fieldDuration(obj, "position")
			if err != nil {
				return goav.Control{}, err
			}
		}
		end, endOK, err := fieldDuration(obj, "end")
		if err != nil {
			return goav.Control{}, err
		}
		if !ok || !endOK {
			return goav.Control{}, commandError("missing_required", "control --json", "segment", "raw segment control needs start and end", nil, []string{`include "start":"10s","end":"20s"`}, nil)
		}
		ctrl = goav.Segment(start, end)
	case goav.ControlSelect:
		active := av.StreamID(firstNonEmpty(fieldString(obj, "active"), fieldString(obj, "stream_id"), fieldString(obj, "stream")))
		ctrl = goav.SelectActive(active)
	case goav.ControlEvent, "deliver":
		raw, ok := obj["event"]
		if !ok {
			return goav.Control{}, commandError("missing_required", "control --json", "event", "raw event control needs event object", nil, []string{`use goav ctl control deliver --json '{"type":"vendor.force_idr"}' at=<tap>`}, nil)
		}
		eventBytes, err := json.Marshal(raw)
		if err != nil {
			return goav.Control{}, commandError("invalid_json", "control --json", "event", err.Error(), nil, nil, err)
		}
		event, err := DecodeRawEvent(eventBytes)
		if err != nil {
			return goav.Control{}, err
		}
		ctrl = goav.Deliver(event)
	default:
		return goav.Control{}, commandError("invalid_value", "control --json", "type", fmt.Sprintf("unknown raw control type %q", typ), nil, []string{"use one of: " + strings.Join(controlCommandNames(), ", ")}, nil)
	}
	ctrl.Reason = firstNonEmpty(fieldString(obj, "reason"), ctrl.Reason)
	if tap := fieldString(obj, "tap"); tap != "" {
		ctrl = ctrl.AtTap(tap)
	}
	if node := fieldString(obj, "node"); node != "" {
		ctrl = ctrl.At(pipeline.NodeRef(node))
	}
	return ctrl, nil
}

// DecodeRawEvent decodes raw event JSON and rejects metadata values that cannot
// be represented by av.Metadata without a lossy conversion.
func DecodeRawEvent(data []byte) (av.Event, error) {
	obj, err := decodeObject(data)
	if err != nil {
		return av.Event{}, commandError("invalid_json", "control deliver --json", "", err.Error(), nil, nil, err)
	}
	typ := av.EventType(fieldString(obj, "type"))
	if typ == "" {
		return av.Event{}, commandError("missing_required", "control deliver --json", "type", "raw event JSON needs type", nil, []string{`include "type":"vendor.force_idr"`}, nil)
	}
	event := av.Event{
		Type:     typ,
		StreamID: av.StreamID(firstNonEmpty(fieldString(obj, "stream_id"), fieldString(obj, "stream"))),
		Reason:   fieldString(obj, "reason"),
	}
	if rawMetadata, ok := obj["metadata"]; ok {
		metadata, err := metadataFromJSON(rawMetadata)
		if err != nil {
			return av.Event{}, err
		}
		event.Metadata = metadata
	}
	return event, nil
}

func decodeObject(data []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var obj map[string]any
	if err := decoder.Decode(&obj); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("raw JSON must contain one object")
	}
	if obj == nil {
		return nil, fmt.Errorf("raw JSON must contain an object")
	}
	return obj, nil
}

func fieldString(obj map[string]any, name string) string {
	value, ok := obj[name]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case bool:
		return strconv.FormatBool(typed)
	default:
		return ""
	}
}

func fieldInt(obj map[string]any, name string) (int, bool) {
	value, ok := obj[name]
	if !ok || value == nil {
		return 0, false
	}
	switch typed := value.(type) {
	case json.Number:
		i, err := strconv.Atoi(typed.String())
		return i, err == nil
	case float64:
		return int(typed), typed == float64(int(typed))
	case string:
		i, err := parseRate(typed)
		return i, err == nil
	default:
		return 0, false
	}
}

func fieldFloat(obj map[string]any, name string) (float64, bool) {
	value, ok := obj[name]
	if !ok || value == nil {
		return 0, false
	}
	switch typed := value.(type) {
	case json.Number:
		f, err := strconv.ParseFloat(typed.String(), 64)
		return f, err == nil
	case float64:
		return typed, true
	case string:
		f, err := strconv.ParseFloat(typed, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func fieldDuration(obj map[string]any, name string) (time.Duration, bool, error) {
	value, ok := obj[name]
	if !ok || value == nil {
		return 0, false, nil
	}
	switch typed := value.(type) {
	case string:
		duration, err := time.ParseDuration(typed)
		if err != nil {
			return 0, true, commandError("invalid_value", "control --json", name, fmt.Sprintf("cannot parse %s: expected duration like 12.5s or 1m30s", name), nil, []string{"use 12.5s", "use 1m30s"}, err)
		}
		return duration, true, nil
	case json.Number:
		i, err := strconv.ParseInt(typed.String(), 10, 64)
		if err != nil {
			return 0, true, commandError("invalid_value", "control --json", name, fmt.Sprintf("cannot parse %s: expected duration string or integer nanoseconds", name), nil, []string{"use \"12.5s\""}, err)
		}
		return time.Duration(i), true, nil
	default:
		return 0, true, commandError("invalid_value", "control --json", name, fmt.Sprintf("cannot parse %s: expected duration string or integer nanoseconds", name), nil, []string{"use \"12.5s\""}, nil)
	}
}

func metadataFromJSON(raw any) (av.Metadata, error) {
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, commandError("invalid_value", "control deliver --json", "metadata", "metadata must be a JSON object", nil, []string{`use "metadata":{"source":"cli"}`}, nil)
	}
	metadata := make(av.Metadata, len(obj))
	for key, value := range obj {
		text, ok := metadataScalarString(value)
		if !ok {
			return nil, commandError(
				"invalid_value",
				"control deliver --json",
				"metadata."+key,
				"metadata values must be string, number, bool, or null",
				nil,
				[]string{"convert nested metadata to a string before sending it"},
				nil,
			)
		}
		metadata[key] = text
	}
	return metadata, nil
}

func metadataScalarString(value any) (string, bool) {
	switch typed := value.(type) {
	case nil:
		return "", true
	case string:
		return typed, true
	case json.Number:
		return typed.String(), true
	case bool:
		return strconv.FormatBool(typed), true
	default:
		return "", false
	}
}

func applySourceOrNodeTarget(task goav.Task, operation string, ctrl goav.Control, source string, node string) (goav.Control, error) {
	if source != "" && node != "" {
		return goav.Control{}, commandError(
			"target_conflict",
			operation,
			source+","+node,
			"source and node are mutually exclusive",
			nil,
			[]string{"use source=<source-name> or node=<node-name>, not both"},
			nil,
		)
	}
	if source != "" {
		if err := ensureNodeKind(task, operation, source, pipeline.NodeSource); err != nil {
			return goav.Control{}, err
		}
		return ctrl.At(pipeline.NodeRef(source)), nil
	}
	if node != "" {
		if err := ensureNode(task, operation, node); err != nil {
			return goav.Control{}, err
		}
		return ctrl.At(pipeline.NodeRef(node)), nil
	}
	return ctrl, nil
}

func ensureTap(task goav.Task, operation string, tap string) error {
	if tap == "" {
		return nil
	}
	taps := task.Taps()
	for _, candidate := range taps {
		if candidate.Name == tap {
			return nil
		}
	}
	available := tapNames(taps)
	suggestions := []string{"run `goav ctl taps`"}
	if nearest := closest(tap, available); nearest != "" {
		suggestions = append([]string{"use at=" + nearest}, suggestions...)
	}
	return commandError(
		"unknown_tap",
		operation,
		tap,
		fmt.Sprintf("unknown tap %q", tap),
		[]string{"available_taps=" + strings.Join(available, ",")},
		suggestions,
		pipeline.ErrUnknownNode,
	)
}

func ensureNode(task goav.Task, operation string, node string) error {
	spec := task.Describe()
	var names []string
	for _, candidate := range spec.Nodes {
		names = append(names, candidate.Name)
		if candidate.Name == node {
			return nil
		}
	}
	suggestions := []string{"run `goav ctl inspect`"}
	if nearest := closest(node, names); nearest != "" {
		suggestions = append([]string{"use node=" + nearest}, suggestions...)
	}
	return commandError(
		"unknown_node",
		operation,
		node,
		fmt.Sprintf("unknown node %q", node),
		[]string{"available_nodes=" + strings.Join(names, ",")},
		suggestions,
		pipeline.ErrUnknownNode,
	)
}

func ensureNodeKind(task goav.Task, operation string, node string, kind pipeline.NodeKind) error {
	spec := task.Describe()
	var names []string
	for _, candidate := range spec.Nodes {
		if candidate.Kind == kind {
			names = append(names, candidate.Name)
		}
		if candidate.Name == node {
			if candidate.Kind == kind {
				return nil
			}
			return commandError(
				"wrong_target_kind",
				operation,
				node,
				fmt.Sprintf("node %q is %s, expected %s", node, candidate.Kind, kind),
				nil,
				[]string{"run `goav ctl inspect` and choose a " + string(kind) + " node"},
				nil,
			)
		}
	}
	suggestions := []string{"run `goav ctl inspect`"}
	if nearest := closest(node, names); nearest != "" {
		suggestions = append([]string{"use source=" + nearest}, suggestions...)
	}
	return commandError(
		"unknown_node",
		operation,
		node,
		fmt.Sprintf("unknown source %q", node),
		[]string{"available_sources=" + strings.Join(names, ",")},
		suggestions,
		pipeline.ErrUnknownNode,
	)
}

func tapNames(taps []snapshot.Tap) []string {
	names := make([]string, 0, len(taps))
	for _, tap := range taps {
		names = append(names, tap.Name)
	}
	sort.Strings(names)
	return names
}

func branchNames(branches []snapshot.Branch) []string {
	names := make([]string, 0, len(branches))
	for _, branch := range branches {
		if branch.Name != "" {
			names = append(names, branch.Name)
		}
	}
	sort.Strings(names)
	return names
}

func cloneMetadata(metadata av.Metadata) av.Metadata {
	if len(metadata) == 0 {
		return nil
	}
	out := make(av.Metadata, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return append([]string(nil), values...)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

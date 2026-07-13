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

	"github.com/thesyncim/goav/control"

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

// NewError builds a structured control-plane refusal. Host-owned custom
// commands, branch steps, and encoders can return this from Apply to preserve
// field-level codes, details, suggestions, and causes over the socket.
func NewError(code, operation, node, message string, details, suggestions []string, cause error) *Error {
	return commandError(code, operation, node, message, details, suggestions, cause)
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
			buildErr.DetailLines(),
			buildErr.FixLines(),
			errors.Unwrap(buildErr),
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

// RateCommand is the args struct for control rate. Rate is task-wide — it
// scales the task's shared timeline — so it takes no source or node target.
type RateCommand struct {
	Value float64 `goavctl:"value,required" usage:"value=<float>" help:"positive playback rate, for example 0.5 or 2"`
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

func applyKeyframe(ctx context.Context, task goav.LiveTask, args any) (ControlResponse, error) {
	cmd := args.(KeyframeCommand)
	if err := ensureTap(task, "control keyframe", cmd.At); err != nil {
		return ControlResponse{}, err
	}
	ctrl := control.Keyframe(cmd.Stream)
	if cmd.At != "" {
		ctrl = ctrl.AtTap(cmd.At)
	}
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control keyframe", err)
	}
	return ControlResponse{Operation: "control keyframe", Result: map[string]any{"stream": cmd.Stream, "at": cmd.At}}, nil
}

func applyBitrate(ctx context.Context, task goav.LiveTask, args any) (ControlResponse, error) {
	cmd := args.(BitrateCommand)
	if err := ensureTap(task, "control bitrate", cmd.At); err != nil {
		return ControlResponse{}, err
	}
	ctrl, err := control.SetBitrate(cmd.Stream, cmd.Value)
	if err != nil {
		return ControlResponse{}, structuredError("control bitrate", err)
	}
	if cmd.At != "" {
		ctrl = ctrl.AtTap(cmd.At)
	}
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control bitrate", err)
	}
	return ControlResponse{Operation: "control bitrate", Result: map[string]any{"stream": cmd.Stream, "value": cmd.Value, "at": cmd.At}}, nil
}

func applySeek(ctx context.Context, task goav.LiveTask, args any) (ControlResponse, error) {
	cmd := args.(SeekCommand)
	ctrl := control.Seek(cmd.Position)
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

func applyRate(ctx context.Context, task goav.LiveTask, args any) (ControlResponse, error) {
	cmd := args.(RateCommand)
	ctrl, err := control.Rate(cmd.Value)
	if err != nil {
		return ControlResponse{}, structuredError("control rate", err)
	}
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control rate", err)
	}
	return ControlResponse{Operation: "control rate", Result: map[string]any{"value": cmd.Value}}, nil
}

func applySegment(ctx context.Context, task goav.LiveTask, args any) (ControlResponse, error) {
	cmd := args.(SegmentCommand)
	ctrl, err := control.Segment(cmd.Start, cmd.End)
	if err != nil {
		return ControlResponse{}, structuredError("control segment", err)
	}
	ctrl, err = applySourceOrNodeTarget(task, "control segment", ctrl, cmd.Source, cmd.Node)
	if err != nil {
		return ControlResponse{}, err
	}
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control segment", err)
	}
	return ControlResponse{Operation: "control segment", Result: map[string]any{"start": cmd.Start.String(), "end": cmd.End.String()}}, nil
}

func applySelect(ctx context.Context, task goav.LiveTask, args any) (ControlResponse, error) {
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
	ctrl := control.SelectActive(cmd.Active)
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

func applyDeliver(ctx context.Context, task goav.LiveTask, args any) (ControlResponse, error) {
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
	ctrl := control.Deliver(event).AtTap(cmd.At)
	if err := task.Control(ctx, ctrl); err != nil {
		return ControlResponse{}, structuredError("control deliver", err)
	}
	return ControlResponse{Operation: "control deliver", Result: map[string]any{"type": cmd.Type, "stream": cmd.Stream, "at": cmd.At}}, nil
}

// Invoke binds args for one allowlisted spec and applies it.
func Invoke(ctx context.Context, task goav.LiveTask, spec CommandSpec, args []string) (ControlResponse, error) {
	bound, err := BindArgs(spec, args)
	if err != nil {
		return ControlResponse{}, err
	}
	if spec.Apply == nil {
		return ControlResponse{}, commandError("unsupported", "control "+spec.Name, "", "command has no apply function", nil, nil, nil)
	}
	return spec.Apply(ctx, task, bound)
}

func executeRawControl(ctx context.Context, task goav.LiveTask, data []byte) (ControlResponse, error) {
	ctrl, err := DecodeRawControl(data)
	if err != nil {
		return ControlResponse{}, err
	}
	if err := ensureTap(task, "control --json", ctrl.Tap()); err != nil {
		return ControlResponse{}, err
	}
	if ctrl.Type() == control.EventType && ctrl.Tap() == "" && ctrl.Node() == "" {
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
	return ControlResponse{Operation: "control --json", Result: map[string]any{"type": ctrl.Type(), "stream": ctrl.StreamID(), "tap": ctrl.Tap()}}, nil
}

func executeRawEvent(ctx context.Context, task goav.LiveTask, data []byte, args []string) (ControlResponse, error) {
	event, err := DecodeRawEvent(data)
	if err != nil {
		return ControlResponse{}, err
	}
	argValues, err := argsMap("control deliver --json", args)
	if err != nil {
		return ControlResponse{}, err
	}
	for key := range argValues {
		if key != "at" {
			return ControlResponse{}, commandError(
				"unknown_field",
				"control deliver --json",
				key,
				fmt.Sprintf("unknown raw event target field %q", key),
				[]string{"known_fields=at"},
				[]string{"use at=<tap-name>", "put event fields inside the JSON object"},
				nil,
			)
		}
	}
	cmd := DeliverCommand{
		Type:     event.Type,
		Stream:   event.StreamID,
		Reason:   event.Reason,
		At:       argValues["at"],
		Metadata: event.Metadata,
	}
	return applyDeliver(ctx, task, cmd)
}

// DecodeRawControl decodes the explicit raw JSON fallback into the real
// control.Control shape. It does not introduce a second control model.
func DecodeRawControl(data []byte) (control.Control, error) {
	obj, err := decodeObject(data)
	if err != nil {
		return control.Control{}, commandError("invalid_json", "control --json", "", err.Error(), nil, nil, err)
	}
	typ := control.Type(fieldString(obj, "type"))
	if typ == "" {
		return control.Control{}, commandError("missing_required", "control --json", "type", "raw control JSON needs type", nil, []string{`include "type":"bitrate"`}, nil)
	}
	var ctrl control.Control
	switch typ {
	case control.KeyframeType:
		if err := validateRawFields("control --json", obj, "type", "stream_id", "tap", "node", "reason"); err != nil {
			return control.Control{}, err
		}
		stream := av.StreamID(fieldString(obj, "stream_id"))
		ctrl = control.Keyframe(stream)
	case control.BitrateType:
		if err := validateRawFields("control --json", obj, "type", "stream_id", "bitrate", "tap", "node", "reason"); err != nil {
			return control.Control{}, err
		}
		stream := av.StreamID(fieldString(obj, "stream_id"))
		bitrate, ok := fieldInt(obj, "bitrate")
		if !ok {
			return control.Control{}, commandError("missing_required", "control --json", "bitrate", "raw bitrate control needs bitrate", nil, []string{`include "bitrate":1200000`}, nil)
		}
		ctrl, err = control.SetBitrate(stream, bitrate)
		if err != nil {
			return control.Control{}, structuredError("control --json", err)
		}
	case control.SeekType:
		if err := validateRawFields("control --json", obj, "type", "position", "tap", "node", "reason"); err != nil {
			return control.Control{}, err
		}
		position, ok, err := fieldDuration(obj, "position")
		if err != nil {
			return control.Control{}, err
		}
		if !ok {
			return control.Control{}, commandError("missing_required", "control --json", "position", "raw seek control needs position", nil, []string{`include "position":"12.5s"`}, nil)
		}
		ctrl = control.Seek(position)
	case control.RateType:
		if err := validateRawFields("control --json", obj, "type", "rate", "reason"); err != nil {
			return control.Control{}, err
		}
		rate, ok := fieldFloat(obj, "rate")
		if !ok {
			return control.Control{}, commandError("missing_required", "control --json", "rate", "raw rate control needs rate", nil, []string{`include "rate":0.5`}, nil)
		}
		ctrl, err = control.Rate(rate)
		if err != nil {
			return control.Control{}, structuredError("control --json", err)
		}
	case control.SegmentType:
		if err := validateRawFields("control --json", obj, "type", "start", "end", "tap", "node", "reason"); err != nil {
			return control.Control{}, err
		}
		start, ok, err := fieldDuration(obj, "start")
		if err != nil {
			return control.Control{}, err
		}
		end, endOK, err := fieldDuration(obj, "end")
		if err != nil {
			return control.Control{}, err
		}
		if !ok || !endOK {
			return control.Control{}, commandError("missing_required", "control --json", "segment", "raw segment control needs start and end", nil, []string{`include "start":"10s","end":"20s"`}, nil)
		}
		ctrl, err = control.Segment(start, end)
		if err != nil {
			return control.Control{}, structuredError("control --json", err)
		}
	case control.SelectType:
		if err := validateRawFields("control --json", obj, "type", "active", "tap", "node", "reason"); err != nil {
			return control.Control{}, err
		}
		active := av.StreamID(fieldString(obj, "active"))
		ctrl = control.SelectActive(active)
	case control.EventType:
		if err := validateRawFields("control --json", obj, "type", "event", "tap", "node", "reason"); err != nil {
			return control.Control{}, err
		}
		raw, ok := obj["event"]
		if !ok {
			return control.Control{}, commandError("missing_required", "control --json", "event", "raw event control needs event object", nil, []string{`use goav ctl control deliver --json '{"type":"vendor.force_idr"}' at=<tap>`}, nil)
		}
		eventBytes, err := json.Marshal(raw)
		if err != nil {
			return control.Control{}, commandError("invalid_json", "control --json", "event", err.Error(), nil, nil, err)
		}
		event, err := DecodeRawEvent(eventBytes)
		if err != nil {
			return control.Control{}, err
		}
		ctrl = control.Deliver(event)
	default:
		return control.Control{}, commandError("invalid_value", "control --json", "type", fmt.Sprintf("unknown raw control type %q", typ), nil, []string{"use one of: " + strings.Join(rawControlTypeNames(), ", "), `use goav ctl control deliver --json '{"type":"vendor.force_idr"}' at=<tap>`}, nil)
	}
	ctrl = ctrl.WithReason(firstNonEmpty(fieldString(obj, "reason"), ctrl.Reason()))
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
	if err := validateRawFields("control deliver --json", obj, "type", "stream_id", "reason", "metadata"); err != nil {
		return av.Event{}, err
	}
	event := av.Event{
		Type:     typ,
		StreamID: av.StreamID(fieldString(obj, "stream_id")),
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
	value, err := decodeJSONValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("raw JSON must contain one object")
	}
	obj, ok := value.(map[string]any)
	if !ok || obj == nil {
		return nil, fmt.Errorf("raw JSON must contain an object")
	}
	return obj, nil
}

func decodeJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delimiter {
	case '{':
		return decodeJSONObject(decoder)
	case '[':
		return decodeJSONArray(decoder)
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func decodeJSONObject(decoder *json.Decoder) (map[string]any, error) {
	obj := make(map[string]any)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("raw JSON object key must be a string")
		}
		if _, exists := obj[key]; exists {
			return nil, fmt.Errorf("duplicate JSON field %q", key)
		}
		value, err := decodeJSONValue(decoder)
		if err != nil {
			return nil, err
		}
		obj[key] = value
	}
	end, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := end.(json.Delim); !ok || delimiter != '}' {
		return nil, fmt.Errorf("raw JSON object is not closed")
	}
	return obj, nil
}

func decodeJSONArray(decoder *json.Decoder) ([]any, error) {
	var values []any
	for decoder.More() {
		value, err := decodeJSONValue(decoder)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	end, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := end.(json.Delim); !ok || delimiter != ']' {
		return nil, fmt.Errorf("raw JSON array is not closed")
	}
	return values, nil
}

func validateRawFields(operation string, obj map[string]any, allowed ...string) error {
	known := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		known[name] = struct{}{}
	}
	for name := range obj {
		if _, ok := known[name]; ok {
			continue
		}
		suggestions := []string{"use canonical raw JSON fields only"}
		if nearest := closest(name, allowed); nearest != "" {
			suggestions = append([]string{"use " + nearest}, suggestions...)
		}
		return commandError(
			"unknown_field",
			operation,
			name,
			fmt.Sprintf("unknown raw JSON field %q", name),
			[]string{"known_fields=" + strings.Join(allowed, ",")},
			suggestions,
			nil,
		)
	}
	return nil
}

func rawControlTypeNames() []string {
	names := []string{
		string(control.BitrateType),
		string(control.EventType),
		string(control.KeyframeType),
		string(control.RateType),
		string(control.SeekType),
		string(control.SegmentType),
		string(control.SelectType),
	}
	sort.Strings(names)
	return names
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

func applySourceOrNodeTarget(task goav.LiveTask, operation string, ctrl control.Control, source string, node string) (control.Control, error) {
	if source != "" && node != "" {
		return control.Control{}, commandError(
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
			return control.Control{}, err
		}
		return ctrl.At(pipeline.NodeRef(source)), nil
	}
	if node != "" {
		if err := ensureNode(task, operation, node); err != nil {
			return control.Control{}, err
		}
		return ctrl.At(pipeline.NodeRef(node)), nil
	}
	return ctrl, nil
}

func ensureTap(task goav.LiveTask, operation string, tap string) error {
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

func ensureNode(task goav.LiveTask, operation string, node string) error {
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

func ensureNodeKind(task goav.LiveTask, operation string, node string, kind pipeline.NodeKind) error {
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

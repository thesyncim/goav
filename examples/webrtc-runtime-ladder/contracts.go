package main

import "time"

type signalRequest struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

type signalResponse struct {
	ID   string `json:"id,omitempty"`
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

type addRenditionResponse struct {
	Rendition        renditionSpec `json:"rendition"`
	NeedsRenegotiate bool          `json:"needsRenegotiate"`
}

type stateResponse struct {
	ID         string           `json:"id"`
	Revision   uint64           `json:"revision"`
	Created    time.Time        `json:"created"`
	Updated    time.Time        `json:"updated"`
	VideoCodec string           `json:"videoCodec,omitempty"`
	AudioCodec string           `json:"audioCodec,omitempty"`
	LastError  string           `json:"lastError,omitempty"`
	Renditions []renditionView  `json:"renditions"`
	VideoGraph graphView        `json:"videoGraph"`
	AudioGraph graphView        `json:"audioGraph"`
	Debug      runtimeDebugView `json:"debug"`
	Events     []debugEvent     `json:"events"`
}

type renditionView struct {
	renditionSpec
	Packets uint64 `json:"packets"`
	Bytes   uint64 `json:"bytes"`
	Bound   bool   `json:"bound"`
}

type graphView struct {
	Name  string     `json:"name"`
	Nodes []nodeView `json:"nodes"`
	Edges []edgeView `json:"edges"`
	Stats graphStats `json:"stats"`
}

type nodeView struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Detail string `json:"detail,omitempty"`
}

type edgeView struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type graphStats struct {
	NodeCount int            `json:"nodeCount"`
	EdgeCount int            `json:"edgeCount"`
	KindCount map[string]int `json:"kindCount"`
	Sources   []string       `json:"sources,omitempty"`
	Sinks     []string       `json:"sinks,omitempty"`
}

type runtimeDebugView struct {
	Revision uint64          `json:"revision"`
	Tasks    []taskDebugView `json:"tasks"`
	Totals   debugTotals     `json:"totals"`
}

type taskDebugView struct {
	Kind     string     `json:"kind"`
	Codec    string     `json:"codec,omitempty"`
	State    string     `json:"state"`
	Graph    graphStats `json:"graph"`
	Attached []string   `json:"attached,omitempty"`
	Waiting  []string   `json:"waiting,omitempty"`
	Packets  uint64     `json:"packets"`
	Bytes    uint64     `json:"bytes"`
}

type debugTotals struct {
	Packets uint64 `json:"packets"`
	Bytes   uint64 `json:"bytes"`
}

type debugEvent struct {
	Seq       uint64            `json:"seq"`
	Time      time.Time         `json:"time"`
	Level     string            `json:"level"`
	Kind      string            `json:"kind"`
	Message   string            `json:"message"`
	Stream    string            `json:"stream,omitempty"`
	Rendition string            `json:"rendition,omitempty"`
	Meta      map[string]string `json:"meta,omitempty"`
}

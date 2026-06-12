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

type addBranchResponse struct {
	Branch           branchSpec `json:"branch"`
	NeedsRenegotiate bool       `json:"needsRenegotiate"`
}

type controlResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

type bitrateRequest struct {
	Bitrate int `json:"bitrate"`
}

type keyframeRequest struct {
	Kind string `json:"kind"`
}

type stateResponse struct {
	ID          string             `json:"id"`
	Revision    uint64             `json:"revision"`
	Created     time.Time          `json:"created"`
	Updated     time.Time          `json:"updated"`
	VideoCodec  string             `json:"videoCodec,omitempty"`
	AudioCodec  string             `json:"audioCodec,omitempty"`
	LastError   string             `json:"lastError,omitempty"`
	BrowserURL  string             `json:"browserUrl,omitempty"`
	Renegotiate renegotiateCommand `json:"renegotiate,omitempty"`
	NativeAudio nativeAudioStatus  `json:"nativeAudio"`
	Branches    []branchView       `json:"branches"`
	VideoGraph  graphView          `json:"videoGraph"`
	AudioGraph  graphView          `json:"audioGraph"`
	Debug       runtimeDebugView   `json:"debug"`
	Video       videoMetricsView   `json:"video"`
	Audio       audioMetricsView   `json:"audio"`
	Scenarios   []scenarioResult   `json:"scenarios"`
	Events      []debugEvent       `json:"events"`
}

type renegotiateCommand struct {
	Seq   uint64   `json:"seq,omitempty"`
	Kinds []string `json:"kinds,omitempty"`
}

type branchView struct {
	branchSpec
	Packets uint64 `json:"packets"`
	Bytes   uint64 `json:"bytes"`
	Bound   bool   `json:"bound"`
	Paused  bool   `json:"paused"`
	State   string `json:"state"`
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

type nativeAudioStatus struct {
	Available bool   `json:"available"`
	Enabled   bool   `json:"enabled"`
	Message   string `json:"message,omitempty"`
}

type videoMetricsView struct {
	Width       int     `json:"width"`
	Height      int     `json:"height"`
	PixelFormat string  `json:"pixelFormat,omitempty"`
	Frames      uint64  `json:"frames"`
	FPS         float64 `json:"fps"`
	Status      string  `json:"status"`
	Valid       bool    `json:"valid"`
	Warning     string  `json:"warning,omitempty"`
	LastPTS     string  `json:"lastPts,omitempty"`
	LastFrameMS int64   `json:"lastFrameMs,omitempty"`
}

type audioMetricsView struct {
	SampleRate   int       `json:"sampleRate"`
	Channels     int       `json:"channels"`
	SampleFormat string    `json:"sampleFormat,omitempty"`
	Frames       uint64    `json:"frames"`
	Samples      uint64    `json:"samples"`
	Packets      uint64    `json:"packets"`
	Bytes        uint64    `json:"bytes"`
	RMS          float64   `json:"rms"`
	Peak         float64   `json:"peak"`
	LossEvents   uint64    `json:"lossEvents"`
	PLCFrames    uint64    `json:"plcFrames"`
	Waveform     []float32 `json:"waveform,omitempty"`
	LastPTS      string    `json:"lastPts,omitempty"`
}

type scenarioResult struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Summary     string    `json:"summary"`
	Nodes       int       `json:"nodes,omitempty"`
	Edges       int       `json:"edges,omitempty"`
	Warnings    []string  `json:"warnings,omitempty"`
	Error       string    `json:"error,omitempty"`
	Fixes       []string  `json:"fixes,omitempty"`
	GeneratedAt time.Time `json:"generatedAt"`
}

type debugEvent struct {
	Seq     uint64            `json:"seq"`
	Time    time.Time         `json:"time"`
	Level   string            `json:"level"`
	Kind    string            `json:"kind"`
	Message string            `json:"message"`
	Stream  string            `json:"stream,omitempty"`
	Branch  string            `json:"branch,omitempty"`
	Meta    map[string]string `json:"meta,omitempty"`
}

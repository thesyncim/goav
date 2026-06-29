package plan

import "github.com/thesyncim/goav/av"

// OperationKind names one kind of planned operation in a task's media plan.
// It is the shared vocabulary between plan reports, snapshots, and tap
// anchors.
type OperationKind string

const (
	// OpProbe is input content detection (format and stream discovery).
	OpProbe OperationKind = "probe"
	// OpDemux reads packets out of a container input.
	OpDemux OperationKind = "demux"
	// OpDepacketize reassembles transport payloads (RTP) into codec packets.
	OpDepacketize OperationKind = "depacketize"
	// OpSelect narrows the input to one matched stream.
	OpSelect OperationKind = "select"
	// OpDecode turns packets into frames.
	OpDecode OperationKind = "decode"
	// OpShape is a shape annotation or solver directive (.Shape, .Auto,
	// .Require, .Prefer); it lowers to no runtime node.
	OpShape OperationKind = "shape"
	// OpTransform is a frame conversion (resize, resample, format convert).
	OpTransform OperationKind = "transform"
	// OpStage is a custom caller-provided stage (.Do).
	OpStage OperationKind = "stage"
	// OpJoin is the planned N-to-1 convergence node (Mix/Composite/Select): N
	// arm chains feed one join operation and the downstream chain hangs off it
	// like any other stream point.
	OpJoin OperationKind = "join"
	// OpEncode turns frames into packets.
	OpEncode OperationKind = "encode"
	// OpCopy forwards packets unchanged (packet-preserving passthrough).
	OpCopy OperationKind = "copy"
	// OpMux writes packets into a container output.
	OpMux OperationKind = "mux"
	// OpWrite delivers muxed bytes to a byte destination.
	OpWrite OperationKind = "write"
	// OpSink delivers media messages to a sink destination.
	OpSink OperationKind = "sink"
	// OpTap is a named attach point for runtime branches.
	OpTap OperationKind = "tap"
)

// StreamSelect reports how a stream chain selects its source stream.
type StreamSelect struct {
	ID       av.StreamID  `json:"id,omitempty"`
	Index    int          `json:"index,omitempty"`
	UseIndex bool         `json:"useIndex,omitempty"`
	Type     av.MediaType `json:"type,omitempty"`
	Codec    av.CodecID   `json:"codec,omitempty"`
	Name     string       `json:"name,omitempty"`
	// Input narrows the selection to the named job input (goav.InputName).
	Input string `json:"input,omitempty"`
}

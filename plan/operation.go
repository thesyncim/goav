package plan

import "github.com/thesyncim/goav/av"

// OperationKind names one kind of planned operation in a task's media plan.
// It is the shared vocabulary between plan reports, snapshots, and tap
// anchors.
type OperationKind string

const (
	OpProbe       OperationKind = "probe"
	OpDemux       OperationKind = "demux"
	OpDepacketize OperationKind = "depacketize"
	OpSelect      OperationKind = "select"
	OpDecode      OperationKind = "decode"
	OpShape       OperationKind = "shape"
	OpTransform   OperationKind = "transform"
	OpStage       OperationKind = "stage"
	// OpJoin is the planned N-to-1 convergence node (Mix/Composite/Select): N
	// arm chains feed one join operation and the downstream chain hangs off it
	// like any other stream point.
	OpJoin   OperationKind = "join"
	OpEncode OperationKind = "encode"
	OpCopy   OperationKind = "copy"
	OpMux    OperationKind = "mux"
	OpWrite  OperationKind = "write"
	OpSink   OperationKind = "sink"
	OpTap    OperationKind = "tap"
)

// StreamSelect reports how a stream chain selects its source stream.
type StreamSelect struct {
	ID       av.StreamID
	Index    int
	UseIndex bool
	Type     av.MediaType
	Codec    av.CodecID
	Name     string
	// Input narrows the selection to the named job input (goav.InputName).
	Input string
}

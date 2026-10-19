package goav

import (
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

var destinationRefSeq atomic.Uint64
var destinationSpecSeq atomic.Uint64

// destinationShareKey is the identity under which outputs share one mux or
// sink: the handle itself. Reusing one destination value from several streams
// or branches feeds one mux; separately constructed destinations stay
// distinct even when they share a name, so sharing is always explicit.
func destinationShareKey(dest destinationSpec) string {
	if dest.id != 0 {
		return "handle:" + strconv.FormatUint(dest.id, 10)
	}
	return ""
}

// Destination is an opaque handle for a file, URI, writer, or media sink. The
// handle is the output's identity: pass the same value to .To(...) from
// several streams or branches and they feed one mux or sink (With keeps that
// identity); separately constructed handles are separate outputs. The zero
// value is intentionally not a valid destination; construct handles with
// Write, URI, Sink, Writer, or Custom.
type Destination struct {
	origin destinationOrigin
	spec   destinationSpec
}

type destinationOrigin uint8

const (
	destinationOriginZero destinationOrigin = iota
	destinationOriginConstructed
)

// DestinationOption configures a destination value (Write, URI, Writer,
// Custom, or Destination.With): Format pins the container, and the
// direction-agnostic media options (Name, MIME, Metadata) satisfy it too. It is
// sealed — only goav option constructors implement it.
type DestinationOption interface {
	applyDestination(*destinationSpec)
}

type destinationRef struct {
	name string
	dest destinationSpec
	id   uint64
}

// newDirectDestinationRef names a destination handle for branch routing.
// Callers derive name through destinationSpec.label with a non-empty
// fallback, so a ref always carries a routing name.
func newDirectDestinationRef(name string, dest destinationSpec) destinationRef {
	id := dest.id
	if id == 0 {
		id = destinationRefSeq.Add(1)
	}
	return destinationRef{
		name: name,
		dest: dest.withName(firstNonEmpty(dest.name, name)),
		id:   id,
	}
}

func destinationSpecFromDestination(dest Destination) (destinationSpec, error) {
	if dest.origin != destinationOriginConstructed {
		return destinationSpec{}, fmt.Errorf("destination is empty; construct it with goav.Write(...), goav.URI(...), goav.Sink(...), goav.Writer(...), or goav.Custom(...)")
	}
	if destinationSpecEmpty(dest.spec) {
		return destinationSpec{}, fmt.Errorf("destination is empty")
	}
	return cloneDestinationSpec(dest.spec), nil
}

func destinationSpecEmpty(dest destinationSpec) bool {
	return dest.sink == nil &&
		dest.custom == nil &&
		dest.output.Name == "" &&
		dest.output.URI == "" &&
		dest.output.Protocol == "" &&
		dest.output.MIMEType == "" &&
		dest.output.Writer == nil &&
		dest.format == "" &&
		dest.resolvedFormat == "" &&
		dest.name == "" &&
		dest.err == nil
}

type branchSourceBinding struct {
	from      string
	tap       string
	tapDomain shape.MediaDomain
	policy    pipeline.RoutePolicy
	label     string
	// stream pins a discovered-stream anchor: the branch hangs off the source
	// node `from` routed by stream id, and the attach planner derives the
	// anchor shape from these stream facts instead of a tap. streamDomain is
	// the source's media domain (packet unless the source produces frames).
	stream       *av.Stream
	streamDomain shape.MediaDomain
}

func duplicateBranchDecodeError(node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(branchDecodeDuplicateCode),
		Code:      branchDecodeDuplicateCode,
		Operation: "build branch",
		Node:      node,
		Reason:    "branch already decodes its input packets",
		fixes: buildErrorFixes([]string{
			"call .Decode() once before frame operations",
			"remove the second .Decode() call",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchDecodeDomainError(node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(branchDecodeDomainMismatchCode),
		Code:      branchDecodeDomainMismatchCode,
		Operation: "build branches",
		Node:      node,
		Reason:    "branch decoding requires a packet-domain stream point",
		fixes: buildErrorFixes([]string{
			"omit .Decode() when the fork already starts after the stream's .Decode()",
			"fork before .Decode() when a packet copy and a decoding fork share the stream: s := in.Video()",
			"attach runtime decode branches from packet taps",
		}),
		cause: errUnsupportedBuild,
	}
}

func cloneDestinationRef(ref destinationRef) destinationRef {
	ref.dest = cloneDestinationSpec(ref.dest)
	return ref
}

func cloneDestinationSpec(dest destinationSpec) destinationSpec {
	return dest
}

func branchDestinationInvalidError(name string, reason string) error {
	return destinationInvalidError("build branch", firstNonEmpty(name, "branch"), reason)
}

func destinationInvalidError(operation string, node string, reason string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(destinationInvalidCode),
		Code:      destinationInvalidCode,
		Operation: operation,
		Node:      node,
		Reason:    reason,
		fixes: buildErrorFixes([]string{
			"use goav.Write(...), goav.URI(...), or goav.Sink(...) for a real destination",
			"pass one destination value to every chain that should share it",
			"use distinct destination values for independent outputs",
		}),
		cause: errUnsupportedBuild,
	}
}

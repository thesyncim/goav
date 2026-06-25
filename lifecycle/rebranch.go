package lifecycle

import "time"

// RebranchArg is one typed argument to Attachment.Rebranch: either a
// replacement branch spec supplied by goav.Branch(...) or a lifecycle policy
// returned by SwitchAt, DrainOldBranch, or AbortOldBranch.
type RebranchArg interface {
	RebranchArg()
}

// SwitchBoundary is the stream position where a rebranch hands delivery over
// from the replaced branch to its replacements.
type SwitchBoundary struct {
	kind      string
	mediaTime time.Duration
}

// Kind returns the boundary kind understood by the runtime.
func (b SwitchBoundary) Kind() string {
	return b.kind
}

// MediaTime returns the media timestamp for an AtMediaTime boundary.
func (b SwitchBoundary) MediaTime() time.Duration {
	return b.mediaTime
}

// NextFrame switches at the next media message: the replacement branch starts
// delivering with the first frame or packet that reaches it.
func NextFrame() SwitchBoundary {
	return SwitchBoundary{kind: "next-frame"}
}

// NextKeyframe switches at the next decodable sync point. Raw frames are
// independently decodable, so on frame streams the next frame qualifies.
func NextKeyframe() SwitchBoundary {
	return SwitchBoundary{kind: "next-keyframe"}
}

// AtMediaTime switches when the replacement branch sees a packet or frame at
// or after the given media timestamp.
func AtMediaTime(position time.Duration) SwitchBoundary {
	return SwitchBoundary{kind: "media-time", mediaTime: position}
}

type rebranchPolicy struct {
	boundary    string
	mediaTime   time.Duration
	invalid     string
	disposition string
}

func (p rebranchPolicy) RebranchArg() {}

// Boundary returns the switch boundary kind, or empty for immediate switch.
func (p rebranchPolicy) Boundary() string {
	return p.boundary
}

// MediaTime returns the configured media-time switch position.
func (p rebranchPolicy) MediaTime() time.Duration {
	return p.mediaTime
}

// Invalid returns non-empty validation detail for invalid policy values.
func (p rebranchPolicy) Invalid() string {
	return p.invalid
}

// Disposition returns the old branch destination disposition selected by the
// policy, or empty for a plain detach.
func (p rebranchPolicy) Disposition() string {
	return p.disposition
}

// SwitchAt delays the rebranch switch to the given stream boundary. Without
// SwitchAt, rebranch switches immediately.
func SwitchAt(boundary SwitchBoundary) RebranchArg {
	policy := rebranchPolicy{boundary: boundary.kind, mediaTime: boundary.mediaTime}
	if boundary.kind == "media-time" && boundary.mediaTime < 0 {
		policy.invalid = "media-time switch boundary must be non-negative"
	}
	return policy
}

// DrainOldBranch finalizes the replaced branch as drained: its destinations
// are committed when it detaches at the switch boundary.
func DrainOldBranch() RebranchArg {
	return rebranchPolicy{disposition: "drain"}
}

// AbortOldBranch finalizes the replaced branch as abandoned: its destinations
// are aborted when it detaches at the switch boundary.
func AbortOldBranch() RebranchArg {
	return rebranchPolicy{disposition: "abort"}
}

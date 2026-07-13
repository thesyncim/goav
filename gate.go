package goav

import (
	"strings"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
)

// This file holds what the two timed-gate families (sync, playout) share:
// stage naming, the refusal for media without a usable PTS timebase, and the
// pre-mutation validation that a stream carries timebase facts. The families
// keep their own literal fix lists so refusals stay exact.

// gateMessageTime extracts the stream and PTS a timed gate schedules on; ok
// is false for media without a valid PTS timebase (and for events).
func gateMessageTime(msg *pipeline.Message) (av.StreamID, time.Duration, bool) {
	switch msg.Kind {
	case pipeline.MessagePacket:
		if msg.Packet == nil || !msg.Packet.PTS.Base.Valid() {
			return "", 0, false
		}
		pts, ok := msg.Packet.PTS.ToDuration()
		return msg.Packet.StreamID, pts, ok
	case pipeline.MessageFrame:
		if msg.Frame == nil || !msg.Frame.PTS.Base.Valid() {
			return "", 0, false
		}
		pts, ok := msg.Frame.PTS.ToDuration()
		return msg.Frame.StreamID, pts, ok
	default:
		return "", 0, false
	}
}

// gateStageName builds the diagnostic node name for a timed gate:
// prefix-<sanitized policy name>, with the prefix doubling as the default name.
func gateStageName(prefix string, name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = prefix
	}
	replacer := strings.NewReplacer(" ", "-", "/", "-", "\\", "-", "\t", "-", "\n", "-")
	return prefix + "-" + replacer.Replace(name)
}

// gateTimebaseError refuses one media message that reached a timed gate
// without a valid PTS timebase; the caller supplies its gate-specific fixes.
func gateTimebaseError(operation string, node string, msg *pipeline.Message, fixes []string) error {
	kind := ""
	switch {
	case msg == nil:
	case msg.Packet != nil:
		kind = "packet"
	case msg.Frame != nil:
		kind = "frame"
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(runtimeBranchInvalidCode),
		Code:      runtimeBranchInvalidCode,
		Operation: operation,
		Node:      node,
		Reason:    "media message has no valid PTS timebase",
		fields:    errDetails(errDetail("message", firstNonEmpty(kind, "unknown"))),
		fixes:     buildErrorFixes(fixes),
		cause:     errUnsupportedBuild,
	}
}

// validateGatePolicyForBranch refuses, before graph mutation, a branch whose
// stream declares no timebase facts a timed gate could schedule on; a valid
// TimeBase or a Codec.ClockRate (RTP-style) satisfies it.
func validateGatePolicyForBranch(operation string, branchName string, stream av.Stream, reason string, fixes []string) error {
	if stream.TimeBase.Valid() {
		return nil
	}
	if stream.Codec.ClockRate != 0 {
		return nil
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(runtimeBranchInvalidCode),
		Code:      runtimeBranchInvalidCode,
		Operation: operation,
		Node:      firstNonEmpty(branchName, string(stream.ID), "branch"),
		Reason:    reason,
		fixes:     buildErrorFixes(fixes),
		cause:     errUnsupportedBuild,
	}
}

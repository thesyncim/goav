package goav

// DetachOption selects how a removed runtime branch's destinations are
// finalized: [DrainBranch] commits them, [AbortBranch] aborts them, and with
// neither they close without a recorded outcome. LiveTask.Detach takes it for
// the detached branch and Attachment.Rebranch for the replaced one. It is
// sealed: only goav option constructors implement it.
type DetachOption interface {
	RebranchArg
	detachDisposition() oldBranchDisposition
}

type detachOption oldBranchDisposition

func (detachOption) rebranchArg() {}

func (o detachOption) detachDisposition() oldBranchDisposition {
	return oldBranchDisposition(o)
}

// DrainBranch finalizes the removed branch as drained: its destinations are
// committed when it detaches.
func DrainBranch() DetachOption {
	return detachOption(oldBranchDrain)
}

// AbortBranch finalizes the removed branch as abandoned: its destinations are
// aborted when it detaches.
func AbortBranch() DetachOption {
	return detachOption(oldBranchAbort)
}

type detachPolicy struct {
	disposition oldBranchDisposition
}

func detachPolicyFromOptions(options []DetachOption) detachPolicy {
	var policy detachPolicy
	for _, option := range options {
		if option == nil {
			continue
		}
		if disposition := option.detachDisposition(); disposition != oldBranchDetach {
			policy.disposition = disposition
		}
	}
	return policy
}

package goav

// DetachOption configures Mutable.Detach. The default detach closes the runtime
// branch and reports its destinations as closed; DrainBranch and AbortBranch
// choose a terminal destination outcome for workflows where the detach itself
// is the commit or abort boundary.
type DetachOption interface {
	applyDetach(*detachPolicy)
}

type detachPolicy struct {
	disposition oldBranchDisposition
}

type detachOptionFunc func(*detachPolicy)

func (f detachOptionFunc) applyDetach(policy *detachPolicy) {
	f(policy)
}

// DrainBranch finalizes the detached branch as drained: its destinations are
// committed when Mutable.Detach removes it. This is the standalone detach twin of
// DrainOldBranch for Rebranch.
func DrainBranch() DetachOption {
	return detachOptionFunc(func(policy *detachPolicy) {
		policy.disposition = oldBranchDrain
	})
}

// AbortBranch finalizes the detached branch as abandoned: its destinations are
// aborted when Mutable.Detach removes it. This is useful for runtime recording or
// diagnostic branches whose output should not be committed.
func AbortBranch() DetachOption {
	return detachOptionFunc(func(policy *detachPolicy) {
		policy.disposition = oldBranchAbort
	})
}

func detachPolicyFromOptions(options []DetachOption) detachPolicy {
	var policy detachPolicy
	for _, option := range options {
		if option == nil {
			continue
		}
		option.applyDetach(&policy)
	}
	return policy
}

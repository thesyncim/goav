package goav

import "github.com/thesyncim/goav/lifecycle"

type detachPolicy struct {
	disposition oldBranchDisposition
}

func detachPolicyFromOptions(options []lifecycle.DetachOption) detachPolicy {
	var policy detachPolicy
	for _, option := range options {
		if option == nil {
			continue
		}
		switch oldBranchDisposition(option.DetachDisposition()) {
		case oldBranchDrain:
			policy.disposition = oldBranchDrain
		case oldBranchAbort:
			policy.disposition = oldBranchAbort
		}
	}
	return policy
}

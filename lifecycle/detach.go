package lifecycle

// DetachOption configures how a runtime branch's destinations are finalized
// when the branch is detached.
type DetachOption interface {
	DetachDisposition() string
}

type detachOption string

func (o detachOption) DetachDisposition() string {
	return string(o)
}

// DrainBranch finalizes the detached branch as drained: its destinations are
// committed when Mutable.Detach removes it.
func DrainBranch() DetachOption {
	return detachOption("drain")
}

// AbortBranch finalizes the detached branch as abandoned: its destinations are
// aborted when Mutable.Detach removes it.
func AbortBranch() DetachOption {
	return detachOption("abort")
}

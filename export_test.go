package goav

// JobPlanForTest exposes the unexported intent normalization to the external
// API tests. The production surface does not export Job.Plan: the Intent it
// returns is the compiler's input, not a caller-facing report (Explain is).
func JobPlanForTest(j *Job) Intent {
	return j.plan()
}

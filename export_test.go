package goav

import "github.com/thesyncim/goav/codec"

// JobPlanForTest exposes the unexported intent normalization to the external
// API tests. The production surface does not export Job.Plan: the Intent it
// returns is the compiler's input, not a caller-facing report (Explain is).
func JobPlanForTest(j *Job) Intent {
	return j.plan()
}

// StreamHasDecodeForTest / StreamEncodeForTest / StreamTapsForTest expose the
// operation-list accessors production code reads stream intents through, so
// the external API tests assert on the same single source of truth —
// streamIntent keeps no parallel decode/encode/tap fields.
func StreamHasDecodeForTest(stream streamIntent) bool {
	return chainHasDecode(stream.Operations)
}

func StreamEncodeForTest(stream streamIntent) codec.CodecSpec {
	return chainEncodeSpec(stream.Operations)
}

func StreamTapsForTest(stream streamIntent) []tapIntent {
	return operationSpecTaps(stream.Operations, stream.Select.Type)
}

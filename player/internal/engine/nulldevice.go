package engine

import (
	"context"
	"time"
)

// RunNullDevice stands in for a speaker when none can be opened (or under
// test): it pulls from the engine at the output rate on the monotonic clock
// and discards the samples, so the device clock, pacing, and the presenter
// work unchanged with nothing audible. It returns when ctx is done.
func RunNullDevice(ctx context.Context, e *Engine) {
	const tick = 5 * time.Millisecond
	start := time.Now()
	var pulled int64
	buf := make([]byte, 0, BytesPerFrame*OutputRate/10)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		due := durationToFrames(time.Since(start))
		for pulled < due {
			n := min(due-pulled, int64(cap(buf)/BytesPerFrame))
			e.Fill(buf[:n*BytesPerFrame])
			pulled += n
		}
	}
}

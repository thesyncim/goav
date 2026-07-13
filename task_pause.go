package goav

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/format"
)

// Pause freezes the task's shared timeline at its current reading. Every paced
// consumer observes the freeze together: the realtime demux pacer and
// clock-aware sources stall on their next due wait, playout gates hold
// delivery, and a message already due (or re-anchored by a seek) still
// delivers — pause holds waits, it does not gate the data plane. Free-running
// tasks (nothing paced, no playout gate) are refused with
// format.ErrRateUnsupported instead of pretending: pausing time that nothing
// reads would be a silent no-op. Idempotent; allowed before Run; refused after
// Close with control.ErrNotRunning.
func (t *task) Pause(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := t.timelineControlRefusal("pause"); err != nil {
		return err
	}
	t.timeline.Pause()
	return nil
}

// Resume continues the paused task timeline from its frozen reading at the
// current rate, so no pause gap enters media time; parked waits recompute and
// proceed. Resuming a running task is a no-op. The refusal contract mirrors
// Pause.
func (t *task) Resume(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := t.timelineControlRefusal("resume"); err != nil {
		return err
	}
	t.timeline.Resume()
	return nil
}

// timelineControlRefusal is the shared gate for every timeline control
// (pause, resume, rate): a closed task has no timeline to control (the
// not-running shape), and a task with no paced consumer is refused — the
// timeline exists but nothing reads it, so the control would be a silent
// no-op.
func (t *task) timelineControlRefusal(operation string) error {
	t.lifecycleMu.Lock()
	closed := t.closed
	t.lifecycleMu.Unlock()
	if closed {
		return fmt.Errorf("goav: %s: task is closed: %w", operation, control.ErrNotRunning)
	}
	if t.timeline == nil || !t.timeline.paced() {
		return fmt.Errorf("goav: %s: nothing on this task is paced, so a timeline control would be a silent no-op; keep the runtime realtime for file inputs, implement UseClock(av.Clock) on the source, or add .Playout(flow.Playout(name)) before the destination: %w", operation, format.ErrRateUnsupported)
	}
	return nil
}

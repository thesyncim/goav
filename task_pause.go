package goav

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/format"
)

// Pause freezes the task's shared timeline (see Task.Pause).
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

// Resume continues the paused task timeline (see Task.Resume).
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

func (t *task) timelineControlRefusal(operation string) error {
	t.lifecycleMu.Lock()
	closed := t.closed
	t.lifecycleMu.Unlock()
	if closed {
		return fmt.Errorf("goav: %s: task is closed: %w", operation, control.ErrNotRunning)
	}
	if t.timeline == nil || !t.timeline.paced() {
		return fmt.Errorf("goav: %s: task has no paced source or playout gate (use WithRealtime(true), UseClock, or .Playout(...)): %w", operation, format.ErrRateUnsupported)
	}
	return nil
}

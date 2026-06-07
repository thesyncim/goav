package main

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/goav"
)

func TestSessionPublishesStateForBranchChanges(t *testing.T) {
	session, err := newSession(context.Background(), goav.Default())
	if err != nil {
		t.Fatalf("newSession() error = %v", err)
	}
	defer session.Close()

	updates, unsubscribe := session.subscribe()
	defer unsubscribe()

	initial := awaitState(t, updates)
	if initial.ID == "" || initial.Revision == 0 {
		t.Fatalf("initial state = %+v, want session id and revision", initial)
	}
	if len(initial.Branches) != len(defaultBranches()) {
		t.Fatalf("branches = %d, want defaults", len(initial.Branches))
	}
	if len(initial.Debug.Tasks) != 2 {
		t.Fatalf("debug tasks = %d, want video and audio", len(initial.Debug.Tasks))
	}
	if len(initial.Events) == 0 {
		t.Fatalf("events empty, want event history")
	}

	_, err = session.addBranch(context.Background(), branchSpec{
		Kind:    "video",
		Codec:   "vp8",
		Width:   160,
		Height:  90,
		Bitrate: 120_000,
	})
	if err != nil {
		t.Fatalf("addBranch() error = %v", err)
	}

	updated := awaitState(t, updates)
	for len(updates) > 0 {
		updated = <-updates
	}
	if updated.Revision <= initial.Revision {
		t.Fatalf("revision = %d, want > %d", updated.Revision, initial.Revision)
	}
	if len(updated.Branches) != len(defaultBranches())+1 {
		t.Fatalf("branches = %d, want added branch", len(updated.Branches))
	}
	if !hasEvent(updated.Events, "branch", "branch track added") {
		t.Fatalf("events = %+v, want branch add event", updated.Events)
	}
}

func awaitState(t *testing.T, updates <-chan stateResponse) stateResponse {
	t.Helper()
	select {
	case state := <-updates:
		return state
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for state")
		return stateResponse{}
	}
}

func hasEvent(events []debugEvent, kind, message string) bool {
	for _, event := range events {
		if event.Kind == kind && event.Message == message {
			return true
		}
	}
	return false
}

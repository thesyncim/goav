package pipeline

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

type failingRecoveringSink struct {
	*recoveringSink
	err error
}

func (s *failingRecoveringSink) Close() error { _ = s.recoveringSink.Close(); return s.err }

func TestDeferredCleanupRetainsErrors(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, shutdown := range []bool{false, true} {
			t.Run(fmt.Sprintf("buffered=%t/shutdown=%t", buffered, shutdown), func(t *testing.T) {
				policy := BufferPolicy{}
				if buffered {
					policy = BufferPolicy{Capacity: 2, Drop: DropBlock}
				}
				g, err := NewGraph(GraphConfig{Buffer: policy, CloseWaitTimeout: time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				src := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
				_, _ = g.AddSource(src, BufferPolicy{})
				run := make(chan error, 1)
				go func() { run <- g.Run(t.Context()) }()
				<-src.ready
				var failures []error
				cycles := 3
				if shutdown {
					cycles = 1
				}
				for i := range cycles {
					failure := fmt.Errorf("recording finalization failed %d", i)
					failures = append(failures, failure)
					s := &failingRecoveringSink{recoveringSink: &recoveringSink{entered: make(chan struct{}), release: make(chan struct{})}, err: failure}
					ref, err := g.AddSink(s, policy)
					if err != nil {
						t.Fatal(err)
					}
					if err := g.Connect(route("src", s.Name())); err != nil {
						t.Fatal(err)
					}
					emitted := make(chan error, 1)
					p := immutablePacket(1)
					go func() { emitted <- src.emitter.Emit(t.Context(), &Message{Kind: MessagePacket, Packet: &p}) }()
					<-s.entered
					if shutdown {
						err = g.Close()
					} else {
						err = g.Remove(ref)
					}
					if !errors.Is(err, ErrCloseWait) {
						t.Fatalf("timeout=%v", err)
					}
					close(s.release)
					if err := <-emitted; err != nil {
						t.Fatal(err)
					}
					deadline := time.Now().Add(time.Second)
					for g.Stats().PendingCleanup != 0 && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					if g.Stats().PendingCleanup != 0 || s.closes.Load() != 1 || s.overlap.Load() {
						t.Fatal("cleanup failed to recover exactly once")
					}
					if !shutdown {
						found := false
						for len(g.Events()) != 0 {
							event := <-g.Events()
							if event.Type == av.EventCleanupError && errors.Is(event.Cause, failure) && event.Metadata["node"] == s.Name() {
								found = true
							}
						}
						if !found {
							t.Fatal("missing structured cleanup failure")
						}
					}
				}
				close(src.release)
				if err := <-run; err != nil {
					t.Fatal(err)
				}
				for range 2 {
					err := g.Close()
					for _, failure := range failures {
						if !errors.Is(err, failure) {
							t.Fatalf("later Close lost %v: %v", failure, err)
						}
					}
					if !strings.Contains(err.Error(), "recovering") {
						t.Fatalf("missing identity: %v", err)
					}
				}
				if nodeTableLen(g) != 2 {
					t.Fatal("failed finalization prevented reclamation")
				}
			})
		}
	}
}

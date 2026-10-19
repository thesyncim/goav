package libcodec_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/libcodec"
)

// TestRuntimesShareTheirBindings builds many runtimes with every backend
// and pins that they share the process's bindings: a runtime leaves no
// memory behind once dropped, and none binds a library or probes the
// hardware again.
func TestRuntimesShareTheirBindings(t *testing.T) {
	build := func() {
		if _, err := goav.New(libcodec.OpenH264(), libcodec.AudioToolbox(), libcodec.VideoToolbox()); err != nil {
			t.Fatal(err)
		}
	}
	build()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const runtimes = 200
	start := time.Now()
	for range runtimes {
		build()
	}
	each := time.Since(start) / runtimes
	runtime.GC()
	runtime.ReadMemStats(&after)
	if leaked := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / runtimes; leaked > 512 {
		t.Errorf("each runtime left %d bytes behind", leaked)
	}
	if each > time.Millisecond {
		t.Errorf("each runtime took %v to build: something binds or probes again", each)
	}
	t.Logf("%v per runtime", each)
}

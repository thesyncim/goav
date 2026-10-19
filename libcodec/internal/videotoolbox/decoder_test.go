package videotoolbox

import (
	"errors"
	"slices"
	"testing"

	"github.com/thesyncim/goav/libcodec/kit"
)

func boundFrameworks(t *testing.T) *frameworks {
	t.Helper()
	f := &frameworks{}
	for _, library := range f.libraries() {
		if err := library.Bind(Name); err != nil {
			t.Skip(err)
		}
	}
	return f
}

// TestDrainReleasesEachImageOnce queues decoded, dropped, and damaged
// pictures the way the output callback does and pins that drain releases
// every queued image exactly once: lent pictures through their owner, the
// rest directly. A double release would free the buffers the test still
// holds.
func TestDrainReleasesEachImageOnce(t *testing.T) {
	f := boundFrameworks(t)
	images := make([]uintptr, 4)
	for i := range images {
		if s := f.cv.create(0, 16, 16, pixelI420, 0, &images[i]); s != 0 {
			t.Fatalf("CVPixelBufferCreate: %d", s)
		}
		defer f.cf.release(images[i])
	}
	queued := func() []picture {
		for _, image := range images {
			f.cf.retain(image) // the callback's reference, the queue's to release
		}
		return []picture{
			{image: images[0], pts: 0},
			{pts: 1}, // dropped: no image
			{image: images[1], pts: 2},
			{status: -12909, image: images[2], pts: 3},
			{image: images[3], pts: 4},
		}
	}
	held := func() bool {
		return !slices.ContainsFunc(images, func(image uintptr) bool { return f.cf.retainCount(image) != 1 })
	}

	d := &decoder{f: f, queue: queued()}
	var lent []int64
	err := d.drain(func(p *kit.Picture) error { lent = append(lent, p.PTS); return nil })
	if !errors.Is(err, kit.ErrDamaged) || !slices.Equal(lent, []int64{0, 2, 4}) || !held() {
		t.Fatalf("drain lent %v, returned %v; each image must be released once", lent, err)
	}

	d.queue = queued()
	stop := errors.New("stop")
	if err := d.drain(func(*kit.Picture) error { return stop }); err != stop || !held() {
		t.Fatalf("drain = %v; an emit failure must release the rest once", err)
	}
}

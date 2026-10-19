package goav_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/webm"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
)

// jitterReader reads at most 512 bytes at a time and sleeps now and then, so
// which file's goroutine gets ahead changes from run to run. A run whose
// output depends on that writes different bytes each time.
type jitterReader struct{ r io.Reader }

func (j jitterReader) Read(p []byte) (int, error) {
	if rand.IntN(4) == 0 {
		time.Sleep(time.Duration(rand.IntN(200)) * time.Microsecond)
	}
	if len(p) > 512 {
		p = p[:512]
	}
	return j.r.Read(p)
}

// orderFixtureSet is made once per process: goavtest names its sources from a
// process-wide counter, so fixtures made again for each -cpu pass would differ.
type orderFixtureSet struct {
	once                           sync.Once
	audioA, audioB, videoA, videoB []byte
	videoLong                      []byte // 150 frames
	// videoAhead and audioAhead start 5 s in.
	videoAhead, audioAhead []byte
}

var orderFixtures orderFixtureSet

func offlineOrderFixtures(t *testing.T) *orderFixtureSet {
	f := &orderFixtures
	f.once.Do(func() {
		f.audioA = mfWebM(t, 0, 0, 0, 48_000, 2, 50)
		f.audioB = mfWebM(t, 0, 0, 0, 24_000, 1, 50)
		f.videoA = mfWebM(t, 64, 48, 30, 0, 0, 0)
		f.videoLong = mfWebM(t, 64, 48, 150, 0, 0, 0)
		f.videoB = mfWebM(t, 32, 24, 30, 0, 0, 0)
		f.videoAhead = shiftedWebM(t, mfWebM(t, 32, 24, 120, 0, 0, 0), av.MediaVideo, 5*time.Second)
		f.audioAhead = shiftedWebM(t, mfWebM(t, 0, 0, 0, 48_000, 2, 200), av.MediaAudio, 5*time.Second)
	})
	return f
}

// shiftedWebM re-encodes one track of a WebM with every frame time moved by.
func shiftedWebM(t *testing.T, data []byte, media av.MediaType, by time.Duration) []byte {
	t.Helper()
	in := goav.FileInput("shift.webm", bytes.NewReader(data))
	stream, spec := in.Video(), codec.VP8()
	if media == av.MediaAudio {
		stream, spec = in.Audio(), codec.Opus()
	}
	var out bytes.Buffer
	mfRun(t, stream.Decode().Do(shiftFrames{by: by}).Encode(spec).To(goav.Write("shifted.webm", &out)))
	return out.Bytes()
}

type shiftFrames struct{ by time.Duration }

func (shiftFrames) Name() string { return "shift" }
func (shiftFrames) Close() error { return nil }
func (s shiftFrames) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg.Kind == pipeline.MessageFrame && msg.Frame != nil {
		// Rescale the shift into the frame's base, checked: decoded frames
		// count in nanoseconds, where by * Den overflows int64 from 9.2 s.
		ts := &msg.Frame.PTS
		delta, ok := av.RescaleValue(int64(s.by), av.TimeBase{Num: 1, Den: int64(time.Second)}, ts.Base)
		if !ok || (delta > 0 && ts.Value > math.MaxInt64-delta) {
			return fmt.Errorf("shift %v does not fit PTS %d in base %v", s.by, ts.Value, ts.Base)
		}
		ts.Value += delta
	}
	return emitter.Emit(ctx, msg)
}

// TestShiftedFixturesStartWhereAsked pins the shift helper the order tests
// build their late files with: a 10 s or 30 s shift once overflowed the
// nanosecond PTS of decoded frames and wrapped negative, which the muxer
// rightly refused as invalid data.
func TestShiftedFixturesStartWhereAsked(t *testing.T) {
	f := offlineOrderFixtures(t)
	for _, tc := range []struct {
		label string
		data  []byte
		by    time.Duration
	}{
		{"video ahead", f.videoAhead, 5 * time.Second},
		{"audio ahead", f.audioAhead, 5 * time.Second},
		{"audio 10s", shiftedWebM(t, mfWebM(t, 0, 0, 0, 48_000, 2, 5), av.MediaAudio, 10*time.Second), 10 * time.Second},
		{"audio 30s", shiftedWebM(t, mfWebM(t, 0, 0, 0, 48_000, 2, 5), av.MediaAudio, 30*time.Second), 30 * time.Second},
		{"video 30s", shiftedWebM(t, mfWebM(t, 32, 24, 3, 0, 0, 0), av.MediaVideo, 30*time.Second), 30 * time.Second},
	} {
		demuxer, err := webm.NewDemuxer(bytes.NewReader(tc.data), webm.DemuxerOptions{})
		if err != nil {
			t.Fatalf("%s: %v", tc.label, err)
		}
		packet := webm.Packet{Data: make([]byte, 0, 1<<16)}
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("%s: first packet: %v", tc.label, err)
		}
		// Opus starts its first packet a pre-skip early.
		if got := time.Duration(packet.TimeNS); got < tc.by-20*time.Millisecond || got > tc.by+20*time.Millisecond {
			t.Fatalf("%s starts at %v, want about %v", tc.label, got, tc.by)
		}
	}
}

// offlineOrderFirst holds each case's first output digest across every -cpu
// pass and GOMAXPROCS setting of the process.
var offlineOrderFirst sync.Map

// TestOfflineJoinsAndLaddersWriteTheSameBytes pins the offline order past a
// single mux: joins over files beside another file's stream, a promoted
// ladder from two files, and two files into one buffered mux write the same
// bytes on every run and at any GOMAXPROCS, though each file is read through
// a reader that jitters which goroutine gets ahead.
func TestOfflineJoinsAndLaddersWriteTheSameBytes(t *testing.T) {
	f := offlineOrderFixtures(t)
	in := func(name string, data []byte) goav.InputSpec {
		return goav.FileInput(name, jitterReader{bytes.NewReader(data)})
	}
	type outputs func(out func(string) goav.Destination) []goav.Output
	cases := []struct {
		name     string
		buffered bool // promoted to the buffered runner
		outputs  outputs
	}{
		{"mix beside a file's video", false, func(out func(string) goav.Destination) []goav.Output {
			o := out("o.webm")
			return []goav.Output{
				goav.Mix(in("a.webm", f.audioA).Audio().Decode(), in("b.webm", f.audioB).Audio().Decode()).Encode(codec.Opus()).To(o),
				in("c.webm", f.videoA).Video().To(o),
			}
		}},
		{"composite beside a file's audio", false, func(out func(string) goav.Destination) []goav.Output {
			o := out("o.webm")
			return []goav.Output{
				goav.Composite(in("a.webm", f.videoA).Video().Decode(), goav.At(in("b.webm", f.videoB).Video().Decode(), 8, 8)).Encode(codec.VP8()).To(o),
				in("c.webm", f.audioA).Audio().To(o),
			}
		}},
		{"ByPTS mix beside a file's video", false, func(out func(string) goav.Destination) []goav.Output {
			o := out("o.webm")
			return []goav.Output{
				goav.Mix(in("a.webm", f.audioA).Audio().Decode(), in("b.webm", f.audioB).Audio().Decode(), goav.ByPTS()).Encode(codec.Opus()).To(o),
				in("c.webm", f.videoA).Video().To(o),
			}
		}},
		{"ByPTS composite with an arm 5s ahead", false, func(out func(string) goav.Destination) []goav.Output {
			o := out("o.webm")
			return []goav.Output{
				goav.Composite(in("a.webm", f.videoA).Video().Decode(), goav.At(in("b.webm", f.videoAhead).Video().Decode(), 8, 8), goav.ByPTS()).Encode(codec.VP8()).To(o),
				in("c.webm", f.audioA).Audio().To(o),
			}
		}},
		{"promoted ladder from two files", true, func(out func(string) goav.Destination) []goav.Output {
			a := in("a.webm", f.videoA).Video().Decode()
			b := in("b.webm", f.videoB).Video().Decode()
			return []goav.Output{
				a.Encode(codec.VP8()).To(out("a-full.webm")),
				a.Resize(32, 24).Encode(codec.VP8()).To(out("a-half.webm")),
				b.Encode(codec.VP8()).To(out("b-full.webm")),
				b.Resize(16, 12).Encode(codec.VP8()).To(out("b-half.webm")),
			}
		}},
		{"two files into one buffered mux", true, func(out func(string) goav.Destination) []goav.Output {
			o := out("o.webm")
			return []goav.Output{
				in("a.webm", f.videoA).Video().Buffer(flow.Blocking(4)).To(o),
				in("b.webm", f.audioB).Audio().To(o),
			}
		}},
		{"two transcodes into one buffered mux", true, func(out func(string) goav.Destination) []goav.Output {
			o := out("o.webm")
			return []goav.Output{
				in("a.webm", f.videoA).Video().Decode().Buffer(flow.Blocking(4)).Encode(codec.VP8()).To(o),
				in("b.webm", f.audioB).Audio().Decode().Encode(codec.Opus()).To(o),
			}
		}},
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			report, err := bundle.Explain(context.Background(), tc.outputs(func(name string) goav.Destination { return goav.Write(name, io.Discard) })...)
			if err != nil {
				t.Fatalf("explain: %v", err)
			}
			buffered := false
			for _, decision := range report.Decisions {
				buffered = buffered || decision.Code == "buffered_execution"
			}
			if buffered != tc.buffered {
				t.Fatalf("buffered_execution = %v, want %v", buffered, tc.buffered)
			}
			for _, procs := range []int{1, 2, 8} {
				runtime.GOMAXPROCS(procs)
				for run := range 3 {
					// Each output gets its own buffer; the digest covers them in
					// the order the case names them.
					var names []string
					files := map[string]*bytes.Buffer{}
					outs := tc.outputs(func(name string) goav.Destination {
						names = append(names, name)
						files[name] = &bytes.Buffer{}
						return goav.Write(name, files[name])
					})
					mfRun(t, outs...)
					digest := sha256.New()
					for _, name := range names {
						digest.Write(files[name].Bytes())
					}
					sum := string(digest.Sum(nil))
					if first, loaded := offlineOrderFirst.LoadOrStore(tc.name, sum); loaded && first.(string) != sum {
						t.Fatalf("GOMAXPROCS=%d run %d wrote bytes that differ from the first run's", procs, run)
					}
				}
			}
		})
	}
}

// countingReader counts the bytes read through it and, at its first EOF,
// runs atEOF.
type countingReader struct {
	r     io.Reader
	read  atomic.Int64
	atEOF func()
	once  sync.Once
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.read.Add(int64(n))
	if err == io.EOF && c.atEOF != nil {
		c.once.Do(c.atEOF)
	}
	return n, err
}

// TestOfflineOrderHoldsAnInputFarAhead pins the memory bound: an input whose
// packets are all later than another's is held at its first packet, so by
// the time the earlier file is read to its end the later one has been read
// barely at all, instead of being pulled into queues and join buffers.
func TestOfflineOrderHoldsAnInputFarAhead(t *testing.T) {
	f := offlineOrderFixtures(t)
	for _, tc := range []struct {
		name    string
		outputs func(early, ahead goav.InputSpec) []goav.Output
		ahead   []byte
	}{
		{"buffered mux", func(early, ahead goav.InputSpec) []goav.Output {
			o := goav.Write("o.webm", io.Discard)
			return []goav.Output{early.Video().Buffer(flow.Blocking(4)).To(o), ahead.Audio().To(o)}
		}, f.audioAhead},
		{"ByPTS composite", func(early, ahead goav.InputSpec) []goav.Output {
			return []goav.Output{goav.Composite(early.Video().Decode(), goav.At(ahead.Video().Decode(), 8, 8), goav.ByPTS()).Encode(codec.VP8()).To(goav.Write("o.webm", io.Discard))}
		}, f.videoAhead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ahead := &countingReader{r: bytes.NewReader(tc.ahead)}
			var readAtEarlyEnd atomic.Int64
			readAtEarlyEnd.Store(-1)
			early := &countingReader{r: bytes.NewReader(f.videoLong), atEOF: func() { readAtEarlyEnd.Store(ahead.read.Load()) }}
			mfRun(t, tc.outputs(goav.FileInput("a.webm", early), goav.FileInput("b.webm", ahead))...)
			got := readAtEarlyEnd.Load()
			if got < 0 || ahead.read.Load() != int64(len(tc.ahead)) {
				t.Fatalf("early file reached EOF: %v; later file read %d of %d bytes", got >= 0, ahead.read.Load(), len(tc.ahead))
			}
			if got > int64(len(tc.ahead))/10 {
				t.Fatalf("the later file was %d of %d bytes in when the earlier one ended, want at most a tenth", got, len(tc.ahead))
			}
		})
	}
}

package kit

import (
	"bytes"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

var (
	testSPS = []byte{0x67, 0x64, 0x00, 0x1f, 0xac, 0xd9}
	testPPS = []byte{0x68, 0xeb, 0xe3, 0xcb}
	testIDR = []byte{0x65, 0x88, 0x84, 0x00, 0x00, 0x03, 0x01}
)

func TestAnnexBAndAVCCRoundTrip(t *testing.T) {
	units := [][]byte{testSPS, testPPS, testIDR}
	annexB := appendAnnexB(nil, units...)
	if got := nalUnits(annexB); !slices.EqualFunc(got, units, bytes.Equal) {
		t.Fatalf("nalUnits = %x", got)
	}
	avcc := appendAVCC(nil, units...)
	got, err := lengthPrefixed(avcc)
	if err != nil || !slices.EqualFunc(got, units, bytes.Equal) {
		t.Fatalf("lengthPrefixed = %x, %v", got, err)
	}
	if _, err := lengthPrefixed([]byte{0, 0, 0, 9, 1}); !errors.Is(err, errBitstream) {
		t.Fatalf("a length past the data = %v", err)
	}
	if !randomAccess(av.CodecH264, testIDR) || randomAccess(av.CodecH264, testSPS) || !parameterSet(av.CodecH264, testPPS) {
		t.Fatal("H.264 NAL classification")
	}
	if !randomAccess(av.CodecHEVC, []byte{19 << 1, 1}) || !parameterSet(av.CodecHEVC, []byte{33 << 1, 1}) {
		t.Fatal("HEVC NAL classification")
	}
	if nalFramed(av.CodecAV1, AnnexB) || nalFramed(av.CodecH264, Raw) || !nalFramed(av.CodecHEVC, AVCC) {
		t.Fatal("NAL framing applies only to H.264 and HEVC")
	}
}

func TestCodecConfigurationRecords(t *testing.T) {
	record := avcC([][]byte{testSPS, testPPS})
	if record[1] != 0x64 || record[3] != 0x1f || record[4] != 0xff {
		t.Fatalf("avcC header % x", record[:5])
	}
	sets, err := configParameterSets(av.CodecH264, record)
	if err != nil || !slices.EqualFunc(sets, [][]byte{testSPS, testPPS}, bytes.Equal) {
		t.Fatalf("avcC sets = %x, %v", sets, err)
	}
	sets, err = configParameterSets(av.CodecH264, appendAnnexB(nil, testSPS, testPPS))
	if err != nil || len(sets) != 2 {
		t.Fatalf("Annex B sets = %x, %v", sets, err)
	}
	vps, sps, pps := []byte{32 << 1, 1, 0xc}, []byte{33 << 1, 1, 0xd}, []byte{34 << 1, 1, 0xe}
	hvcc := append(make([]byte, 22), 3)
	hvcc[0] = 1
	for _, set := range [][]byte{vps, sps, pps} {
		hvcc = append(hvcc, set[0]>>1&0x3f, 0, 1, 0, byte(len(set)))
		hvcc = append(hvcc, set...)
	}
	sets, err = configParameterSets(av.CodecHEVC, hvcc)
	if err != nil || !slices.EqualFunc(sets, [][]byte{vps, sps, pps}, bytes.Equal) {
		t.Fatalf("hvcC sets = %x, %v", sets, err)
	}
}

func TestNV12RoundTripsThroughThePool(t *testing.T) {
	const w, h = 6, 4
	y, u, v := make([]byte, w*h), []byte{1, 2, 3, 4, 5, 6}, []byte{7, 8, 9, 10, 11, 12}
	for i := range y {
		y[i] = byte(i)
	}
	pic := Picture{Width: w, Height: h, Format: av.PixelFormatI420, Planes: [3][]byte{y, u, v}, Strides: [3]int{w, 3, 3}}
	nv12, _ := toNV12(pic, nil)
	if nv12.Format != NV12 || !bytes.Equal(nv12.Planes[1], []byte{1, 7, 2, 8, 3, 9, 4, 10, 5, 11, 6, 12}) {
		t.Fatalf("NV12 chroma = %v", nv12.Planes[1])
	}
	var frame av.Frame
	format, err := fillFrame(&frame, &nv12)
	if err != nil || format != av.PixelFormatI420 || !bytes.Equal(frame.Planes[1].Buffer.Bytes, u) || !bytes.Equal(frame.Planes[2].Buffer.Bytes, v) || !bytes.Equal(frame.Planes[0].Buffer.Bytes, y) {
		t.Fatalf("fillFrame = %s %v, planes %v", format, err, frame.Planes)
	}
	if frame.Planes[0].Buffer.Owner == nil {
		t.Fatal("a copied picture is not published in pooled planes")
	}
	frame.Release()
}

// countingOwner counts the references a lent picture's planes take.
type countingOwner struct{ refs int }

func (o *countingOwner) Retain()   { o.refs++ }
func (o *countingOwner) Release()  { o.refs-- }
func (o *countingOwner) Refs() int { return o.refs }

func TestLentPicturesPublishInPlace(t *testing.T) {
	owner := &countingOwner{refs: 1}
	y, c := make([]byte, 16), make([]byte, 4)
	pic := Picture{Width: 4, Height: 4, Format: av.PixelFormatI420, Planes: [3][]byte{y, c, c}, Strides: [3]int{4, 2, 2}, Owner: owner}
	var frame av.Frame
	if _, err := fillFrame(&frame, &pic); err != nil {
		t.Fatal(err)
	}
	if owner.refs != 4 || &frame.Planes[0].Buffer.Bytes[0] != &y[0] {
		t.Fatalf("lent planes: %d refs, copied %v", owner.refs, &frame.Planes[0].Buffer.Bytes[0] != &y[0])
	}
	frame.Release()
	owner.Release()
	if owner.refs != 0 {
		t.Fatalf("%d references left after every holder released", owner.refs)
	}
}

type params struct {
	Rate   int32   `c:"iRate"`
	On     bool    `c:"bOn"`
	Scale  float32 `c:"fScale"`
	Layers [2]struct {
		Width uint16 `c:"iWidth"`
	} `c:",first"`
	Inner struct {
		Mode int8 `c:"iMode"`
	} `c:",inline"`
	Hidden int32
}

func TestParamsReachEveryTaggedField(t *testing.T) {
	p := ParamsOf[params]()
	names := []string{}
	for _, knob := range p.Knobs() {
		names = append(names, knob.Name)
	}
	if !slices.Equal(names, []string{"bOn", "fScale", "iMode", "iRate", "iWidth"}) {
		t.Fatalf("knobs = %v", names)
	}
	var dst params
	if err := p.Apply(&dst, [][2]string{{"iRate", "-7"}, {"bOn", "true"}, {"fScale", "1.5"}, {"iWidth", "640"}, {"iMode", "3"}}); err != nil {
		t.Fatal(err)
	}
	if dst.Rate != -7 || !dst.On || dst.Scale != 1.5 || dst.Layers[0].Width != 640 || dst.Inner.Mode != 3 {
		t.Fatalf("applied %+v", dst)
	}
	desc := codec.Descriptor{ID: av.CodecH264, Backend: codec.BackendInfo{Name: "x"}, Knobs: []codec.Knob{}, RawKnobs: p.Knobs()}
	if _, err := desc.ResolveKnobs(codec.CodecSettings{Custom: av.Metadata{"x-params": "iMode=300"}}, false); !errors.Is(err, codec.ErrInvalidSetting) {
		t.Fatalf("an int8 out of range = %v", err)
	}
}

func TestBindRefusesWithTheFix(t *testing.T) {
	var syms struct {
		f func() `sym:"nothing_here"`
	}
	for name, lib := range map[string]Library{
		"empty":    {Name: "libx", Fix: "install libx", Symbols: &syms},
		"relative": {Name: "libx", Path: "libx.so", Fix: "install libx", Symbols: &syms},
		"missing":  {Name: "libx", Path: filepath.Join(t.TempDir(), "libx.so"), Fix: "install libx", Symbols: &syms},
	} {
		err := lib.Bind("x")
		var e *Error
		if !errors.As(err, &e) || !errors.Is(err, ErrUnavailable) || !errors.Is(err, codec.ErrUnavailable) || e.Fix != "install libx" {
			t.Errorf("%s: Bind = %v", name, err)
		}
	}
}

func TestHandlesForget(t *testing.T) {
	var h Handles[string]
	a, b := h.New("a"), h.New("b")
	if a == 0 || a == b {
		t.Fatalf("handles %d, %d", a, b)
	}
	if v, ok := h.Get(b); !ok || v != "b" {
		t.Fatal("Get")
	}
	h.Delete(a)
	h.Delete(b)
	if _, ok := h.Get(a); ok || h.Len() != 0 {
		t.Fatal("Delete")
	}
}

// decodeOrder is a session that returns each unit's picture at once: in
// decode order, as VideoToolbox does.
type decodeOrder struct{ y, c []byte }

func (s *decodeOrder) Decode(unit *Packet, emit func(*Picture) error) error {
	return emit(&Picture{Width: 4, Height: 4, Format: av.PixelFormatI420, Planes: [3][]byte{s.y, s.c, s.c}, Strides: [3]int{4, 2, 2}, PTS: unit.PTS})
}
func (s *decodeOrder) Flush(func(*Picture) error) error { return nil }
func (s *decodeOrder) Close() error                     { return nil }

// TestReorderPresentsInPTSOrder feeds an I P B B P B B pattern in decode
// order and pins that a Reorder backend's pictures leave in presentation
// order, each as soon as no later unit can present before it.
func TestReorderPresentsInPTSOrder(t *testing.T) {
	c := &Codec{ID: av.CodecAV1, Media: av.MediaVideo, Backend: "fake", Reorder: true,
		NewVideoDecoder: func(VideoConfig) (VideoDecoder, error) {
			return &decodeOrder{y: make([]byte, 16), c: make([]byte, 4)}, nil
		}}
	d := &videoDecoder{c: c, desc: c.descriptor(codec.ModeDecode)}
	base := av.TimeBase{Num: 1, Den: 1}
	if err := d.Open(t.Context(), codec.DecodeConfig{Stream: av.Stream{TimeBase: base, Codec: av.CodecParameters{ID: av.CodecAV1}}}); err != nil {
		t.Fatal(err)
	}
	// (pts, dts) in decode order; DTS runs two frames behind for the B-frames.
	units := [][2]int64{{0, -2}, {3, -1}, {1, 0}, {2, 1}, {6, 2}, {4, 3}, {5, 4}}
	var order []int64
	var result codec.DecodeResult
	for _, u := range units {
		packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}}, PTS: av.Timestamp{Value: u[0], Base: base}, DTS: av.Timestamp{Value: u[1], Base: base}, Keyframe: u[0] == 0}
		if err := d.DecodeInto(t.Context(), &packet, &result); err != nil {
			t.Fatal(err)
		}
		for _, f := range result.Frames {
			order = append(order, f.PTS.Value)
			f.Release()
		}
		result.Reset()
	}
	if err := d.FlushInto(t.Context(), &result); err != nil {
		t.Fatal(err)
	}
	for _, f := range result.Frames {
		order = append(order, f.PTS.Value)
	}
	if !slices.Equal(order, []int64{0, 1, 2, 3, 4, 5, 6}) {
		t.Fatalf("presentation order %v", order)
	}
}

// damagedAt is a session whose library rejects the unit at one PTS.
type damagedAt struct {
	decodeOrder
	pts []int64
}

func (s *damagedAt) Decode(unit *Packet, emit func(*Picture) error) error {
	if slices.Contains(s.pts, unit.PTS) {
		return Damaged("fake", "decode", -1)
	}
	return s.decodeOrder.Decode(unit, emit)
}

// TestDamagedUnitIsDroppedAndHealedByAKeyframe pins the recovery from a
// unit the library rejects: the unit is dropped, upstream is asked for a
// keyframe once until a keyframe heals the stream, and the units after it
// still decode.
func TestDamagedUnitIsDroppedAndHealedByAKeyframe(t *testing.T) {
	c := &Codec{ID: av.CodecAV1, Media: av.MediaVideo, Backend: "fake",
		NewVideoDecoder: func(VideoConfig) (VideoDecoder, error) {
			return &damagedAt{decodeOrder{y: make([]byte, 16), c: make([]byte, 4)}, []int64{2, 3, 5}}, nil
		}}
	d := &videoDecoder{c: c, desc: c.descriptor(codec.ModeDecode)}
	base := av.TimeBase{Num: 1, Den: 1}
	config := codec.DecodeConfig{Stream: av.Stream{TimeBase: base, Codec: av.CodecParameters{ID: av.CodecAV1}}}
	config.Resilience.RequestKeyframes = true
	if err := d.Open(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	var decoded []int64
	requests := 0
	var result codec.DecodeResult
	for pts := range int64(7) {
		packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}}, PTS: av.Timestamp{Value: pts, Base: base}, Keyframe: pts == 0 || pts == 4}
		if err := d.DecodeInto(t.Context(), &packet, &result); err != nil {
			t.Fatalf("unit %d: %v", pts, err)
		}
		for _, f := range result.Frames {
			decoded = append(decoded, f.PTS.Value)
			f.Release()
		}
		for _, r := range result.Requests {
			if r.Type == codec.ControlRequestKeyframe {
				requests++
			}
		}
		result.Reset()
	}
	if !slices.Equal(decoded, []int64{0, 1, 4, 6}) || requests != 2 {
		t.Fatalf("decoded %v with %d keyframe requests, want [0 1 4 6] and 2", decoded, requests)
	}
}

func TestWindowComparesNumbers(t *testing.T) {
	w := Window{From: "2.3", Before: "3"}
	for version, want := range map[string]bool{"2.3.0": true, "2.10.1": true, "2.2.9": false, "3": false, "3.0.1": false} {
		if w.Contains(version) != want {
			t.Errorf("%v contains %s = %v", w, version, !want)
		}
	}
	if !(Window{}).Contains("0.1") || !(Window{From: "1"}).Contains("10") {
		t.Error("an open bound refused a version")
	}
}

// zlib is a library every macOS and Linux host has, to bind for real.
type zlib struct {
	bound func(uint64) uint64 `sym:"compressBound"`
}

func zlibLibrary(t *testing.T, symbols *zlib) Library {
	t.Helper()
	path := find("/usr/lib/libz.1.dylib", "/lib/x86_64-linux-gnu/libz.so.1", "/usr/lib/x86_64-linux-gnu/libz.so.1", "/lib/aarch64-linux-gnu/libz.so.1", "/usr/lib/aarch64-linux-gnu/libz.so.1", "/usr/lib64/libz.so.1")
	if path == "" {
		t.Skip("no zlib")
	}
	return Library{Name: "libz", Path: path, Fix: "install zlib", Symbols: symbols}
}

// TestLibrariesBindOncePerFile registers many declarations of one library,
// each with its own symbols, and pins that the file binds once.
func TestLibrariesBindOncePerFile(t *testing.T) {
	path := zlibLibrary(t, &zlib{}).Path
	for range 50 {
		symbols := &zlib{}
		c := Codec{ID: "fake", Media: av.MediaVideo, Backend: "fake", Libraries: []Library{zlibLibrary(t, symbols)},
			NewVideoDecoder: func(VideoConfig) (VideoDecoder, error) { return nil, nil }}
		if err := c.bind(); err != nil || symbols.bound(100) < 100 {
			t.Fatalf("bind = %v", err)
		}
	}
	files := 0
	boundLibraries.Range(func(key, _ any) bool {
		if key.(boundKey).path == path {
			files++
		}
		return true
	})
	if files != 1 {
		t.Fatalf("%s bound %d times", path, files)
	}
}

// TestStatusesSayWhyABackendCannotServe pins the status vocabulary and the
// fix each status carries.
func TestStatusesSayWhyABackendCannotServe(t *testing.T) {
	declare := func(lib Library, abi Window) Codec {
		return Codec{ID: "fake", Media: av.MediaVideo, Backend: "fake", Libraries: []Library{lib}, ABI: abi,
			Version:         func() (string, error) { return "1.2", nil },
			Releases:        []Release{{Version: "2.0"}},
			NewVideoDecoder: func(VideoConfig) (VideoDecoder, error) { return nil, nil }}
	}
	installed := zlibLibrary(t, &zlib{})
	missing := Library{Name: "libfake", System: []string{filepath.Join(t.TempDir(), "libfake.so")}, Fix: "install libfake", Symbols: &zlib{}}
	for _, tc := range []struct {
		c              Codec
		disable        string
		status, reason string
	}{
		{declare(installed, Window{}), "", statusActive, ""},
		{declare(missing, Window{}), "", statusNotInstalled, "fix: goav codecs install fake, or install libfake"},
		{declare(installed, Window{From: "2"}), "", statusOutsideWindow, "1.2 is outside the releases this backend mirrors (2 or later)"},
		{declare(installed, Window{}), "other, fake", statusDisabled, "is turned off by " + disableEnv},
	} {
		t.Setenv(disableEnv, tc.disable)
		err := tc.c.bind()
		status := statusActive
		if err != nil {
			status = statusOf(err)
		}
		if status != tc.status || (tc.reason != "" && (err == nil || !strings.Contains(err.Error(), tc.reason))) {
			t.Errorf("status %s (%v), want %s (%s)", status, err, tc.status, tc.reason)
		}
	}
}

// TestDeclarationsReadTheirOriginAndTakeOnlyTheirKnobs pins two things a
// backend author no longer writes: where the code lives, read from the
// constructors, and that a backend declaring no knobs takes no settings.
func TestDeclarationsReadTheirOriginAndTakeOnlyTheirKnobs(t *testing.T) {
	c := Codec{ID: "fake", Media: av.MediaVideo, Backend: "fake", NewVideoDecoder: func(VideoConfig) (VideoDecoder, error) { return nil, nil }}
	if err := c.bind(); err != nil {
		t.Fatal(err)
	}
	desc := c.descriptor(codec.ModeDecode)
	if desc.Backend.Package != "github.com/thesyncim/goav/libcodec/kit" || desc.Backend.Module != "github.com/thesyncim/goav/libcodec" {
		t.Errorf("origin %s in %s", desc.Backend.Package, desc.Backend.Module)
	}
	if err := desc.Check(codec.Need{Settings: map[string]string{"speed": "4"}}); err == nil {
		t.Error("a backend without knobs took a setting")
	}
}

package videotoolbox

import (
	"runtime"
	"sync"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/libcodec/kit"
)

// decoder is one VTDecompressionSession. Each access unit becomes a
// CMSampleBuffer stamped with its PTS; VideoToolbox returns pictures in
// decode order through the output callback, which retains each CVPixelBuffer
// and queues it, and the kit reorders them. Pictures are lent to goav in
// place: the buffer stays locked and retained until the last plane is
// released.
type decoder struct {
	f        *frameworks
	id       av.CodecID
	session  uintptr
	format   uintptr
	handle   uintptr
	record   callbackRecord
	num, den int64

	mu     sync.Mutex
	queue  []picture
	closed bool
}

type picture struct {
	status int32
	image  uintptr
	pts    int64
}

const (
	readOnly        = 1 // kCVPixelBufferLock_ReadOnly
	assureMemoryNow = 1 // kCMBlockBufferAssureMemoryNowFlag
)

// decoded is VTDecompressionOutputCallback: its two trailing CMTime
// arguments are passed by value and never read, so they are not declared.
func decoded(refcon, frame uintptr, status int32, _ uint32, image uintptr) {
	d, ok := decoderSessions.Get(refcon)
	if !ok {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	if image != 0 {
		d.f.cf.retain(image)
	}
	d.queue = append(d.queue, picture{status: status, image: image, pts: int64(frame)})
}

func newDecoder(f *frameworks, id av.CodecID, config kit.VideoConfig) (kit.VideoDecoder, error) {
	d := &decoder{f: f, id: id, num: max(config.TimeBase.Num, 1), den: config.TimeBase.Den}
	if err := d.formatFrom(config.ParameterSets); err != nil {
		return nil, err
	}
	d.handle = decoderSessions.New(d)
	spec := f.dictionary()
	defer f.cf.release(spec)
	f.cf.dictionarySet(spec, f.vt.requireHardwareDecoder, f.cf.booleanTrue)
	attributes := f.dictionary()
	defer f.cf.release(attributes)
	f.setOwned(attributes, f.cv.formatKey, f.int32Number(pixelI420))
	f.setOwned(attributes, f.cv.surfaceKey, f.dictionary())
	d.record = callbackRecord{Callback: decodedCallback(), Refcon: d.handle}
	if s := f.vt.decompressionCreate(0, d.format, spec, attributes, &d.record, &d.session); s != 0 {
		d.Close()
		return nil, &kit.Error{Backend: Name, Op: "open", Name: "VTDecompressionSessionCreate", Reason: "refused the stream (status " + status(s) + ")", Fix: "VideoToolbox decodes 8-bit 4:2:0 " + string(id) + " in hardware; use another backend for other formats"}
	}
	return d, nil
}

// formatFrom builds the CMVideoFormatDescription of the parameter sets.
func (d *decoder) formatFrom(sets [][]byte) error {
	if len(sets) == 0 {
		return &kit.Error{Backend: Name, Op: "open", Reason: "has no parameter sets to describe the stream"}
	}
	var pinner runtime.Pinner
	defer pinner.Unpin()
	pointers := make([]unsafe.Pointer, len(sets))
	sizes := make([]int, len(sets))
	for i, set := range sets {
		pinner.Pin(&set[0])
		pointers[i], sizes[i] = unsafe.Pointer(&set[0]), len(set)
	}
	var s int32
	if d.id == hevc {
		if d.f.cm.formatFromHEVC == nil {
			return &kit.Error{Backend: Name, Op: "open", Name: "hevc", Reason: "is not offered by this macOS"}
		}
		s = d.f.cm.formatFromHEVC(0, len(sets), &pointers[0], &sizes[0], 4, 0, &d.format)
	} else {
		s = d.f.cm.formatFromH264(0, len(sets), &pointers[0], &sizes[0], 4, &d.format)
	}
	if s != 0 {
		return kit.Fail(Name, "open", "CMVideoFormatDescriptionCreateFrom parameter sets", int64(s))
	}
	return nil
}

func (d *decoder) Decode(unit *kit.Packet, emit func(*kit.Picture) error) error {
	f := d.f
	var block, sample uintptr
	if s := f.cm.blockCreate(0, 0, len(unit.Data), 0, 0, 0, len(unit.Data), assureMemoryNow, &block); s != 0 {
		return kit.Fail(Name, "decode", "CMBlockBufferCreateWithMemoryBlock", int64(s))
	}
	defer f.cf.release(block)
	if s := f.cm.blockReplace(unsafe.Pointer(&unit.Data[0]), block, 0, len(unit.Data)); s != 0 {
		return kit.Fail(Name, "decode", "CMBlockBufferReplaceDataBytes", int64(s))
	}
	timing := cmTimingInfo{PTS: timeAt(unit.PTS*d.num, d.den)}
	size := len(unit.Data)
	if s := f.cm.sampleCreate(0, block, d.format, 1, 1, &timing, 1, &size, &sample); s != 0 {
		return kit.Fail(Name, "decode", "CMSampleBufferCreateReady", int64(s))
	}
	defer f.cf.release(sample)
	var info uint32
	s := f.vt.decodeFrame(d.session, sample, 0, uintptr(unit.PTS), &info)
	err := d.drain(emit)
	if s != 0 && err == nil {
		err = decodeFailure("VTDecompressionSessionDecodeFrame", s)
	}
	return err
}

func (d *decoder) Flush(emit func(*kit.Picture) error) error {
	d.f.vt.finishDelayed(d.session)
	d.f.vt.waitAsync(d.session)
	return d.drain(emit)
}

// drain lends the queued pictures in decode order and releases each queued
// image exactly once, through its owner when lent or here when not. A
// picture VideoToolbox dropped is skipped; one it could not decode (bad
// data) is dropped too, and reported once the rest are lent, so the kit
// asks for a keyframe and decoding goes on. Any other failure ends the
// session's run with the library's status.
func (d *decoder) drain(emit func(*kit.Picture) error) error {
	d.mu.Lock()
	queue := d.queue
	d.queue = nil
	d.mu.Unlock()
	var failure error
	for i, p := range queue {
		switch {
		case p.status != 0:
			if p.image != 0 {
				d.f.cf.release(p.image)
			}
			if failure == nil {
				failure = decodeFailure("the decompression output callback", p.status)
			}
		case p.image == 0:
		default:
			if err := d.lend(p, emit); err != nil {
				d.releaseImages(queue[i+1:])
				return err
			}
		}
	}
	return failure
}

func (d *decoder) releaseImages(queue []picture) {
	for _, p := range queue {
		if p.image != 0 {
			d.f.cf.release(p.image)
		}
	}
}

// decodeFailure is the error for a decode status: bad data drops the unit,
// anything else fails the decode.
func decodeFailure(call string, s int32) error {
	switch s {
	case -12909, -8969: // kVTVideoDecoderBadDataErr, codecBadDataErr
		return kit.Damaged(Name, call, int64(s))
	}
	return kit.Fail(Name, "decode", call, int64(s))
}

// lend publishes one decoded picture in place, its CVPixelBuffer locked and
// owned by the planes.
func (d *decoder) lend(p picture, emit func(*kit.Picture) error) error {
	f := d.f
	owner := &pixelOwner{f: f, buffer: p.image}
	owner.refs.Store(1)
	defer owner.Release()
	if s := f.cv.lock(p.image, readOnly); s != 0 {
		owner.refs.Store(0)
		f.cf.release(p.image)
		return kit.Fail(Name, "decode", "CVPixelBufferLockBaseAddress", int64(s))
	}
	if format := f.cv.pixelFormat(p.image); format != pixelI420 || f.cv.planeCount(p.image) != 3 {
		return &kit.Error{Backend: Name, Op: "decode", Reason: "returned pixel format " + fourCCString(format) + ", not y420"}
	}
	pic := kit.Picture{
		Width:  f.cv.width(p.image),
		Height: f.cv.height(p.image),
		Format: av.PixelFormatI420,
		Owner:  owner,
		PTS:    p.pts,
	}
	for i := range 3 {
		stride, rows := f.cv.planeStride(p.image, i), f.cv.planeHeight(p.image, i)
		pic.Planes[i] = unsafe.Slice((*byte)(f.cv.planeBase(p.image, i)), stride*rows)
		pic.Strides[i] = stride
	}
	return emit(&pic)
}

func fourCCString(v uint32) string {
	return string([]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
}

func (d *decoder) Close() error {
	f := d.f
	if d.session != 0 {
		f.vt.waitAsync(d.session)
		f.vt.decompressionEnd(d.session)
		f.cf.release(d.session)
		d.session = 0
	}
	d.mu.Lock()
	d.closed = true
	queue := d.queue
	d.queue = nil
	d.mu.Unlock()
	d.releaseImages(queue)
	if d.format != 0 {
		f.cf.release(d.format)
		d.format = 0
	}
	if d.handle != 0 {
		decoderSessions.Delete(d.handle)
		d.handle = 0
	}
	return nil
}

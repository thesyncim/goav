package openh264

import (
	"strconv"
	"unsafe"

	"github.com/thesyncim/goav/native/kit"
)

func kitItoa(n int64) string { return strconv.FormatInt(n, 10) }

// decoder is one ISVCDecoder session. Each access unit carries its PTS in
// uiInBsTimeStamp, which OpenH264 hands back on the picture it decodes.
type decoder struct {
	lib    *library
	handle uintptr
	vt     *decoderVtable
	info   bufferInfo
	dst    [3]*byte
}

func (l *library) decoderTable(handle uintptr) (*decoderVtable, error) {
	l.decoderOnce.Do(func() {
		l.decoderErr = kit.BindVTable(handle, &l.decoder)
	})
	return &l.decoder, l.decoderErr
}

func newDecoder(l *library, config kit.VideoConfig) (kit.VideoDecoder, error) {
	d := &decoder{lib: l}
	if status := l.createDecoder(&d.handle); status != 0 || d.handle == 0 {
		return nil, kit.Fail(Name, "open", "WelsCreateDecoder", int64(status))
	}
	vt, err := l.decoderTable(vtableOf(d.handle))
	if err != nil {
		l.destroyDecoder(d.handle)
		return nil, err
	}
	d.vt = vt
	knobs := config.Knobs
	trace := logLevels[knobs.String("log")]
	d.vt.SetOption(d.handle, decoderOptionTraceLevel, unsafe.Pointer(&trace))
	param := decodingParam{
		TargetDqLayer:     0xff,
		EcActiveIdc:       map[string]int32{"off": 0, "frame": 1, "slice": 2}[knobs.String("conceal")],
		VideoPropertySize: 8,
		VideoBsType:       0, // VIDEO_BITSTREAM_AVC
	}
	if status := d.vt.Initialize(d.handle, &param); status != 0 {
		d.Close()
		return nil, kit.Fail(Name, "open", "Initialize", int64(status))
	}
	if threads := int32(knobs.Int("threads")); threads > 0 {
		d.vt.SetOption(d.handle, decoderOptionThreads, unsafe.Pointer(&threads))
	}
	return d, nil
}

func (d *decoder) Decode(unit *kit.Packet, emit func(*kit.Picture) error) error {
	d.info = bufferInfo{InTimeStamp: uint64(unit.PTS)}
	d.dst = [3]*byte{}
	state := d.vt.DecodeFrameNoDelay(d.handle, unsafe.SliceData(unit.Data), int32(len(unit.Data)), &d.dst, &d.info)
	if state != 0 && d.info.BufferStatus != 1 {
		// A damaged unit decodes nothing; the kit asks for a keyframe on
		// loss, so report it only when the library says it is unusable.
		if state&0x1000 != 0 || state&0x4000 != 0 {
			return kit.Fail(Name, "decode", "DecodeFrameNoDelay", int64(state))
		}
		return nil
	}
	return d.output(emit)
}

func (d *decoder) output(emit func(*kit.Picture) error) error {
	if d.info.BufferStatus != 1 || d.dst[0] == nil {
		return nil
	}
	s := d.info.System
	w, h := int(s.Width), int(s.Height)
	ys, cs := int(s.Stride[0]), int(s.Stride[1])
	ch := (h + 1) / 2
	pic := kit.Picture{
		Width:   w,
		Height:  h,
		Format:  kit.I420,
		Planes:  [3][]byte{unsafe.Slice(d.dst[0], ys*h), unsafe.Slice(d.dst[1], cs*ch), unsafe.Slice(d.dst[2], cs*ch)},
		Strides: [3]int{ys, cs, cs},
		PTS:     int64(d.info.OutTimeStamp),
	}
	return emit(&pic)
}

// Flush drains the pictures the decoder holds for reordering.
func (d *decoder) Flush(emit func(*kit.Picture) error) error {
	for range 32 {
		d.info = bufferInfo{}
		d.dst = [3]*byte{}
		d.vt.FlushFrame(d.handle, &d.dst, &d.info)
		if d.info.BufferStatus != 1 {
			return nil
		}
		if err := d.output(emit); err != nil {
			return err
		}
	}
	return nil
}

func (d *decoder) Close() error {
	if d.handle == 0 {
		return nil
	}
	if d.vt != nil {
		d.vt.Uninitialize(d.handle)
	}
	d.lib.destroyDecoder(d.handle)
	d.handle = 0
	return nil
}

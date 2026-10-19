package h264

import "github.com/thesyncim/goav/internal/bitio"

// SPS is what a sequence parameter set says about the picture: the profile
// and level, and the display size after cropping.
type SPS struct {
	Profile byte
	Level   byte
	Width   int
	Height  int
}

// ParseSPS reads an SPS NAL unit (its header byte included, no start code).
func ParseSPS(nal []byte) (SPS, error) {
	if len(nal) < 4 || nal[0]&0x1f != NALSPS {
		return SPS{}, ErrInvalid
	}
	r := fieldReader{r: bitio.Reader{Data: RBSP(nal[1:]), Err: ErrInvalid}}
	profile := r.bits(8)
	r.bits(8) // constraint flags
	level := r.bits(8)
	r.ue() // seq_parameter_set_id
	chroma := uint64(1)
	separate := false
	switch profile {
	case 100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135:
		chroma = r.ue()
		if chroma == 3 {
			separate = r.flag()
		}
		r.ue()   // bit_depth_luma_minus8
		r.ue()   // bit_depth_chroma_minus8
		r.flag() // qpprime_y_zero_transform_bypass_flag
		if r.flag() {
			lists := 8
			if chroma == 3 {
				lists = 12
			}
			for i := 0; i < lists && r.err == nil; i++ {
				if r.flag() {
					size := 16
					if i >= 6 {
						size = 64
					}
					r.skipScalingList(size)
				}
			}
		}
	}
	r.ue() // log2_max_frame_num_minus4
	switch r.ue() {
	case 0:
		r.ue() // log2_max_pic_order_cnt_lsb_minus4
	case 1:
		r.flag() // delta_pic_order_always_zero_flag
		r.se()   // offset_for_non_ref_pic
		r.se()   // offset_for_top_to_bottom_field
		cycle := r.ue()
		if cycle > 255 {
			return SPS{}, ErrInvalid
		}
		for range cycle {
			r.se()
		}
	}
	r.ue()   // max_num_ref_frames
	r.flag() // gaps_in_frame_num_value_allowed_flag
	widthMBs := r.ue()
	heightUnits := r.ue()
	frameMBsOnly := r.flag()
	if !frameMBsOnly {
		r.flag() // mb_adaptive_frame_field_flag
	}
	r.flag() // direct_8x8_inference_flag
	var left, right, top, bottom uint64
	if r.flag() {
		left, right, top, bottom = r.ue(), r.ue(), r.ue(), r.ue()
	}
	if r.err != nil || widthMBs >= 1<<12 || heightUnits >= 1<<12 || chroma > 3 {
		return SPS{}, ErrInvalid
	}
	fieldFactor := uint64(2)
	if frameMBsOnly {
		fieldFactor = 1
	}
	width := (widthMBs + 1) * 16
	height := fieldFactor * (heightUnits + 1) * 16
	cropX, cropY := uint64(1), fieldFactor
	if !separate && chroma != 0 {
		if chroma != 3 {
			cropX = 2
		}
		if chroma == 1 {
			cropY *= 2
		}
	}
	if left+right >= width || top+bottom >= height || (left+right)*cropX >= width || (top+bottom)*cropY >= height {
		return SPS{}, ErrInvalid
	}
	return SPS{
		Profile: byte(profile),
		Level:   byte(level),
		Width:   int(width - (left+right)*cropX),
		Height:  int(height - (top+bottom)*cropY),
	}, nil
}

// fieldReader keeps the first read error, so a parse reads its fields
// straight through and checks once.
type fieldReader struct {
	r   bitio.Reader
	err error
}

func (f *fieldReader) keep(err error) {
	if f.err == nil {
		f.err = err
	}
}

func (f *fieldReader) bits(n int) uint64 {
	v, err := f.r.Bits(n)
	f.keep(err)
	return v
}

func (f *fieldReader) flag() bool { return f.bits(1) == 1 }

func (f *fieldReader) ue() uint64 {
	v, err := f.r.UE()
	f.keep(err)
	return v
}

func (f *fieldReader) se() int64 {
	v, err := f.r.SE()
	f.keep(err)
	return v
}

func (f *fieldReader) skipScalingList(size int) {
	last, next := int64(8), int64(8)
	for range size {
		if next != 0 {
			next = (last + f.se() + 256) % 256
		}
		if next != 0 {
			last = next
		}
	}
}

// RBSP returns the payload of a NAL unit with its emulation-prevention bytes
// (the 0x03 of each 0x000003) removed. It copies only when there are some.
func RBSP(payload []byte) []byte {
	var out []byte
	zeros, from := 0, 0
	for i, b := range payload {
		if zeros >= 2 && b == 3 {
			out = append(out, payload[from:i]...)
			from, zeros = i+1, 0
			continue
		}
		if b == 0 {
			zeros++
		} else {
			zeros = 0
		}
	}
	if out == nil {
		return payload
	}
	return append(out, payload[from:]...)
}

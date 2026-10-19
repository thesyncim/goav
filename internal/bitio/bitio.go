// Package bitio reads the big-endian bit fields codec headers are made of.
package bitio

// Reader reads bit fields from Data, most significant bit first. Reading
// past the end reports Err, so each caller keeps its own error contract.
type Reader struct {
	Data []byte
	Err  error
	bit  int
}

// Bits reads an n-bit unsigned field, n at most 64.
func (r *Reader) Bits(n int) (uint64, error) {
	if n < 0 || n > 64 || len(r.Data)*8-r.bit < n {
		return 0, r.Err
	}
	var value uint64
	for range n {
		shift := 7 - (r.bit & 7)
		value = value<<1 | uint64(r.Data[r.bit>>3]>>shift&1)
		r.bit++
	}
	return value, nil
}

// Bool reads one bit.
func (r *Reader) Bool() (bool, error) {
	value, err := r.Bits(1)
	return value != 0, err
}

// SkipUVLC skips an unsigned variable-length code.
func (r *Reader) SkipUVLC() error {
	zeros := 0
	for {
		bit, err := r.Bits(1)
		if err != nil {
			return err
		}
		if bit == 1 {
			break
		}
		if zeros++; zeros >= 32 {
			return nil
		}
	}
	_, err := r.Bits(zeros)
	return err
}

// UE reads an unsigned Exp-Golomb code, the ue(v) of H.264 and HEVC
// parameter sets.
func (r *Reader) UE() (uint64, error) {
	zeros := 0
	for {
		bit, err := r.Bits(1)
		if err != nil {
			return 0, err
		}
		if bit == 1 {
			break
		}
		if zeros++; zeros > 31 {
			return 0, r.Err
		}
	}
	value, err := r.Bits(zeros)
	return 1<<zeros - 1 + value, err
}

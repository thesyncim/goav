package ts

import (
	"encoding/binary"
	"testing"
)

// validation is what validateTS learned about a stream it accepted.
type validation struct {
	packets  int
	nulls    int
	pcrs     []pcrAt // every PCR, with the index of its packet
	pes      map[uint16]int
	psiGaps  []int64 // PCR time between PAT repetitions
	pmtPID   uint16
	pcrPID   uint16
	streamTy map[uint16]byte
}

type pcrAt struct {
	index int
	value int64
}

// validateTS checks a transport stream structurally, independently of the
// muxer's code: packet framing, per-PID continuity counters, PAT and PMT
// sections and their CRC-32/MPEG-2 (computed bit by bit here), PSI before
// media, PCR intervals of at most 40ms, a PCR that never runs ahead of a
// DTS, and PTS at or after DTS.
func validateTS(t *testing.T, data []byte) validation {
	t.Helper()
	if len(data) == 0 || len(data)%188 != 0 {
		t.Fatalf("stream of %d bytes is not whole 188-byte packets", len(data))
	}
	v := validation{pes: map[uint16]int{}, streamTy: map[uint16]byte{}, pmtPID: 0xffff, pcrPID: 0xffff}
	cc := map[uint16]int{}
	lastPCR, lastPAT := int64(-1), int64(-1)
	for i := 0; i < len(data); i += 188 {
		p := data[i : i+188]
		index := i / 188
		v.packets++
		if p[0] != 0x47 {
			t.Fatalf("packet %d: sync byte %#x", index, p[0])
		}
		if p[1]&0x80 != 0 || p[3]&0xc0 != 0 {
			t.Fatalf("packet %d: error or scrambling bits set", index)
		}
		pid := binary.BigEndian.Uint16(p[1:]) & 0x1fff
		start := p[1]&0x40 != 0
		control := p[3] >> 4 & 3
		counter := int(p[3] & 0xf)
		if control == 0 {
			t.Fatalf("packet %d: reserved adaptation_field_control", index)
		}
		if pid == 0x1fff {
			v.nulls++
			continue
		}
		payload := p[4:]
		discontinuity := false
		if control&2 != 0 {
			n := int(p[4])
			if control == 2 && n != 183 || control == 3 && n > 182 {
				t.Fatalf("packet %d: adaptation field length %d with control %d", index, n, control)
			}
			if n > 0 {
				flags := p[5]
				discontinuity = flags&0x80 != 0
				if flags&0x10 != 0 {
					if n < 7 {
						t.Fatalf("packet %d: PCR flag in a %d-byte adaptation field", index, n)
					}
					b := p[6:]
					base := int64(b[0])<<25 | int64(b[1])<<17 | int64(b[2])<<9 | int64(b[3])<<1 | int64(b[4]>>7)
					pcr := base*300 + (int64(b[4]&1)<<8 | int64(b[5]))
					if pid != v.pcrPID {
						t.Fatalf("packet %d: PCR on PID %d, the PMT names %d", index, pid, v.pcrPID)
					}
					if lastPCR >= 0 && !discontinuity {
						if gap := clockDiff(pcr, lastPCR); gap < 0 || gap > 40*27000 {
							t.Fatalf("packet %d: PCR gap %.2fms", index, float64(gap)/27000)
						}
					}
					lastPCR = pcr
					v.pcrs = append(v.pcrs, pcrAt{index, pcr})
				}
				for j := 6; j < 5+n && flags == 0; j++ {
					if p[j] != 0xff {
						t.Fatalf("packet %d: stuffing byte %#x", index, p[j])
					}
				}
			}
			payload = p[5+n:]
		}
		if control&1 != 0 {
			if prev, ok := cc[pid]; ok && !discontinuity && counter != (prev+1)&0xf {
				t.Fatalf("packet %d PID %d: continuity counter %d after %d", index, pid, counter, prev)
			}
			cc[pid] = counter
		} else if prev, ok := cc[pid]; ok && counter != prev {
			t.Fatalf("packet %d PID %d: an adaptation-only packet moved the counter %d to %d", index, pid, prev, counter)
		}
		if control&1 == 0 || !start {
			continue
		}
		switch {
		case pid == 0 || pid == v.pmtPID:
			if payload[0] != 0 {
				t.Fatalf("packet %d: pointer_field %d", index, payload[0])
			}
			s := payload[1:]
			n := 3 + int(binary.BigEndian.Uint16(s[1:])&0x0fff)
			section := s[:n]
			if got, want := bitwiseCRC(section[:n-4]), binary.BigEndian.Uint32(section[n-4:]); got != want {
				t.Fatalf("packet %d: section CRC %#x, computed %#x", index, want, got)
			}
			if pid == 0 {
				if section[0] != 0 {
					t.Fatalf("packet %d: PAT table id %d", index, section[0])
				}
				v.pmtPID = binary.BigEndian.Uint16(section[10:]) & 0x1fff
				if lastPAT >= 0 && lastPCR >= 0 {
					v.psiGaps = append(v.psiGaps, clockDiff(lastPCR, lastPAT))
				}
				lastPAT = lastPCR
				continue
			}
			if section[0] != 2 {
				t.Fatalf("packet %d: PMT table id %d", index, section[0])
			}
			v.pcrPID = binary.BigEndian.Uint16(section[8:]) & 0x1fff
			info := int(binary.BigEndian.Uint16(section[10:]) & 0x0fff)
			for es := section[12+info : n-4]; len(es) >= 5; {
				pid := binary.BigEndian.Uint16(es[1:]) & 0x1fff
				v.streamTy[pid] = es[0]
				es = es[5+int(binary.BigEndian.Uint16(es[3:])&0x0fff):]
			}
		default:
			if _, ok := v.streamTy[pid]; !ok {
				t.Fatalf("packet %d: PES on PID %d before a PMT names it", index, pid)
			}
			if payload[0] != 0 || payload[1] != 0 || payload[2] != 1 {
				t.Fatalf("packet %d: PES start code missing", index)
			}
			v.pes[pid]++
			flags := payload[7] >> 6
			pts := pesTime(payload[9:])
			dts := pts
			if flags == 3 {
				dts = pesTime(payload[14:])
			}
			if flags < 2 {
				t.Fatalf("packet %d: PES without a PTS", index)
			}
			if clockDiff(pts*300, dts*300) < 0 {
				t.Fatalf("packet %d: PTS %d before DTS %d", index, pts, dts)
			}
			if lastPCR >= 0 && clockDiff(dts*300, lastPCR) < 0 {
				t.Fatalf("packet %d: PCR %d ahead of DTS %d", index, lastPCR/300, dts)
			}
		}
	}
	if len(v.pcrs) == 0 {
		t.Fatal("no PCR")
	}
	return v
}

// clockDiff is a-b on the 33-bit (times 300) PCR clock, across a wrap.
func clockDiff(a, b int64) int64 {
	const m = int64(1) << 33 * 300
	d := ((a-b)%m + m) % m
	if d >= m/2 {
		d -= m
	}
	return d
}

func pesTime(b []byte) int64 {
	return int64(b[0]>>1&7)<<30 | int64(b[1])<<22 | int64(b[2]>>1)<<15 | int64(b[3])<<7 | int64(b[4]>>1)
}

// bitwiseCRC is CRC-32/MPEG-2 one bit at a time, a second implementation
// to hold the muxer's table to.
func bitwiseCRC(b []byte) uint32 {
	c := uint32(0xffffffff)
	for _, v := range b {
		for i := 7; i >= 0; i-- {
			bit := uint32(v>>i&1) ^ c>>31
			c <<= 1
			if bit == 1 {
				c ^= 0x04c11db7
			}
		}
	}
	return c
}

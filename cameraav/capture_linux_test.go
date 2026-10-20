//go:build linux && (amd64 || arm64)

package cameraav

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"golang.org/x/sys/unix"
)

func TestV4L2Layouts(t *testing.T) {
	if unsafe.Sizeof(v4lCapability{}) != 104 || unsafe.Sizeof(v4lFormat{}) != 208 || unsafe.Sizeof(v4lRequest{}) != 20 || unsafe.Sizeof(v4lBuffer{}) != 88 {
		t.Fatal("V4L2 ABI size mismatch")
	}
	b := v4lBuffer{}
	if unsafe.Offsetof(b.Time) != 24 || unsafe.Offsetof(b.Offset) != 64 || unsafe.Offsetof(b.Length) != 72 {
		t.Fatal("V4L2 ABI offset mismatch")
	}
}

func linuxTestNode() unix.Stat_t {
	return unix.Stat_t{Mode: unix.S_IFCHR | 0600, Rdev: unix.Mkdev(81, 0), Dev: 1, Ino: 2}
}

// Every native operation used by the open path is replaced. The format reply
// negotiates the requested dimensions with no padding; callers can override a
// single boundary to exercise validation without opening a real device.
func linuxTestOpenOps(t *testing.T) linuxOps {
	t.Helper()
	return linuxOps{
		glob:  func(string) ([]string, error) { return []string{"/dev/video0"}, nil },
		stat:  func(_ string, s *unix.Stat_t) error { *s = linuxTestNode(); return nil },
		fstat: func(_ int, s *unix.Stat_t) error { *s = linuxTestNode(); return nil },
		open: func(_ string, flags int, _ uint32) (int, error) {
			if flags&unix.O_PATH != 0 {
				return 100, nil
			}
			return 101, nil
		},
		close: func(int) error { return nil },
		ioctl: func(_ int, nr, _ uint32, p unsafe.Pointer, _ uintptr) error {
			switch nr {
			case 0: // QUERYCAP
				c := (*v4lCapability)(p)
				c.Capabilities = 1 | 0x04000000
				copy(c.Card[:], "Test camera")
			case 5: // S_FMT
				f := (*v4lFormat)(p)
				get, put := binary.LittleEndian.Uint32, binary.LittleEndian.PutUint32
				stride, rows := get(f.Data[:]), get(f.Data[4:])*3/2
				if get(f.Data[8:]) == fourYUYV {
					stride *= 2
					rows = get(f.Data[4:])
				}
				put(f.Data[16:], stride)
				put(f.Data[20:], stride*rows)
			case 8, 22: // REQBUFS, S_PARM: accept the requested values.
			case 9: // QUERYBUF
				b := (*v4lBuffer)(p)
				b.Length, b.Offset = 32, uint64(b.Index)*4096
			default:
				t.Fatalf("unexpected ioctl %d", nr)
			}
			return nil
		},
		mmap: func(_ int, _ int64, length, _, _ int) ([]byte, error) {
			return make([]byte, length), nil
		},
		munmap: func([]byte) error { return nil },
	}
}

func TestLinuxOpenRejectsNonVideoNodesBeforeDeviceOpen(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode uint32
		dev  uint64
	}{
		{"regular file", unix.S_IFREG, 0},
		{"watchdog", unix.S_IFCHR, unix.Mkdev(10, 130)},
		{"block device", unix.S_IFBLK, unix.Mkdev(81, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ops := linuxTestOpenOps(t)
			ops.stat = func(_ string, s *unix.Stat_t) error {
				*s = unix.Stat_t{Mode: tc.mode, Rdev: tc.dev}
				return nil
			}
			opened := false
			ops.open = func(string, int, uint32) (int, error) {
				opened = true
				return 101, nil
			}
			_, err := openV4LDevice("/custom/path", unix.O_RDWR, &ops)
			if !errors.Is(err, ErrUnsupported) || opened {
				t.Fatalf("err=%v, opened=%v; invalid nodes must never be opened", err, opened)
			}
		})
	}
}

func TestLinuxOpenPinsAndRevalidatesDeviceIdentity(t *testing.T) {
	ops := linuxTestOpenOps(t)
	var sequence []string
	stat, fstat, open, closeFD := ops.stat, ops.fstat, ops.open, ops.close
	ops.stat = func(path string, s *unix.Stat_t) error {
		sequence = append(sequence, "stat")
		return stat(path, s)
	}
	ops.open = func(path string, flags int, mode uint32) (int, error) {
		if flags&unix.O_PATH != 0 {
			if path != "/custom/camera" || flags != unix.O_PATH|unix.O_CLOEXEC {
				t.Fatalf("pin open(%q, %#x)", path, flags)
			}
			sequence = append(sequence, "pin")
		} else {
			if path != "/proc/self/fd/100" || flags != unix.O_RDWR|unix.O_NONBLOCK|unix.O_CLOEXEC {
				t.Fatalf("device open(%q, %#x)", path, flags)
			}
			sequence = append(sequence, "activate")
		}
		return open(path, flags, mode)
	}
	ops.fstat = func(fd int, s *unix.Stat_t) error {
		if fd == 100 {
			sequence = append(sequence, "fstat-pin")
		} else {
			sequence = append(sequence, "fstat-device")
		}
		return fstat(fd, s)
	}
	ops.close = func(fd int) error {
		if fd != 100 {
			t.Fatalf("successful open unexpectedly closed device fd %d", fd)
		}
		sequence = append(sequence, "close-pin")
		return closeFD(fd)
	}
	fd, err := openV4LDevice("/custom/camera", unix.O_RDWR, &ops)
	want := []string{"stat", "pin", "fstat-pin", "activate", "fstat-device", "close-pin"}
	if err != nil || fd != 101 || !reflect.DeepEqual(sequence, want) {
		t.Fatalf("fd=%d, err=%v, sequence=%v", fd, err, sequence)
	}

	for _, tc := range []struct {
		name string
		fd   int
		edit func(*unix.Stat_t)
	}{
		{"pin changed major", 100, func(s *unix.Stat_t) { s.Rdev = unix.Mkdev(10, 130) }},
		{"pin changed inode", 100, func(s *unix.Stat_t) { s.Ino++ }},
		{"pin changed filesystem", 100, func(s *unix.Stat_t) { s.Dev++ }},
		{"opened changed inode", 101, func(s *unix.Stat_t) { s.Ino++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ops := linuxTestOpenOps(t)
			ops.fstat = func(fd int, s *unix.Stat_t) error {
				*s = linuxTestNode()
				if fd == tc.fd {
					tc.edit(s)
				}
				return nil
			}
			activated, closed := 0, 0
			open := ops.open
			ops.open = func(path string, flags int, mode uint32) (int, error) {
				if flags&unix.O_PATH == 0 {
					activated++
				}
				return open(path, flags, mode)
			}
			ops.close = func(int) error { closed++; return nil }
			_, err := openV4LDevice("/custom/camera", unix.O_RDWR, &ops)
			wantActivated := 0
			if tc.fd == 101 {
				wantActivated = 1
			}
			if !errors.Is(err, ErrNoDevice) || activated != wantActivated || closed != 1+activated {
				t.Fatalf("err=%v, activated=%d, closed=%d", err, activated, closed)
			}
		})
	}
}

func TestLinuxEnumerationReportsPermissionOnlyWhenNoDeviceIsAccessible(t *testing.T) {
	for _, cause := range []error{unix.EACCES, unix.EPERM} {
		for _, boundary := range []string{"stat", "open", "caps"} {
			t.Run(cause.Error()+"/"+boundary, func(t *testing.T) {
				ops := linuxTestOpenOps(t)
				switch boundary {
				case "stat":
					ops.stat = func(string, *unix.Stat_t) error { return cause }
				case "open":
					open := ops.open
					ops.open = func(path string, flags int, mode uint32) (int, error) {
						if flags&unix.O_PATH == 0 {
							return -1, cause
						}
						return open(path, flags, mode)
					}
				case "caps":
					ops.ioctl = func(int, uint32, uint32, unsafe.Pointer, uintptr) error { return cause }
				}
				_, err := linuxDevices(context.Background(), &ops)
				if !errors.Is(err, ErrPermission) || !errors.Is(err, cause) {
					t.Fatalf("lost permission classification or cause: %v", err)
				}
			})
		}
	}
	ops := linuxTestOpenOps(t)
	ops.glob = func(string) ([]string, error) { return []string{"/denied", "/accessible"}, nil }
	stat := ops.stat
	ops.stat = func(path string, s *unix.Stat_t) error {
		if path == "/denied" {
			return unix.EACCES
		}
		return stat(path, s)
	}
	devices, err := linuxDevices(context.Background(), &ops)
	if err != nil || len(devices) != 1 || devices[0] != (Device{ID: "/accessible", Name: "Test camera"}) {
		t.Fatalf("devices=%v, err=%v", devices, err)
	}
}

func TestLinuxFormatOnlyFallsBackOnEINVAL(t *testing.T) {
	for _, cause := range []error{unix.EINVAL, unix.EBUSY, unix.EIO, unix.EPERM} {
		t.Run(cause.Error(), func(t *testing.T) {
			ops := linuxTestOpenOps(t)
			ioctl := ops.ioctl
			calls := 0
			ops.ioctl = func(fd int, nr, dir uint32, p unsafe.Pointer, size uintptr) error {
				calls++
				if calls == 1 {
					return cause
				}
				return ioctl(fd, nr, dir, p, size)
			}
			d := linuxCapture{ops: &ops, config: Config{Width: 4, Height: 4}}
			err := d.setFormat()
			if cause == unix.EINVAL {
				if err != nil || calls != 2 || d.format != fourYUYV {
					t.Fatalf("err=%v, calls=%d, format=%#x", err, calls, d.format)
				}
			} else if !errors.Is(err, cause) || errors.Is(err, ErrUnsupported) || calls != 1 {
				t.Fatalf("driver failure was hidden: err=%v, calls=%d", err, calls)
			}
		})
	}
}

func TestLinuxFormatValidatesNativeLayoutAtOpen(t *testing.T) {
	for _, tc := range []struct {
		name         string
		format       uint32
		stride, size uint32
		wantMin      int
	}{
		{"zero stride", fourNV12, 0, 24, 0},
		{"short stride", fourNV12, 3, 24, 0},
		{"short size", fourNV12, 4, 23, 0},
		{"missing row padding", fourNV12, 6, 34, 0},
		{"large stride", fourNV12, math.MaxUint32, math.MaxUint32, 0},
		{"large size", fourNV12, 4, maxNativeBuffer + 1, 0},
		{"padded NV12", fourNV12, 6, 36, 34},
		{"short YUYV stride", fourYUYV, 7, 32, 0},
		{"padded YUYV", fourYUYV, 10, 40, 38},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ops := linuxTestOpenOps(t)
			ops.ioctl = func(_ int, _ uint32, _ uint32, p unsafe.Pointer, _ uintptr) error {
				f := (*v4lFormat)(p)
				if binary.LittleEndian.Uint32(f.Data[8:]) != tc.format {
					return unix.EINVAL
				}
				binary.LittleEndian.PutUint32(f.Data[16:], tc.stride)
				binary.LittleEndian.PutUint32(f.Data[20:], tc.size)
				return nil
			}
			d := linuxCapture{ops: &ops, config: Config{Width: 4, Height: 4}}
			err := d.setFormat()
			if tc.wantMin == 0 {
				if !errors.Is(err, ErrUnsupported) {
					t.Fatalf("invalid layout accepted: %v", err)
				}
			} else if err != nil || d.minBytes != tc.wantMin || d.stride != int(tc.stride) || d.sizeImage != int(tc.size) {
				t.Fatalf("err=%v, layout=%d/%d/%d", err, d.minBytes, d.stride, d.sizeImage)
			}
		})
	}
}

func TestLinuxOpenBoundsEveryMappingAndTotalNativeMemory(t *testing.T) {
	for _, tc := range []struct {
		name          string
		count, length uint32
		wantMaps      int
		wantError     bool
	}{
		{"short mapping", 4, 23, 0, true},
		{"large mapping", 4, maxNativeBuffer + 1, 0, true},
		{"large total", 32, 8 << 20, 16, true},
		{"valid", 4, 32, 4, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ops := linuxTestOpenOps(t)
			ioctl := ops.ioctl
			ops.ioctl = func(fd int, nr, dir uint32, p unsafe.Pointer, size uintptr) error {
				switch nr {
				case 8:
					(*v4lRequest)(p).Count = tc.count
					return nil
				case 9:
					(*v4lBuffer)(p).Length = tc.length
					return nil
				}
				return ioctl(fd, nr, dir, p, size)
			}
			mapped, unmapped, closed := 0, 0, 0
			ops.mmap = func(_ int, _ int64, length, prot, flags int) ([]byte, error) {
				if length != int(tc.length) || prot != unix.PROT_READ|unix.PROT_WRITE || flags != unix.MAP_SHARED {
					t.Fatalf("unexpected mmap: %d/%d/%d", length, prot, flags)
				}
				mapped++
				// The fake reserves no real mapping: the budget check must
				// use the requested native length, not this slice's size.
				return []byte{1}, nil
			}
			ops.munmap = func([]byte) error { unmapped++; return nil }
			ops.close = func(int) error { closed++; return nil }
			c := Config{Device: "/dev/video0", Width: 4, Height: 4, FPS: 30}
			d, err := openLinuxDevice(context.Background(), c, &ops)
			if (err != nil) != tc.wantError || mapped != tc.wantMaps {
				t.Fatalf("err=%v, mapped=%d; want error=%v maps=%d", err, mapped, tc.wantError, tc.wantMaps)
			}
			if d != nil {
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if unmapped != mapped || closed != 2 {
				t.Fatalf("incomplete cleanup: mapped=%d unmapped=%d closed=%d", mapped, unmapped, closed)
			}
		})
	}
}

type linuxFrameFixture struct {
	capture     linuxCapture
	record      v4lBuffer
	dqErr, qErr error
	dequeued    int
	queued      int
	badQueue    bool
	clock       unix.Timespec
	clockErr    error
	clockReads  int
}

func newLinuxFrameFixture(t *testing.T, format uint32) *linuxFrameFixture {
	t.Helper()
	c := testConfig(4, 2)
	t.Cleanup(c.buffers.close)
	data := []byte{1, 2, 3, 4, 99, 99, 5, 6, 7, 8, 99, 99, 11, 21, 12, 22, 99, 99}
	stride, minBytes := 6, 16
	if format == fourYUYV {
		data = []byte{1, 10, 2, 20, 3, 11, 4, 21, 99, 99, 5, 12, 6, 22, 7, 13, 8, 23, 99, 99}
		stride, minBytes = 10, 18
	}
	f := &linuxFrameFixture{
		capture: linuxCapture{fd: 101, config: c, format: format, stride: stride, minBytes: minBytes, sizeImage: len(data), buffers: [][]byte{data}},
		record:  v4lBuffer{Type: v4lCapture, Memory: v4lMmap, BytesUsed: uint32(len(data)), Flags: v4lTimestampMonotonic, Time: unix.Timeval{Sec: 100}},
		clock:   unix.Timespec{Sec: 100, Nsec: 50_000_000},
	}
	f.capture.ops = &linuxOps{
		ioctl: func(_ int, nr, _ uint32, p unsafe.Pointer, _ uintptr) error {
			switch nr {
			case 17:
				f.dequeued++
				*(*v4lBuffer)(p) = f.record
				return f.dqErr
			case 15:
				f.queued++
				if *(*v4lBuffer)(p) != (v4lBuffer{Index: f.record.Index, Type: v4lCapture, Memory: v4lMmap}) {
					f.badQueue = true
				}
				return f.qErr
			default:
				panic("unexpected frame ioctl")
			}
		},
		gettime: func(id int32, ts *unix.Timespec) error {
			if id != unix.CLOCK_MONOTONIC {
				panic("capture used a non-monotonic fallback clock")
			}
			f.clockReads++
			*ts = f.clock
			return f.clockErr
		},
	}
	return f
}

func TestLinuxReadFrameDropsShortAndErrorFramesThenContinues(t *testing.T) {
	for _, format := range []uint32{fourNV12, fourYUYV} {
		for _, reason := range []string{"short", "error"} {
			t.Run(string(rune(format&0xff))+"/"+reason, func(t *testing.T) {
				f := newLinuxFrameFixture(t, format)
				valid := f.record
				if reason == "short" {
					f.record.BytesUsed = uint32(f.capture.minBytes - 1)
				} else {
					f.record.Flags |= v4lBufferError
				}
				emitted := 0
				emit := func(frame *av.Frame) {
					emitted++
					defer releaseFrame(frame)
					if !bytes.Equal(frame.Planes[0].Buffer.Bytes, []byte{1, 2, 3, 4, 5, 6, 7, 8}) ||
						!bytes.Equal(frame.Planes[1].Buffer.Bytes, []byte{11, 12}) ||
						!bytes.Equal(frame.Planes[2].Buffer.Bytes, []byte{21, 22}) {
						t.Errorf("active pixels or padded row conversion are wrong: %v", frame.Planes)
					}
				}
				if err := f.capture.readFrame(emit); err != nil || emitted != 0 {
					t.Fatalf("bad frame: err=%v emitted=%d", err, emitted)
				}
				f.record = valid
				// Padding after the final row need not be present in bytesused.
				f.record.BytesUsed = uint32(f.capture.minBytes)
				if err := f.capture.readFrame(emit); err != nil || emitted != 1 {
					t.Fatalf("following frame: err=%v emitted=%d", err, emitted)
				}
				if f.queued != 2 || f.badQueue || f.capture.config.buffers.dropped.Load() != 1 {
					t.Fatalf("queued=%d badQueue=%v dropped=%d", f.queued, f.badQueue, f.capture.config.buffers.dropped.Load())
				}
			})
		}
	}
}

func TestLinuxReadFrameRequeuesOnFailureAndReleasesCopiedFrame(t *testing.T) {
	for _, failure := range []string{"bytesused", "timestamp", "qbuf", "index", "again"} {
		t.Run(failure, func(t *testing.T) {
			f := newLinuxFrameFixture(t, fourNV12)
			switch failure {
			case "bytesused":
				f.record.BytesUsed++
			case "timestamp":
				f.record.Time.Usec = 1e6
			case "qbuf":
				f.qErr = unix.EIO
			case "index":
				f.record.Index = 2
			case "again":
				f.dqErr = unix.EAGAIN
			}
			emitted := false
			err := f.capture.readFrame(func(frame *av.Frame) { emitted = true; releaseFrame(frame) })
			wantQueue := 1
			if failure == "index" || failure == "again" {
				wantQueue = 0
			}
			if (err == nil) != (failure == "again") || emitted || f.queued != wantQueue || f.badQueue {
				t.Fatalf("err=%v emitted=%v queued=%d badQueue=%v", err, emitted, f.queued, f.badQueue)
			}
			if failure == "qbuf" && !errors.Is(err, unix.EIO) {
				t.Fatalf("lost QBUF cause: %v", err)
			}
			for i := range f.capture.config.buffers.slots {
				slot := &f.capture.config.buffers.slots[i]
				if slot.busy.Load() || slot.owners[0].Refs() != 1 || slot.owners[1].Refs() != 1 || slot.owners[2].Refs() != 1 {
					t.Fatalf("pool slot %d leaked after %s", i, failure)
				}
			}
		})
	}
}

func TestLinuxTimestampTypeAndZeroFallback(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flags    uint32
		time     unix.Timeval
		fallback bool
	}{
		{"monotonic", v4lTimestampMonotonic, unix.Timeval{Sec: 100, Usec: 123}, false},
		{"monotonic start of exposure", v4lTimestampMonotonic | 0x10000, unix.Timeval{Sec: 100, Usec: 123}, false},
		{"unknown", 0, unix.Timeval{Sec: 1_700_000_000}, true},
		{"copy", 0x4000, unix.Timeval{Sec: 1_700_000_000}, true},
		{"unknown mask combination", 0x6000, unix.Timeval{Sec: 1_700_000_000}, true},
		{"zero monotonic", v4lTimestampMonotonic, unix.Timeval{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLinuxFrameFixture(t, fourNV12)
			f.record.Flags, f.record.Time = tc.flags, tc.time
			got, keep, err := f.capture.timestamp(&f.record)
			want, reads := int64(100_000_123_000), 0
			if tc.fallback {
				want, reads = 100_050_000_000, 1
			}
			if err != nil || !keep || got != want || f.clockReads != reads {
				t.Fatalf("timestamp=%d keep=%v err=%v clock reads=%d", got, keep, err, f.clockReads)
			}
		})
	}
}

func TestLinuxTimestampPreservesNativeGapsAcrossFallback(t *testing.T) {
	f := newLinuxFrameFixture(t, fourNV12)
	var delivered []int64
	emit := func(frame *av.Frame) {
		delivered = append(delivered, frame.PTS.Value)
		releaseFrame(frame)
	}
	for _, record := range []struct {
		flags uint32
		usec  int64
	}{
		{v4lTimestampMonotonic, 0},
		{0, 0},                          // Unknown clock: deliver arrival at 100.05s.
		{v4lTimestampMonotonic, 40_000}, // Predates arrival: drop, don't shift it.
		{v4lTimestampMonotonic, 100_000},
		{v4lTimestampMonotonic, 100_000}, // Equal timestamps need no fabricated increment.
	} {
		f.record.Flags, f.record.Time.Usec = record.flags, record.usec
		if err := f.capture.readFrame(emit); err != nil {
			t.Fatal(err)
		}
	}
	want := []int64{100_000_000_000, 100_050_000_000, 100_100_000_000, 100_100_000_000}
	if !reflect.DeepEqual(delivered, want) || f.capture.config.buffers.dropped.Load() != 1 || f.queued != 5 {
		t.Fatalf("delivered=%v dropped=%d queued=%d", delivered, f.capture.config.buffers.dropped.Load(), f.queued)
	}
	f.record.Time.Usec = 90_000
	if err := f.capture.readFrame(emit); err == nil || len(delivered) != len(want) || f.queued != 6 {
		t.Fatalf("native clock regression must fail after requeue: err=%v delivered=%v queued=%d", err, delivered, f.queued)
	}
}

func TestLinuxTimestampRejectsMalformedOrRegressingClocks(t *testing.T) {
	for _, tc := range []struct {
		name string
		time unix.Timeval
	}{
		{"negative seconds", unix.Timeval{Sec: -1}},
		{"negative micros", unix.Timeval{Sec: 1, Usec: -1}},
		{"overflow micros", unix.Timeval{Sec: 1, Usec: 1e6}},
		{"overflow seconds", unix.Timeval{Sec: math.MaxInt64 / 1_000_000_000, Usec: 999_999}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLinuxFrameFixture(t, fourNV12)
			f.record.Time = tc.time
			if _, _, err := f.capture.timestamp(&f.record); err == nil {
				t.Fatal("malformed native timestamp accepted")
			}
		})
	}
	f := newLinuxFrameFixture(t, fourNV12)
	f.record.Flags = 0
	f.clockErr = unix.EIO
	if _, _, err := f.capture.timestamp(&f.record); !errors.Is(err, unix.EIO) {
		t.Fatalf("clock failure hidden: %v", err)
	}
	f.clockErr = nil
	for _, clock := range []unix.Timespec{
		{Sec: -1}, {Sec: 1, Nsec: -1}, {Sec: 1, Nsec: 1e9},
		{Sec: math.MaxInt64 / 1_000_000_000, Nsec: 999_999_999},
	} {
		f.clock = clock
		if _, _, err := f.capture.timestamp(&f.record); err == nil {
			t.Fatalf("malformed fallback clock accepted: %v", clock)
		}
	}
	f.clock = unix.Timespec{Sec: 100}
	f.capture.lastPTS, f.capture.havePTS = 101e9, true
	if _, _, err := f.capture.timestamp(&f.record); err == nil {
		t.Fatal("fallback clock regression accepted")
	}
}

func TestLinuxDequeueCopyRequeueHasNoPerFrameAllocations(t *testing.T) {
	for _, tc := range []struct {
		name   string
		format uint32
		flags  uint32
	}{
		{"NV12/native clock", fourNV12, v4lTimestampMonotonic},
		{"YUYV/native clock", fourYUYV, v4lTimestampMonotonic},
		{"NV12/fallback clock", fourNV12, 0},
		{"YUYV/fallback clock", fourYUYV, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLinuxFrameFixture(t, tc.format)
			f.record.Flags = tc.flags
			emitted := 0
			emit := func(frame *av.Frame) { emitted++; releaseFrame(frame) }
			allocs := testing.AllocsPerRun(1000, func() {
				if err := f.capture.readFrame(emit); err != nil {
					t.Fatal(err)
				}
			})
			if allocs != 0 || emitted != 1001 || f.dequeued != 1001 || f.queued != 1001 || f.badQueue {
				t.Fatalf("allocs/frame=%g emitted=%d dequeued=%d queued=%d badQueue=%v", allocs, emitted, f.dequeued, f.queued, f.badQueue)
			}
		})
	}
}

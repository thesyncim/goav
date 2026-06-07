package matroska

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	matroskaFieldCorpusEnv          = "GOAV_MATROSKA_FIELD_CORPUS"
	fieldCorpusPacketCapacityEnv    = "GOAV_FIELD_CORPUS_PACKET_CAP"
	defaultFieldCorpusPacketCap     = 16 << 20
	defaultFieldCorpusMaxLaceFrames = 256
)

func TestExternalMatroskaFieldCorpus(t *testing.T) {
	files := matroskaFieldCorpusFiles(t)
	packetCap := fieldCorpusPacketCap(t)
	for i, file := range files {
		t.Run(fieldCorpusCaseName(i, file), func(t *testing.T) {
			if packets := scanMatroskaFieldCorpusFile(t, file, packetCap); packets == 0 {
				t.Fatalf("no packets scanned from %s", file)
			}
		})
	}
}

func BenchmarkExternalMatroskaFieldCorpusScan(b *testing.B) {
	files := matroskaFieldCorpusFiles(b)
	packetCap := fieldCorpusPacketCap(b)
	for i, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fieldCorpusCaseName(i, file), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(info.Size())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if packets := scanMatroskaFieldCorpusFile(b, file, packetCap); packets == 0 {
					b.Fatalf("no packets scanned from %s", file)
				}
			}
		})
	}
}

func matroskaFieldCorpusFiles(tb testing.TB) []string {
	tb.Helper()
	return fieldCorpusFiles(tb, matroskaFieldCorpusEnv, map[string]struct{}{
		".mkv":  {},
		".mka":  {},
		".mks":  {},
		".mk3d": {},
	})
}

func fieldCorpusFiles(tb testing.TB, env string, extensions map[string]struct{}) []string {
	tb.Helper()
	value := strings.TrimSpace(os.Getenv(env))
	if value == "" {
		tb.Skipf("%s is not set", env)
	}
	var files []string
	for _, entry := range filepath.SplitList(value) {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		info, err := os.Stat(entry)
		if err != nil {
			tb.Fatalf("stat %s: %v", entry, err)
		}
		if !info.IsDir() {
			if fieldCorpusExtensionAllowed(entry, extensions) {
				files = append(files, entry)
			}
			continue
		}
		err = filepath.WalkDir(entry, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !fieldCorpusExtensionAllowed(path, extensions) {
				return nil
			}
			files = append(files, path)
			return nil
		})
		if err != nil {
			tb.Fatalf("walk %s: %v", entry, err)
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		tb.Skipf("%s did not contain matching corpus files", env)
	}
	return files
}

func fieldCorpusExtensionAllowed(path string, extensions map[string]struct{}) bool {
	_, ok := extensions[strings.ToLower(filepath.Ext(path))]
	return ok
}

func fieldCorpusPacketCap(tb testing.TB) int {
	tb.Helper()
	value := strings.TrimSpace(os.Getenv(fieldCorpusPacketCapacityEnv))
	if value == "" {
		return defaultFieldCorpusPacketCap
	}
	n, err := strconv.ParseInt(value, 10, 0)
	if err != nil || n <= 0 {
		tb.Fatalf("%s must be a positive byte count", fieldCorpusPacketCapacityEnv)
	}
	return int(n)
}

func fieldCorpusCaseName(index int, file string) string {
	name := filepath.Base(file)
	replacer := strings.NewReplacer("/", "_", "\\", "_", " ", "_", ":", "_")
	return fmt.Sprintf("%03d_%s", index, replacer.Replace(name))
}

func scanMatroskaFieldCorpusFile(tb testing.TB, file string, packetCap int) int {
	tb.Helper()
	input, err := os.Open(file)
	if err != nil {
		tb.Fatal(err)
	}
	defer input.Close()

	demuxer, err := NewDemuxer(input, DemuxerOptions{
		MaxLaceFrames:  defaultFieldCorpusMaxLaceFrames,
		MaxLacePayload: packetCap,
	})
	if err != nil {
		tb.Fatal(err)
	}
	if len(demuxer.Tracks()) == 0 {
		tb.Fatalf("%s has no tracks", file)
	}
	packet := Packet{Data: make([]byte, 0, packetCap)}
	packets := 0
	for {
		err := demuxer.ReadPacket(&packet)
		if errors.Is(err, io.EOF) {
			return packets
		}
		if errors.Is(err, ErrPayloadTooSmall) || errors.Is(err, ErrLaceTooLarge) {
			tb.Fatalf("%s has a packet or lace payload larger than %d bytes; set %s to a larger byte count", file, packetCap, fieldCorpusPacketCapacityEnv)
		}
		if err != nil {
			tb.Fatal(err)
		}
		if packet.TrackID == 0 {
			tb.Fatalf("%s yielded packet with zero track ID", file)
		}
		packets++
	}
}

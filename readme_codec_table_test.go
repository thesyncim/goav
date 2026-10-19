package goav_test

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/runconfig"
)

// TestReadmeCodecTableMatchesBundle pins the README's "What's in the box"
// codec rows to what bundle registers on the platform running the test (CI
// runs it on linux/amd64 and darwin/arm64): every codec listed without a
// qualifier has an active backend for that mode here, and one listed with
// "(`-tags X`)" is registered behind build tag X. A codec only some
// platforms decode must not appear unqualified.
func TestReadmeCodecTableMatchesBundle(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	config, err := runconfig.NewConfig(bundle.Options()...)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]av.CodecID{"VP8": av.CodecVP8, "VP9": av.CodecVP9, "AV1": av.CodecAV1, "H.264": av.CodecH264, "Opus": av.CodecOpus, "AAC": av.CodecAAC, "AAC-LC": av.CodecAAC}
	tagPattern := regexp.MustCompile("^`-tags ([a-z0-9_]+)`$")
	rows := 0
	for line := range strings.Lines(string(data)) {
		if !strings.HasPrefix(line, "| Video |") && !strings.HasPrefix(line, "| Audio |") {
			continue
		}
		rows++
		cells := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|")
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		if len(cells) != 3 {
			t.Fatalf("codec row %q has %d cells, want 3", line, len(cells))
		}
		for column, mode := range map[int]codec.Mode{1: codec.ModeDecode, 2: codec.ModeEncode} {
			for _, entry := range splitOutsideParens(cells[column]) {
				name, qualifier, _ := strings.Cut(entry, " (")
				qualifier = strings.TrimSuffix(qualifier, ")")
				id, ok := ids[name]
				if !ok {
					t.Fatalf("README codec %q is not mapped to a codec id; add it to this test", name)
				}
				descriptors, err := config.Codecs.Find(id, mode)
				if err != nil {
					t.Errorf("README lists %s %s, but bundle registers no %s %s", name, mode, id, mode)
					continue
				}
				if tag := tagPattern.FindStringSubmatch(qualifier); tag != nil {
					if !slices.ContainsFunc(descriptors, func(d codec.Descriptor) bool { return slices.Contains(d.Capabilities.BuildTags, tag[1]) }) {
						t.Errorf("README says %s %s needs -tags %s, but no bundled descriptor is behind that tag", name, mode, tag[1])
					}
					continue
				}
				if !slices.ContainsFunc(descriptors, func(d codec.Descriptor) bool { return d.Backend.Status == "active" }) {
					t.Errorf("README lists %s %s unqualified, but no bundled backend is active on this platform: %+v", name, mode, descriptors)
				}
			}
		}
	}
	if rows != 2 {
		t.Fatalf("found %d README codec rows, want the Video and Audio rows", rows)
	}
}

// splitOutsideParens splits a table cell on the commas that sit outside
// parentheses: "Opus (in WebM, or MP4), VP8" is two entries.
func splitOutsideParens(cell string) []string {
	var entries []string
	depth, start := 0, 0
	for i, r := range cell {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				entries = append(entries, strings.TrimSpace(cell[start:i]))
				start = i + 1
			}
		}
	}
	return append(entries, strings.TrimSpace(cell[start:]))
}

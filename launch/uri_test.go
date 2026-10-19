package launch_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/launch"
)

// oneFrameIVF is a VP8 IVF file with one frame.
func oneFrameIVF() []byte {
	var data bytes.Buffer
	var header [32]byte
	copy(header[:4], "DKIF")
	binary.LittleEndian.PutUint16(header[6:8], 32)
	copy(header[8:12], "VP80")
	binary.LittleEndian.PutUint16(header[12:14], 16)
	binary.LittleEndian.PutUint16(header[14:16], 16)
	binary.LittleEndian.PutUint32(header[16:20], 1000)
	binary.LittleEndian.PutUint32(header[20:24], 1)
	data.Write(header[:])
	var frame [12]byte
	binary.LittleEndian.PutUint32(frame[:4], 3)
	data.Write(frame[:])
	data.Write([]byte{0x10, 0x20, 0x30})
	return data.Bytes()
}

// TestProgramsOpenURIs pins uri steps: a URL works wherever a file does,
// bare or as uri, once URIs grants its scheme; the program builds the graph
// its Go recipe builds, and Describe names the scheme without the secret.
func TestProgramsOpenURIs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "clip.ivf", time.Unix(1700000000, 0), bytes.NewReader(oneFrameIVF()))
	}))
	defer srv.Close()
	clip := srv.URL + "/clip.ivf?token=SECRET"

	_, err := launch.Compile(`uri "`+clip+`" ! video ! file out.ivf`, launch.Files(newMemFS()))
	if err == nil || !strings.Contains(err.Error(), "may not open http URLs") || !strings.Contains(err.Error(), `launch.URIs("http")`) {
		t.Fatalf("ungranted uri err = %v", err)
	}
	if strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("refusal leaks the token:\n%v", err)
	}

	for _, program := range []string{
		`uri "` + clip + `" ! video ! file out.ivf`,
		`"` + clip + `" ! video ! file out.ivf`,
		`in := "` + clip + `"; in ! video ! file out.ivf`,
	} {
		fs := newMemFS()
		outs, err := launch.Compile(program, launch.Files(fs), launch.URIs("http"))
		if err != nil {
			t.Fatalf("%s: %v", program, err)
		}
		in := goav.URIInput(clip)
		want := shapeOf(t, []goav.Output{in.Video().To(goav.Write("out.ivf", &bytes.Buffer{}))})
		if got := shapeOf(t, outs); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: graph %+v, want %+v", program, got, want)
		}
		spec, err := bundle.Describe(outs...)
		if err != nil {
			t.Fatal(err)
		}
		described, _ := json.Marshal(spec)
		if !strings.Contains(string(described), "protocol=http") || strings.Contains(string(described), "SECRET") {
			t.Fatalf("describe = %s", described)
		}
		if err := bundle.Run(context.Background(), outs...); err != nil {
			t.Fatalf("%s: run: %v", program, err)
		}
		if out := fs.outputs["out.ivf"]; out == nil || !bytes.HasPrefix(out.Bytes(), []byte("DKIF")) {
			t.Fatalf("%s: out.ivf not written", program)
		}
	}
}

// TestURIsKeepTheFileSandbox pins that a file URL is a file: it opens
// through Files whatever URIs grants, so Dir keeps an attached program in
// its directory.
func TestURIsKeepTheFileSandbox(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "in.ivf"), oneFrameIVF(), 0o644); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "secret.ivf")
	if err := os.WriteFile(outside, oneFrameIVF(), 0o644); err != nil {
		t.Fatal(err)
	}
	fs, err := launch.Dir(dir)
	if err != nil {
		t.Fatal(err)
	}
	outs, err := launch.Compile(`uri "file:in.ivf" ! video ! "out.ivf"`, launch.Files(fs))
	if err != nil {
		t.Fatal(err)
	}
	if err := bundle.Run(context.Background(), outs...); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "out.ivf")); err != nil {
		t.Fatal(err)
	}
	for _, escape := range []string{
		`uri "` + fileURL(outside) + `" ! video ! file out2.ivf`,
		`"../` + filepath.Base(filepath.Dir(outside)) + `/secret.ivf" ! video ! file out2.ivf`,
	} {
		outs, err := launch.Compile(escape, launch.Files(fs), launch.URIs())
		if err != nil {
			t.Fatalf("%s: %v", escape, err)
		}
		if err := bundle.Run(context.Background(), outs...); err == nil {
			t.Fatalf("%s read outside its directory", escape)
		}
	}
	if _, err := launch.Compile(`"file:in.ivf" ! video ! file out.ivf`); err == nil || !strings.Contains(err.Error(), "may not read files") {
		t.Fatalf("file URL without Files err = %v", err)
	}
}

// TestRefusalMasksURLCredentials pins that a refusal's source excerpt masks
// a URL's userinfo and secret query values in place, keeping the caret.
func TestRefusalMasksURLCredentials(t *testing.T) {
	_, err := launch.Compile(`"https://alice:hunter2@cdn.example/a.ivf?token=abc&q=1" ! video ! resise 64x48 ! file out.ivf`, launch.URIs())
	want := "launch: 1:67: unknown element \"resise\"\n" +
		"    \"https://*************@cdn.example/a.ivf?token=***&q=1\" ! video ! resise 64x48 ! file out.ivf\n" +
		"                                                                      ^^^^^^\n" +
		"    did you mean \"resize\"?"
	if err == nil || err.Error() != want {
		t.Fatalf("rendered refusal:\n%v\nwant:\n%s", err, want)
	}
}

// fileURL is the file URL of a local path: file:///C:/x on Windows, whose
// paths have no leading slash.
func fileURL(path string) string {
	p := filepath.ToSlash(path)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return "file://" + p
}

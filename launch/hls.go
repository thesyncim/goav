package launch

import (
	"fmt"
	"io"
	"path"
	"reflect"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/container/hls"
)

// HLS packages a program's hls outputs into server, which serves them over
// HTTP, instead of writing them to its files. Low-latency HLS (parts=) needs
// it: a server holds the blocking requests LL-HLS players make.
func HLS(server *hls.Server) Option {
	return func(c *config) { c.hls = server }
}

// hls: package what arrives as HTTP Live Streaming.
func hlsElement() *element {
	type hlsArgs struct {
		Path    string        `launch:",positional,required" help:"playlist path"`
		Segment time.Duration `launch:"segment" help:"target segment length (4s)"`
		Window  int           `launch:"window" help:"live: list the last N segments (all when 0)"`
		Parts   time.Duration `launch:"parts" help:"low-latency: publish parts this long (needs an HLS server)"`
	}
	el := define[hlsArgs]("hls", "package as HLS: a playlist and fMP4 segments beside it", "destination")
	el.dest = func(c *compiler, args reflect.Value) (value, error) {
		a := argsOf[hlsArgs](args)
		key := "\x00hls:" + a.Path
		if d, ok := c.dests[key]; ok {
			return value{kind: kindDest, dest: d}, nil
		}
		if a.Window < 0 {
			return value{}, &Error{Pos: -1, Msg: fmt.Sprintf("window=%d lists no segments", a.Window), Hint: "write window=6 for a live playlist, or leave it out to keep every segment"}
		}
		var store hls.Store
		switch {
		case c.cfg.hls != nil:
			store = c.cfg.hls
		case a.Parts > 0:
			return value{}, &Error{Pos: -1, Msg: "low-latency HLS needs a server for its blocking requests", Hint: "serve the program: goav run --serve :8080 '...', or compile with launch.HLS(hls.NewServer())"}
		case c.cfg.files == nil:
			return value{}, &Error{Pos: -1, Msg: "this program may not write files", Hint: "compile with launch.Files(...) to allow it, or package into a server with launch.HLS(...)"}
		default:
			store = hlsStore{fs: c.cfg.files}
		}
		options := []hls.Option{hls.Window(a.Window)}
		if a.Segment > 0 {
			options = append(options, hls.SegmentDuration(a.Segment))
		}
		if a.Parts > 0 {
			options = append(options, hls.Parts(a.Parts))
		}
		d := goav.Custom(a.Path, hls.New(a.Path, store, options...))
		c.dests[key] = d
		return value{kind: kindDest, dest: d}, nil
	}
	return el
}

// hlsStore writes a packager's files through a program's FileSystem. A file
// is published whole by writing it under a temporary name and renaming it,
// when the file system renames; segments leave a live playlist's store when
// it removes.
type hlsStore struct{ fs FileSystem }

type renamer interface {
	Rename(oldname, newname string) error
}

type remover interface{ Remove(name string) error }

func (s hlsStore) Create(name string) (io.WriteCloser, error) {
	target := name
	r, renames := s.fs.(renamer)
	if renames {
		dir, file := path.Split(name)
		target = dir + "." + file + ".tmp"
	}
	w, err := s.fs.Create(target)
	if err != nil {
		return nil, err
	}
	return &hlsFile{w: w, publish: func() error {
		if !renames {
			return nil
		}
		return r.Rename(target, name)
	}}, nil
}

func (s hlsStore) Remove(name string) error {
	if r, ok := s.fs.(remover); ok {
		return r.Remove(name)
	}
	return nil
}

type hlsFile struct {
	w       io.Writer
	publish func() error
}

func (f *hlsFile) Write(p []byte) (int, error) { return f.w.Write(p) }

func (f *hlsFile) Close() error {
	if c, ok := f.w.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return f.publish()
}

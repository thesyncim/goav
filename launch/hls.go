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

// hls: package what arrives as HTTP Live Streaming, through the program's
// files.
func hlsElement() *element {
	type hlsArgs struct {
		Path    string        `launch:",positional,required" help:"playlist path"`
		Segment time.Duration `launch:"segment" help:"target segment length (4s)"`
		Window  int           `launch:"window" help:"live: list the last N segments (all when 0)"`
	}
	el := define[hlsArgs]("hls", "package as HLS: a playlist and fMP4 segments beside it", "destination")
	el.dest = func(c *compiler, args reflect.Value) (value, error) {
		a := argsOf[hlsArgs](args)
		key := "\x00hls:" + a.Path
		if d, ok := c.dests[key]; ok {
			return value{kind: kindDest, dest: d}, nil
		}
		if c.cfg.files == nil {
			return value{}, &Error{Pos: -1, Msg: "this program may not write files", Hint: "compile with launch.Files(...) to allow it"}
		}
		if a.Window < 0 {
			return value{}, &Error{Pos: -1, Msg: fmt.Sprintf("window=%d lists no segments", a.Window), Hint: "write window=6 for a live playlist, or leave it out to keep every segment"}
		}
		options := []hls.Option{hls.Window(a.Window)}
		if a.Segment > 0 {
			options = append(options, hls.SegmentDuration(a.Segment))
		}
		dir, name := path.Split(a.Path)
		d := goav.Custom(a.Path, hls.New(name, hlsStore{fs: c.cfg.files, dir: dir}, options...))
		c.dests[key] = d
		return value{kind: kindDest, dest: d}, nil
	}
	return el
}

// hlsStore writes a packager's files through a program's FileSystem. A file
// is published whole by writing it under a temporary name and renaming it,
// when the file system renames; segments leave a live playlist's store when
// it removes.
type hlsStore struct {
	fs  FileSystem
	dir string
}

type renamer interface {
	Rename(oldname, newname string) error
}

type remover interface{ Remove(name string) error }

func (s hlsStore) Create(name string) (io.WriteCloser, error) {
	final := path.Join(s.dir, name)
	target := final
	r, renames := s.fs.(renamer)
	if renames {
		target = path.Join(s.dir, "."+name+".tmp")
	}
	w, err := s.fs.Create(target)
	if err != nil {
		return nil, err
	}
	return &hlsFile{w: w, publish: func() error {
		if !renames {
			return nil
		}
		return r.Rename(target, final)
	}}, nil
}

func (s hlsStore) Remove(name string) error {
	if r, ok := s.fs.(remover); ok {
		return r.Remove(path.Join(s.dir, name))
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

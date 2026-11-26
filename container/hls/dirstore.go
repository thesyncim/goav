package hls

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Dir is a Store in a directory of the local file system. It publishes a
// file by writing a temporary one beside it and renaming it into place,
// readable by the web server that serves the directory.
func Dir(path string) Store { return dirStore(path) }

type dirStore string

// resolve confines name inside the store directory, rejecting absolute paths,
// backslashes, and parent-directory traversal.
func (d dirStore) resolve(name string) (string, error) {
	if !fs.ValidPath(name) || name == "." || strings.Contains(name, "\\") {
		return "", fmt.Errorf("hls: invalid store path %q", name)
	}
	base := filepath.Clean(string(d))
	final := filepath.Join(base, filepath.FromSlash(name))
	if back, err := filepath.Rel(base, final); err != nil || back == ".." || strings.HasPrefix(back, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("hls: path %q escapes directory", name)
	}
	return final, nil
}

func (d dirStore) Create(name string) (io.WriteCloser, error) {
	final, err := d.resolve(name)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Dir(final), "."+filepath.Base(final)+".*")
	if err != nil {
		return nil, err
	}
	return &publishing{File: f, final: final}, nil
}

func (d dirStore) Remove(name string) error {
	final, err := d.resolve(name)
	if err != nil {
		return err
	}
	if err := os.Remove(final); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

type publishing struct {
	*os.File
	final string
	state storeFileState
}

func (p *publishing) abort() {
	if p.state != storeFileOpen {
		return
	}
	p.state = storeFileAborted
	name := p.File.Name()
	_ = p.File.Close()
	_ = os.Remove(name)
}

func (p *publishing) Close() error {
	if p.state != storeFileOpen {
		return nil
	}
	p.state = storeFileClosed
	name := p.File.Name()
	if err := p.File.Chmod(0o644); err != nil {
		_ = p.File.Close()
		_ = os.Remove(name)
		return err
	}
	if err := p.File.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	if err := os.Rename(name, p.final); err != nil {
		_ = os.Remove(name)
		return err
	}
	return nil
}

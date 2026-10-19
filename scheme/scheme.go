// Package scheme holds goav's built-in URL schemes, the providers
// goav.URIInput and goav.URI resolve through: File for file URLs and bare
// paths, and HTTP for http and https. bundle.Schemes registers them on a
// runtime; registering one yourself replaces it there, last wins:
//
//	rt, err := bundle.New(runconfig.Scheme("https", scheme.HTTP(scheme.Client(myClient))))
//
// Both use the standard library only. Transports with dependencies live in
// nested modules and export their schemes the same way (webrtcav.WHEP); a
// future RTMP, SRT, or HLS input is one more provider.Scheme, a
// provider.ByteSource when it carries a container (FLV, MPEG-TS, fMP4
// segments) and a provider.Source when it carries media.
package scheme

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

// File is the file scheme: file:///abs/path.mp4, a relative file:path.mp4,
// or a bare path given to goav.URIInput or goav.URI. Inputs open
// random-access (MP4 with its index at the end seeks); destinations create
// the file and its parent directories when the run opens them.
func File() provider.Scheme { return fileScheme{} }

type fileScheme struct{}

func (fileScheme) Input(u *url.URL) (provider.Input, error) {
	path, err := filePath(u)
	if err != nil {
		return nil, err
	}
	return fileInput(path), nil
}

func (fileScheme) Destination(u *url.URL) (provider.Destination, error) {
	path, err := filePath(u)
	if err != nil {
		return nil, err
	}
	return fileDestination(path), nil
}

func filePath(u *url.URL) (string, error) {
	if u.Host != "" && u.Host != "localhost" {
		return "", fmt.Errorf("file URL names host %q: only local files open (file:///path)", u.Host)
	}
	path := u.Path
	if u.Opaque != "" {
		path = u.Opaque
	}
	if path == "" {
		return "", errors.New("file URL has no path")
	}
	if runtime.GOOS == "windows" && len(path) > 2 && path[0] == '/' && path[2] == ':' {
		path = path[1:]
	}
	return filepath.FromSlash(path), nil
}

type fileInput string

func (fileInput) SourceShape() shape.Spec { return shape.Spec{} }

func (f fileInput) OpenBytes(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.Open(string(f))
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.IsDir() {
		file.Close()
		return nil, fmt.Errorf("%s is a directory", f)
	}
	return sizedFile{File: file, size: info.Size()}, nil
}

// sizedFile is an open input file: random access through *os.File, and its
// size for containers that need it.
type sizedFile struct {
	*os.File
	size int64
}

func (f sizedFile) Size() int64 { return f.size }

type fileDestination string

func (f fileDestination) Name() string { return filepath.Base(string(f)) }

func (fileDestination) Contract() provider.Contract {
	return provider.Contract{ByteStream: true, Seekable: true, Protocol: av.ProtocolFile}
}

func (f fileDestination) Open(ctx context.Context, _ provider.Info) (provider.Writer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if dir := filepath.Dir(string(f)); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return os.Create(string(f))
}

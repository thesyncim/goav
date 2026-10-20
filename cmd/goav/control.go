package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/thesyncim/goav/launch"
)

// controlFlags are goav run's control-plane flags.
type controlFlags struct {
	address   string // unix://PATH or tcp://HOST:PORT
	tokenFile string // where a tcp server's token is written
	dir       string // the files attached programs may touch
	remote    bool   // serve tcp beyond loopback
}

// validate refuses a tcp address other hosts can reach unless --control-remote
// says so: the token is the only guard, and it crosses the network in clear.
func (c controlFlags) validate() error {
	hostport, ok := strings.CutPrefix(c.address, "tcp://")
	if !ok || c.remote {
		return nil
	}
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return fmt.Errorf("goav run: --control %s: %w", c.address, err)
	}
	if ip := net.ParseIP(host); host == "localhost" || ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("goav run: --control %s serves beyond this machine; use tcp://127.0.0.1:PORT, or add --control-remote to serve the network (clients still need the token)", c.address)
}

// issueToken makes a random token for a tcp server and writes it,
// owner-only, to the token file goav ctl reads.
func (c controlFlags) issueToken() (string, string, error) {
	secret := make([]byte, 24)
	if _, err := rand.Read(secret); err != nil {
		return "", "", err
	}
	token := hex.EncodeToString(secret)
	path, err := tokenPath(c.tokenFile, c.address)
	if err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", "", err
	}
	// Replace the directory entry with a fresh 0600 file. WriteFile would
	// preserve an existing file's permissions and follow a symlink.
	file, err := os.CreateTemp(filepath.Dir(path), ".goav-token-*")
	if err != nil {
		return "", "", err
	}
	defer os.Remove(file.Name())
	if _, err := file.WriteString(token + "\n"); err != nil {
		file.Close()
		return "", "", err
	}
	if err := file.Close(); err != nil {
		return "", "", err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return "", "", err
	}
	return token, path, nil
}

// tokenPath is the token file for a tcp address: the one named, else one
// per address in the user's config directory, where goav run and goav ctl on
// the same machine both find it.
func tokenPath(named, address string) (string, error) {
	if named != "" {
		return named, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("goav: no config directory for the control token (%w); pass --control-token FILE", err)
	}
	name := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' {
			return r
		}
		return '_'
	}, strings.TrimPrefix(address, "tcp://"))
	return filepath.Join(dir, "goav", "control-"+name+".token"), nil
}

// withToken is the address goav ctl dials: a tcp address carries the
// server's token (tcp://TOKEN@host:port), read from the token file.
func withToken(address, tokenFile string) (string, error) {
	hostport, ok := strings.CutPrefix(address, "tcp://")
	if !ok || strings.Contains(hostport, "@") {
		return address, nil
	}
	path, err := tokenPath(tokenFile, address)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("goav ctl: %s needs the token goav run wrote to %s: %w; pass --control-token FILE with a copy of it", address, path, err)
	}
	return "tcp://" + strings.TrimSpace(string(data)) + "@" + hostport, nil
}

// within is the files programs attached over the control plane may touch:
// only those under dir (a launch.Dir refuses a path that leaves it), recorded
// for the summary like the program's own.
func (f *cliFiles) within(dir string) (*rootedFiles, error) {
	files, err := launch.Dir(dir)
	if err != nil {
		return nil, fmt.Errorf("goav run: --control-dir %s: %w", dir, err)
	}
	return &rootedFiles{files: files.(dirFiles), dir: dir, record: f}, nil
}

// dirFiles is what a launch.Dir does: open, create, rename, and remove.
type dirFiles interface {
	launch.FileSystem
	Rename(oldname, newname string) error
	Remove(name string) error
}

// rootedFiles is a launch.Dir that records what it writes.
type rootedFiles struct {
	files  dirFiles
	dir    string
	record *cliFiles
}

func (f *rootedFiles) Open(name string) (io.Reader, error) { return f.files.Open(name) }

func (f *rootedFiles) Create(name string) (io.Writer, error) {
	file, err := f.files.Create(name)
	if err == nil {
		f.record.note("", filepath.Join(f.dir, name))
	}
	return file, err
}

func (f *rootedFiles) Rename(oldname, newname string) error {
	if err := f.files.Rename(oldname, newname); err != nil {
		return err
	}
	f.record.note(filepath.Join(f.dir, oldname), filepath.Join(f.dir, newname))
	return nil
}

func (f *rootedFiles) Remove(name string) error {
	if err := f.files.Remove(name); err != nil {
		return err
	}
	f.record.note(filepath.Join(f.dir, name), "")
	return nil
}

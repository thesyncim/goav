package main

import (
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestControlTokenReplacesExistingFilePrivately(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.token")
	if err := os.WriteFile(path, []byte("old token\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	flags := controlFlags{address: "tcp://127.0.0.1:1234", tokenFile: path}
	token, gotPath, err := flags.issueToken()
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != path || len(token) != 48 {
		t.Fatalf("issued token length %d at %q", len(token), gotPath)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != token+"\n" {
		t.Fatalf("token file does not contain the issued token: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("existing token file kept permissions %v, want 0600", info.Mode().Perm())
	}
	address, err := withToken(flags.address, path)
	if err != nil || !strings.Contains(address, token+"@") {
		t.Fatalf("client cannot read the replacement token: %v", err)
	}
}

func TestControlTokenDoesNotFollowSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "unrelated.txt")
	const original = "leave this file alone\n"
	if err := os.WriteFile(target, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "control.token")
	if err := os.Symlink(target, path); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlinks unavailable: %v", err)
		}
		t.Fatal(err)
	}
	flags := controlFlags{address: "tcp://127.0.0.1:1234", tokenFile: path}
	if _, _, err := flags.issueToken(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != original {
		t.Fatalf("issuing a token overwrote the symlink target: %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("token path was not replaced with a regular file: %v", err)
	}
}

func TestRunControlPortConflictPreservesExistingToken(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	path := filepath.Join(t.TempDir(), "control.token")
	const existing = "running server token\n"
	if err := os.WriteFile(path, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	code := run([]string{"run", "--control", "tcp://" + listener.Addr().String(), "--control-token", path,
		"--control-dir", t.TempDir(), "testsrc video size=16x16 live ! null"}, io.Discard, &stderr)
	if code != 1 {
		t.Fatalf("occupied control port returned %d: %s", code, stderr.String())
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != existing {
		t.Fatalf("failed listener changed the existing server's token: %v", err)
	}
}

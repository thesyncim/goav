//go:build darwin && (amd64 || arm64)

package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

//go:embed Info.plist
var embeddedInfoPlist []byte

// ensureMacOSAppBundle wraps and re-execs the current binary inside a stable,
// ad-hoc signed macOS .app bundle with Info.plist camera & screen usage keys
// when launched via plain `go run` (no wrapper script or `cd` needed).
func ensureMacOSAppBundle(headless bool) {
	if headless || os.Getenv("GOAV_STUDIO_BUNDLED") == "1" {
		return
	}
	exe, err := os.Executable()
	if err != nil {
		return
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	if strings.Contains(exe, ".app/Contents/MacOS/") {
		return
	}
	binBytes, err := os.ReadFile(exe)
	if err != nil {
		return
	}
	sum := sha256.New()
	sum.Write(binBytes)
	sum.Write(embeddedInfoPlist)
	key := hex.EncodeToString(sum.Sum(nil))

	cacheDir, err := os.UserCacheDir()
	if err != nil {
		cacheDir = os.TempDir()
	}
	root := filepath.Join(cacheDir, "goav", "studio-app")
	app := filepath.Join(root, "goav Studio.app")
	macOSDir := filepath.Join(app, "Contents", "MacOS")
	appBin := filepath.Join(macOSDir, "goav-studio")
	plistPath := filepath.Join(app, "Contents", "Info.plist")
	stampPath := filepath.Join(root, ".build-key")

	curStamp, _ := os.ReadFile(stampPath)
	if string(curStamp) != key || !fileExecutable(appBin) {
		if err := os.MkdirAll(macOSDir, 0o755); err != nil {
			return
		}
		if err := os.WriteFile(appBin, binBytes, 0o755); err != nil {
			return
		}
		if err := os.WriteFile(plistPath, embeddedInfoPlist, 0o644); err != nil {
			return
		}
		_ = exec.Command("codesign", "--force", "--sign", "-", app).Run()
		_ = os.WriteFile(stampPath, []byte(key), 0o644)
	}

	args := append([]string{appBin}, os.Args[1:]...)
	env := append(os.Environ(), "GOAV_STUDIO_BUNDLED=1")
	_ = syscall.Exec(appBin, args, env)
}

func fileExecutable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
}

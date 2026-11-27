//go:build darwin && (amd64 || arm64)

package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

//go:embed Info.plist
var embeddedInfoPlist []byte

// ensureMacOSAppBundle wraps and relaunches the current binary inside a stable,
// ad-hoc signed macOS .app bundle via LaunchServices (`open -W -n -a`).
// Launching through LaunchServices gives `goav Studio.app` its own macOS TCC
// identity (instead of inheriting the parent terminal's TCC attribution), so
// macOS prompts cleanly for Camera and Screen Recording access.
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

	openArgs := []string{"-W", "-n", "-a", app}
	if len(os.Args) > 1 {
		openArgs = append(openArgs, "--args")
		openArgs = append(openArgs, os.Args[1:]...)
	}
	fmt.Fprintf(os.Stderr, "launching %s...\n", app)
	cmd := exec.Command("open", openArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "goav Studio launch error: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func fileExecutable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
}

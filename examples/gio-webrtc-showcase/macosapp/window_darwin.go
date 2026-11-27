//go:build darwin && (amd64 || arm64)

// Package macosapp opens a native macOS NSWindow hosting a WKWebView without cgo,
// using purego and the Objective-C runtime directly.
package macosapp

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/ebitengine/purego"
	"github.com/ebitengine/purego/objc"
)

// Cocoa/AppKit requires UI and main-menu calls on OS thread 0.
// Locking in init() pins the main goroutine to thread 0 before main() can migrate.
func init() {
	runtime.LockOSThread()
}

type nsPoint struct{ X, Y float64 }
type nsSize struct{ Width, Height float64 }
type nsRect struct {
	Origin nsPoint
	Size   nsSize
}

func sel(name string) objc.SEL { return objc.RegisterName(name) }
func cls(name string) objc.ID  { return objc.ID(objc.GetClass(name)) }
func sendID(id objc.ID, name string, args ...any) objc.ID {
	return objc.Send[objc.ID](id, sel(name), args...)
}
func nsString(s string) objc.ID {
	return sendID(cls("NSString"), "stringWithUTF8String:", s)
}

var (
	loadOnce    sync.Once
	loadErr     error
	delegateCls objc.Class
)

func loadFrameworks() error {
	loadOnce.Do(func() {
		libs := []string{
			"/System/Library/Frameworks/Foundation.framework/Foundation",
			"/System/Library/Frameworks/AppKit.framework/AppKit",
			"/System/Library/Frameworks/WebKit.framework/WebKit",
		}
		for _, lib := range libs {
			if _, err := purego.Dlopen(lib, purego.RTLD_NOW|purego.RTLD_GLOBAL); err != nil {
				loadErr = fmt.Errorf("macosapp: dlopen %s: %w", lib, err)
				return
			}
		}
		delegateCls, loadErr = objc.RegisterClass(
			"GoAVStudioWindowDelegateV1",
			objc.GetClass("NSObject"),
			nil,
			nil,
			[]objc.MethodDef{
				{
					Cmd: sel("windowWillClose:"),
					Fn: func(self objc.ID, _cmd objc.SEL, _note objc.ID) {
						stopApp()
					},
				},
			},
		)
	})
	return loadErr
}

func stopApp() {
	app := sendID(cls("NSApplication"), "sharedApplication")
	if app == 0 {
		return
	}
	objc.Send[struct{}](app, sel("stop:"), objc.ID(0))
	// Post a dummy application-defined event so [NSApp run] exits immediately.
	ev := objc.Send[objc.ID](
		cls("NSEvent"),
		sel("otherEventWithType:location:modifierFlags:timestamp:windowNumber:context:subtype:data1:data2:"),
		uint64(15), // NSEventTypeApplicationDefined
		nsPoint{},
		uint64(0),
		float64(0),
		int64(0),
		objc.ID(0),
		int16(0),
		int64(0),
		int64(0),
	)
	if ev != 0 {
		objc.Send[struct{}](app, sel("postEvent:atStart:"), ev, true)
	}
}

// Run opens a native macOS window pointing at url and blocks on the Cocoa main
// run loop until the user closes the window or presses Cmd+Q.
// Must be called from the main goroutine (main.main).
func Run(title, url string, width, height float64) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := loadFrameworks(); err != nil {
		return err
	}

	pool := sendID(sendID(cls("NSAutoreleasePool"), "alloc"), "init")
	defer sendID(pool, "release")

	app := sendID(cls("NSApplication"), "sharedApplication")
	objc.Send[bool](app, sel("setActivationPolicy:"), int64(0)) // NSApplicationActivationPolicyRegular

	installMainMenu(app, title)

	// Configure WKWebView with autoplay enabled for live WebRTC tracks.
	cfg := sendID(sendID(cls("WKWebViewConfiguration"), "alloc"), "init")
	objc.Send[struct{}](cfg, sel("setMediaTypesRequiringUserActionForPlayback:"), uint64(0))
	objc.Send[struct{}](cfg, sel("setAllowsAirPlayForMediaPlayback:"), true)

	frame := nsRect{Size: nsSize{Width: width, Height: height}}
	webView := objc.Send[objc.ID](
		sendID(cls("WKWebView"), "alloc"),
		sel("initWithFrame:configuration:"),
		frame,
		cfg,
	)
	// Avoid white flash before dark page paints.
	noVal := sendID(cls("NSNumber"), "numberWithBool:", false)
	objc.Send[struct{}](webView, sel("setValue:forKey:"), noVal, nsString("drawsBackground"))

	const (
		styleTitled         = 1 << 0
		styleClosable       = 1 << 1
		styleMiniaturizable = 1 << 2
		styleResizable      = 1 << 3
		backingBuffered     = 2
	)
	window := objc.Send[objc.ID](
		sendID(cls("NSWindow"), "alloc"),
		sel("initWithContentRect:styleMask:backing:defer:"),
		frame,
		uint64(styleTitled|styleClosable|styleMiniaturizable|styleResizable),
		uint64(backingBuffered),
		false,
	)

	objc.Send[struct{}](window, sel("setTitle:"), nsString(title))
	objc.Send[struct{}](window, sel("setMinSize:"), nsSize{Width: 1024, Height: 680})
	if dark := sendID(cls("NSAppearance"), "appearanceNamed:", nsString("NSAppearanceNameDarkAqua")); dark != 0 {
		objc.Send[struct{}](window, sel("setAppearance:"), dark)
	}
	if bg := objc.Send[objc.ID](cls("NSColor"), sel("colorWithSRGBRed:green:blue:alpha:"), 0.055, 0.063, 0.082, 1.0); bg != 0 {
		objc.Send[struct{}](window, sel("setBackgroundColor:"), bg)
	}
	objc.Send[struct{}](window, sel("setTitlebarAppearsTransparent:"), true)
	objc.Send[struct{}](window, sel("setContentView:"), webView)

	delegate := sendID(sendID(objc.ID(delegateCls), "alloc"), "init")
	objc.Send[struct{}](window, sel("setDelegate:"), delegate)

	nsURL := sendID(cls("NSURL"), "URLWithString:", nsString(url))
	// cachePolicy = 1 (NSURLRequestReloadIgnoringLocalCacheData) prevents stale HTML/JS across rebuilds.
	req := objc.Send[objc.ID](cls("NSURLRequest"), sel("requestWithURL:cachePolicy:timeoutInterval:"), nsURL, uint64(1), float64(30))
	sendID(webView, "loadRequest:", req)

	objc.Send[struct{}](window, sel("center"))
	objc.Send[struct{}](window, sel("makeKeyAndOrderFront:"), objc.ID(0))
	objc.Send[struct{}](app, sel("activateIgnoringOtherApps:"), true)
	objc.Send[struct{}](app, sel("run"))
	return nil
}

func installMainMenu(app objc.ID, appTitle string) {
	mainMenu := sendID(sendID(cls("NSMenu"), "alloc"), "init")

	// App menu (Cmd+Q)
	appMenuItem := sendID(sendID(cls("NSMenuItem"), "alloc"), "init")
	appMenu := sendID(sendID(cls("NSMenu"), "alloc"), "initWithTitle:", nsString(appTitle))
	quitItem := objc.Send[objc.ID](
		sendID(cls("NSMenuItem"), "alloc"),
		sel("initWithTitle:action:keyEquivalent:"),
		nsString("Quit "+appTitle),
		sel("terminate:"),
		nsString("q"),
	)
	sendID(appMenu, "addItem:", quitItem)
	sendID(appMenuItem, "setSubmenu:", appMenu)
	sendID(mainMenu, "addItem:", appMenuItem)

	// Edit menu (Cut / Copy / Paste / Select All) so Cmd+C works on generated Go code.
	editMenuItem := sendID(sendID(cls("NSMenuItem"), "alloc"), "init")
	editMenu := sendID(sendID(cls("NSMenu"), "alloc"), "initWithTitle:", nsString("Edit"))
	for _, entry := range [][3]string{
		{"Cut", "cut:", "x"},
		{"Copy", "copy:", "c"},
		{"Paste", "paste:", "v"},
		{"Select All", "selectAll:", "a"},
	} {
		item := objc.Send[objc.ID](
			sendID(cls("NSMenuItem"), "alloc"),
			sel("initWithTitle:action:keyEquivalent:"),
			nsString(entry[0]),
			sel(entry[1]),
			nsString(entry[2]),
		)
		sendID(editMenu, "addItem:", item)
	}
	sendID(editMenuItem, "setSubmenu:", editMenu)
	sendID(mainMenu, "addItem:", editMenuItem)

	sendID(app, "setMainMenu:", mainMenu)
}

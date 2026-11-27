package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thesyncim/goav/examples/gio-webrtc-showcase/macosapp"
)

func main() {
	addr := flag.String("addr", "", "HTTP listen address (default 127.0.0.1:8080, auto-picks free port if busy; use :8080 for LAN)")
	headless := flag.Bool("headless", false, "run without opening the native macOS window")
	flag.Parse()

	ensureMacOSAppBundle(*headless)

	var listener net.Listener
	var err error
	if *addr != "" {
		listener, err = net.Listen("tcp", *addr)
	} else {
		listener, err = net.Listen("tcp", "127.0.0.1:8080")
		if err != nil {
			listener, err = net.Listen("tcp", "127.0.0.1:0")
		}
	}
	if err != nil {
		log.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	baseURL := fmt.Sprintf("http://127.0.0.1:%s", port)

	st := newStudio(baseURL)
	if err := st.Start(context.Background()); err != nil {
		log.Fatalf("start studio pipeline: %v", err)
	}
	defer st.Close()

	srv := newStudioServer(st)
	mux := http.NewServeMux()
	srv.routes(mux)
	httpServer := &http.Server{Handler: mux}

	go func() {
		log.Printf("goav Studio ready -> %s (viewer room: %s/watch)", baseURL, baseURL)
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("http server error: %v", err)
		}
	}()

	if *headless {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		return
	}

	// In native macOS app mode, automatically request/detect camera & screen access
	// once the window is up, and wire real camera/screen into the unified goav.Select task.
	go func() {
		time.Sleep(500 * time.Millisecond)
		camOK := st.RequestCameraAccess(context.Background()) == nil
		st.refreshDevices()
		snap := st.Snapshot()
		scrOK := snap.Devices.ScreenAccess
		target := "pattern"
		switch {
		case camOK && scrOK:
			target = "pip"
		case camOK:
			target = "camera"
		case scrOK:
			target = "screen"
		}
		if target != "pattern" {
			next := snap.Config
			next.Scene = target
			_ = st.UpdateConfig(context.Background(), next)
		}
	}()

	// Open the native CGO_ENABLED=0 macOS NSWindow + WKWebView on the main thread.
	if err := macosapp.Run("goav Studio — Pure Go Media & WebRTC", baseURL, 1440, 920); err != nil {
		log.Printf("native window unavailable (%v); continuing headless at %s", err, baseURL)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		<-ctx.Done()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

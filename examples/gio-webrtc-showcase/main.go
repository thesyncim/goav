//go:build cgo

package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	gioapp "gioui.org/app"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/std"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	headless := flag.Bool("headless", false, "serve the browser peer without opening the Gio control room")
	flag.Parse()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	browserURL := "http://localhost" + listenPort(listener.Addr().String())

	showcase := newServer(std.New(goav.WithEventCapacity(2048)), browserURL)
	mux := http.NewServeMux()
	showcase.routes(mux)
	httpServer := &http.Server{Handler: logRequest(mux)}

	go func() {
		log.Printf("gio webrtc showcase browser peer listening on %s", browserURL)
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	if *headless {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		<-ctx.Done()
		_ = httpServer.Shutdown(context.Background())
		return
	}

	go func() {
		if err := runControlRoom(showcase, browserURL); err != nil {
			log.Printf("gio control room stopped: %v", err)
		}
		_ = httpServer.Shutdown(context.Background())
	}()
	gioapp.Main()
}

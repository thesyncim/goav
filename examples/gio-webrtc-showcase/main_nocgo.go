//go:build !cgo

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

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	headless := flag.Bool("headless", true, "serve the browser peer without opening the Gio control room")
	flag.Parse()

	if !*headless {
		log.Print("CGO_ENABLED=0 build: Gio control room unavailable; serving the browser peer headlessly")
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	browserURL := "http://localhost" + listenPort(listener.Addr().String())

	showcase := newServer(bundle.MustNew(goav.WithEventCapacity(2048)), browserURL)
	mux := http.NewServeMux()
	showcase.routes(mux)
	httpServer := &http.Server{Handler: logRequest(mux)}

	go func() {
		log.Printf("gio webrtc showcase browser peer listening on %s", browserURL)
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	_ = httpServer.Shutdown(context.Background())
}

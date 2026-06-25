package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/thesyncim/goav/bundle"
	goavruntime "github.com/thesyncim/goav/runtime"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	app := newServer(bundle.MustNew(goavruntime.WithEventCapacity(1024)))
	mux := http.NewServeMux()
	app.routes(mux)

	log.Printf("webrtc runtime ladder listening on http://localhost%s", listenPort(*addr))
	if err := http.ListenAndServe(*addr, logRequest(mux)); err != nil {
		log.Fatal(err)
	}
}

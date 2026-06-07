package main

import (
	"embed"
	"net/http"
)

//go:embed static/*
var staticFiles embed.FS

func serveStatic(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		http.ServeFileFS(w, r, staticFiles, "static/index.html")
		return
	}
	http.FileServerFS(staticFiles).ServeHTTP(w, r)
}

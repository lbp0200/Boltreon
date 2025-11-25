package main

import (
	"flag"
	"github.com/lbp0200/Boltreon/internal/server"
	"log"
	"net"
	"os"

	"github.com/lbp0200/Boltreon/internal/store"
)

func main() {
	addr := flag.String("addr", ":6379", "listen addr")
	dbPath := flag.String("dir", os.TempDir(), "badger dir")
	flag.Parse()

	db, err := store.NewBoltreonStore(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	handler := &server.Handler{Db: db}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Boltreon listening on %s", *addr)
	if err := handler.ServeTCP(ln); err != nil {
		log.Fatal(err)
	}
}

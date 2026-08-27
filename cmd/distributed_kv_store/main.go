package main

import (
	"log"
	"os"

	"github.com/pranavbhole123/distributed_kv_store/internal/config"
	"github.com/pranavbhole123/distributed_kv_store/internal/server"
	"github.com/pranavbhole123/distributed_kv_store/internal/store"
	"github.com/pranavbhole123/distributed_kv_store/internal/wal"
)

const maxLength = 3000 // max lenght of value can be configured bys user or taken from user input from config file

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("usage: %s <config.yaml>", os.Args[0])
	}

	cfg, err := config.Load(os.Args[1])
	if err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	//before starting replay the wal
	wal, err := wal.NewWAL(cfg.WALPath())

	if err != nil {
		log.Fatalf("error openign wal :%v", err)
	}
	// now replay the entries
	entries, err := wal.Replay()
	if err != nil {
		log.Fatal(err)
	}

	memStore := store.NewMemoryStore(maxLength)

	for _, entry := range entries {
		switch entry.Op {
		case "SET":
			memStore.Set(entry.Key, entry.Value)
		case "DELETE":
			memStore.Delete(entry.Key)
		}
	}

	server := server.NewServer(cfg.Self.HTTPAddr, memStore, wal)

	log.Fatalf("msg: %v", server.Start())

}

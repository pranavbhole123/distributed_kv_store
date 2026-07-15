package main

import (
	"log"

	"github.com/pranavbhole123/distributed_kv_store/internal/server"
	"github.com/pranavbhole123/distributed_kv_store/internal/store"
	"github.com/pranavbhole123/distributed_kv_store/internal/wal"
)

const maxLength = 3000 // max lenght of value can be configured bys user or taken from user input from config file

func main() {

	//before starting replay the wal
	wal, err := wal.NewWAL("data/wal.log")

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

	server := server.NewServer(8080, memStore, wal)

	log.Fatalf("msg: %v", server.Start())

}

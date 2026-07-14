package main

import (
	"log"

	"github.com/pranavbhole123/distributed_kv_store/internal/server"
	"github.com/pranavbhole123/distributed_kv_store/internal/store"
)


const maxLength = 3000 // max lenght of value can be configured bys user or taken from user input from config file

func main(){
	
	memStore := store.NewMemoryStore(maxLength)

	
	server := server.NewServer(8080 , memStore)

	log.Fatalf("msg: %v", server.Start())
	
}
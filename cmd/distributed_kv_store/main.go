package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/pranavbhole123/distributed_kv_store/internal/config"
	"github.com/pranavbhole123/distributed_kv_store/internal/node"
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

	n, err := node.New(cfg, maxLength)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := n.Start(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("run node: %v", err)
	}

}

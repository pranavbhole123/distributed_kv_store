// Package node is the composition root for one KV-store process.
package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/pranavbhole123/distributed_kv_store/internal/config"
	"github.com/pranavbhole123/distributed_kv_store/internal/raft"
	"github.com/pranavbhole123/distributed_kv_store/internal/server"
	"github.com/pranavbhole123/distributed_kv_store/internal/store"
	"github.com/pranavbhole123/distributed_kv_store/internal/transport"
	"google.golang.org/grpc"
)

type Node struct {
	cfg       config.Config
	raft      *raft.Raft
	logStore  raft.LogStore
	transport *transport.GRPCTransport
	http      *server.Server

	grpcServer   *grpc.Server
	grpcListener net.Listener
	stopOnce     sync.Once
}

func New(cfg config.Config, maxValueLength int) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	logStore, err := raft.NewFileLogStore(cfg.RaftLogPath())
	if err != nil {
		return nil, fmt.Errorf("open Raft log: %w", err)
	}
	closeLog := true
	defer func() {
		if closeLog {
			_ = logStore.Close()
		}
	}()

	memoryStore := store.NewMemoryStore(maxValueLength)

	grpcTransport := transport.NewGRPCTransport()
	raftNode, err := raft.NewWithLog(cfg, raft.NewFileStableStore(cfg.RaftStatePath()), logStore, storeApplier{store: memoryStore}, grpcTransport)
	if err != nil {
		_ = grpcTransport.Close()
		return nil, err
	}

	n := &Node{
		cfg:       cfg,
		raft:      raftNode,
		logStore:  logStore,
		transport: grpcTransport,
	}
	n.http = server.NewServer(cfg.Self.HTTPAddr, memoryStore, n)
	closeLog = false
	return n, nil
}

// Start begins the gRPC Raft listener, election loop, and HTTP server. It
// blocks until the HTTP server is shut down or fails.
func (n *Node) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", n.cfg.Self.RaftAddr)
	if err != nil {
		return fmt.Errorf("listen for Raft gRPC at %s: %w", n.cfg.Self.RaftAddr, err)
	}
	n.grpcListener = listener
	n.grpcServer = grpc.NewServer()
	transport.RegisterRaftRPCServer(n.grpcServer, n.raft)
	go func() {
		_ = n.grpcServer.Serve(listener)
	}()

	n.raft.Start(ctx)
	go func() {
		<-ctx.Done()
		_ = n.Stop(context.Background())
	}()

	err = n.http.Start()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (n *Node) Stop(ctx context.Context) error {
	var stopErr error
	n.stopOnce.Do(func() {
		n.raft.Stop()
		if n.grpcServer != nil {
			n.grpcServer.Stop()
		}
		if n.grpcListener != nil {
			if err := n.grpcListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				stopErr = err
			}
		}
		if err := n.http.Shutdown(ctx); err != nil && stopErr == nil {
			stopErr = err
		}
		if err := n.transport.Close(); err != nil && stopErr == nil {
			stopErr = err
		}
		if err := n.logStore.Close(); err != nil && stopErr == nil {
			stopErr = err
		}
	})
	return stopErr
}

// The following methods implement server.ClusterStatus.
func (n *Node) LeaderID() int { return n.raft.LeaderID() }

func (n *Node) CurrentTerm() uint64 { return n.raft.CurrentTerm() }

func (n *Node) LeaderHTTPAddr() (string, bool) {
	leaderID := n.raft.LeaderID()
	if leaderID == raft.UnknownLeader {
		return "", false
	}
	if leaderID == n.cfg.Self.ID {
		return n.cfg.Self.HTTPAddr, true
	}
	peer, found := n.cfg.PeerByID(leaderID)
	if !found {
		return "", false
	}
	return peer.HTTPAddr, true
}

// WritesReady remains false until Phase 4, when Raft replication can commit a
// command on a majority before the HTTP handler acknowledges the client.
func (n *Node) WritesReady() bool { return false }

type storeApplier struct {
	store store.Store
}

func (a storeApplier) Apply(entry raft.LogEntry) error {
	switch entry.Operation {
	case raft.SetOperation:
		return a.store.Set(entry.Key, entry.Value)
	case raft.DeleteOperation:
		return a.store.Delete(entry.Key)
	default:
		return fmt.Errorf("apply unknown Raft operation %q", entry.Operation)
	}
}

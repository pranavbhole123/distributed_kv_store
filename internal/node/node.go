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
	"github.com/pranavbhole123/distributed_kv_store/internal/wal"
	"google.golang.org/grpc"
)

type Node struct {
	cfg       config.Config
	raft      *raft.Raft
	store     store.Store
	wal       *wal.WAL
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

	w, err := wal.NewWAL(cfg.WALPath())
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	closeWAL := true
	defer func() {
		if closeWAL {
			_ = w.Close()
		}
	}()

	memoryStore := store.NewMemoryStore(maxValueLength)
	entries, err := w.Replay()
	if err != nil {
		return nil, fmt.Errorf("replay WAL: %w", err)
	}
	for _, entry := range entries {
		switch entry.Op {
		case "SET":
			if err := memoryStore.Set(entry.Key, entry.Value); err != nil {
				return nil, fmt.Errorf("replay SET for key %q: %w", entry.Key, err)
			}
		case "DELETE":
			// Deleting a key which is already absent is harmless during replay.
			_ = memoryStore.Delete(entry.Key)
		default:
			return nil, fmt.Errorf("replay WAL: unknown operation %q", entry.Op)
		}
	}

	grpcTransport := transport.NewGRPCTransport()
	raftNode, err := raft.New(cfg, raft.NewFileStableStore(cfg.RaftStatePath()), grpcTransport)
	if err != nil {
		_ = grpcTransport.Close()
		return nil, err
	}

	n := &Node{
		cfg:       cfg,
		raft:      raftNode,
		store:     memoryStore,
		wal:       w,
		transport: grpcTransport,
	}
	n.http = server.NewServer(cfg.Self.HTTPAddr, memoryStore, w, n)
	closeWAL = false
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
		if err := n.wal.Close(); err != nil && stopErr == nil {
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

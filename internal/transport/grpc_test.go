package transport

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/pranavbhole123/distributed_kv_store/internal/config"
	"github.com/pranavbhole123/distributed_kv_store/internal/raft"
	"google.golang.org/grpc"
)

func TestGRPCTransportDeliversRaftRPCs(t *testing.T) {
	target, err := raft.New(config.Config{
		Self:    config.Node{ID: 2, RaftAddr: "unused", HTTPAddr: "unused"},
		Peers:   []config.Node{{ID: 1, RaftAddr: "unused-peer", HTTPAddr: "unused-peer"}},
		DataDir: "unused",

		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}, raft.NewFileStableStore(filepath.Join(t.TempDir(), "raft-state.json")), nil)
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	RegisterRaftRPCServer(server, target)
	go server.Serve(listener)
	t.Cleanup(func() {
		server.Stop()
		listener.Close()
	})

	transport := NewGRPCTransport()
	t.Cleanup(func() { _ = transport.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	peer := config.Node{ID: 2, RaftAddr: listener.Addr().String(), HTTPAddr: "unused"}

	vote, err := transport.RequestVote(ctx, peer, raft.RequestVoteRequest{Term: 1, CandidateID: 1})
	if err != nil {
		t.Fatalf("RequestVote() error = %v", err)
	}
	if !vote.VoteGranted || vote.Term != 1 {
		t.Fatalf("RequestVote() = %+v, want granted vote in term 1", vote)
	}

	heartbeat, err := transport.AppendEntries(ctx, peer, raft.AppendEntriesRequest{Term: 1, LeaderID: 1})
	if err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}
	if !heartbeat.Success || target.LeaderID() != 1 {
		t.Fatalf("AppendEntries() = %+v, target leader = %d; want successful heartbeat from leader 1", heartbeat, target.LeaderID())
	}
}

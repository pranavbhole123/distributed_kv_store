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
		Self: config.Node{ID: 2, RaftAddr: "unused", HTTPAddr: "unused"},
		Peers: []config.Node{
			{ID: 1, RaftAddr: "unused-peer-1", HTTPAddr: "unused-peer-1"},
			{ID: 3, RaftAddr: "unused-peer-3", HTTPAddr: "unused-peer-3"},
		},
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

func TestNoopLogEntryRoundTripsThroughProto(t *testing.T) {
	request := raft.AppendEntriesRequest{
		Term:     4,
		LeaderID: 1,
		Entries: []raft.LogEntry{
			{Index: 7, Term: 4, Operation: raft.NoopOperation},
		},
	}
	roundTripped := appendEntriesFromProto(appendEntriesToProto(request))
	if len(roundTripped.Entries) != 1 || roundTripped.Entries[0] != request.Entries[0] {
		t.Fatalf("round-tripped entries = %+v, want %+v", roundTripped.Entries, request.Entries)
	}
}

func TestTwoFreshNodesElectLeaderWithThirdPeerOffline(t *testing.T) {
	listener1 := newTestListener(t)
	listener2 := newTestListener(t)
	offlineListener := newTestListener(t)
	offlineAddress := offlineListener.Addr().String()
	if err := offlineListener.Close(); err != nil {
		t.Fatal(err)
	}

	node1Config := threeNodeConfig(1, listener1.Addr().String(), listener2.Addr().String(), offlineAddress)
	node2Config := threeNodeConfig(2, listener1.Addr().String(), listener2.Addr().String(), offlineAddress)
	transport1 := NewGRPCTransport()
	transport2 := NewGRPCTransport()
	node1, err := raft.New(node1Config, raft.NewFileStableStore(filepath.Join(t.TempDir(), "node-1-state.json")), transport1)
	if err != nil {
		t.Fatal(err)
	}
	node2, err := raft.New(node2Config, raft.NewFileStableStore(filepath.Join(t.TempDir(), "node-2-state.json")), transport2)
	if err != nil {
		t.Fatal(err)
	}

	server1 := grpc.NewServer()
	server2 := grpc.NewServer()
	RegisterRaftRPCServer(server1, node1)
	RegisterRaftRPCServer(server2, node2)
	go func() { _ = server1.Serve(listener1) }()
	go func() { _ = server2.Serve(listener2) }()
	t.Cleanup(func() {
		node1.Stop()
		node2.Stop()
		server1.Stop()
		server2.Stop()
		_ = listener1.Close()
		_ = listener2.Close()
		_ = transport1.Close()
		_ = transport2.Close()
	})

	node1.Start(context.Background())
	node2.Start(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		leaders := 0
		for _, node := range []*raft.Raft{node1, node2} {
			if node.IsLeader() {
				leaders++
			}
		}
		if leaders == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("two healthy nodes did not elect one leader; states are node1=%v node2=%v", node1.State(), node2.State())
}

func newTestListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func threeNodeConfig(id int, node1Address, node2Address, node3Address string) config.Config {
	nodes := []config.Node{
		{ID: 1, RaftAddr: node1Address, HTTPAddr: "127.0.0.1:18080"},
		{ID: 2, RaftAddr: node2Address, HTTPAddr: "127.0.0.1:18081"},
		{ID: 3, RaftAddr: node3Address, HTTPAddr: "127.0.0.1:18082"},
	}
	var self config.Node
	peers := make([]config.Node, 0, 2)
	for _, node := range nodes {
		if node.ID == id {
			self = node
		} else {
			peers = append(peers, node)
		}
	}
	return config.Config{
		Self:               self,
		Peers:              peers,
		DataDir:            "unused",
		ElectionTimeoutMin: 100 * time.Millisecond,
		ElectionTimeoutMax: 200 * time.Millisecond,
		HeartbeatInterval:  25 * time.Millisecond,
	}
}

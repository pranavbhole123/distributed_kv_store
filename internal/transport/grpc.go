package transport

import (
	"context"
	"fmt"
	"sync"

	"github.com/pranavbhole123/distributed_kv_store/internal/config"
	"github.com/pranavbhole123/distributed_kv_store/internal/raft"
	raftpb "github.com/pranavbhole123/distributed_kv_store/internal/transport/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCTransport implements raft.Transport for outgoing RPCs. Connections are
// created lazily and reused for later heartbeats and elections.
type GRPCTransport struct {
	mu      sync.Mutex
	clients map[string]raftpb.RaftServiceClient
	conns   map[string]*grpc.ClientConn
}

var _ raft.Transport = (*GRPCTransport)(nil)

func NewGRPCTransport() *GRPCTransport {
	return &GRPCTransport{
		clients: make(map[string]raftpb.RaftServiceClient),
		conns:   make(map[string]*grpc.ClientConn),
	}
}

func (t *GRPCTransport) RequestVote(ctx context.Context, peer config.Node, request raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	client, err := t.client(peer.RaftAddr)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	response, err := client.RequestVote(ctx, requestVoteToProto(request))
	if err != nil {
		return raft.RequestVoteResponse{}, fmt.Errorf("RequestVote to node %d at %s: %w", peer.ID, peer.RaftAddr, err)
	}
	return requestVoteResponseFromProto(response), nil
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, peer config.Node, request raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	client, err := t.client(peer.RaftAddr)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	response, err := client.AppendEntries(ctx, appendEntriesToProto(request))
	if err != nil {
		return raft.AppendEntriesResponse{}, fmt.Errorf("AppendEntries to node %d at %s: %w", peer.ID, peer.RaftAddr, err)
	}
	return appendEntriesResponseFromProto(response), nil
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, peer config.Node, request raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	client, err := t.client(peer.RaftAddr)
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	response, err := client.InstallSnapshot(ctx, installSnapshotToProto(request))
	if err != nil {
		return raft.InstallSnapshotResponse{}, fmt.Errorf("InstallSnapshot to node %d at %s: %w", peer.ID, peer.RaftAddr, err)
	}
	return installSnapshotResponseFromProto(response), nil
}

func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var firstErr error
	for address, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close gRPC connection to %s: %w", address, err)
		}
	}
	t.clients = make(map[string]raftpb.RaftServiceClient)
	t.conns = make(map[string]*grpc.ClientConn)
	return firstErr
}

func (t *GRPCTransport) client(address string) (raftpb.RaftServiceClient, error) {
	t.mu.Lock()
	if client, ok := t.clients[address]; ok {
		t.mu.Unlock()
		return client, nil
	}
	t.mu.Unlock()

	// NewClient creates a reusable ClientConn without waiting for a network
	// connection. The RPC context controls each actual request's deadline, so a
	// dead peer cannot block client creation for healthy peers.
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("create gRPC client for peer %s: %w", address, err)
	}
	client := raftpb.NewRaftServiceClient(conn)

	t.mu.Lock()
	defer t.mu.Unlock()
	// Another RPC may have created this peer connection while NewClient was
	// running. Keep one shared connection and close the duplicate.
	if existing, ok := t.clients[address]; ok {
		if err := conn.Close(); err != nil {
			return nil, fmt.Errorf("close duplicate gRPC connection to %s: %w", address, err)
		}
		return existing, nil
	}
	t.conns[address] = conn
	t.clients[address] = client
	return client, nil
}

// RaftRPCServer maps incoming protobuf/gRPC requests onto a Raft instance.
type RaftRPCServer struct {
	raftpb.UnimplementedRaftServiceServer
	raft *raft.Raft
}

func NewRaftRPCServer(node *raft.Raft) *RaftRPCServer {
	return &RaftRPCServer{raft: node}
}

func (s *RaftRPCServer) RequestVote(ctx context.Context, request *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	response := s.raft.HandleRequestVote(ctx, requestVoteFromProto(request))
	return requestVoteResponseToProto(response), nil
}

func (s *RaftRPCServer) AppendEntries(ctx context.Context, request *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	response := s.raft.HandleAppendEntries(ctx, appendEntriesFromProto(request))
	return appendEntriesResponseToProto(response), nil
}

func (s *RaftRPCServer) InstallSnapshot(ctx context.Context, request *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	response, err := s.raft.HandleInstallSnapshot(ctx, installSnapshotFromProto(request))
	if err != nil {
		return nil, err
	}
	return installSnapshotResponseToProto(response), nil
}

func RegisterRaftRPCServer(server *grpc.Server, node *raft.Raft) {
	raftpb.RegisterRaftServiceServer(server, NewRaftRPCServer(node))
}

package raft

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pranavbhole123/distributed_kv_store/internal/config"
)

type localTransport struct {
	mu    sync.RWMutex
	nodes map[int]*Raft
}

func newLocalTransport() *localTransport {
	return &localTransport{nodes: make(map[int]*Raft)}
}

func (t *localTransport) register(node *Raft) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[node.ID()] = node
}

func (t *localTransport) unregister(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.nodes, id)
}

func (t *localTransport) RequestVote(ctx context.Context, peer config.Node, request RequestVoteRequest) (RequestVoteResponse, error) {
	t.mu.RLock()
	node := t.nodes[peer.ID]
	t.mu.RUnlock()
	if node == nil {
		return RequestVoteResponse{}, errors.New("peer unavailable")
	}
	select {
	case <-ctx.Done():
		return RequestVoteResponse{}, ctx.Err()
	default:
		return node.HandleRequestVote(ctx, request), nil
	}
}

func (t *localTransport) AppendEntries(ctx context.Context, peer config.Node, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	t.mu.RLock()
	node := t.nodes[peer.ID]
	t.mu.RUnlock()
	if node == nil {
		return AppendEntriesResponse{}, errors.New("peer unavailable")
	}
	select {
	case <-ctx.Done():
		return AppendEntriesResponse{}, ctx.Err()
	default:
		return node.HandleAppendEntries(ctx, request), nil
	}
}

func TestThreeNodesElectOneLeader(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	_ = waitForStableLeader(t, nodes, time.Second)
}

func TestRemainingNodesReplaceStoppedLeader(t *testing.T) {
	nodes, transport := newTestCluster(t, 3)
	oldLeader := waitForStableLeader(t, nodes, time.Second)
	oldLeader.Stop()
	transport.unregister(oldLeader.ID())

	active := make([]*Raft, 0, 2)
	for _, node := range nodes {
		if node.ID() != oldLeader.ID() {
			active = append(active, node)
		}
	}
	newLeader := waitForStableLeader(t, active, time.Second)
	if newLeader.ID() == oldLeader.ID() {
		t.Fatalf("old leader %d was elected again", oldLeader.ID())
	}
}

func TestIsolatedNodeCannotElectItself(t *testing.T) {
	transport := newLocalTransport()
	cfg := testConfig(1, 3)
	node, err := New(cfg, NewFileStableStore(filepath.Join(t.TempDir(), "raft-state.json")), transport)
	if err != nil {
		t.Fatal(err)
	}
	transport.register(node)
	node.Start(context.Background())
	t.Cleanup(node.Stop)

	time.Sleep(350 * time.Millisecond)
	if node.IsLeader() {
		t.Fatal("isolated node became leader without a majority")
	}
}

func TestHigherTermHeartbeatStepsCandidateDown(t *testing.T) {
	node, err := New(testConfig(1, 3), NewFileStableStore(filepath.Join(t.TempDir(), "raft-state.json")), newLocalTransport())
	if err != nil {
		t.Fatal(err)
	}

	node.startElection(context.Background())
	if node.State() != Candidate || node.CurrentTerm() != 1 || node.votedFor != 1 {
		t.Fatalf("after election start: state=%v term=%d vote=%d; want candidate, term 1, self vote", node.State(), node.CurrentTerm(), node.votedFor)
	}

	response := node.HandleAppendEntries(context.Background(), AppendEntriesRequest{Term: 2, LeaderID: 2})
	if !response.Success || node.State() != Follower || node.CurrentTerm() != 2 || node.votedFor != NoVote || node.LeaderID() != 2 {
		t.Fatalf("after higher-term heartbeat: response=%+v state=%v term=%d vote=%d leader=%d", response, node.State(), node.CurrentTerm(), node.votedFor, node.LeaderID())
	}
}

func TestVoteIsPersistedAndNotGrantedTwiceInOneTerm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.json")
	node, err := New(testConfig(1, 3), NewFileStableStore(path), newLocalTransport())
	if err != nil {
		t.Fatal(err)
	}
	if response := node.HandleRequestVote(context.Background(), RequestVoteRequest{Term: 4, CandidateID: 2}); !response.VoteGranted {
		t.Fatal("first vote was not granted")
	}

	afterRestart, err := New(testConfig(1, 3), NewFileStableStore(path), newLocalTransport())
	if err != nil {
		t.Fatal(err)
	}
	if response := afterRestart.HandleRequestVote(context.Background(), RequestVoteRequest{Term: 4, CandidateID: 3}); response.VoteGranted {
		t.Fatal("node granted a second vote in the same term after restart")
	}
}

func newTestCluster(t *testing.T, size int) ([]*Raft, *localTransport) {
	t.Helper()
	transport := newLocalTransport()
	nodes := make([]*Raft, 0, size)
	for id := 1; id <= size; id++ {
		node, err := NewWithLog(
			testConfig(id, size),
			NewFileStableStore(filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", id), "raft-state.json")),
			newMemoryLogStore(),
			&recordingApplier{},
			transport,
		)
		if err != nil {
			t.Fatal(err)
		}
		transport.register(node)
		nodes = append(nodes, node)
	}
	for _, node := range nodes {
		node.Start(context.Background())
		t.Cleanup(node.Stop)
	}
	return nodes, transport
}

func testConfig(id, size int) config.Config {
	peers := make([]config.Node, 0, size-1)
	for otherID := 1; otherID <= size; otherID++ {
		if otherID == id {
			continue
		}
		peers = append(peers, config.Node{
			ID:       otherID,
			RaftAddr: fmt.Sprintf("raft-%d", otherID),
			HTTPAddr: fmt.Sprintf("http-%d", otherID),
		})
	}
	return config.Config{
		Self:               config.Node{ID: id, RaftAddr: fmt.Sprintf("raft-%d", id), HTTPAddr: fmt.Sprintf("http-%d", id)},
		Peers:              peers,
		DataDir:            "unused-in-test",
		ElectionTimeoutMin: 60 * time.Millisecond,
		ElectionTimeoutMax: 120 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
	}
}

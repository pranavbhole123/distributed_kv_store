package raft

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/pranavbhole123/distributed_kv_store/internal/config"
)

// partitionNetwork gives each Raft node its own transport endpoint, so tests
// can model directional failures without teaching production Raft about test
// partitions.
type partitionNetwork struct {
	mu      sync.RWMutex
	nodes   map[int]*Raft
	blocked map[networkLink]bool
}

type networkLink struct{ from, to int }

type networkEndpoint struct {
	network *partitionNetwork
	from    int
}

func newPartitionNetwork() *partitionNetwork {
	return &partitionNetwork{
		nodes:   make(map[int]*Raft),
		blocked: make(map[networkLink]bool),
	}
}

func (n *partitionNetwork) endpoint(id int) Transport {
	return networkEndpoint{network: n, from: id}
}

func (n *partitionNetwork) register(node *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[node.ID()] = node
}

func (n *partitionNetwork) unregister(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.nodes, id)
}

func (n *partitionNetwork) isolate(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for otherID := range n.nodes {
		if otherID == id {
			continue
		}
		n.blocked[networkLink{from: id, to: otherID}] = true
		n.blocked[networkLink{from: otherID, to: id}] = true
	}
}

func (n *partitionNetwork) heal(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for otherID := range n.nodes {
		if otherID == id {
			continue
		}
		delete(n.blocked, networkLink{from: id, to: otherID})
		delete(n.blocked, networkLink{from: otherID, to: id})
	}
}

func (e networkEndpoint) RequestVote(ctx context.Context, peer config.Node, request RequestVoteRequest) (RequestVoteResponse, error) {
	node, err := e.network.target(e.from, peer.ID)
	if err != nil {
		return RequestVoteResponse{}, err
	}
	select {
	case <-ctx.Done():
		return RequestVoteResponse{}, ctx.Err()
	default:
		return node.HandleRequestVote(ctx, request), nil
	}
}

func (e networkEndpoint) AppendEntries(ctx context.Context, peer config.Node, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	node, err := e.network.target(e.from, peer.ID)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	select {
	case <-ctx.Done():
		return AppendEntriesResponse{}, ctx.Err()
	default:
		return node.HandleAppendEntries(ctx, request), nil
	}
}

func (e networkEndpoint) InstallSnapshot(ctx context.Context, peer config.Node, request InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	node, err := e.network.target(e.from, peer.ID)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}
	select {
	case <-ctx.Done():
		return InstallSnapshotResponse{}, ctx.Err()
	default:
		return node.HandleInstallSnapshot(ctx, request)
	}
}

func (n *partitionNetwork) target(from, to int) (*Raft, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.blocked[networkLink{from: from, to: to}] {
		return nil, fmt.Errorf("network link %d -> %d is partitioned", from, to)
	}
	node := n.nodes[to]
	if node == nil {
		return nil, fmt.Errorf("node %d is unavailable", to)
	}
	return node, nil
}

type kvApplier struct {
	mu      sync.RWMutex
	data    map[string]string
	entries []LogEntry
}

func newKVApplier() *kvApplier {
	return &kvApplier{data: make(map[string]string)}
}

func (a *kvApplier) Apply(entry LogEntry) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = append(a.entries, entry)
	switch entry.Operation {
	case NoopOperation:
	case SetOperation:
		a.data[entry.Key] = entry.Value
	case DeleteOperation:
		delete(a.data, entry.Key)
	default:
		return fmt.Errorf("unknown operation %q", entry.Operation)
	}
	return nil
}

func (a *kvApplier) value(key string) (string, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	value, found := a.data[key]
	return value, found
}

type failureNode struct {
	id       int
	config   config.Config
	stable   *FileStableStore
	logPath  string
	logStore *FileLogStore
	raft     *Raft
	applier  *kvApplier
	online   bool
}

type failureCluster struct {
	t       *testing.T
	dir     string
	network *partitionNetwork
	nodes   map[int]*failureNode
}

func newFailureCluster(t *testing.T) *failureCluster {
	t.Helper()
	c := &failureCluster{
		t:       t,
		dir:     t.TempDir(),
		network: newPartitionNetwork(),
		nodes:   make(map[int]*failureNode, 3),
	}
	for id := 1; id <= 3; id++ {
		c.startNode(id)
	}
	t.Cleanup(c.close)
	return c
}

func (c *failureCluster) startNode(id int) {
	c.t.Helper()
	nodeDir := filepath.Join(c.dir, fmt.Sprintf("node-%d", id))
	stable := NewFileStableStore(filepath.Join(nodeDir, "raft-state.json"))
	logPath := filepath.Join(nodeDir, "raft-log.wal")
	logStore, err := NewFileLogStore(logPath)
	if err != nil {
		c.t.Fatal(err)
	}
	applier := newKVApplier()
	r, err := NewWithLog(testConfig(id, 3), stable, logStore, applier, c.network.endpoint(id))
	if err != nil {
		_ = logStore.Close()
		c.t.Fatal(err)
	}
	node := &failureNode{
		id:       id,
		config:   testConfig(id, 3),
		stable:   stable,
		logPath:  logPath,
		logStore: logStore,
		raft:     r,
		applier:  applier,
		online:   true,
	}
	c.nodes[id] = node
	c.network.register(r)
	r.Start(context.Background())
}

func (c *failureCluster) stopNode(id int) {
	c.t.Helper()
	node := c.nodes[id]
	if node == nil || !node.online {
		return
	}
	c.network.unregister(id)
	node.raft.Stop()
	if err := node.logStore.Close(); err != nil {
		c.t.Fatal(err)
	}
	node.online = false
}

func (c *failureCluster) restartNode(id int) {
	c.stopNode(id)
	c.startNode(id)
}

func (c *failureCluster) activeRafts() []*Raft {
	active := make([]*Raft, 0, 3)
	for id := 1; id <= 3; id++ {
		if node := c.nodes[id]; node != nil && node.online {
			active = append(active, node.raft)
		}
	}
	return active
}

func (c *failureCluster) otherIDs(id int) []int {
	others := make([]int, 0, 2)
	for candidate := 1; candidate <= 3; candidate++ {
		if candidate != id {
			others = append(others, candidate)
		}
	}
	return others
}

func (c *failureCluster) close() {
	for id := 1; id <= 3; id++ {
		c.stopNode(id)
	}
}

func waitForProposal(t *testing.T, r *Raft, command Command, timeout time.Duration) uint64 {
	t.Helper()
	index, result, err := r.Propose(command)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.Index != index || got.Err != nil {
			t.Fatalf("proposal result = %+v, want success for index %d", got, index)
		}
		return index
	case <-time.After(timeout):
		t.Fatalf("proposal index %d was not committed and applied", index)
		return 0
	}
}

func waitForValue(t *testing.T, nodes []*failureNode, key, value string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allApplied := true
		for _, node := range nodes {
			got, found := node.applier.value(key)
			if !found || got != value {
				allApplied = false
				break
			}
		}
		if allApplied {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("nodes did not all apply %q=%q", key, value)
}

func raftLog(r *Raft) []LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]LogEntry(nil), r.log...)
}

func waitForStableLeader(t *testing.T, nodes []*Raft, timeout time.Duration) *Raft {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *Raft
		leaders := 0
		for _, node := range nodes {
			if node.IsLeader() {
				leader = node
				leaders++
			}
		}
		if leaders == 1 {
			converged := true
			for _, node := range nodes {
				if node.ID() != leader.ID() && (node.State() != Follower || node.LeaderID() != leader.ID()) {
					converged = false
					break
				}
			}
			if converged {
				return leader
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a stable leader and follower convergence")
	return nil
}

func TestFailureLeaderWriteReachesAllAvailableFollowersAndCommits(t *testing.T) {
	c := newFailureCluster(t)
	leader := waitForStableLeader(t, c.activeRafts(), time.Second)

	index := waitForProposal(t, leader, Command{Operation: SetOperation, Key: "colour", Value: "blue"}, time.Second)
	all := []*failureNode{c.nodes[1], c.nodes[2], c.nodes[3]}
	waitForValue(t, all, "colour", "blue", time.Second)
	for _, node := range all {
		if node.raft.CommitIndex() < index || node.raft.LastApplied() < index {
			t.Fatalf("node %d commit=%d applied=%d, want both at least %d", node.id, node.raft.CommitIndex(), node.raft.LastApplied(), index)
		}
	}
}

func TestFailureFollowerCatchesUpAfterMissingEntries(t *testing.T) {
	c := newFailureCluster(t)
	leader := waitForStableLeader(t, c.activeRafts(), time.Second)
	laggerID := c.otherIDs(leader.ID())[0]
	c.network.isolate(laggerID)

	waitForProposal(t, leader, Command{Operation: SetOperation, Key: "first", Value: "1"}, time.Second)
	waitForProposal(t, leader, Command{Operation: SetOperation, Key: "second", Value: "2"}, time.Second)
	if _, found := c.nodes[laggerID].applier.value("second"); found {
		t.Fatal("partitioned follower applied an entry it never received")
	}

	c.network.heal(laggerID)
	leader = waitForStableLeader(t, c.activeRafts(), 2*time.Second)
	waitForValue(t, []*failureNode{c.nodes[1], c.nodes[2], c.nodes[3]}, "first", "1", 2*time.Second)
	waitForValue(t, []*failureNode{c.nodes[1], c.nodes[2], c.nodes[3]}, "second", "2", 2*time.Second)
	leaderLog := raftLog(leader)
	for id := 1; id <= 3; id++ {
		if got := raftLog(c.nodes[id].raft); !reflect.DeepEqual(got, leaderLog) {
			t.Fatalf("node %d log = %+v, want leader log %+v", id, got, leaderLog)
		}
	}
}

func TestFailureLeaderDiesBeforeMajorityDoesNotAcknowledgeClient(t *testing.T) {
	c := newFailureCluster(t)
	leader := waitForStableLeader(t, c.activeRafts(), time.Second)
	c.network.isolate(leader.ID())

	index, result, err := leader.Propose(Command{Operation: SetOperation, Key: "unsafe", Value: "write"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if leader.CommitIndex() >= index {
		t.Fatalf("isolated leader committed index %d without a majority", index)
	}
	c.stopNode(leader.ID())

	select {
	case got := <-result:
		t.Fatalf("client received result after pre-majority leader failure: %+v", got)
	case <-time.After(150 * time.Millisecond):
	}

	newLeader := waitForStableLeader(t, c.activeRafts(), 2*time.Second)
	if _, found := c.nodes[newLeader.ID()].applier.value("unsafe"); found {
		t.Fatal("new leader applied a write that never reached a majority")
	}
}

func TestFailureCommittedWriteSurvivesLeaderDeathAndNewLeaderAppliesIt(t *testing.T) {
	c := newFailureCluster(t)
	leader := waitForStableLeader(t, c.activeRafts(), time.Second)
	followers := c.otherIDs(leader.ID())
	laggerID, quorumFollowerID := followers[0], followers[1]
	c.network.isolate(laggerID)

	waitForProposal(t, leader, Command{Operation: SetOperation, Key: "durable", Value: "yes"}, time.Second)
	c.stopNode(leader.ID())
	c.network.heal(laggerID)

	newLeader := waitForStableLeader(t, c.activeRafts(), 2*time.Second)
	if newLeader.ID() != quorumFollowerID {
		t.Fatalf("stale follower %d became leader instead of log-bearing follower %d", newLeader.ID(), quorumFollowerID)
	}
	waitForValue(t, []*failureNode{c.nodes[quorumFollowerID], c.nodes[laggerID]}, "durable", "yes", 2*time.Second)
}

func TestFailureRestartedFollowerCatchesUpFromDurableLog(t *testing.T) {
	c := newFailureCluster(t)
	leader := waitForStableLeader(t, c.activeRafts(), time.Second)
	waitForProposal(t, leader, Command{Operation: SetOperation, Key: "before", Value: "restart"}, time.Second)
	waitForValue(t, []*failureNode{c.nodes[1], c.nodes[2], c.nodes[3]}, "before", "restart", time.Second)
	laggerID := c.otherIDs(leader.ID())[0]
	c.network.isolate(laggerID)

	waitForProposal(t, leader, Command{Operation: SetOperation, Key: "survives", Value: "restart"}, time.Second)
	c.restartNode(laggerID)
	if got, found := c.nodes[laggerID].applier.value("before"); !found || got != "restart" {
		t.Fatalf("restarted follower did not rebuild its committed durable state: found=%t value=%q", found, got)
	}
	if _, found := c.nodes[laggerID].applier.value("survives"); found {
		t.Fatal("restarted follower applied a missing uncommitted suffix before catch-up")
	}
	c.network.heal(laggerID)

	waitForStableLeader(t, c.activeRafts(), 2*time.Second)
	waitForValue(t, []*failureNode{c.nodes[1], c.nodes[2], c.nodes[3]}, "survives", "restart", 2*time.Second)
}

func TestFailurePartitionedOldLeaderCannotAcknowledgeAndLaggingReadIsStale(t *testing.T) {
	c := newFailureCluster(t)
	leader := waitForStableLeader(t, c.activeRafts(), time.Second)
	laggerID := c.otherIDs(leader.ID())[0]
	c.network.isolate(laggerID)

	waitForProposal(t, leader, Command{Operation: SetOperation, Key: "fresh", Value: "leader"}, time.Second)
	if _, found := c.nodes[laggerID].applier.value("fresh"); found {
		t.Fatal("lagging follower returned a fresh value despite its partition")
	}

	// Now isolate the old leader too. It still believes it is leader, but has no
	// route to either follower and therefore may append locally only.
	c.network.isolate(leader.ID())
	_, result, err := leader.Propose(Command{Operation: SetOperation, Key: "must-not-ack", Value: "x"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		t.Fatalf("partitioned old leader acknowledged a write: %+v", got)
	case <-time.After(150 * time.Millisecond):
	}
}

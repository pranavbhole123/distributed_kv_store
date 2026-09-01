package raft

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestLeaderProposeReplicatesAndCommitsOnMajority(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	leader := waitForLeader(t, nodes, time.Second)
	waitForFollowerConvergence(t, nodes, leader.ID(), time.Second)

	for _, node := range nodes {
		if node.ID() == leader.ID() {
			continue
		}
		if _, _, err := node.Propose(Command{Operation: SetOperation, Key: "x", Value: "1"}); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("follower %d Propose() error = %v, want ErrNotLeader", node.ID(), err)
		}
		break
	}

	index, result, err := leader.Propose(Command{Operation: SetOperation, Key: "x", Value: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if index != 2 {
		t.Fatalf("proposal index = %d, want 2 after the leader's no-op", index)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if leader.CommitIndex() >= index && followersWithEntry(nodes, leader.ID(), index) >= 1 {
			select {
			case got := <-result:
				if got.Index != index || got.Err != nil {
					t.Fatalf("proposal result = %+v, want successful index %d", got, index)
				}
				return
			default:
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("proposal was not replicated, committed, and applied: leader commit=%d applied=%d followers with entry=%d", leader.CommitIndex(), leader.LastApplied(), followersWithEntry(nodes, leader.ID(), index))
}

func TestReplicationBacktracksNextIndexAndRepairsFollower(t *testing.T) {
	transport := newLocalTransport()
	dir := t.TempDir()
	leaderLog := newMemoryLogStore()
	if err := leaderLog.Append([]LogEntry{
		{Index: 1, Term: 1, Operation: SetOperation, Key: "a", Value: "1"},
		{Index: 2, Term: 1, Operation: SetOperation, Key: "b", Value: "2"},
		{Index: 3, Term: 2, Operation: SetOperation, Key: "c", Value: "3"},
	}); err != nil {
		t.Fatal(err)
	}
	followerLog := newMemoryLogStore()
	if err := followerLog.Append([]LogEntry{
		{Index: 1, Term: 1, Operation: SetOperation, Key: "a", Value: "1"},
	}); err != nil {
		t.Fatal(err)
	}

	leaderStable := NewFileStableStore(filepath.Join(dir, "leader-state.json"))
	if err := leaderStable.Save(StableState{CurrentTerm: 2, VotedFor: NoVote}); err != nil {
		t.Fatal(err)
	}
	leader, err := NewWithLog(testConfig(1, 3), leaderStable, leaderLog, nil, transport)
	if err != nil {
		t.Fatal(err)
	}
	follower, err := NewWithLog(testConfig(2, 3), NewFileStableStore(filepath.Join(dir, "follower-state.json")), followerLog, nil, transport)
	if err != nil {
		t.Fatal(err)
	}
	transport.register(follower)

	leader.mu.Lock()
	if err := leader.becomeLeaderLocked(); err != nil {
		leader.mu.Unlock()
		t.Fatal(err)
	}
	leader.mu.Unlock()
	leader.replicateToPeer(context.Background(), follower.ID())

	leader.mu.RLock()
	next := leader.nextIndex[follower.ID()]
	matched := leader.matchIndex[follower.ID()]
	committed := leader.commitIndex
	leader.mu.RUnlock()
	if next != 5 || matched != 4 {
		t.Fatalf("leader replication state next=%d match=%d; want next=5 match=4", next, matched)
	}
	if committed != 4 {
		t.Fatalf("leader commit index = %d, want 4 after leader plus one follower replicated current-term no-op", committed)
	}

	follower.mu.RLock()
	entries := append([]LogEntry(nil), follower.log...)
	follower.mu.RUnlock()
	if len(entries) != 4 || entries[2].Term != 2 || entries[2].Value != "3" || entries[3].Operation != NoopOperation {
		t.Fatalf("follower log = %+v, want repaired leader log", entries)
	}
}

func followersWithEntry(nodes []*Raft, leaderID int, index uint64) int {
	followers := 0
	for _, node := range nodes {
		if node.ID() == leaderID {
			continue
		}
		node.mu.RLock()
		hasEntry := uint64(len(node.log)) >= index
		node.mu.RUnlock()
		if hasEntry {
			followers++
		}
	}
	return followers
}

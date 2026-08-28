package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/pranavbhole123/distributed_kv_store/internal/config"
)

// State describes a node's role in the current Raft term.
type State uint8

const (
	Follower State = iota
	Candidate
	Leader
)

const UnknownLeader = -1

// RequestVoteRequest has no log-position fields yet. Those are added with the
// replicated log in Phase 4.
type RequestVoteRequest struct {
	Term        uint64
	CandidateID int
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesRequest is an empty heartbeat in Phase 3. It will carry log
// entries and commit information in Phase 4.
type AppendEntriesRequest struct {
	Term     uint64
	LeaderID int
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

// Transport is the only way Raft communicates with another node. Production
// gRPC and the in-memory test transport both implement this interface.
type Transport interface {
	RequestVote(context.Context, config.Node, RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(context.Context, config.Node, AppendEntriesRequest) (AppendEntriesResponse, error)
}

type Raft struct {
	id        int
	peers     []config.Node
	transport Transport
	stable    StableStore

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration

	mu          sync.RWMutex
	state       State
	currentTerm uint64
	votedFor    int
	leaderID    int

	resetElection chan struct{}
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

func New(cfg config.Config, stable StableStore, transport Transport) (*Raft, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Raft configuration: %w", err)
	}
	if stable == nil {
		return nil, fmt.Errorf("Raft stable store cannot be nil")
	}

	persisted, err := stable.Load()
	if err != nil {
		return nil, fmt.Errorf("load Raft stable state: %w", err)
	}
	return &Raft{
		id:                 cfg.Self.ID,
		peers:              append([]config.Node(nil), cfg.Peers...),
		transport:          transport,
		stable:             stable,
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		state:              Follower,
		currentTerm:        persisted.CurrentTerm,
		votedFor:           persisted.VotedFor,
		leaderID:           UnknownLeader,
		resetElection:      make(chan struct{}, 1),
	}, nil
}

func (r *Raft) Start(parent context.Context) {
	r.mu.Lock()
	if r.cancel != nil {
		r.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel
	r.wg.Add(1)
	r.mu.Unlock()

	go r.run(ctx)
}

func (r *Raft) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	r.cancel = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
		r.wg.Wait()
	}
}

func (r *Raft) ID() int { return r.id }

func (r *Raft) State() State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *Raft) CurrentTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

func (r *Raft) LeaderID() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderID
}

func (r *Raft) IsLeader() bool { return r.State() == Leader }

func (r *Raft) run(ctx context.Context) {
	defer r.wg.Done()
	electionTimer := time.NewTimer(r.randomElectionTimeout())
	heartbeats := time.NewTicker(r.heartbeatInterval)
	defer electionTimer.Stop()
	defer heartbeats.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.resetElection:
			resetTimer(electionTimer, r.randomElectionTimeout())
		case <-electionTimer.C:
			if !r.IsLeader() {
				r.startElection(ctx)
			}
			resetTimer(electionTimer, r.randomElectionTimeout())
		case <-heartbeats.C:
			if r.IsLeader() {
				r.sendHeartbeats(ctx)
			}
		}
	}
}

func (r *Raft) HandleRequestVote(_ context.Context, req RequestVoteRequest) RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.currentTerm {
		return RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
	}
	if req.Term > r.currentTerm {
		if err := r.becomeFollowerLocked(req.Term, UnknownLeader); err != nil {
			return RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
		}
	}
	if r.votedFor != NoVote && r.votedFor != req.CandidateID {
		return RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
	}

	// Persist the vote before granting it: after a crash this node must still
	// remember that it has exhausted its one vote for this term.
	if r.votedFor != req.CandidateID {
		if err := r.saveStableLocked(r.currentTerm, req.CandidateID); err != nil {
			return RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
		}
		r.votedFor = req.CandidateID
	}
	r.signalElectionReset()
	return RequestVoteResponse{Term: r.currentTerm, VoteGranted: true}
}

func (r *Raft) HandleAppendEntries(_ context.Context, req AppendEntriesRequest) AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.currentTerm {
		return AppendEntriesResponse{Term: r.currentTerm, Success: false}
	}
	if req.Term > r.currentTerm {
		if err := r.becomeFollowerLocked(req.Term, req.LeaderID); err != nil {
			return AppendEntriesResponse{Term: r.currentTerm, Success: false}
		}
	} else {
		r.state = Follower
		r.leaderID = req.LeaderID
	}
	r.signalElectionReset()
	return AppendEntriesResponse{Term: r.currentTerm, Success: true}
}

func (r *Raft) startElection(parent context.Context) {
	r.mu.Lock()
	term, err := r.becomeCandidateLocked()
	r.mu.Unlock()
	if err != nil {
		return
	}

	majority := (len(r.peers)+1)/2 + 1
	if majority == 1 {
		r.mu.Lock()
		if r.state == Candidate && r.currentTerm == term {
			r.becomeLeaderLocked()
		}
		r.mu.Unlock()
		return
	}

	responses := make(chan RequestVoteResponse, len(r.peers))
	for _, peer := range r.peers {
		go func(peer config.Node) {
			ctx, cancel := context.WithTimeout(parent, r.electionTimeoutMin/2)
			defer cancel()
			response, err := r.transport.RequestVote(ctx, peer, RequestVoteRequest{Term: term, CandidateID: r.id})
			if err == nil {
				responses <- response
			}
		}(peer)
	}

	votes := 1
	for received := 0; received < len(r.peers); received++ {
		select {
		case <-parent.Done():
			return
		case response := <-responses:
			r.mu.Lock()
			if response.Term > r.currentTerm {
				_ = r.becomeFollowerLocked(response.Term, UnknownLeader)
				r.mu.Unlock()
				return
			}
			if r.state != Candidate || r.currentTerm != term {
				r.mu.Unlock()
				return
			}
			if response.Term == term && response.VoteGranted {
				votes++
				if votes >= majority {
					r.becomeLeaderLocked()
					r.mu.Unlock()
					r.sendHeartbeats(parent)
					return
				}
			}
			r.mu.Unlock()
		case <-time.After(r.electionTimeoutMin / 2):
			return
		}
	}
}

func (r *Raft) sendHeartbeats(parent context.Context) {
	r.mu.RLock()
	if r.state != Leader {
		r.mu.RUnlock()
		return
	}
	term := r.currentTerm
	r.mu.RUnlock()

	for _, peer := range r.peers {
		go func(peer config.Node) {
			ctx, cancel := context.WithTimeout(parent, r.electionTimeoutMin/2)
			defer cancel()
			response, err := r.transport.AppendEntries(ctx, peer, AppendEntriesRequest{Term: term, LeaderID: r.id})
			if err == nil && response.Term > term {
				r.mu.Lock()
				if response.Term > r.currentTerm {
					_ = r.becomeFollowerLocked(response.Term, UnknownLeader)
				}
				r.mu.Unlock()
			}
		}(peer)
	}
}

func (r *Raft) becomeCandidateLocked() (uint64, error) {
	newTerm := r.currentTerm + 1
	if err := r.saveStableLocked(newTerm, r.id); err != nil {
		return 0, err
	}
	r.currentTerm = newTerm
	r.votedFor = r.id
	r.state = Candidate
	r.leaderID = UnknownLeader
	return newTerm, nil
}

func (r *Raft) becomeLeaderLocked() {
	r.state = Leader
	r.leaderID = r.id
}

// becomeFollowerLocked durably advances the term when necessary. It is called
// with r.mu held.
func (r *Raft) becomeFollowerLocked(term uint64, leaderID int) error {
	if term < r.currentTerm {
		return nil
	}
	if term > r.currentTerm {
		if err := r.saveStableLocked(term, NoVote); err != nil {
			return err
		}
		r.currentTerm = term
		r.votedFor = NoVote
	}
	r.state = Follower
	r.leaderID = leaderID
	return nil
}

func (r *Raft) saveStableLocked(term uint64, votedFor int) error {
	return r.stable.Save(StableState{CurrentTerm: term, VotedFor: votedFor})
}

func (r *Raft) signalElectionReset() {
	select {
	case r.resetElection <- struct{}{}:
	default:
	}
}

func (r *Raft) randomElectionTimeout() time.Duration {
	return r.electionTimeoutMin + time.Duration(rand.Int63n(int64(r.electionTimeoutMax-r.electionTimeoutMin)))
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}

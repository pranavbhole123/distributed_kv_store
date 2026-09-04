package raft

import (
	"context"
	"errors"
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

// ErrNotLeader tells a client to retry its proposal against the current leader.
var ErrNotLeader = errors.New("Raft node is not the leader")

// ErrNoApplier prevents a leader from accepting a write it could commit but
// never make visible locally.
var ErrNoApplier = errors.New("Raft node has no state-machine applier")

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

// Command is a state-machine mutation proposed by a client. Raft assigns its
// log index and term; callers must not choose either.
type Command struct {
	Operation Operation
	Key       string
	Value     string
}

func (c Command) Validate() error {
	if c.Operation == NoopOperation {
		return errors.New("client command cannot be a Raft NOOP")
	}
	return LogEntry{Index: 1, Operation: c.Operation, Key: c.Key, Value: c.Value}.Validate()
}

// Result is delivered after a proposal becomes visible in the state machine.
// Phase 4.3 creates and retains the channel; Phase 4.4's apply loop delivers
// its result after applying the committed entry.
type Result struct {
	Index uint64
	Err   error
}

// Transport is the only way Raft communicates with another node. Production
// gRPC and the in-memory test transport both implement this interface.
type Transport interface {
	RequestVote(context.Context, config.Node, RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(context.Context, config.Node, AppendEntriesRequest) (AppendEntriesResponse, error)
}

// Applier makes committed log entries visible in a state machine. Raft only
// controls ordering and commitment; Node supplies the KV-store implementation.
type Applier interface {
	Apply(LogEntry) error
}

type Raft struct {
	id        int
	peers     []config.Node
	transport Transport
	stable    StableStore
	logStore  LogStore
	applier   Applier

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration

	mu           sync.RWMutex
	state        State
	currentTerm  uint64
	votedFor     int
	leaderID     int
	log          []LogEntry
	snapshotMeta SnapshotMetadata
	commitIndex  uint64
	lastApplied  uint64
	nextIndex    map[int]uint64
	matchIndex   map[int]uint64

	// At most one outbound AppendEntries exchange per follower runs at a time.
	// This keeps delayed responses from racing each other and regressing the
	// leader's per-follower sending cursor.
	replicating    map[int]bool
	pendingResults map[uint64]chan Result // way to let the waiting http client know when the work is done
	applyErr       error

	resetElection chan struct{}
	replicateNow  chan struct{}
	applyNow      chan struct{}
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

func New(cfg config.Config, stable StableStore, transport Transport) (*Raft, error) {
	return NewWithLog(cfg, stable, newMemoryLogStore(), nil, transport)
}

// NewWithLog constructs Raft from its durable election state and Raft log.
// Because the current state machine is in memory, committed entries are replayed
// into applier at every process start. Uncommitted entries remain durable but
// deliberately invisible.
func NewWithLog(cfg config.Config, stable StableStore, logStore LogStore, applier Applier, transport Transport) (*Raft, error) {
	return newWithLogAndSnapshot(cfg, stable, logStore, applier, transport, SnapshotMetadata{})
}

// newWithLogAndSnapshot is the common constructor used by the future snapshot
// loader. Phase 5.1 has no SnapshotStore yet, so public construction starts
// with the implicit empty snapshot at index and term zero.
func newWithLogAndSnapshot(cfg config.Config, stable StableStore, logStore LogStore, applier Applier, transport Transport, snapshotMeta SnapshotMetadata) (*Raft, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Raft configuration: %w", err)
	}
	if stable == nil {
		return nil, fmt.Errorf("Raft stable store cannot be nil")
	}
	if logStore == nil {
		return nil, fmt.Errorf("Raft log store cannot be nil")
	}
	if err := snapshotMeta.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Raft snapshot metadata: %w", err)
	}

	persisted, err := stable.Load()
	if err != nil {
		return nil, fmt.Errorf("load Raft stable state: %w", err)
	}
	entries, err := logStore.Load()
	if err != nil {
		return nil, fmt.Errorf("load Raft log: %w", err)
	}
	if err := validateLogSuffix(entries, snapshotMeta); err != nil {
		return nil, fmt.Errorf("invalid durable Raft log: %w", err)
	}
	lastLogIndex := lastLogIndexFor(entries, snapshotMeta)
	if persisted.CommitIndex < snapshotMeta.LastIncludedIndex || persisted.CommitIndex > lastLogIndex {
		return nil, fmt.Errorf("commit index %d is outside snapshot/log range %d..%d", persisted.CommitIndex, snapshotMeta.LastIncludedIndex, lastLogIndex)
	}
	if persisted.LastApplied < snapshotMeta.LastIncludedIndex {
		return nil, fmt.Errorf("last applied index %d precedes snapshot index %d", persisted.LastApplied, snapshotMeta.LastIncludedIndex)
	}

	r := &Raft{
		id:                 cfg.Self.ID,
		peers:              append([]config.Node(nil), cfg.Peers...),
		transport:          transport,
		stable:             stable,
		logStore:           logStore,
		applier:            applier,
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		state:              Follower,
		currentTerm:        persisted.CurrentTerm,
		votedFor:           persisted.VotedFor,
		leaderID:           UnknownLeader,
		log:                entries,
		snapshotMeta:       snapshotMeta,
		commitIndex:        persisted.CommitIndex,
		lastApplied:        snapshotMeta.LastIncludedIndex,
		nextIndex:          make(map[int]uint64, len(cfg.Peers)),
		matchIndex:         make(map[int]uint64, len(cfg.Peers)),
		replicating:        make(map[int]bool, len(cfg.Peers)),
		pendingResults:     make(map[uint64]chan Result),
		resetElection:      make(chan struct{}, 1),
		replicateNow:       make(chan struct{}, 1),
		applyNow:           make(chan struct{}, 1),
	}
	for r.lastApplied < r.commitIndex {
		entry, found := r.entryAtLocked(r.lastApplied + 1)
		if !found {
			return nil, fmt.Errorf("committed log entry %d is absent", r.lastApplied+1)
		}
		if entry.Operation != NoopOperation {
			if r.applier == nil {
				return nil, fmt.Errorf("committed log entry %d requires a state-machine applier", entry.Index)
			}
			if err := r.applier.Apply(entry); err != nil {
				return nil, fmt.Errorf("replay committed log entry %d: %w", entry.Index, err)
			}
		}
		r.lastApplied++
	}
	if persisted.LastApplied != r.lastApplied {
		if err := stable.Save(StableState{
			CurrentTerm: r.currentTerm,
			VotedFor:    r.votedFor,
			CommitIndex: r.commitIndex,
			LastApplied: r.lastApplied,
		}); err != nil {
			return nil, fmt.Errorf("persist recovered last applied index: %w", err)
		}
	}
	return r, nil
}

func (r *Raft) Start(parent context.Context) {
	r.mu.Lock()
	if r.cancel != nil {
		r.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel
	r.wg.Add(2)
	r.mu.Unlock()

	go r.run(ctx)
	go r.applyLoop(ctx)
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

func (r *Raft) CommitIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.commitIndex
}

func (r *Raft) LastApplied() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastApplied
}

// SnapshotMetadata returns the current compacted-prefix boundary. Phase 5.1
// only establishes this addressing model; later phases persist and advance it.
func (r *Raft) SnapshotMetadata() SnapshotMetadata {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshotMeta
}

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
				r.replicateAll(ctx)
			}
		case <-r.replicateNow:
			if r.IsLeader() {
				r.replicateAll(ctx)
			}
		}
	}
}

// Propose durably appends a client command to this leader's log and starts
// replication immediately. The returned channel is completed by the Phase 4.4
// apply loop, only after the entry is committed and applied.
func (r *Raft) Propose(command Command) (uint64, <-chan Result, error) {
	if err := command.Validate(); err != nil {
		return 0, nil, fmt.Errorf("invalid Raft command: %w", err)
	}

	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return 0, nil, ErrNotLeader
	}
	if r.applier == nil {
		r.mu.Unlock()
		return 0, nil, ErrNoApplier
	}
	if r.applyErr != nil {
		err := r.applyErr
		r.mu.Unlock()
		return 0, nil, fmt.Errorf("Raft state machine is unavailable: %w", err)
	}
	lastLogIndex, _ := r.lastLogInfoLocked()
	entry := LogEntry{
		Index:     lastLogIndex + 1,
		Term:      r.currentTerm,
		Operation: command.Operation,
		Key:       command.Key,
		Value:     command.Value,
	}
	if err := r.logStore.Append([]LogEntry{entry}); err != nil {
		r.mu.Unlock()
		return 0, nil, fmt.Errorf("persist proposed log entry %d: %w", entry.Index, err)
	}
	r.log = append(r.log, entry)
	result := make(chan Result, 1)
	r.pendingResults[entry.Index] = result
	r.mu.Unlock()

	r.signalReplication()
	return entry.Index, result, nil
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
	if !r.candidateLogIsUpToDateLocked(req.LastLogIndex, req.LastLogTerm) {
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
	// A current-term leader is still valid while this follower catches up, even
	// if its log prefix does not currently match.
	r.signalElectionReset()

	if !r.matchesPrefixLocked(req.PrevLogIndex, req.PrevLogTerm) {
		return AppendEntriesResponse{Term: r.currentTerm, Success: false}
	}
	truncateFrom, entriesToAppend, err := r.replicationPlanLocked(req.PrevLogIndex, req.Entries)
	if err != nil {
		return AppendEntriesResponse{Term: r.currentTerm, Success: false}
	}
	if truncateFrom != 0 {
		if err := r.logStore.TruncateFrom(truncateFrom); err != nil {
			return AppendEntriesResponse{Term: r.currentTerm, Success: false}
		}
		// The durable prefix is now authoritative even if appending the new
		// suffix later fails; the leader will retry the missing suffix.
		offset, found := r.logOffsetLocked(truncateFrom)
		if !found {
			return AppendEntriesResponse{Term: r.currentTerm, Success: false}
		}
		r.log = r.log[:offset]
	}
	if len(entriesToAppend) > 0 {
		if err := r.logStore.Append(entriesToAppend); err != nil {
			return AppendEntriesResponse{Term: r.currentTerm, Success: false}
		}
		r.log = append(r.log, entriesToAppend...)
	}

	newCommitIndex := min(req.LeaderCommit, r.lastLogIndexLocked())
	if newCommitIndex > r.commitIndex {
		if err := r.saveStableValuesLocked(r.currentTerm, r.votedFor, newCommitIndex, r.lastApplied); err != nil {
			return AppendEntriesResponse{Term: r.currentTerm, Success: false}
		}
		r.commitIndex = newCommitIndex
		r.signalApply()
	}
	return AppendEntriesResponse{Term: r.currentTerm, Success: true}
}

func (r *Raft) startElection(parent context.Context) {
	r.mu.Lock()
	term, err := r.becomeCandidateLocked()
	lastLogIndex, lastLogTerm := r.lastLogInfoLocked()
	r.mu.Unlock()
	if err != nil {
		return
	}

	majority := (len(r.peers)+1)/2 + 1
	if majority == 1 {
		r.mu.Lock()
		if r.state == Candidate && r.currentTerm == term {
			err = r.becomeLeaderLocked()
		}
		r.mu.Unlock()
		if err == nil {
			r.signalReplication()
		}
		return
	}

	responses := make(chan RequestVoteResponse, len(r.peers))
	for _, peer := range r.peers {
		go func(peer config.Node) {
			ctx, cancel := context.WithTimeout(parent, r.electionTimeoutMin/2)
			defer cancel()
			response, err := r.transport.RequestVote(ctx, peer, RequestVoteRequest{
				Term:         term,
				CandidateID:  r.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
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
					err = r.becomeLeaderLocked()
					r.mu.Unlock()
					if err == nil {
						r.signalReplication()
					}
					return
				}
			}
			r.mu.Unlock()
		case <-time.After(r.electionTimeoutMin / 2):
			return
		}
	}
}

// replicateAll starts independent replication exchanges for all followers.
// A follower's exchange is serialized by replicating[peerID], while different
// followers can make network progress concurrently.
func (r *Raft) replicateAll(parent context.Context) {
	for _, peer := range r.peers {
		go r.replicateToPeer(parent, peer.ID)
	}
}

// replicateToPeer drives one follower backwards to a shared prefix, then
// forwards through the leader's missing suffix. It never holds r.mu during an
// RPC, so inbound requests and other followers stay independent.
func (r *Raft) replicateToPeer(parent context.Context, peerID int) {
	peer, ok := r.peerByID(peerID)
	if !ok {
		return
	}
	if !r.beginReplication(peerID) {
		return
	}
	defer r.endReplication(peerID)

	ctx, cancel := context.WithTimeout(parent, r.electionTimeoutMin/2)
	defer cancel()
	for {
		request, ok := r.buildAppendEntries(peerID)
		if !ok {
			return
		}
		response, err := r.transport.AppendEntries(ctx, peer, request)
		if err != nil {
			return
		}
		if !r.handleAppendEntriesResponse(peerID, request, response) {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// buildAppendEntries snapshots the precise prefix and suffix for one peer.
// nextIndex is the first entry the follower is missing, so its predecessor is
// the prefix the follower must prove it has before accepting the suffix.
func (r *Raft) buildAppendEntries(peerID int) (AppendEntriesRequest, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state != Leader {
		return AppendEntriesRequest{}, false
	}
	next, ok := r.nextIndex[peerID]
	if !ok || next <= r.snapshotMeta.LastIncludedIndex || next > r.lastLogIndexLocked()+1 {
		return AppendEntriesRequest{}, false
	}

	previous := next - 1
	previousTerm, found := r.termAtLocked(previous)
	if !found {
		return AppendEntriesRequest{}, false
	}
	entries, found := r.entriesFromLocked(next)
	if !found {
		return AppendEntriesRequest{}, false
	}
	return AppendEntriesRequest{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: previous,
		PrevLogTerm:  previousTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}, true
}

// handleAppendEntriesResponse updates the replication facts learned from one
// RPC. It returns true only when a same-term rejection should be retried with
// an earlier prefix.
func (r *Raft) handleAppendEntriesResponse(peerID int, request AppendEntriesRequest, response AppendEntriesResponse) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if response.Term > r.currentTerm {
		_ = r.becomeFollowerLocked(response.Term, UnknownLeader)
		return false
	}
	if r.state != Leader || request.Term != r.currentTerm || response.Term != r.currentTerm {
		return false
	}
	if !response.Success {
		if r.nextIndex[peerID] <= 1 {
			return false
		}
		r.nextIndex[peerID]--
		return true
	}

	matched := request.PrevLogIndex + uint64(len(request.Entries))
	if matched > r.matchIndex[peerID] {
		r.matchIndex[peerID] = matched
	}
	if r.nextIndex[peerID] < r.matchIndex[peerID]+1 {
		r.nextIndex[peerID] = r.matchIndex[peerID] + 1
	}
	if r.advanceCommitIndexLocked() {
		r.signalReplication()
	}
	return false
}

// advanceCommitIndexLocked commits the greatest current-term entry known to be
// replicated on a quorum, counting this leader as one replica. It reports
// whether the persisted commit index advanced.
func (r *Raft) advanceCommitIndexLocked() bool {
	for index := r.lastLogIndexLocked(); index > r.commitIndex; index-- {
		term, found := r.termAtLocked(index)
		if !found || term != r.currentTerm {
			continue
		}
		matched := 1 // the leader always has its own log entry
		for _, peer := range r.peers {
			if r.matchIndex[peer.ID] >= index {
				matched++
			}
		}
		if matched < r.majority() {
			continue
		}
		if err := r.saveStableValuesLocked(r.currentTerm, r.votedFor, index, r.lastApplied); err != nil {
			return false
		}
		r.commitIndex = index
		r.signalApply()
		return true
	}
	return false
}

func (r *Raft) beginReplication(peerID int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Leader || r.replicating[peerID] {
		return false
	}
	r.replicating[peerID] = true
	return true
}

func (r *Raft) endReplication(peerID int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.replicating, peerID)
}

func (r *Raft) peerByID(peerID int) (config.Node, bool) {
	for _, peer := range r.peers {
		if peer.ID == peerID {
			return peer, true
		}
	}
	return config.Node{}, false
}

func (r *Raft) majority() int {
	return (len(r.peers)+1)/2 + 1
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

// becomeLeaderLocked appends a current-term no-op before becoming leader.
// Committing that no-op establishes commitment of any preceding entries from
// earlier terms which this leader inherited but cannot directly commit itself.
func (r *Raft) becomeLeaderLocked() error {
	lastLogIndex, _ := r.lastLogInfoLocked()
	noop := LogEntry{Index: lastLogIndex + 1, Term: r.currentTerm, Operation: NoopOperation}
	if err := r.logStore.Append([]LogEntry{noop}); err != nil {
		return fmt.Errorf("persist leader no-op at index %d: %w", noop.Index, err)
	}
	r.log = append(r.log, noop)
	r.state = Leader
	r.leaderID = r.id
	lastLogIndex = noop.Index
	r.nextIndex = make(map[int]uint64, len(r.peers))
	r.matchIndex = make(map[int]uint64, len(r.peers))
	for _, peer := range r.peers {
		r.nextIndex[peer.ID] = lastLogIndex + 1
		r.matchIndex[peer.ID] = 0
	}
	return nil
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

func (r *Raft) lastLogInfoLocked() (index uint64, term uint64) {
	index = r.lastLogIndexLocked()
	term, _ = r.termAtLocked(index)
	return index, term
}

func validateLogSuffix(entries []LogEntry, snapshotMeta SnapshotMetadata) error {
	expectedIndex := snapshotMeta.LastIncludedIndex + 1
	for _, entry := range entries {
		if err := entry.Validate(); err != nil {
			return err
		}
		if entry.Index != expectedIndex {
			return fmt.Errorf("log entry index %d, want %d after snapshot index %d", entry.Index, expectedIndex, snapshotMeta.LastIncludedIndex)
		}
		expectedIndex++
	}
	return nil
}

func lastLogIndexFor(entries []LogEntry, snapshotMeta SnapshotMetadata) uint64 {
	if len(entries) == 0 {
		return snapshotMeta.LastIncludedIndex
	}
	return entries[len(entries)-1].Index
}

func (r *Raft) firstLogIndexLocked() uint64 {
	return r.snapshotMeta.LastIncludedIndex + 1
}

func (r *Raft) lastLogIndexLocked() uint64 {
	return lastLogIndexFor(r.log, r.snapshotMeta)
}

// logOffsetLocked converts an absolute Raft index to its relative slice
// offset. Snapshot entries deliberately have no offset because they were
// compacted away.
func (r *Raft) logOffsetLocked(index uint64) (int, bool) {
	if index < r.firstLogIndexLocked() || index > r.lastLogIndexLocked() {
		return 0, false
	}
	offset := int(index - r.firstLogIndexLocked())
	if offset < 0 || offset >= len(r.log) || r.log[offset].Index != index {
		return 0, false
	}
	return offset, true
}

func (r *Raft) entryAtLocked(index uint64) (LogEntry, bool) {
	offset, found := r.logOffsetLocked(index)
	if !found {
		return LogEntry{}, false
	}
	return r.log[offset], true
}

// termAtLocked returns the term for the implicit empty entry at index zero,
// the compacted snapshot boundary, or a remaining in-memory log entry.
func (r *Raft) termAtLocked(index uint64) (uint64, bool) {
	if index == r.snapshotMeta.LastIncludedIndex {
		return r.snapshotMeta.LastIncludedTerm, true
	}
	entry, found := r.entryAtLocked(index)
	if !found {
		return 0, false
	}
	return entry.Term, true
}

func (r *Raft) entriesFromLocked(index uint64) ([]LogEntry, bool) {
	if index < r.firstLogIndexLocked() || index > r.lastLogIndexLocked()+1 {
		return nil, false
	}
	if index == r.lastLogIndexLocked()+1 {
		return nil, true
	}
	offset, found := r.logOffsetLocked(index)
	if !found {
		return nil, false
	}
	return append([]LogEntry(nil), r.log[offset:]...), true
}

func (r *Raft) candidateLogIsUpToDateLocked(candidateIndex, candidateTerm uint64) bool {
	localIndex, localTerm := r.lastLogInfoLocked()
	if candidateTerm != localTerm {
		return candidateTerm > localTerm
	}
	return candidateIndex >= localIndex
}

func (r *Raft) matchesPrefixLocked(index, term uint64) bool {
	localTerm, found := r.termAtLocked(index)
	return found && localTerm == term
}

// replicationPlanLocked determines the smallest durable operations needed for
// an AppendEntries request. Normal replication only appends; conflict repair
// truncates an uncommitted suffix first, then appends the leader's suffix.
func (r *Raft) replicationPlanLocked(prevLogIndex uint64, entries []LogEntry) (truncateFrom uint64, entriesToAppend []LogEntry, err error) {
	for offset, entry := range entries {
		expectedIndex := prevLogIndex + uint64(offset) + 1
		if err := entry.Validate(); err != nil {
			return 0, nil, err
		}
		if entry.Index != expectedIndex {
			return 0, nil, fmt.Errorf("entry index %d does not follow previous index %d", entry.Index, expectedIndex-1)
		}

		if entry.Index <= r.lastLogIndexLocked() {
			existing, found := r.entryAtLocked(entry.Index)
			if !found {
				return 0, nil, fmt.Errorf("entry %d is compacted into the snapshot", entry.Index)
			}
			if existing == entry {
				continue
			}
			if existing.Term == entry.Term {
				return 0, nil, fmt.Errorf("entry %d has matching term but different command", entry.Index)
			}
			if entry.Index <= r.commitIndex {
				return 0, nil, fmt.Errorf("cannot replace committed entry %d", entry.Index)
			}
			return entry.Index, entries[offset:], nil
		}
		if entry.Index != r.lastLogIndexLocked()+1 {
			return 0, nil, fmt.Errorf("entry %d leaves a log gap", entry.Index)
		}
		return 0, entries[offset:], nil
	}
	return 0, nil, nil
}

func (r *Raft) saveStableLocked(term uint64, votedFor int) error {
	return r.saveStableValuesLocked(term, votedFor, r.commitIndex, r.lastApplied)
}

func (r *Raft) saveStableValuesLocked(term uint64, votedFor int, commitIndex uint64, lastApplied uint64) error {
	return r.stable.Save(StableState{
		CurrentTerm: term,
		VotedFor:    votedFor,
		CommitIndex: commitIndex,
		LastApplied: lastApplied,
	})
}

func (r *Raft) signalElectionReset() {
	select {
	case r.resetElection <- struct{}{}:
	default:
	}
}

// signalReplication coalesces triggers: one outstanding signal is enough,
// because replication always builds requests from the latest leader state.
func (r *Raft) signalReplication() {
	select {
	case r.replicateNow <- struct{}{}:
	default:
	}
}

// signalApply wakes the single apply loop. Signals are intentionally
// coalesced: the loop drains every entry through the latest commit index.
func (r *Raft) signalApply() {
	select {
	case r.applyNow <- struct{}{}:
	default:
	}
}

// applyLoop is the sole owner of state-machine application. It snapshots one
// committed entry under the Raft mutex, releases the mutex for the potentially
// slow state-machine call, then persists lastApplied before notifying a client.
func (r *Raft) applyLoop(ctx context.Context) {
	defer r.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.applyNow:
			if err := r.applyCommitted(); err != nil {
				r.recordApplyFailure(err)
				return
			}
		}
	}
}

func (r *Raft) applyCommitted() error {
	for {
		r.mu.RLock()
		if r.lastApplied >= r.commitIndex {
			r.mu.RUnlock()
			return nil
		}
		entry, found := r.entryAtLocked(r.lastApplied + 1)
		if !found {
			r.mu.RUnlock()
			return fmt.Errorf("committed log entry %d is absent", r.lastApplied+1)
		}
		if entry.Operation != NoopOperation && r.applier == nil {
			r.mu.RUnlock()
			return ErrNoApplier
		}
		applier := r.applier
		r.mu.RUnlock()

		if entry.Operation != NoopOperation {
			if err := applier.Apply(entry); err != nil {
				return fmt.Errorf("apply committed log entry %d: %w", entry.Index, err)
			}
		}

		r.mu.Lock()
		// There is only one apply loop, and committed log entries are immutable,
		// so this should always be the next entry. Keep the check explicit: it
		// protects the persistence checkpoint from future concurrency changes.
		if r.lastApplied+1 != entry.Index {
			r.mu.Unlock()
			return fmt.Errorf("apply order changed: last applied %d, entry %d", r.lastApplied, entry.Index)
		}
		if err := r.saveStableValuesLocked(r.currentTerm, r.votedFor, r.commitIndex, entry.Index); err != nil {
			r.mu.Unlock()
			return fmt.Errorf("persist last applied index %d: %w", entry.Index, err)
		}
		r.lastApplied = entry.Index
		if result, waiting := r.pendingResults[entry.Index]; waiting {
			result <- Result{Index: entry.Index}
			close(result)
			delete(r.pendingResults, entry.Index)
		}
		r.mu.Unlock()
	}
}

func (r *Raft) recordApplyFailure(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.applyErr != nil {
		return
	}
	r.applyErr = err
	for index, result := range r.pendingResults {
		result <- Result{Index: index, Err: err}
		close(result)
		delete(r.pendingResults, index)
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

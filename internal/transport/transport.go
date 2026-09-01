// Package transport adapts Raft's Go-level RPC types to concrete network
// transports. Raft itself deliberately does not import this package.
package transport

import (
	"github.com/pranavbhole123/distributed_kv_store/internal/raft"
	raftpb "github.com/pranavbhole123/distributed_kv_store/internal/transport/proto"
)

func requestVoteToProto(request raft.RequestVoteRequest) *raftpb.RequestVoteRequest {
	return &raftpb.RequestVoteRequest{
		Term:         request.Term,
		CandidateId:  int32(request.CandidateID),
		LastLogIndex: request.LastLogIndex,
		LastLogTerm:  request.LastLogTerm,
	}
}

func requestVoteFromProto(request *raftpb.RequestVoteRequest) raft.RequestVoteRequest {
	return raft.RequestVoteRequest{
		Term:         request.GetTerm(),
		CandidateID:  int(request.GetCandidateId()),
		LastLogIndex: request.GetLastLogIndex(),
		LastLogTerm:  request.GetLastLogTerm(),
	}
}

func requestVoteResponseToProto(response raft.RequestVoteResponse) *raftpb.RequestVoteResponse {
	return &raftpb.RequestVoteResponse{Term: response.Term, VoteGranted: response.VoteGranted}
}

func requestVoteResponseFromProto(response *raftpb.RequestVoteResponse) raft.RequestVoteResponse {
	return raft.RequestVoteResponse{Term: response.GetTerm(), VoteGranted: response.GetVoteGranted()}
}

func appendEntriesToProto(request raft.AppendEntriesRequest) *raftpb.AppendEntriesRequest {
	entries := make([]*raftpb.LogEntry, 0, len(request.Entries))
	for _, entry := range request.Entries {
		entries = append(entries, logEntryToProto(entry))
	}
	return &raftpb.AppendEntriesRequest{
		Term:         request.Term,
		LeaderId:     int32(request.LeaderID),
		PrevLogIndex: request.PrevLogIndex,
		PrevLogTerm:  request.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: request.LeaderCommit,
	}
}

func appendEntriesFromProto(request *raftpb.AppendEntriesRequest) raft.AppendEntriesRequest {
	entries := make([]raft.LogEntry, 0, len(request.GetEntries()))
	for _, entry := range request.GetEntries() {
		entries = append(entries, logEntryFromProto(entry))
	}
	return raft.AppendEntriesRequest{
		Term:         request.GetTerm(),
		LeaderID:     int(request.GetLeaderId()),
		PrevLogIndex: request.GetPrevLogIndex(),
		PrevLogTerm:  request.GetPrevLogTerm(),
		Entries:      entries,
		LeaderCommit: request.GetLeaderCommit(),
	}
}

func appendEntriesResponseToProto(response raft.AppendEntriesResponse) *raftpb.AppendEntriesResponse {
	return &raftpb.AppendEntriesResponse{Term: response.Term, Success: response.Success}
}

func appendEntriesResponseFromProto(response *raftpb.AppendEntriesResponse) raft.AppendEntriesResponse {
	return raft.AppendEntriesResponse{Term: response.GetTerm(), Success: response.GetSuccess()}
}

func logEntryToProto(entry raft.LogEntry) *raftpb.LogEntry {
	return &raftpb.LogEntry{
		Index:     entry.Index,
		Term:      entry.Term,
		Operation: operationToProto(entry.Operation),
		Key:       entry.Key,
		Value:     entry.Value,
	}
}

func logEntryFromProto(entry *raftpb.LogEntry) raft.LogEntry {
	return raft.LogEntry{
		Index:     entry.GetIndex(),
		Term:      entry.GetTerm(),
		Operation: operationFromProto(entry.GetOperation()),
		Key:       entry.GetKey(),
		Value:     entry.GetValue(),
	}
}

func operationToProto(operation raft.Operation) raftpb.Operation {
	switch operation {
	case raft.NoopOperation:
		return raftpb.Operation_OPERATION_UNSPECIFIED
	case raft.SetOperation:
		return raftpb.Operation_OPERATION_SET
	case raft.DeleteOperation:
		return raftpb.Operation_OPERATION_DELETE
	default:
		return raftpb.Operation_OPERATION_UNSPECIFIED
	}
}

func operationFromProto(operation raftpb.Operation) raft.Operation {
	switch operation {
	case raftpb.Operation_OPERATION_UNSPECIFIED:
		return raft.NoopOperation
	case raftpb.Operation_OPERATION_SET:
		return raft.SetOperation
	case raftpb.Operation_OPERATION_DELETE:
		return raft.DeleteOperation
	default:
		return "invalid"
	}
}

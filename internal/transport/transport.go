// Package transport adapts Raft's Go-level RPC types to concrete network
// transports. Raft itself deliberately does not import this package.
package transport

import (
	"github.com/pranavbhole123/distributed_kv_store/internal/raft"
	raftpb "github.com/pranavbhole123/distributed_kv_store/internal/transport/proto"
)

func requestVoteToProto(request raft.RequestVoteRequest) *raftpb.RequestVoteRequest {
	return &raftpb.RequestVoteRequest{
		Term:        request.Term,
		CandidateId: int32(request.CandidateID),
	}
}

func requestVoteFromProto(request *raftpb.RequestVoteRequest) raft.RequestVoteRequest {
	return raft.RequestVoteRequest{
		Term:        request.GetTerm(),
		CandidateID: int(request.GetCandidateId()),
	}
}

func requestVoteResponseToProto(response raft.RequestVoteResponse) *raftpb.RequestVoteResponse {
	return &raftpb.RequestVoteResponse{Term: response.Term, VoteGranted: response.VoteGranted}
}

func requestVoteResponseFromProto(response *raftpb.RequestVoteResponse) raft.RequestVoteResponse {
	return raft.RequestVoteResponse{Term: response.GetTerm(), VoteGranted: response.GetVoteGranted()}
}

func appendEntriesToProto(request raft.AppendEntriesRequest) *raftpb.AppendEntriesRequest {
	return &raftpb.AppendEntriesRequest{Term: request.Term, LeaderId: int32(request.LeaderID)}
}

func appendEntriesFromProto(request *raftpb.AppendEntriesRequest) raft.AppendEntriesRequest {
	return raft.AppendEntriesRequest{Term: request.GetTerm(), LeaderID: int(request.GetLeaderId())}
}

func appendEntriesResponseToProto(response raft.AppendEntriesResponse) *raftpb.AppendEntriesResponse {
	return &raftpb.AppendEntriesResponse{Term: response.Term, Success: response.Success}
}

func appendEntriesResponseFromProto(response *raftpb.AppendEntriesResponse) raft.AppendEntriesResponse {
	return raft.AppendEntriesResponse{Term: response.GetTerm(), Success: response.GetSuccess()}
}

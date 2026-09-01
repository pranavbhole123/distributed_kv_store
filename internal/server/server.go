package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/pranavbhole123/distributed_kv_store/internal/store"
)

const writeTimeout = 2 * time.Second

// fist think of what all things we need in this
// we alson need the store interface in this
type Server struct {
	addr           string
	store          store.Store
	httpSrv        *http.Server
	cluster        ClusterStatus
	writes         WriteCoordinator
	maxValueLength int
}

// ClusterStatus supplies only the cluster information needed by HTTP handlers.
// Keeping this interface here prevents the HTTP package from depending on Raft.
type ClusterStatus interface {
	LeaderID() int
	CurrentTerm() uint64
	LeaderHTTPAddr() (string, bool)
}

// WriteCoordinator is the complete write boundary required by HTTP. It keeps
// handlers independent of Raft types and lets Node own the wait for a proposal
// to become committed and applied.
type WriteCoordinator interface {
	IsLeader() bool
	ProposeSet(context.Context, string, string) error
	ProposeDelete(context.Context, string) error
	LeaderHTTPAddr() (string, bool)
}

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewServer(addr string, store store.Store, cluster ClusterStatus, writes WriteCoordinator, maxValueLength int) *Server {
	return &Server{
		addr:           addr,
		store:          store,
		cluster:        cluster,
		writes:         writes,
		maxValueLength: maxValueLength,
	}
}

// /get?key=
func (s *Server) getHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing query parameter: key", http.StatusBadRequest)
		return
	}

	value, err := s.store.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, err = w.Write([]byte(value))
	if err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
		return
	}
}

func (s *Server) setHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.requireLeaderForWrite(w, r) {
		return
	}

	var req SetRequest
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "key cannot be empty", http.StatusBadRequest)
		return
	}
	if len(req.Value) > s.maxValueLength {
		http.Error(w, fmt.Sprintf("value cannot be greater than %d", s.maxValueLength), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), writeTimeout)
	defer cancel()
	if err := s.writes.ProposeSet(ctx, req.Key, req.Value); err != nil {
		s.writeFailure(w, r, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.requireLeaderForWrite(w, r) {
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing query parameter: key", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), writeTimeout)
	defer cancel()
	if err := s.writes.ProposeDelete(ctx, key); err != nil {
		s.writeFailure(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) requireLeaderForWrite(w http.ResponseWriter, r *http.Request) bool {
	if s.writes == nil {
		http.Error(w, "cluster writes are not configured", http.StatusServiceUnavailable)
		return false
	}
	if s.writes.IsLeader() {
		return true
	}
	s.redirectToLeader(w, r)
	return false
}

func (s *Server) writeFailure(w http.ResponseWriter, r *http.Request, err error) {
	// Leadership can change between requireLeaderForWrite and Propose. Re-read
	// it before returning an error so the client gets the most useful next hop.
	if !s.writes.IsLeader() {
		s.redirectToLeader(w, r)
		return
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "write was not confirmed by a Raft majority; retry", http.StatusServiceUnavailable)
		return
	}
	http.Error(w, "write was not confirmed: "+err.Error(), http.StatusServiceUnavailable)
}

func (s *Server) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	address, known := s.writes.LeaderHTTPAddr()
	if !known {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no Raft leader is known", http.StatusServiceUnavailable)
		return
	}
	target := (&url.URL{Scheme: "http", Host: address, Path: r.URL.Path, RawQuery: r.URL.RawQuery}).String()
	http.Redirect(w, r, target, http.StatusTemporaryRedirect)
}

func (s *Server) leaderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cluster == nil {
		http.Error(w, "cluster mode is not configured", http.StatusServiceUnavailable)
		return
	}

	address, known := s.cluster.LeaderHTTPAddr()
	response := struct {
		LeaderID       int    `json:"leader_id"`
		LeaderHTTPAddr string `json:"leader_http_addr,omitempty"`
		Term           uint64 `json:"term"`
		Known          bool   `json:"known"`
	}{
		LeaderID:       s.cluster.LeaderID(),
		LeaderHTTPAddr: address,
		Term:           s.cluster.CurrentTerm(),
		Known:          known,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// we need a function to start the server
func (s *Server) Start() error {

	mux := http.NewServeMux()
	// we need to make 4 different path
	mux.HandleFunc("/get", s.getHandler)
	mux.HandleFunc("/set", s.setHandler)
	mux.HandleFunc("/delete", s.deleteHandler)
	mux.HandleFunc("/leader", s.leaderHandler)
	mux.HandleFunc("/health", s.healthHandler)

	s.httpSrv = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	return s.httpSrv.ListenAndServe()

}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

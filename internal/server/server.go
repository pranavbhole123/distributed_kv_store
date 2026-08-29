package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/pranavbhole123/distributed_kv_store/internal/store"
)

// fist think of what all things we need in this
// we alson need the store interface in this
type Server struct {
	addr    string
	store   store.Store
	httpSrv *http.Server
	cluster ClusterStatus
}

// ClusterStatus supplies only the cluster information needed by HTTP handlers.
// Keeping this interface here prevents the HTTP package from depending on Raft.
type ClusterStatus interface {
	LeaderID() int
	CurrentTerm() uint64
	LeaderHTTPAddr() (string, bool)
	WritesReady() bool
}

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewServer(addr string, store store.Store, cluster ClusterStatus) *Server {
	return &Server{
		addr:    addr,
		store:   store,
		cluster: cluster,
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
	if s.cluster == nil || !s.cluster.WritesReady() {
		http.Error(w, "writes are unavailable until Raft log replication is implemented", http.StatusServiceUnavailable)
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

	// Phase 4.5 replaces this guard with a Raft proposal. A direct store write
	// here would bypass majority replication and violate Raft safety.
}

func (s *Server) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cluster == nil || !s.cluster.WritesReady() {
		http.Error(w, "writes are unavailable until Raft log replication is implemented", http.StatusServiceUnavailable)
		return
	}

	// Phase 4.5 replaces this guard with a Raft proposal.
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

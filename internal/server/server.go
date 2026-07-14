package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pranavbhole123/distributed_kv_store/internal/store"
)

// fist think of what all things we need in this
// we alson need the store interface in this
type Server struct {
	port    int
	store   store.Store
	httpSrv *http.Server
}

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewServer(port int, store store.Store) *Server {
	return &Server{
		port:  port,
		store: store,
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


	if err := s.store.Set(req.Key, req.Value); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusCreated)

	if _, err := w.Write([]byte("key stored successfully")); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
		return
	}
}

func (s *Server) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing query parameter: key", http.StatusBadRequest)
		return
	}
	

	if err := s.store.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("key deleted successfully"))
	if err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

// we need a function to start the server
func (s *Server) Start() error {

	mux := http.NewServeMux()
	// we need to make 4 different path
	mux.HandleFunc("/get", s.getHandler)
	mux.HandleFunc("/set", s.setHandler)
	mux.HandleFunc("/delete", s.deleteHandler)

	s.httpSrv = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	return s.httpSrv.ListenAndServe()

}

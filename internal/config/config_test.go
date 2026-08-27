package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadYAMLConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-1.yaml")
	contents := []byte(`
node:
  id: 1
  raft_addr: localhost:7001
  http_addr: localhost:8080
peers:
  - id: 2
    raft_addr: localhost:7002
    http_addr: localhost:8081
  - id: 3
    raft_addr: localhost:7003
    http_addr: localhost:8082
data_dir: data/node-1
timing:
  election_timeout_min: 300ms
  election_timeout_max: 600ms
  heartbeat_interval: 50ms
`)
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ClusterSize() != 3 || cfg.Majority() != 2 {
		t.Fatalf("ClusterSize() = %d, Majority() = %d; want 3 and 2", cfg.ClusterSize(), cfg.Majority())
	}
	if peer, found := cfg.PeerByID(2); !found || peer.HTTPAddr != "localhost:8081" {
		t.Fatalf("PeerByID(2) = %+v, %t; want peer 2", peer, found)
	}
}

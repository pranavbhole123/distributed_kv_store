// Package config describes the fixed membership of a KV store cluster.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultElectionTimeoutMin = 300 * time.Millisecond
	DefaultElectionTimeoutMax = 600 * time.Millisecond
	DefaultHeartbeatInterval  = 50 * time.Millisecond
)

// Node identifies one member of the cluster. RaftAddr is reserved for the
// node-to-node Raft transport; HTTPAddr is used by clients and leader redirects.
type Node struct {
	ID       int    `yaml:"id"`
	RaftAddr string `yaml:"raft_addr"`
	HTTPAddr string `yaml:"http_addr"`
}

// Config is immutable cluster configuration supplied when a process starts.
// Peers never includes Self.
type Config struct {
	Self    Node
	Peers   []Node
	DataDir string

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

func (c Config) ClusterSize() int {
	return len(c.Peers) + 1
}

func (c Config) Majority() int {
	return c.ClusterSize()/2 + 1
}

func (c Config) PeerByID(id int) (Node, bool) {
	for _, peer := range c.Peers {
		if peer.ID == id {
			return peer, true
		}
	}
	return Node{}, false
}

func (c Config) WALPath() string {
	return filepath.Join(c.DataDir, "wal.log")
}

func (c Config) RaftStatePath() string {
	return filepath.Join(c.DataDir, "raft-state.json")
}

func (c Config) Validate() error {
	if c.ClusterSize() != 3 {
		return fmt.Errorf("Phase 3 requires exactly 3 static cluster members, got %d", c.ClusterSize())
	}
	if c.Self.ID < 0 {
		return errors.New("node ID must be non-negative")
	}
	if c.Self.HTTPAddr == "" {
		return errors.New("local HTTP address cannot be empty")
	}
	if c.DataDir == "" {
		return errors.New("data directory cannot be empty")
	}
	if c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax <= 0 {
		return errors.New("election timeouts must be positive")
	}
	if c.ElectionTimeoutMin >= c.ElectionTimeoutMax {
		return errors.New("minimum election timeout must be smaller than maximum election timeout")
	}
	if c.HeartbeatInterval <= 0 || c.HeartbeatInterval >= c.ElectionTimeoutMin {
		return errors.New("heartbeat interval must be positive and smaller than the minimum election timeout")
	}

	ids := map[int]struct{}{c.Self.ID: {}}
	raftAddrs := map[string]struct{}{}
	httpAddrs := map[string]struct{}{c.Self.HTTPAddr: {}}
	if c.Self.RaftAddr != "" {
		raftAddrs[c.Self.RaftAddr] = struct{}{}
	}
	for _, peer := range c.Peers {
		if peer.ID < 0 || peer.RaftAddr == "" || peer.HTTPAddr == "" {
			return fmt.Errorf("peer %d must have a non-negative ID, Raft address, and HTTP address", peer.ID)
		}
		if _, exists := ids[peer.ID]; exists {
			return fmt.Errorf("duplicate node ID %d", peer.ID)
		}
		if _, exists := raftAddrs[peer.RaftAddr]; exists {
			return fmt.Errorf("duplicate Raft address %q", peer.RaftAddr)
		}
		if _, exists := httpAddrs[peer.HTTPAddr]; exists {
			return fmt.Errorf("duplicate HTTP address %q", peer.HTTPAddr)
		}
		ids[peer.ID] = struct{}{}
		raftAddrs[peer.RaftAddr] = struct{}{}
		httpAddrs[peer.HTTPAddr] = struct{}{}
	}
	return nil
}

type fileConfig struct {
	Node    Node   `yaml:"node"`
	Peers   []Node `yaml:"peers"`
	DataDir string `yaml:"data_dir"`
	Timing  timing `yaml:"timing"`
}

type timing struct {
	ElectionTimeoutMin time.Duration `yaml:"election_timeout_min"`
	ElectionTimeoutMax time.Duration `yaml:"election_timeout_max"`
	HeartbeatInterval  time.Duration `yaml:"heartbeat_interval"`
}

// Load reads immutable cluster membership from a YAML file. The executable takes
// the file path as its only argument, for example: ./kvstore configs/node-1.yaml.
func Load(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read configuration %q: %w", path, err)
	}

	var raw fileConfig
	if err := yaml.Unmarshal(contents, &raw); err != nil {
		return Config{}, fmt.Errorf("parse configuration %q: %w", path, err)
	}

	cfg := Config{
		Self:    raw.Node,
		Peers:   raw.Peers,
		DataDir: raw.DataDir,

		ElectionTimeoutMin: raw.Timing.ElectionTimeoutMin,
		ElectionTimeoutMax: raw.Timing.ElectionTimeoutMax,
		HeartbeatInterval:  raw.Timing.HeartbeatInterval,
	}
	if cfg.ElectionTimeoutMin == 0 {
		cfg.ElectionTimeoutMin = DefaultElectionTimeoutMin
	}
	if cfg.ElectionTimeoutMax == 0 {
		cfg.ElectionTimeoutMax = DefaultElectionTimeoutMax
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid configuration %q: %w", path, err)
	}
	return cfg, nil
}

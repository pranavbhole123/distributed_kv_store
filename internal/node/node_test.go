package node

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/pranavbhole123/distributed_kv_store/internal/config"
)

func TestNodeStartsServesHealthAndStops(t *testing.T) {
	cfg := config.Config{
		Self: config.Node{
			ID:       1,
			RaftAddr: testAddress(t),
			HTTPAddr: testAddress(t),
		},
		Peers: []config.Node{
			{ID: 2, RaftAddr: "127.0.0.1:31002", HTTPAddr: "127.0.0.1:32002"},
			{ID: 3, RaftAddr: "127.0.0.1:31003", HTTPAddr: "127.0.0.1:32003"},
		},
		DataDir:            t.TempDir(),
		ElectionTimeoutMin: 100 * time.Millisecond,
		ElectionTimeoutMax: 200 * time.Millisecond,
		HeartbeatInterval:  25 * time.Millisecond,
		SnapshotThreshold:  2,
	}
	n, err := New(cfg, 20)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startResult := make(chan error, 1)
	go func() { startResult <- n.Start(ctx) }()

	client := &http.Client{Timeout: 50 * time.Millisecond}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + cfg.Self.HTTPAddr + "/health")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("GET /health status = %d, want 200", response.StatusCode)
			}
			break
		}
		select {
		case startErr := <-startResult:
			t.Fatalf("Node.Start() returned before serving health: %v", startErr)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if time.Now().After(deadline) {
		t.Fatal("node did not serve /health before timeout")
	}

	cancel()
	select {
	case err := <-startResult:
		if err != nil {
			t.Fatalf("Node.Start() after shutdown = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Node.Start() did not return after cancellation")
	}
}

func testAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

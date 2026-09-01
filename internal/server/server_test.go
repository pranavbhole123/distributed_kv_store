package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pranavbhole123/distributed_kv_store/internal/store"
)

type fakeWriteCoordinator struct {
	leader      bool
	leaderAddr  string
	leaderKnown bool
	setCalls    int
	deleteCalls int
	setKey      string
	setValue    string
	deleteKey   string
	proposalErr error
	stepDownSet bool
}

func (c *fakeWriteCoordinator) IsLeader() bool { return c.leader }

func (c *fakeWriteCoordinator) ProposeSet(_ context.Context, key, value string) error {
	c.setCalls++
	c.setKey, c.setValue = key, value
	if c.stepDownSet {
		c.leader = false
	}
	return c.proposalErr
}

func (c *fakeWriteCoordinator) ProposeDelete(_ context.Context, key string) error {
	c.deleteCalls++
	c.deleteKey = key
	return c.proposalErr
}

func (c *fakeWriteCoordinator) LeaderHTTPAddr() (string, bool) {
	return c.leaderAddr, c.leaderKnown
}

func TestSetHandlerProposesOnlyOnLeader(t *testing.T) {
	coordinator := &fakeWriteCoordinator{leader: true}
	s := NewServer("unused", store.NewMemoryStore(20), nil, coordinator, 20)
	request := httptest.NewRequest(http.MethodPost, "/set", strings.NewReader(`{"key":"colour","value":"blue"}`))
	response := httptest.NewRecorder()

	s.setHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if coordinator.setCalls != 1 || coordinator.setKey != "colour" || coordinator.setValue != "blue" {
		t.Fatalf("proposal = calls:%d key:%q value:%q; want one SET colour=blue", coordinator.setCalls, coordinator.setKey, coordinator.setValue)
	}
	if _, err := s.store.Get("colour"); err == nil {
		t.Fatal("handler wrote directly to MemoryStore instead of proposing through Raft")
	}
}

func TestWriteHandlersRedirectFollowerToKnownLeader(t *testing.T) {
	coordinator := &fakeWriteCoordinator{leader: false, leaderAddr: "leader.example:8081", leaderKnown: true}
	s := NewServer("unused", store.NewMemoryStore(20), nil, coordinator, 20)
	request := httptest.NewRequest(http.MethodPost, "/set?source=follower", strings.NewReader(`{"key":"a","value":"1"}`))
	response := httptest.NewRecorder()

	s.setHandler(response, request)

	if response.Code != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Location"); got != "http://leader.example:8081/set?source=follower" {
		t.Fatalf("redirect = %q", got)
	}
	if coordinator.setCalls != 0 {
		t.Fatalf("follower called ProposeSet %d times", coordinator.setCalls)
	}
}

func TestWriteHandlersRejectWhenNoLeaderIsKnown(t *testing.T) {
	coordinator := &fakeWriteCoordinator{leader: false}
	s := NewServer("unused", store.NewMemoryStore(20), nil, coordinator, 20)
	request := httptest.NewRequest(http.MethodDelete, "/delete?key=a", nil)
	response := httptest.NewRecorder()

	s.deleteHandler(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %s", response.Code, response.Body.String())
	}
	if coordinator.deleteCalls != 0 {
		t.Fatalf("node without a leader called ProposeDelete %d times", coordinator.deleteCalls)
	}
}

func TestIsolatedLeaderWriteReturnsRetryableFailure(t *testing.T) {
	coordinator := &fakeWriteCoordinator{leader: true, proposalErr: context.DeadlineExceeded}
	s := NewServer("unused", store.NewMemoryStore(20), nil, coordinator, 20)
	request := httptest.NewRequest(http.MethodDelete, "/delete?key=a", nil)
	response := httptest.NewRecorder()

	s.deleteHandler(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
}

func TestWriteFailureAfterLeadershipChangeRedirects(t *testing.T) {
	coordinator := &fakeWriteCoordinator{leader: true, leaderAddr: "leader.example:8081", leaderKnown: true, stepDownSet: true}
	coordinator.proposalErr = errors.New("lost leadership")
	s := NewServer("unused", store.NewMemoryStore(20), nil, coordinator, 20)

	// Simulate the leadership change that happens between the initial leader
	// check and the result returning from Node.ProposeSet.
	request := httptest.NewRequest(http.MethodPost, "/set", strings.NewReader(`{"key":"a","value":"1"}`))
	response := httptest.NewRecorder()
	s.setHandler(response, request)
	if response.Code != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307: %s", response.Code, response.Body.String())
	}
	if coordinator.setCalls != 1 {
		t.Fatalf("ProposeSet calls = %d, want 1", coordinator.setCalls)
	}
}

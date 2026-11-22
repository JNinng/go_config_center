package discovery

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// 经 Leader Seed 完成发现与 Join：OnLeader 收到 Leader 地址，Join 调用打到 Leader。
func TestRunJoinsThroughLeaderSeed(t *testing.T) {
	var joinCount atomic.Int32
	var leaderTS *httptest.Server
	leaderTS = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/discovery":
			_ = json.NewEncoder(w).Encode(map[string]string{
				"node_id":          "n1",
				"role":             "leader",
				"leader_addr":      "127.0.0.1:7946",
				"leader_http_addr": leaderTS.URL,
			})
		case "/v1/cluster/join":
			joinCount.Add(1)
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "added"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer leaderTS.Close()

	onLeader := make(chan string, 4)
	opts := Options{
		NodeID:        "n2",
		RaftAddr:      "127.0.0.1:7947",
		HTTPAdvertise: "http://127.0.0.1:8081",
		Seeds:         []string{leaderTS.URL},
		AdminToken:    "tok",
		RefreshPeriod: 100 * time.Millisecond,
		OnLeader:      func(_, addr string) { onLeader <- addr },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, quietLogger(), opts)

	select {
	case addr := <-onLeader:
		if addr != leaderTS.URL {
			t.Fatalf("OnLeader = %q", addr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("发现代理未在期限内完成 Join")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && joinCount.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if n := joinCount.Load(); n != 1 {
		t.Fatalf("Join 调用次数 = %d, want 1", n)
	}
	// 加入后进入刷新期：OnLeader 周期上报，Join 不再重复发生。
	time.Sleep(400 * time.Millisecond)
	if n := joinCount.Load(); n != 1 {
		t.Fatalf("刷新期 Join 次数 = %d, want 1", n)
	}
}

// 经 Follower Seed 的缓存地址找到 Leader（DESIGN §4.5：取 Leader 地址再试）。
func TestRunJoinsViaFollowerCache(t *testing.T) {
	leaderTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer leaderTS.Close()

	followerTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"node_id":          "n1",
			"role":             "follower",
			"leader_addr":      "127.0.0.1:7946",
			"leader_http_addr": leaderTS.URL,
		})
	}))
	defer followerTS.Close()

	onLeader := make(chan string, 1)
	opts := Options{
		NodeID: "n2", RaftAddr: "127.0.0.1:7947", HTTPAdvertise: "http://127.0.0.1:8081",
		Seeds: []string{followerTS.URL}, AdminToken: "tok",
		RefreshPeriod: 100 * time.Millisecond,
		OnLeader: func(_, addr string) {
			select {
			case onLeader <- addr:
			default:
			}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, quietLogger(), opts)

	select {
	case addr := <-onLeader:
		if addr != leaderTS.URL {
			t.Fatalf("OnLeader = %q, want %q", addr, leaderTS.URL)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("未通过 Follower 缓存发现 Leader")
	}
}

// Seed 全部不可达：持续退避重试，不退出、不 Bootstrap。
func TestRunKeepsRetryingWhenSeedsUnreachable(t *testing.T) {
	opts := Options{
		NodeID: "n2", RaftAddr: "127.0.0.1:7947", HTTPAdvertise: "http://127.0.0.1:8081",
		Seeds: []string{"http://127.0.0.1:1"}, // 不可达端口
	}
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		Run(ctx, quietLogger(), opts)
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("Seed 不可达时发现代理不应退出")
	case <-time.After(1500 * time.Millisecond):
		// 期间至少经历一次退避，进程仍在重试。
	}
	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("ctx 取消后 Run 未返回")
	}
}

// isSelfAddr 契约：seed 等于通告地址或 Raft 地址即判定为自身——扫描时据此跳过，
// seeds 只含自身则不发任何请求。
func TestIsSelfAddr(t *testing.T) {
	cases := []struct {
		seed, advertise, raft string
		want                  bool
	}{
		{"http://127.0.0.1:8080", "http://127.0.0.1:8080", "127.0.0.1:7946", true},
		{"127.0.0.1:7946", "http://127.0.0.1:8080", "127.0.0.1:7946", true},
		{"http://localhost:8080", "http://localhost:8080", "127.0.0.1:7946", true},
		{"http://127.0.0.1:8082", "http://127.0.0.1:8080", "127.0.0.1:7946", false},
	}
	for _, c := range cases {
		if got := isSelfAddr(c.seed, c.advertise, c.raft); got != c.want {
			t.Fatalf("isSelfAddr(%q) = %v, want %v", c.seed, got, c.want)
		}
	}
}

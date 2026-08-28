package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/jninng/go_config_center/internal/store"
	"github.com/jninng/go_config_center/internal/testutil"
)

func mustServer(t *testing.T, ready func() bool, snap func() (uint64, error)) (*Server, func()) {
	t.Helper()
	dsn := testutil.NewDB(t, "gcc_api_test")
	st, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return New(nil, st, "tok", ready, snap), st.Close
}

// DESIGN §4.4：Serving 门闩为 false 期间读 API 返回 503 + Retry-After。
func TestGetReturns503WhenNotServing(t *testing.T) {
	ready := &atomic.Bool{}
	srv, cleanup := mustServer(t, func() bool { return ready.Load() }, nil)
	defer cleanup()
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	ready.Store(false)
	resp, err := http.Get(ts.URL + "/v1/namespaces/dev/configs/a")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("门闩关闭时 GET = %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("503 响应应带 Retry-After")
	}

	// 门闩打开后同一请求进入正常读路径（条目不存在 → 404）。
	ready.Store(true)
	resp, err = http.Get(ts.URL + "/v1/namespaces/dev/configs/a")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("门闩打开后 GET = %d, want 404（进入正常读路径）", resp.StatusCode)
	}
}

// 手动触发快照：认证 + 成功/无新日志两种响应。
func TestSnapshotTriggerEndpoint(t *testing.T) {
	srv, cleanup := mustServer(t, func() bool { return true }, func() (uint64, error) {
		return 42, nil
	})
	defer cleanup()
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	get := func(auth string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/cluster/snapshot", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp := get("")
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("无认证 = %d, want 401", resp.StatusCode)
	}

	resp = get("Bearer tok")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("触发快照 = %d, want 200", resp.StatusCode)
	}

	// 无新日志可快照不是错误（幂等触发）。
	srv2, cleanup2 := mustServer(t, func() bool { return true }, func() (uint64, error) {
		return 0, ErrNothingNewToSnapshot
	})
	defer cleanup2()
	ts2 := httptest.NewServer(srv2.Routes())
	defer ts2.Close()
	req, _ := http.NewRequest(http.MethodPost, ts2.URL+"/v1/cluster/snapshot", nil)
	req.Header.Set("Authorization", "Bearer tok")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("无新日志快照 = %d, want 200", resp2.StatusCode)
	}

	// 非 Leader：本票返回 503（307 重定向属后续票）。
	srv3, cleanup3 := mustServer(t, func() bool { return true }, func() (uint64, error) {
		return 0, ErrNoLeader
	})
	defer cleanup3()
	ts3 := httptest.NewServer(srv3.Routes())
	defer ts3.Close()
	req, _ = http.NewRequest(http.MethodPost, ts3.URL+"/v1/cluster/snapshot", nil)
	req.Header.Set("Authorization", "Bearer tok")
	resp3, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("非 Leader 触发 = %d, want 503", resp3.StatusCode)
	}
}

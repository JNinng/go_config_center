package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
	"github.com/jninng/go_config_center/internal/testutil"
)

func mustStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := testutil.NewDB(t, "gcc_api_test")
	st, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func mustServer(t *testing.T, ready func() bool, snap func() (uint64, error)) (*Server, *store.Store, func()) {
	t.Helper()
	st := mustStore(t)
	return New(nil, st, "tok", ready, snap), st, func() {}
}

// fakeProposer 以预置结果模拟 Raft 提案，用于端点语义映射测试。
type fakeProposer struct {
	res raftlog.Result
	err error
}

func (f fakeProposer) Propose(context.Context, []byte) (raftlog.Result, error) {
	return f.res, f.err
}

// DESIGN §4.4：Serving 门闩为 false 期间读 API 返回 503 + Retry-After。
func TestGetReturns503WhenNotServing(t *testing.T) {
	ready := &atomic.Bool{}
	srv, _, cleanup := mustServer(t, func() bool { return ready.Load() }, nil)
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
	srv, _, cleanup := mustServer(t, func() bool { return true }, func() (uint64, error) {
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
	srv2, _, cleanup2 := mustServer(t, func() bool { return true }, func() (uint64, error) {
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
	srv3, _, cleanup3 := mustServer(t, func() bool { return true }, func() (uint64, error) {
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

// 删除端点（DESIGN §5.1）：认证、expected_version 参数校验、200/404/409 与提案层错误映射。
func TestDeleteEndpoint(t *testing.T) {
	st := mustStore(t)

	do := func(t *testing.T, p Proposer, auth, url string) *http.Response {
		t.Helper()
		ts := httptest.NewServer(New(p, st, "tok", func() bool { return true }, nil).Routes())
		defer ts.Close()
		req, _ := http.NewRequest(http.MethodDelete, ts.URL+url, nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	resp := do(t, fakeProposer{}, "", "/v1/namespaces/dev/configs/a")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("无认证 DELETE = %d, want 401", resp.StatusCode)
	}

	resp = do(t, fakeProposer{res: raftlog.OK(3, false)}, "Bearer tok", "/v1/namespaces/dev/configs/a")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("成功删除 = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["version"] != float64(3) || body["status"] != "deleted" {
		t.Fatalf("body = %v, want version=3 status=deleted", body)
	}

	resp = do(t, fakeProposer{res: raftlog.Conflict(2)}, "Bearer tok", "/v1/namespaces/dev/configs/a?expected_version=2")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("CAS 冲突 = %d, want 409", resp.StatusCode)
	}
	body = nil
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["error"] != "version_conflict" || body["current_version"] != float64(2) {
		t.Fatalf("body = %v, want version_conflict + current_version=2", body)
	}

	resp = do(t, fakeProposer{res: raftlog.Result{Status: raftlog.StatusNotFound}}, "Bearer tok", "/v1/namespaces/dev/configs/ghost")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("删除不存在条目 = %d, want 404", resp.StatusCode)
	}

	for _, bad := range []string{"abc", "-1"} {
		resp = do(t, fakeProposer{}, "Bearer tok", fmt.Sprintf("/v1/namespaces/dev/configs/a?expected_version=%s", bad))
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected_version=%q = %d, want 400", bad, resp.StatusCode)
		}
	}

	resp = do(t, fakeProposer{err: ErrNoLeader}, "Bearer tok", "/v1/namespaces/dev/configs/a")
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("无 Leader 删除 = %d, want 503 + Retry-After", resp.StatusCode)
	}
}

// versions 端点（DESIGN §5.1）：倒序、limit/before_version 分页、Tombstone content 为 null。
func TestVersionsEndpoint(t *testing.T) {
	st := mustStore(t)
	ctx := context.Background()
	withTags := raftlog.PublishData{Namespace: t.Name(), DataId: "a", DataType: "TEXT", Content: "v1", Tags: map[string]string{"team": "core"}}
	if _, err := st.ApplyPublish(ctx, withTags, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyPublish(ctx, raftlog.PublishData{Namespace: t.Name(), DataId: "a", DataType: "TEXT", Content: "v2"}, 2, 1); err != nil {
		t.Fatal(err)
	}
	if res, err := st.ApplyDelete(ctx, raftlog.DeleteData{Namespace: t.Name(), DataId: "a", Actor: "admin"}, 3, 1); err != nil || res.Status != raftlog.StatusOK {
		t.Fatalf("delete = %+v %v", res, err)
	}

	ts := httptest.NewServer(New(nil, st, "tok", func() bool { return true }, nil).Routes())
	defer ts.Close()

	get := func(t *testing.T, query string) []map[string]any {
		t.Helper()
		resp, err := http.Get(ts.URL + "/v1/namespaces/" + t.Name() + "/configs/a/versions" + query)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET versions%s = %d, want 200", query, resp.StatusCode)
		}
		var records []map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
			t.Fatal(err)
		}
		return records
	}

	records := get(t, "")
	if len(records) != 3 || records[0]["version"] != float64(3) || records[2]["version"] != float64(1) {
		t.Fatalf("records = %v, want 倒序 [3 2 1]", records)
	}
	if records[0]["op"] != "delete" || records[0]["content"] != nil {
		t.Fatalf("墓碑记录 = %v, want op=delete content null", records[0])
	}
	if records[2]["content"] != "v1" || records[2]["tags"].(map[string]any)["team"] != "core" {
		t.Fatalf("publish 记录 = %v, want content=v1 + tags", records[2])
	}

	if records = get(t, "?limit=1"); len(records) != 1 || records[0]["version"] != float64(3) {
		t.Fatalf("limit=1 = %v, want [3]", records)
	}
	if records = get(t, "?before_version=2"); len(records) != 1 || records[0]["version"] != float64(1) {
		t.Fatalf("before_version=2 = %v, want [1]", records)
	}
	if records = get(t, "?limit=0"); len(records) != 3 {
		t.Fatalf("limit=0 取默认页大小 = %d 条, want 3", len(records))
	}

	resp, err := http.Get(ts.URL + "/v1/namespaces/x/configs/a/versions?limit=abc")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("limit=abc = %d, want 400", resp.StatusCode)
	}
}

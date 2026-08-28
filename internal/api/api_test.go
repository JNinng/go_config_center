package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
	"github.com/jninng/go_config_center/internal/testutil"
	"github.com/jninng/go_config_center/internal/watch"
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
	return New(nil, st, nil, "tok", ready, snap), st, func() {}
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
		ts := httptest.NewServer(New(p, st, watch.NewHub(), "tok", func() bool { return true }, nil).Routes())
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

	ts := httptest.NewServer(New(nil, st, nil, "tok", func() bool { return true }, nil).Routes())
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

// 回滚端点（DESIGN §5.1）：认证、body 校验、200/404/409 语义映射。
func TestRollbackEndpoint(t *testing.T) {
	st := mustStore(t)

	do := func(t *testing.T, p Proposer, auth, body string) *http.Response {
		t.Helper()
		ts := httptest.NewServer(New(p, st, watch.NewHub(), "tok", func() bool { return true }, nil).Routes())
		defer ts.Close()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/namespaces/dev/configs/a/rollback", strings.NewReader(body))
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

	resp := do(t, fakeProposer{}, "", `{"to_version":1}`)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("无认证回滚 = %d, want 401", resp.StatusCode)
	}

	resp = do(t, fakeProposer{res: raftlog.OK(4, false)}, "Bearer tok", `{"to_version":1}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("回滚 = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["version"] != float64(4) {
		t.Fatalf("body = %v, want version=4", body)
	}

	resp = do(t, fakeProposer{res: raftlog.Result{Status: raftlog.StatusInvalidRollback, CurrentVersion: 3}}, "Bearer tok", `{"to_version":3}`)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("无效目标 = %d, want 409", resp.StatusCode)
	}
	body = nil
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["error"] != "invalid_rollback" || body["current_version"] != float64(3) {
		t.Fatalf("body = %v, want invalid_rollback + current_version=3", body)
	}

	resp = do(t, fakeProposer{res: raftlog.Result{Status: raftlog.StatusNotFound}}, "Bearer tok", `{"to_version":99}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("目标不存在 = %d, want 404", resp.StatusCode)
	}

	for _, bad := range []string{`{}`, `{"to_version":0}`, `{"to_version":-1}`, `not-json`} {
		resp = do(t, fakeProposer{}, "Bearer tok", bad)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("body %q = %d, want 400", bad, resp.StatusCode)
		}
	}
}

func watchGet(t *testing.T, base, query string) (int, map[string]any, time.Duration) {
	t.Helper()
	start := time.Now()
	resp, err := http.Get(base + query)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	elapsed := time.Since(start)
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body, elapsed
}

// Watch 长轮询快路径与超时（DESIGN §4.3）：
// v 落后 → 立即返回当前状态；v 已是最新 → 挂起至 timeout，回显 v 供原样重挂。
func TestWatchImmediateAndTimeout(t *testing.T) {
	st := mustStore(t)
	hub := watch.NewHub()
	ts := httptest.NewServer(New(nil, st, hub, "tok", func() bool { return true }, nil).Routes())
	defer ts.Close()
	ctx := context.Background()
	base := ts.URL + "/v1/namespaces/dev/configs/a/watch"

	for i := 1; i <= 2; i++ {
		if _, err := st.ApplyPublish(ctx, raftlog.PublishData{Namespace: "dev", DataId: "a", DataType: "TEXT", Content: fmt.Sprintf("v%d", i)}, uint64(i), 1); err != nil {
			t.Fatal(err)
		}
	}
	code, body, elapsed := watchGet(t, base, "?v=1&timeout=5s")
	if code != http.StatusOK || body["status"] != "active" || elapsed > 2*time.Second {
		t.Fatalf("v=1（落后）= %d %v %v, want 立即 active", code, body, elapsed)
	}
	entry := body["entry"].(map[string]any)
	if entry["version"] != float64(2) || entry["content"] != "v2" {
		t.Fatalf("entry = %v, want v2 条目", entry)
	}

	code, body, _ = watchGet(t, base, "?v=2&timeout=150ms")
	if code != http.StatusOK || body["status"] != "timeout" || body["version"] != float64(2) {
		t.Fatalf("v=2（最新）= %d %v, want timeout 回显 v2", code, body)
	}

	// 条目此刻删除（墓碑 v3），供后续 deleted 断言使用。
	if res, err := st.ApplyDelete(ctx, raftlog.DeleteData{Namespace: "dev", DataId: "a"}, 3, 1); err != nil || res.Status != raftlog.StatusOK {
		t.Fatalf("delete = %+v %v", res, err)
	}

	code, body, elapsed = watchGet(t, base, "?v=2&timeout=5s")
	if code != http.StatusOK || body["status"] != "deleted" || body["version"] != float64(3) || elapsed > 2*time.Second {
		t.Fatalf("v=2 墓碑 v3 = %d %v %v, want 立即 deleted version=3", code, body, elapsed)
	}

	// 从未存在的条目：挂起至超时（等待创建是合法场景）。
	code, body, _ = watchGet(t, base, "?v=0&timeout=150ms")
	_ = code
	code, body, _ = watchGet(t, ts.URL+"/v1/namespaces/dev/configs/ghost/watch", "?v=0&timeout=150ms")
	if code != http.StatusOK || body["status"] != "timeout" {
		t.Fatalf("ghost = %d %v, want timeout", code, body)
	}

	for _, bad := range []string{"?v=abc", "?timeout=abc", "?timeout=0s", "?v=-1"} {
		resp, err := http.Get(base + bad)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", bad, resp.StatusCode)
		}
	}
}

// 验收：挂起期间 Publish/Delete 唤醒挂起连接，立即返回。
func TestWatchWakeOnNotify(t *testing.T) {
	st := mustStore(t)
	hub := watch.NewHub()
	ts := httptest.NewServer(New(nil, st, hub, "tok", func() bool { return true }, nil).Routes())
	defer ts.Close()
	ctx := context.Background()

	watchCh := make(chan map[string]any, 1)
	go func() {
		resp, err := http.Get(ts.URL + "/v1/namespaces/dev/configs/a/watch?v=0&timeout=10s")
		if err != nil {
			close(watchCh)
			return
		}
		defer resp.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		watchCh <- body
	}()

	// 等挂起连接完成快路径检查并注册，再模拟 FSM Apply 的唤醒。
	time.Sleep(200 * time.Millisecond)
	if _, err := st.ApplyPublish(ctx, raftlog.PublishData{Namespace: "dev", DataId: "a", DataType: "TEXT", Content: "woken"}, 1, 1); err != nil {
		t.Fatal(err)
	}
	hub.Notify("dev", "a", watch.Event{Version: 1})

	select {
	case body := <-watchCh:
		if body == nil || body["status"] != "active" {
			t.Fatalf("唤醒 = %v, want active", body)
		}
		entry := body["entry"].(map[string]any)
		if entry["version"] != float64(1) || entry["content"] != "woken" {
			t.Fatalf("entry = %v, want 唤醒后重读的最新条目", entry)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("挂起未在期限内被唤醒")
	}
}

// 验收：客户端断连后 handler 及时退出，服务端不悬挂。
func TestWatchClientDisconnectReleasesHandler(t *testing.T) {
	st := mustStore(t)
	hub := watch.NewHub()
	ts := httptest.NewServer(New(nil, st, hub, "tok", func() bool { return true }, nil).Routes())

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/namespaces/dev/configs/a/watch?v=0&timeout=60s", nil)
	req = req.WithContext(ctx)
	go http.DefaultClient.Do(req) // 取消即断连，响应无从读取
	time.Sleep(200 * time.Millisecond)
	cancel()

	closed := make(chan struct{})
	go func() {
		ts.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("断连后 Server.Close 未及时完成：挂起 handler 悬挂")
	}
}

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store/pg"
	"github.com/jninng/go_config_center/internal/testutil"
	"github.com/jninng/go_config_center/internal/watch"
)

func mustStore(t *testing.T) *pg.Store {
	t.Helper()
	dsn := testutil.NewDB(t, "gcc_api_test")
	st, err := pg.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pg.Open: %v", err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func mustServer(t *testing.T, ready func() bool, snap func() (uint64, error)) (*Server, *pg.Store, func()) {
	t.Helper()
	st := mustStore(t)
	return New(Deps{Proposer: nil, Store: st, Hub: nil, AdminToken: "tok", Ready: ready, Snapshot: snap}), st, func() {}
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

	// 触发返回 ErrNoLeader → 503 + Retry-After。
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
		ts := httptest.NewServer(New(Deps{Proposer: p, Store: st, Hub: watch.NewHub(), AdminToken: "tok", Ready: func() bool { return true }, Snapshot: nil}).Routes())
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

// apply_failed 语义（DESIGN §4.1）：replayed 携带 lineage 错位说明与日志 index，
// 其余失败透传 FSM 给出的原因，不得出现空 message。
func TestApplyFailedMappingCarriesReason(t *testing.T) {
	st := mustStore(t)

	do := func(t *testing.T, p Proposer) map[string]any {
		t.Helper()
		ts := httptest.NewServer(New(Deps{Proposer: p, Store: st, Hub: watch.NewHub(), AdminToken: "tok", Ready: func() bool { return true }}).Routes())
		defer ts.Close()
		sub, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(sub, http.MethodPut, ts.URL+"/v1/namespaces/dev/configs/a",
			strings.NewReader(`{"content":"v","data_type":"TEXT","expected_version":1}`))
		req.Header.Set("Authorization", "Bearer tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("apply 失败 = %d, want 500", resp.StatusCode)
		}
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return body
	}

	body := do(t, fakeProposer{res: raftlog.Result{Status: raftlog.StatusReplayed, Index: 23}})
	if body["error"] != "apply_failed" {
		t.Fatalf("body = %v, want error=apply_failed", body)
	}
	msg, _ := body["message"].(string)
	if !strings.Contains(msg, "23") || !strings.Contains(msg, "lineage") {
		t.Fatalf("replayed message = %q, want 含日志 index 与 lineage 指引", msg)
	}

	body = do(t, fakeProposer{res: raftlog.Result{Status: raftlog.StatusError, Err: "载荷非法"}})
	if body["message"] != "载荷非法" {
		t.Fatalf("error message = %v, want 透传 FSM 原因", body["message"])
	}
}

// versions 端点（DESIGN §5.1）：倒序、limit/before_version 分页、Tombstone content 为 null。
func TestVersionsEndpoint(t *testing.T) {
	st := mustStore(t)
	ctx := context.Background()
	withTags := raftlog.PublishData{Namespace: t.Name(), DataId: "a", DataType: "TEXT", Content: "v1", Tags: map[string]string{"team": "core"}}
	if _, err := st.ApplyPublish(ctx, withTags, time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyPublish(ctx, raftlog.PublishData{Namespace: t.Name(), DataId: "a", DataType: "TEXT", Content: "v2"}, time.Now(), 2, 1); err != nil {
		t.Fatal(err)
	}
	if res, err := st.ApplyDelete(ctx, raftlog.DeleteData{Namespace: t.Name(), DataId: "a", Actor: "admin"}, time.Now(), 3, 1); err != nil || res.Status != raftlog.StatusOK {
		t.Fatalf("delete = %+v %v", res, err)
	}

	ts := httptest.NewServer(New(Deps{Proposer: nil, Store: st, Hub: nil, AdminToken: "tok", Ready: func() bool { return true }, Snapshot: nil}).Routes())
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
		ts := httptest.NewServer(New(Deps{Proposer: p, Store: st, Hub: watch.NewHub(), AdminToken: "tok", Ready: func() bool { return true }, Snapshot: nil}).Routes())
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
	ts := httptest.NewServer(New(Deps{Store: st, Hub: hub, AdminToken: "tok", Ready: func() bool { return true }}).Routes())
	defer ts.Close()
	ctx := context.Background()
	base := ts.URL + "/v1/namespaces/dev/configs/a/watch"

	for i := 1; i <= 2; i++ {
		if _, err := st.ApplyPublish(ctx, raftlog.PublishData{Namespace: "dev", DataId: "a", DataType: "TEXT", Content: fmt.Sprintf("v%d", i)}, time.Now(), uint64(i), 1); err != nil {
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
	if res, err := st.ApplyDelete(ctx, raftlog.DeleteData{Namespace: "dev", DataId: "a"}, time.Now(), 3, 1); err != nil || res.Status != raftlog.StatusOK {
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
	ts := httptest.NewServer(New(Deps{Store: st, Hub: hub, AdminToken: "tok", Ready: func() bool { return true }}).Routes())
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
	if _, err := st.ApplyPublish(ctx, raftlog.PublishData{Namespace: "dev", DataId: "a", DataType: "TEXT", Content: "woken"}, time.Now(), 1, 1); err != nil {
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
	ts := httptest.NewServer(New(Deps{Store: st, Hub: hub, AdminToken: "tok", Ready: func() bool { return true }}).Routes())

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

// 验收：无 token / 错 token 的写请求 401，正确 token 通过。
func TestWriteEndpointsRequireAdminToken(t *testing.T) {
	st := mustStore(t)
	ts := httptest.NewServer(New(Deps{Proposer: fakeProposer{res: raftlog.OK(1, true)}, Store: st, Hub: watch.NewHub(), AdminToken: "tok", Ready: func() bool { return true }, Snapshot: nil}).Routes())
	defer ts.Close()

	cases := []struct {
		method, path, body string
	}{
		{http.MethodPut, "/v1/namespaces/dev/configs/a", `{"content":"x","data_type":"TEXT"}`},
		{http.MethodDelete, "/v1/namespaces/dev/configs/a", ""},
		{http.MethodPost, "/v1/namespaces/dev/configs/a/rollback", `{"to_version":1}`},
		{http.MethodPost, "/v1/cluster/snapshot", ""},
	}
	for _, c := range cases {
		for _, auth := range []string{"", "Bearer wrong"} {
			req, _ := http.NewRequest(c.method, ts.URL+c.path, strings.NewReader(c.body))
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("%s %s auth=%q = %d, want 401", c.method, c.path, auth, resp.StatusCode)
			}
		}
	}

	// 正确 token 通过认证进入业务路径（fakeProposer 返回 OK → 200）。
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/namespaces/dev/configs/a", strings.NewReader(`{"content":"x","data_type":"TEXT"}`))
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("正确 token PUT = %d, want 200", resp.StatusCode)
	}
}

// 验收：/healthz 在 PG 断连时转为不健康。
func TestHealthzReflectsPGConnectivity(t *testing.T) {
	st := mustStore(t)
	ts := httptest.NewServer(New(Deps{Proposer: nil, Store: st, Hub: watch.NewHub(), AdminToken: "tok", Ready: func() bool { return true }, Snapshot: nil}).Routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PG 正常时 healthz = %d, want 200", resp.StatusCode)
	}

	st.Close() // 模拟 PG 断连（连接池关闭，Ping 失败）
	resp, err = http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("PG 断连后 healthz = %d, want 503", resp.StatusCode)
	}
}

// 验收：/readyz 反映 Serving 门闩（全量恢复期间 not ready，恢复后 ready）。
func TestReadyzReflectsServingLatch(t *testing.T) {
	ready := &atomic.Bool{}
	srv, _, cleanup := mustServer(t, func() bool { return ready.Load() }, nil)
	defer cleanup()
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	ready.Store(false)
	resp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("门闩关闭时 readyz = %d, want 503 + Retry-After", resp.StatusCode)
	}

	ready.Store(true)
	resp, err = http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("门闩打开后 readyz = %d, want 200", resp.StatusCode)
	}
}

// 写门闩（DESIGN §4.1）：Follower 已知 Leader → 307（保留路径与查询串）；
// Leader 地址未知或 candidate → 503 + Retry-After；Leader 放行。
func TestWriteGateRedirects(t *testing.T) {
	st := mustStore(t)
	mk := func(info ClusterInfo) *httptest.Server {
		return httptest.NewServer(New(Deps{
			Proposer:    fakeProposer{res: raftlog.OK(7, true)},
			Store:       st,
			AdminToken:  "tok",
			Ready:       func() bool { return true },
			ClusterInfo: func() ClusterInfo { return info },
		}).Routes())
	}

	ts := mk(ClusterInfo{Role: "follower", LeaderHTTP: "http://leader:8080"})
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/namespaces/dev/configs/a?expected_version=3", nil)
	req.Header.Set("Authorization", "Bearer tok")
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noRedirect.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("follower DELETE = %d, want 307", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "http://leader:8080/v1/namespaces/dev/configs/a?expected_version=3" {
		t.Fatalf("Location = %q, want 保留路径与查询串", loc)
	}
	ts.Close()

	for name, info := range map[string]ClusterInfo{
		"follower 无 Leader 地址": {Role: "follower"},
		"candidate":            {Role: "candidate"},
	} {
		ts = mk(info)
		req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/namespaces/dev/configs/a", strings.NewReader(`{"content":"x","data_type":"TEXT"}`))
		req.Header.Set("Authorization", "Bearer tok")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
			t.Fatalf("%s PUT = %d, want 503 + Retry-After", name, resp.StatusCode)
		}
		ts.Close()
	}

	ts = mk(ClusterInfo{Role: "leader"})
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/namespaces/dev/configs/a", strings.NewReader(`{"content":"x","data_type":"TEXT"}`))
	req.Header.Set("Authorization", "Bearer tok")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("leader PUT = %d, want 200（放行进入提案）", resp.StatusCode)
	}
	ts.Close()
}

// /internal/discovery（DESIGN §5.3）：不认证，返回 node_id/role/leader_addr/leader_http_addr。
func TestDiscoveryEndpoint(t *testing.T) {
	st := mustStore(t)
	ts := httptest.NewServer(New(Deps{
		Store: st,
		Ready: func() bool { return true },
		ClusterInfo: func() ClusterInfo {
			return ClusterInfo{NodeID: "n1", Role: "leader", LeaderAddr: "127.0.0.1:7946", LeaderHTTP: "http://127.0.0.1:8080"}
		},
	}).Routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/internal/discovery")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("discovery = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["node_id"] != "n1" || body["role"] != "leader" ||
		body["leader_addr"] != "127.0.0.1:7946" || body["leader_http_addr"] != "http://127.0.0.1:8080" {
		t.Fatalf("body = %v", body)
	}
}

// join 端点：Leader 上执行 JoinFunc，幂等（exists → 200）；参数缺失 400；Follower 307。
func TestJoinEndpoint(t *testing.T) {
	st := mustStore(t)
	var calls []string
	mk := func(info ClusterInfo) *httptest.Server {
		return httptest.NewServer(New(Deps{
			Store: st, AdminToken: "tok",
			Ready:       func() bool { return true },
			ClusterInfo: func() ClusterInfo { return info },
			Join: func(nodeID, raftAddr, httpAddr string) (bool, error) {
				calls = append(calls, nodeID+"@"+raftAddr)
				return len(calls) > 1, nil // 第二次视为已存在
			},
		}).Routes())
	}

	ts := mk(ClusterInfo{NodeID: "n1", Role: "leader", LeaderHTTP: "http://n1:8080"})
	do := func(t *testing.T, body string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/cluster/join", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	resp := do(t, `{"node_id":"n2","raft_addr":"127.0.0.1:7947","http_addr":"http://127.0.0.1:8081"}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("首次 join = %d, want 202", resp.StatusCode)
	}
	resp = do(t, `{"node_id":"n2","raft_addr":"127.0.0.1:7947","http_addr":"http://127.0.0.1:8081"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("重复 join = %d, want 200（幂等）", resp.StatusCode)
	}
	resp = do(t, `{"raft_addr":"127.0.0.1:7947"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("缺 node_id = %d, want 400", resp.StatusCode)
	}
	ts.Close()

	// Follower 收到 Join → 307 指向 Leader。
	followerTS := mk(ClusterInfo{Role: "follower", LeaderHTTP: "http://n1:8080"})
	req, _ := http.NewRequest(http.MethodPost, followerTS.URL+"/v1/cluster/join", strings.NewReader(`{"node_id":"n2","raft_addr":"127.0.0.1:7947"}`))
	req.Header.Set("Authorization", "Bearer tok")
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noRedirect.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("follower join = %d, want 307", resp.StatusCode)
	}
	followerTS.Close()
}

// cluster/status（DESIGN §5.2）：返回本节点视角与成员表。
func TestStatusEndpoint(t *testing.T) {
	st := mustStore(t)
	ts := httptest.NewServer(New(Deps{
		Store: st, AdminToken: "tok",
		Ready: func() bool { return true },
		ClusterInfo: func() ClusterInfo {
			return ClusterInfo{NodeID: "n1", Role: "leader", Term: 5, LeaderID: "n1",
				LeaderAddr: "127.0.0.1:7946", LeaderHTTP: "http://127.0.0.1:8080",
				LastApplied: 42, Restoring: false}
		},
		Members: func() ([]MemberInfo, error) {
			return []MemberInfo{
				{NodeID: "n1", Addr: "127.0.0.1:7946", Suffrage: "Voter"},
				{NodeID: "n2", Addr: "127.0.0.1:7947", Suffrage: "Staging"},
			}, nil
		},
	}).Routes())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/cluster/status", nil)
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		NodeID      string `json:"node_id"`
		Role        string `json:"role"`
		Term        uint64 `json:"term"`
		LastApplied uint64 `json:"last_applied_index"`
		Restoring   bool   `json:"restoring"`
		MemberCount int    `json:"-"`
		Members     []struct {
			NodeID   string `json:"node_id"`
			Addr     string `json:"addr"`
			Suffrage string `json:"suffrage"`
		} `json:"members"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.NodeID != "n1" || body.Role != "leader" || body.Term != 5 ||
		body.LastApplied != 42 || body.Restoring || len(body.Members) != 2 ||
		body.Members[1].Suffrage != "Staging" {
		t.Fatalf("status body = %+v", body)
	}
}

// 移除端点（DESIGN §5.2）：200 / 404 not_member / 409 quorum_guard；非 Leader 307。
func TestRemovePeerEndpoint(t *testing.T) {
	st := mustStore(t)
	mk := func(info ClusterInfo, remove RemoveFunc) *httptest.Server {
		return httptest.NewServer(New(Deps{
			Store: st, AdminToken: "tok",
			Ready:       func() bool { return true },
			ClusterInfo: func() ClusterInfo { return info },
			Remove:      remove,
		}).Routes())
	}

	ts := mk(ClusterInfo{NodeID: "n1", Role: "leader", LeaderHTTP: "http://n1:8080"},
		func(nodeID string) error {
			switch nodeID {
			case "n3":
				return nil
			case "ghost":
				return ErrNotMember
			default:
				return ErrQuorumGuard
			}
		})
	do := func(t *testing.T, url string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodDelete, url, nil)
		req.Header.Set("Authorization", "Bearer tok")
		noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}
		resp, err := noRedirect.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	resp := do(t, ts.URL+"/v1/cluster/peers/n3")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("移除成员 = %d, want 200", resp.StatusCode)
	}
	resp = do(t, ts.URL+"/v1/cluster/peers/ghost")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("移除非成员 = %d, want 404", resp.StatusCode)
	}
	resp = do(t, ts.URL+"/v1/cluster/peers/n2")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("护栏拒绝 = %d, want 409", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["error"] != "quorum_guard" {
		t.Fatalf("body = %v, want error=quorum_guard", body)
	}
	ts.Close()

	followerTS := mk(ClusterInfo{Role: "follower", LeaderHTTP: "http://n1:8080"}, nil)
	resp = do(t, followerTS.URL+"/v1/cluster/peers/n3")
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("follower 移除 = %d, want 307", resp.StatusCode)
	}
	followerTS.Close()
}

// 条目列表与 Namespace 清单端点（DESIGN §5.1）：仅存活条目、keyset 分页、
// 响应不含 content、next_after_id 到末尾省略；Serving 门闩关闭时 503。
func TestListConfigsAndNamespacesEndpoints(t *testing.T) {
	st := mustStore(t)
	ctx := context.Background()
	for _, id := range []string{"b", "a", "c"} {
		if _, err := st.ApplyPublish(ctx, raftlog.PublishData{Namespace: "dev", DataId: id, DataType: "TEXT", Content: "body-of-" + id}, time.Now(), 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.ApplyPublish(ctx, raftlog.PublishData{Namespace: "prod", DataId: "a", DataType: "TEXT", Content: "x"}, time.Now(), 0, 0); err != nil {
		t.Fatal(err)
	}
	if res, err := st.ApplyDelete(ctx, raftlog.DeleteData{Namespace: "dev", DataId: "b"}, time.Now(), 0, 0); err != nil || res.Status != raftlog.StatusOK {
		t.Fatalf("delete = %+v %v", res, err)
	}

	ready := &atomic.Bool{}
	ts := httptest.NewServer(New(Deps{Store: st, AdminToken: "tok", Ready: func() bool { return ready.Load() }}).Routes())
	defer ts.Close()
	ready.Store(true)

	resp, err := http.Get(ts.URL + "/v1/namespaces")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var nsBody struct {
		Namespaces []string `json:"namespaces"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&nsBody); err != nil {
		t.Fatal(err)
	}
	if len(nsBody.Namespaces) != 2 || nsBody.Namespaces[0] != "dev" || nsBody.Namespaces[1] != "prod" {
		t.Fatalf("namespaces = %v, want [dev prod]", nsBody.Namespaces)
	}

	getList := func(t *testing.T, query string) (map[string]any, []map[string]any) {
		t.Helper()
		resp, err := http.Get(ts.URL + "/v1/namespaces/dev/configs" + query)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET configs%s = %d, want 200", query, resp.StatusCode)
		}
		var body struct {
			Entries     []map[string]any `json:"entries"`
			NextAfterID string           `json:"next_after_id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return map[string]any{"next": body.NextAfterID}, body.Entries
	}

	_, entries := getList(t, "?limit=2")
	if len(entries) != 2 || entries[0]["data_id"] != "a" || entries[1]["data_id"] != "c" {
		t.Fatalf("entries = %v, want [a c]（b 已删除不出现）", entries)
	}
	if _, ok := entries[0]["content"]; ok {
		t.Fatalf("列表元数据不应含 content 字段: %v", entries[0])
	}
	if entries[0]["version"] != float64(1) || entries[0]["data_type"] != "TEXT" {
		t.Fatalf("元数据 = %v", entries[0])
	}

	meta, _ := getList(t, "?limit=1")
	if meta["next"] != "a" {
		t.Fatalf("next_after_id = %v, want a", meta["next"])
	}
	meta, entries = getList(t, "?limit=1&after_id=a")
	if len(entries) != 1 || entries[0]["data_id"] != "c" {
		t.Fatalf("after_id=a = %v, want [c]", entries)
	}
	if meta["next"] != "c" {
		t.Fatalf("next_after_id = %v, want c", meta["next"])
	}

	// limit 参数非法 → 400；门闩关闭 → 503。
	resp, err = http.Get(ts.URL + "/v1/namespaces/dev/configs?limit=abc")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("limit=abc = %d, want 400", resp.StatusCode)
	}
	ready.Store(false)
	resp, err = http.Get(ts.URL + "/v1/namespaces/dev/configs")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("门闩关闭 = %d, want 503", resp.StatusCode)
	}
}

// CORS（ADR-0012）：全部响应带许可头，OPTIONS 预检在路由与认证之前 204 短路。
func TestCORSHeaders(t *testing.T) {
	st := mustStore(t)
	ts := httptest.NewServer(New(Deps{Store: st, AdminToken: "tok", Ready: func() bool { return true }}).Routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("GET /healthz 缺 Access-Control-Allow-Origin: %v", resp.Header)
	}

	req, _ := http.NewRequest(http.MethodOptions, ts.URL+"/v1/namespaces/dev/configs", nil)
	req.Header.Set("Origin", "http://127.0.0.1:8080")
	req.Header.Set("Access-Control-Request-Method", "PUT")
	req.Header.Set("Access-Control-Request-Headers", "authorization, content-type")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("OPTIONS 预检 = %d, want 204", resp.StatusCode)
	}
	if resp.Header.Get("Access-Control-Allow-Headers") == "" || resp.Header.Get("Access-Control-Allow-Methods") == "" {
		t.Fatalf("预检响应缺许可头: %v", resp.Header)
	}
}

// 控制台挂载（ADR-0012）：GET / 由 Console handler 兜底，API 精确路径不受遮蔽。
func TestConsoleMountDoesNotShadowAPI(t *testing.T) {
	st := mustStore(t)
	console := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body>console-stub</body></html>"))
	})
	ts := httptest.NewServer(New(Deps{Store: st, AdminToken: "tok", Ready: func() bool { return true }, Console: console}).Routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), "console-stub") {
		t.Fatalf("GET / = %d %q, want 控制台兜底", resp.StatusCode, string(b))
	}

	resp, err = http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("GET /healthz = %d, want 200 JSON（不被控制台遮蔽）", resp.StatusCode)
	}

	resp, err = http.Get(ts.URL + "/v1/namespaces")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/namespaces = %d, want 200", resp.StatusCode)
	}

	// 未注册路径被 GET / 通配覆盖后，POST 命中该模式但方法不符 → 405（ServeMux 语义，
	// 挂载控制台前为 404）；已注册写路径不受影响。
	resp, err = http.Post(ts.URL+"/not-registered", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /not-registered = %d, want 405", resp.StatusCode)
	}
}

// 删除 Namespace 端点（DESIGN §5.1）：认证、成功 200 entries、Apply 失败 500、Follower 307。
func TestDeleteNamespaceEndpoint(t *testing.T) {
	st := mustStore(t)
	do := func(t *testing.T, p Proposer, info ClusterInfo, auth, url string) *http.Response {
		t.Helper()
		ts := httptest.NewServer(New(Deps{Proposer: p, Store: st, AdminToken: "tok",
			Ready: func() bool { return true }, ClusterInfo: func() ClusterInfo { return info }}).Routes())
		defer ts.Close()
		req, _ := http.NewRequest(http.MethodDelete, ts.URL+url, nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}
		resp, err := noRedirect.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	leader := ClusterInfo{NodeID: "n1", Role: "leader", LeaderHTTP: "http://n1:8080"}

	resp := do(t, fakeProposer{}, leader, "", "/v1/namespaces/dev")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("无认证 = %d, want 401", resp.StatusCode)
	}

	resp = do(t, fakeProposer{res: raftlog.Result{Status: raftlog.StatusOK, Entries: 3}}, leader, "Bearer tok", "/v1/namespaces/dev")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("删除 = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Status  string `json:"status"`
		Entries int64  `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "deleted" || body.Entries != 3 {
		t.Fatalf("body = %+v, want deleted entries=3", body)
	}

	resp = do(t, fakeProposer{res: raftlog.Result{Status: raftlog.StatusOK, Entries: 0}}, leader, "Bearer tok", "/v1/namespaces/empty")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("删空 ns = %d, want 200", resp.StatusCode)
	}
	body = struct {
		Status  string `json:"status"`
		Entries int64  `json:"entries"`
	}{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Entries != 0 {
		t.Fatalf("删空 ns entries = %d, want 0（幂等）", body.Entries)
	}

	resp = do(t, fakeProposer{res: raftlog.Result{Status: raftlog.StatusError, Err: "boom"}}, leader, "Bearer tok", "/v1/namespaces/dev")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("apply 失败 = %d, want 500", resp.StatusCode)
	}

	resp = do(t, fakeProposer{}, ClusterInfo{Role: "follower", LeaderHTTP: "http://n1:8080"}, "Bearer tok", "/v1/namespaces/dev")
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("follower 删除 = %d, want 307", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "http://n1:8080/v1/namespaces/dev" {
		t.Fatalf("Location = %q, want 指向 Leader 且保留路径", loc)
	}
}

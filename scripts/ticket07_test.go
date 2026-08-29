//go:build e2e

// 票 07 验收：Admin Token + healthz + metrics
// 覆盖 .scratch/config-center-mvp/issues/07-auth-health-metrics.md 全部验收项。
package e2e

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTicket07(t *testing.T) {
	begin(t)
	cfg := singleNodeConfig(t)
	const port = 8080
	const ns, id = "dev", "db.password"
	entryPath := fmt.Sprintf("/v1/namespaces/%s/configs/%s", ns, id)
	// wrong 用任意错 token 发请求（断言 401）。
	wrong := func(method, path string, body any) httpResult {
		return reqBearer(method, httpBase(port)+path, body, 5*time.Second, "Bearer wrong")
	}

	section("构建与准备")
	os.RemoveAll("data")
	dropTables(t, pgDB)

	n := startNode(t, cfg, true, ticketLog(t, "07-n1.log"))
	if !waitHealth(port, 6*time.Second) {
		t.Fatal("节点未就绪")
	}
	check(t, healthOK(port), "服务就绪")

	section("验收 1：无 token / 错 token 401，正确 token 通过")
	r := req(clientDirect, "PUT", port, entryPath, map[string]any{"content": "x", "data_type": "TEXT"}, 5*time.Second, false)
	check(t, r.Code == 401, "无 token PUT → 401（实际 %d）", r.Code)
	r = reqBearer("PUT", httpBase(port)+entryPath, map[string]any{"content": "x", "data_type": "TEXT"}, 5*time.Second, "Bearer wrong-token")
	check(t, r.Code == 401, "错 token PUT → 401（实际 %d）", r.Code)
	r = putSettled(port, entryPath, map[string]any{"content": "v1", "data_type": "TEXT"})
	check(t, r.Code == 200, "正确 token PUT → 200（%d）", r.Code)
	r = wrong("POST", entryPath+"/rollback", map[string]any{"to_version": 1})
	check(t, r.Code == 401, "错 token rollback → 401（实际 %d）", r.Code)
	r = wrong("POST", "/v1/cluster/snapshot", nil)
	check(t, r.Code == 401, "错 token snapshot → 401（实际 %d）", r.Code)
	// 读接口不认证
	r = req(clientDirect, "GET", port, entryPath, nil, 5*time.Second, false)
	check(t, r.Code == 200, "读接口无 token → 200（实际 %d）", r.Code)

	section("验收 4 准备：制造四类 Apply 结果")
	putJSON(port, entryPath, map[string]any{"content": "x", "data_type": "TEXT", "expected_version": 99}) // version_conflict
	req(clientDirect, "DELETE", port, fmt.Sprintf("/v1/namespaces/%s/configs/never.exists", ns), nil, 5*time.Second, true)
	req(clientDirect, "POST", port, entryPath+"/rollback", map[string]any{"to_version": 1}, 5*time.Second, true) // invalid_rollback（= 当前版本）

	section("验收 2：/healthz 反映 PG 连通（当前健康）")
	body := string(req(clientDirect, "GET", port, "/healthz", nil, 5*time.Second, false).Body)
	check(t, strings.Contains(body, `"status":"ok"`), "healthz → ok（%s）", trunc(body))

	section("验收 3 + 2：/readyz ready")
	body = string(req(clientDirect, "GET", port, "/readyz", nil, 5*time.Second, false).Body)
	check(t, strings.Contains(body, `"status":"ready"`), "readyz → ready（%s）", trunc(body))

	section("验收 4：/metrics 含 §7 最低集合，fsm_apply_results_total 按 result 区分")
	metrics := string(req(clientDirect, "GET", port, "/metrics", nil, 5*time.Second, false).Body)
	for _, name := range []string{
		"raft_role", "raft_term", "raft_commit_index",
		"fsm_last_applied_index", "fsm_apply_duration_seconds",
		"watch_waiters", "watch_notifications_total",
		"snapshot_duration_seconds", "snapshot_size_bytes", "restore_skipped_total",
		"http_request_duration_seconds",
	} {
		check(t, strings.Contains(metrics, "# HELP "+name+" "), "指标存在: %s", name)
	}
	for _, result := range []string{"ok", "version_conflict", "not_found", "invalid_rollback"} {
		check(t, strings.Contains(metrics, fmt.Sprintf("fsm_apply_results_total{result=%q}", result)),
			"fsm_apply_results_total{result=%q} 存在", result)
	}
	check(t, strings.Contains(metrics, `http_request_duration_seconds_count{route="PUT /v1/namespaces/{ns}/configs/{data_id}"}`),
		"http_request_duration_seconds 带 route 标签")

	n.kill()
	finish(t)
}

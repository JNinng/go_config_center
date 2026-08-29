//go:build e2e

// 票 13 验收：控制台（ADR-0012）与条目列表端点。
// 控制台构建产物不入库——本票断言对占位页与已构建两种二进制均须成立：
// 根路径可访问、SPA fallback、API 不被遮蔽、CORS 头、Namespace 清单与
// 条目列表（仅存活条目、keyset 分页、不含 content）。
package e2e

import (
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTicket13(t *testing.T) {
	begin(t)
	cfg := singleNodeConfig(t)
	const port = 8080
	const ns = "console-demo"
	log1 := ticketLog(t, "13-n1.log")

	section("准备：清空 data 目录与 PG 表")
	os.RemoveAll("data")
	dropTables(t, pgDB)

	section("准备：--bootstrap 拉起单节点")
	n := startNode(t, cfg, true, log1)
	if !waitHealth(port, 6*time.Second) {
		t.Fatal("节点未在 6s 内就绪")
	}
	t.Cleanup(func() { check(t, n.alive(), "验收全程节点进程存活") })

	section("验收 1：根路径服务控制台 HTML（构建产物或占位页）")
	r := req(clientDirect, "GET", port, "/", nil, 5*time.Second, false)
	check(t, r.Code == 200, "GET / = 200（实际 %d）", r.Code)
	check(t, strings.HasPrefix(r.Header.Get("Content-Type"), "text/html"), "Content-Type 为 text/html（实际 %q）", r.Header.Get("Content-Type"))
	check(t, strings.Contains(string(r.Body), `id="app"`), "响应为控制台入口 HTML")

	section("验收 2：SPA fallback——未知路径回退入口页而非 404")
	r = req(clientDirect, "GET", port, "/console/whatever/deep/route", nil, 5*time.Second, false)
	check(t, r.Code == 200, "未知路径 = 200（实际 %d）", r.Code)
	check(t, strings.Contains(string(r.Body), `id="app"`), "未知路径回退控制台入口 HTML")

	section("验收 3：API 不被根路径兜底遮蔽")
	r = req(clientDirect, "GET", port, "/healthz", nil, 5*time.Second, false)
	check(t, r.Code == 200, "/healthz = 200（实际 %d）", r.Code)
	check(t, strings.Contains(string(r.Body), `"ok"`), "/healthz 仍为 JSON")
	r = req(clientDirect, "GET", port, "/v1/namespaces", nil, 5*time.Second, false)
	check(t, r.Code == 200 && strings.Contains(string(r.Body), `"namespaces"`), "/v1/namespaces 可用（%d %s）", r.Code, trunc(string(r.Body)))
	r = req(clientDirect, "POST", port, "/not-registered", map[string]any{}, 5*time.Second, false)
	check(t, r.Code == http.StatusMethodNotAllowed, "未注册路径 POST = 405（ServeMux 语义，实际 %d）", r.Code)

	section("验收 4：CORS 头与 OPTIONS 预检")
	r = req(clientDirect, "GET", port, "/healthz", nil, 5*time.Second, false)
	check(t, r.Header.Get("Access-Control-Allow-Origin") == "*", "响应带 Access-Control-Allow-Origin: *")
	r = tryHTTP(clientDirect, "OPTIONS", httpBase(port)+"/v1/namespaces/dev/configs", nil, 5*time.Second, false)
	check(t, r.Code == http.StatusNoContent, "OPTIONS 预检 = 204（实际 %d）", r.Code)
	check(t, r.Header.Get("Access-Control-Allow-Headers") != "", "预检响应带 Access-Control-Allow-Headers")

	section("验收 5：Namespace 清单与条目列表（仅存活、keyset 分页、不含 content）")
	for _, id := range []string{"c", "a", "b"} {
		r := putSettled(port, "/v1/namespaces/"+ns+"/configs/"+id,
			map[string]any{"content": "body-of-" + id, "data_type": "TEXT"})
		check(t, r.Code == 200, "PUT %s = 200（实际 %d）", id, r.Code)
	}
	r = putJSON(port, "/v1/namespaces/other-ns/configs/x", map[string]any{"content": "x", "data_type": "TEXT"})
	check(t, r.Code == 200, "另一 Namespace 写入 = 200（实际 %d）", r.Code)
	r = req(clientDirect, "DELETE", port, "/v1/namespaces/"+ns+"/configs/b?expected_version=1", nil, 5*time.Second, true)
	check(t, r.Code == 200, "删除 b = 200（实际 %d）", r.Code)

	r = req(clientDirect, "GET", port, "/v1/namespaces", nil, 5*time.Second, false)
	nsList := decode[struct {
		Namespaces []string `json:"namespaces"`
	}](t, r)
	check(t, len(nsList.Namespaces) == 2 && nsList.Namespaces[0] == "console-demo" && nsList.Namespaces[1] == "other-ns",
		"namespaces = %v, want 去重字典序 [console-demo other-ns]", nsList.Namespaces)

	r = req(clientDirect, "GET", port, "/v1/namespaces/"+ns+"/configs?limit=2", nil, 5*time.Second, false)
	type listResp struct {
		Entries []struct {
			DataID string `json:"data_id"`
			Body   string `json:"content"`
		} `json:"entries"`
		NextAfterID string `json:"next_after_id"`
	}
	page1 := decode[listResp](t, r)
	check(t, len(page1.Entries) == 2 && page1.Entries[0].DataID == "a" && page1.Entries[1].DataID == "c",
		"第一页 = [a c]（b 已删除不出现，实际 %s）", trunc(string(r.Body)))
	check(t, page1.Entries[0].Body == "", "列表元数据不含 content")
	check(t, page1.NextAfterID == "c", "next_after_id = c（实际 %q）", page1.NextAfterID)

	r = req(clientDirect, "GET", port, "/v1/namespaces/"+ns+"/configs?limit=2&after_id=c", nil, 5*time.Second, false)
	page2 := decode[listResp](t, r)
	check(t, len(page2.Entries) == 0 && page2.NextAfterID == "", "末页后为空且无游标（实际 %s）", trunc(string(r.Body)))

	r = req(clientDirect, "GET", port, "/v1/namespaces/"+ns+"/configs?limit=abc", nil, 5*time.Second, false)
	check(t, r.Code == 400, "limit=abc = 400（实际 %d）", r.Code)

	finish(t)
}

//go:build e2e

// 票 14 验收：Namespace 管理（DESIGN §5.1 扩展）。
// "新增"：Namespace 隐式创建（首次 Publish 生效），控制台引导创建首条即出现；
// "删除"：DELETE /v1/namespaces/{ns} 原子删除该 ns 下全部存活条目并各写 Tombstone
// （History 保留、可逐条回滚），幂等（空 ns 200 entries:0）。
package e2e

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestTicket14(t *testing.T) {
	begin(t)
	cfg := singleNodeConfig(t)
	const port = 8080
	const ns = "ns-mgmt"
	log1 := ticketLog(t, "14-n1.log")

	section("准备：清空 data 目录与 PG 表")
	os.RemoveAll("data")
	dropTables(t, pgDB)

	section("准备：--bootstrap 拉起单节点")
	n := startNode(t, cfg, true, log1)
	if !waitHealth(port, 6*time.Second) {
		t.Fatal("节点未在 6s 内就绪")
	}
	t.Cleanup(func() { check(t, n.alive(), "验收全程节点进程存活") })

	section("验收 1：Namespace 隐式创建——向新 Namespace 发布首条即出现于清单")
	put := func(id string) bool {
		r := putSettled(port, "/v1/namespaces/"+ns+"/configs/"+id,
			map[string]any{"content": "c-" + id, "data_type": "TEXT"})
		return r.Code == 200
	}
	check(t, put("a"), "向新 Namespace 发布首条 200")
	r := req(clientDirect, "GET", port, "/v1/namespaces", nil, 5*time.Second, false)
	nsList := decode[struct {
		Namespaces []string `json:"namespaces"`
	}](t, r)
	check(t, len(nsList.Namespaces) == 1 && nsList.Namespaces[0] == ns,
		"namespaces = %v, want 隐式创建 [%s]", nsList.Namespaces, ns)

	section("验收 2：删除 Namespace 原子删光全部条目并写 Tombstone")
	check(t, put("b"), "再发布 b 200")
	check(t, put("c"), "再发布 c 200")
	check(t, put("a"), "a 二次发布 200（版本递增）")

	r = req(clientDirect, "DELETE", port, "/v1/namespaces/"+ns, nil, 5*time.Second, true)
	dr := decode[struct {
		Status  string `json:"status"`
		Entries int64  `json:"entries"`
	}](t, r)
	check(t, r.Code == 200 && dr.Status == "deleted" && dr.Entries == 3,
		"删除 Namespace = 200 deleted entries=3（%s）", trunc(string(r.Body)))

	r = req(clientDirect, "GET", port, "/v1/namespaces", nil, 5*time.Second, false)
	nsList = decode[struct {
		Namespaces []string `json:"namespaces"`
	}](t, r)
	check(t, len(nsList.Namespaces) == 0, "删除后 namespaces 为空（无存活条目）")

	check(t, psqlQ(t, pgDB, "SELECT count(*) FROM biz_configs WHERE namespace='"+ns+"'") == "0",
		"biz_configs 已清空")
	check(t, psqlQ(t, pgDB, "SELECT count(*) FROM biz_config_history WHERE namespace='"+ns+"' AND op='delete'") == "3",
		"History 追加 3 条 delete Tombstone")
	check(t, psqlQ(t, pgDB,
		"SELECT count(*) FROM biz_config_history WHERE namespace='"+ns+"' AND op='delete' AND content IS NULL") == "3",
		"Tombstone content 为 NULL")
	check(t, psqlQ(t, pgDB,
		"SELECT count(*) FROM biz_config_history WHERE namespace='"+ns+"' AND data_id='a' AND op='delete' AND version=3") == "1",
		"墓碑版本 = 删前版本+1（a 删前 v2 → 墓碑 v3）")

	r = req(clientDirect, "GET", port, "/v1/namespaces/"+ns+"/configs/a", nil, 5*time.Second, false)
	check(t, r.Code == 404, "删除后 GET 单条 = 404（实际 %d）", r.Code)

	section("验收 3：幂等——再次删除（空 Namespace）200 entries:0")
	r = req(clientDirect, "DELETE", port, "/v1/namespaces/"+ns, nil, 5*time.Second, true)
	dr = decode[struct {
		Status  string `json:"status"`
		Entries int64  `json:"entries"`
	}](t, r)
	check(t, r.Code == 200 && dr.Status == "deleted" && dr.Entries == 0,
		"重复删除 = 200 entries:0（幂等，%s）", trunc(string(r.Body)))

	section("验收 4：认证——无 token 删除 401")
	r = req(clientDirect, "DELETE", port, "/v1/namespaces/"+ns, nil, 5*time.Second, false)
	check(t, r.Code == 401, "无 token 删除 = 401（实际 %d）", r.Code)

	section("验收 5：删除后可逐条回滚重建")
	r = req(clientDirect, "POST", port, "/v1/namespaces/"+ns+"/configs/a/rollback",
		map[string]any{"to_version": 2}, 5*time.Second, true)
	rb := decode[publishResp](t, r)
	check(t, r.Code == 200 && rb.Version == 4, "回滚到墓碑前 v2 = 200 v4（%s）", trunc(string(r.Body)))
	r = req(clientDirect, "GET", port, "/v1/namespaces/"+ns+"/configs/a", nil, 5*time.Second, false)
	check(t, r.Code == 200 && strings.Contains(string(r.Body), `"content":"c-a"`),
		"重建条目可读，内容回到删除前（%s）", trunc(string(r.Body)))

	finish(t)
}

//go:build e2e

// 票 04 验收：Rollback
// 覆盖 .scratch/config-center-mvp/issues/04-rollback.md 全部验收项。
package e2e

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestTicket04(t *testing.T) {
	begin(t)
	const port = 8080
	const ns, id = "dev", "db.password"
	entryPath := fmt.Sprintf("/v1/namespaces/%s/configs/%s", ns, id)
	rollbackPath := entryPath + "/rollback"
	consistentQ := fmt.Sprintf(
		"SELECT (SELECT version FROM biz_configs WHERE namespace='%s' AND data_id='%s') = (SELECT max(version) FROM biz_config_history WHERE namespace='%s' AND data_id='%s')",
		ns, id, ns, id)

	section("构建与准备")
	os.RemoveAll("data")
	dropTables(t, pgDB)

	n := startNode(t, "config.yaml", true, ticketLog(t, "04-n1.log"))
	if !waitHealth(port, 6*time.Second) {
		t.Fatal("节点未就绪")
	}
	check(t, healthOK(port), "服务就绪")

	section("准备数据：v1/v2/v3")
	r := putSettled(port, entryPath, map[string]any{"content": "content-v1", "data_type": "TEXT", "tags": map[string]string{"rev": "1"}})
	check(t, r.Code == 200, "v1 发布（%d）", r.Code)
	putJSON(port, entryPath, map[string]any{"content": "content-v2", "data_type": "TEXT"})
	putJSON(port, entryPath, map[string]any{"content": "content-v3", "data_type": "TEXT"})

	section("验收 1：rollback to v1 → v4，内容与 v1 相同，History 追加 publish")
	r = req(clientDirect, "POST", port, rollbackPath, map[string]any{"to_version": 1}, 5*time.Second, false)
	check(t, r.Code == 401, "无 token 回滚 → 401（实际 %d）", r.Code)
	r = req(clientDirect, "POST", port, rollbackPath, map[string]any{"to_version": 1}, 10*time.Second, true)
	pr := decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 4, "rollback to v1 → 200 v4（%d %s）", r.Code, trunc(string(r.Body)))
	e := decode[entryResp](t, req(clientDirect, "GET", port, entryPath, nil, 5*time.Second, false))
	check(t, e.Content == "content-v1" && e.Version == 4, "GET 内容回到 v1 且 version=4")
	op := psqlQ(t, pgDB, fmt.Sprintf("SELECT op FROM biz_config_history WHERE namespace='%s' AND data_id='%s' AND version=4", ns, id))
	check(t, op == "publish", "History v4 为 publish 记录（实际 %s）", op)

	section("验收 2：链式回滚")
	r = req(clientDirect, "POST", port, rollbackPath, map[string]any{"to_version": 2}, 10*time.Second, true)
	pr = decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 5, "回滚后的条目可继续回滚到 v2 → 200 v5（%d）", r.Code)
	e = decode[entryResp](t, req(clientDirect, "GET", port, entryPath, nil, 5*time.Second, false))
	check(t, e.Content == "content-v2", "GET 内容为 v2")

	section("验收 3：无效目标 404/409")
	r = req(clientDirect, "POST", port, rollbackPath, map[string]any{"to_version": 99}, 10*time.Second, true)
	check(t, r.Code == 404, "to_version=99 不存在 → 404（实际 %d）", r.Code)
	r = req(clientDirect, "POST", port, rollbackPath, map[string]any{"to_version": 5}, 10*time.Second, true)
	pr = decode[publishResp](t, r)
	check(t, r.Code == 409 && pr.Error == "invalid_rollback", "to_version=5（≥ 当前版本）→ 409 invalid_rollback（%d）", r.Code)
	r = req(clientDirect, "DELETE", port, entryPath, nil, 10*time.Second, true)
	pr = decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 6, "删除 → v6 墓碑（%d）", r.Code)
	r = req(clientDirect, "POST", port, rollbackPath, map[string]any{"to_version": 6}, 10*time.Second, true)
	check(t, r.Code == 409, "to_version=6 指向墓碑 → 409（实际 %d）", r.Code)
	r = req(clientDirect, "POST", port, rollbackPath, map[string]any{"to_version": 3}, 10*time.Second, true)
	pr = decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 7, "删除后回滚到墓碑之前的 v3 → 重建为 v7（%d）", r.Code)

	section("验收 4：并发 Publish 与 Rollback 竞态，终态与日志顺序一致（无 TOCTOU）")
	var wg sync.WaitGroup
	for i := 1; i <= 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req(clientDirect, "PUT", port, entryPath,
				map[string]any{"content": fmt.Sprintf("conc-%d", i), "data_type": "TEXT"}, 15*time.Second, true)
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		req(clientDirect, "POST", port, rollbackPath, map[string]any{"to_version": 1}, 15*time.Second, true)
	}()
	wg.Wait()
	time.Sleep(500 * time.Millisecond)
	check(t, psqlQ(t, pgDB, consistentQ) == "t", "竞态后 biz_configs.version == max(History.version)")
	nogap := psqlQ(t, pgDB, fmt.Sprintf("SELECT (SELECT max(version) FROM biz_config_history WHERE namespace='%s' AND data_id='%s') = (SELECT count(*) FROM biz_config_history WHERE namespace='%s' AND data_id='%s')", ns, id, ns, id))
	check(t, nogap == "t", "版本序列无空洞：max(version) == History 行数")
	finalContent := psqlQ(t, pgDB, fmt.Sprintf("SELECT h.content FROM biz_config_history h WHERE h.namespace='%s' AND h.data_id='%s' AND h.version = (SELECT version FROM biz_configs WHERE namespace='%s' AND data_id='%s')", ns, id, ns, id))
	curContent := psqlQ(t, pgDB, fmt.Sprintf("SELECT content FROM biz_configs WHERE namespace='%s' AND data_id='%s'", ns, id))
	check(t, finalContent == curContent, "当前内容与最终版本的 History 记录一致")

	n.kill()
	finish(t)
}

//go:build e2e

// 票 03 验收：CAS + Delete/Tombstone + History 查询
// 覆盖 .scratch/config-center-mvp/issues/03-cas-delete-tombstone-history.md 全部验收项。
package e2e

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// versionSeq 提取 versions 响应中的版本号序列（保持响应顺序）。
func versionSeq(t *testing.T, r httpResult) []int64 {
	t.Helper()
	out := []int64{}
	for _, rec := range decode[[]versionRecord](t, r) {
		out = append(out, rec.Version)
	}
	return out
}

func TestTicket03(t *testing.T) {
	begin(t)
	const port = 8080
	const ns, id = "dev", "db.password"
	path := func(dataID string) string { return fmt.Sprintf("/v1/namespaces/%s/configs/%s", ns, dataID) }
	consistentQ := fmt.Sprintf(
		"SELECT (SELECT version FROM biz_configs WHERE namespace='%s' AND data_id='%s') = (SELECT max(version) FROM biz_config_history WHERE namespace='%s' AND data_id='%s')",
		ns, id, ns, id)

	section("构建与准备：清空 data 目录与 PG 表")
	os.RemoveAll("data")
	dropTables(t, pgDB)

	section("启动单节点")
	n := startNode(t, "config.yaml", true, ticketLog(t, "03-n1.log"))
	if !waitHealth(port, 6*time.Second) {
		t.Fatal("节点未就绪")
	}
	check(t, healthOK(port), "服务就绪")

	section("验收 1：expected_version 省略 = LWW，版本序列无空洞")
	r := putSettled(port, path(id), map[string]any{"content": "v1", "data_type": "TEXT"})
	pr := decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 1, "首次 PUT → v1（%d %s）", r.Code, trunc(string(r.Body)))
	r = putJSON(port, path(id), map[string]any{"content": "v2", "data_type": "TEXT"})
	pr = decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 2, "第二次 PUT → v2（%d）", r.Code)
	r = putJSON(port, path(id), map[string]any{"content": "v3", "data_type": "TEXT"})
	pr = decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 3, "第三次 PUT → v3（%d）", r.Code)
	hist := psqlQ(t, pgDB, fmt.Sprintf("SELECT count(*) FROM biz_config_history WHERE namespace='%s' AND data_id='%s' AND op='publish'", ns, id))
	check(t, hist == "3", "History 3 条 publish，序列 1/2/3 无空洞（实际 %s）", hist)

	section("验收 2：expected_version=0 = create-if-absent")
	r = putJSON(port, path(id), map[string]any{"content": "x", "data_type": "TEXT", "expected_version": 0})
	pr = decode[publishResp](t, r)
	check(t, r.Code == 409 && pr.CurrentVersion == 3, "对存活条目 expected=0 → 409 + current_version=3（%d）", r.Code)
	r = putJSON(port, path("fresh.id"), map[string]any{"content": "new", "data_type": "TEXT", "expected_version": 0})
	pr = decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Created, "对不存在条目 expected=0 → 200 created（%d）", r.Code)

	section("验收 3：expected_version=N 匹配通过 / 不匹配 409")
	r = putJSON(port, path(id), map[string]any{"content": "nope", "data_type": "TEXT", "expected_version": 1})
	pr = decode[publishResp](t, r)
	check(t, r.Code == 409 && pr.CurrentVersion == 3, "expected=1 ≠ 当前 3 → 409 + current_version=3（%d）", r.Code)
	r = putJSON(port, path(id), map[string]any{"content": "v4", "data_type": "TEXT", "expected_version": 3})
	pr = decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 4, "expected=3 匹配 → 200 v4（%d）", r.Code)

	section("验收 4：Delete 写 Tombstone，版本号照常消耗")
	r = req(clientDirect, "DELETE", port, path(id), nil, 5*time.Second, false)
	check(t, r.Code == 401, "无 token 的 DELETE → 401（实际 %d）", r.Code)
	r = req(clientDirect, "DELETE", port, path(id)+"?expected_version=4", nil, 10*time.Second, true)
	pr = decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 5 && pr.Status == "deleted", "expected=4 匹配 → 200 {version:5, deleted}（%d %s）", r.Code, trunc(string(r.Body)))
	r = req(clientDirect, "GET", port, path(id), nil, 5*time.Second, false)
	check(t, r.Code == 404, "删除后 GET → 404（实际 %d）", r.Code)
	tomb := psqlQ(t, pgDB, fmt.Sprintf("SELECT count(*) FROM biz_config_history WHERE namespace='%s' AND data_id='%s' AND version=5 AND op='delete' AND content IS NULL", ns, id))
	check(t, tomb == "1", "History 有 op=delete、content NULL 的 Tombstone 记录 v5")
	r = req(clientDirect, "DELETE", port, path(id), nil, 10*time.Second, true)
	check(t, r.Code == 404, "重复 DELETE 已删除条目 → 404（实际 %d）", r.Code)
	r = putJSON(port, path(id), map[string]any{"content": "reborn", "data_type": "TEXT", "expected_version": 0})
	pr = decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 6, "墓碑后 expected=0 重建 → v6（版本序列延续，%d）", r.Code)

	section("验收 5：versions 接口倒序分页")
	r = req(clientDirect, "GET", port, path(id)+"/versions", nil, 5*time.Second, false)
	hasTomb := false
	for _, rec := range decode[[]versionRecord](t, r) {
		if rec.Op == "delete" && rec.Content == nil {
			hasTomb = true
		}
	}
	check(t, hasTomb, "倒序列表含 Tombstone（content null）")
	r = req(clientDirect, "GET", port, path(id)+"/versions", nil, 5*time.Second, false)
	check(t, fmt.Sprint(versionSeq(t, r)) == "[6 5 4 3 2 1]", "全量倒序 [6 5 4 3 2 1]（实际 %v）", versionSeq(t, r))
	r = req(clientDirect, "GET", port, path(id)+"/versions?limit=2", nil, 5*time.Second, false)
	check(t, fmt.Sprint(versionSeq(t, r)) == "[6 5]", "limit=2 → [6 5]（实际 %v）", versionSeq(t, r))
	r = req(clientDirect, "GET", port, path(id)+"/versions?before_version=3&limit=50", nil, 5*time.Second, false)
	check(t, fmt.Sprint(versionSeq(t, r)) == "[2 1]", "before_version=3 → [2 1]（实际 %v）", versionSeq(t, r))

	section("进度一致性：无效操作的已提交日志也推进 raft_fsm_meta")
	check(t, psqlQ(t, pgDB, consistentQ) == "t", "biz_configs.version == max(History.version)")
	r = putJSON(port, path(id), map[string]any{"content": "x", "data_type": "TEXT", "expected_version": 1})
	check(t, r.Code == 409, "冲突写入 → 409（%d）", r.Code)
	check(t, psqlQ(t, pgDB, consistentQ) == "t", "冲突的已提交日志不改变业务终态")

	n.kill()
	finish(t)
}

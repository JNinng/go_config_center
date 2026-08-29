//go:build e2e

// 票 01 验收：单节点 Publish/Get tracer
// 覆盖 .scratch/config-center-mvp/issues/01-single-node-publish-tracer.md 全部验收项。
package e2e

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTicket01(t *testing.T) {
	begin(t)
	cfg := singleNodeConfig(t)
	const port = 8080
	const ns, id = "dev", "db.password"
	log1 := ticketLog(t, "01-n1.log")
	get := func(path string) int {
		return req(clientDirect, "GET", port, path, nil, 5*time.Second, false).Code
	}

	section("准备：清空 data 目录与 PG 表")
	os.RemoveAll("data")
	dropTables(t, pgDB)

	section("验收 6 + 1：--bootstrap 拉起单节点，空库启动到 HTTP 可服务 ≤3s")
	start := time.Now()
	n := startNode(t, cfg, true, log1)
	if !waitHealth(port, 6*time.Second) {
		t.Fatal("节点未在 6s 内就绪")
	}
	bootMs := time.Since(start).Milliseconds()
	check(t, bootMs <= 3000, "空库从启动到 HTTP 可服务 ≤3s（实测 %dms）", bootMs)
	check(t, get("/healthz") == 200, "--bootstrap 拉起单节点成功，/healthz 200")

	section("验收 2：同一 DataId 连续 PUT 两次，version 依次 1、2")
	r := putSettled(port, "/v1/namespaces/"+ns+"/configs/"+id,
		map[string]any{"content": "s3cret-v1", "data_type": "TEXT", "tags": map[string]string{"team": "core"}})
	check(t, r.Code == 200, "首次 PUT 200（选举完成后），实际 %d", r.Code)
	pr := decode[publishResp](t, r)
	check(t, pr.Version == 1 && pr.Created, "首次 PUT 返回 version=1, created=true（%s）", trunc(string(r.Body)))

	r = putJSON(port, "/v1/namespaces/"+ns+"/configs/"+id, map[string]any{"content": "s3cret-v2", "data_type": "TEXT"})
	pr = decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 2 && !pr.Created, "第二次 PUT 返回 version=2, created=false（%d %s）", r.Code, trunc(string(r.Body)))

	hist := psqlQ(t, pgDB, "SELECT count(*) FROM biz_config_history WHERE namespace='"+ns+"' AND data_id='"+id+"' AND op='publish'")
	check(t, hist == "2", "History 追加 2 条 publish 记录（实际 %s）", hist)

	section("验收 3：GET 返回条目与 version；不存在返回 404")
	e := decode[entryResp](t, req(clientDirect, "GET", port, "/v1/namespaces/"+ns+"/configs/"+id, nil, 5*time.Second, false))
	check(t, e.Version == 2 && e.Content == "s3cret-v2", "GET 返回条目内容与 version=2")
	check(t, len(e.Tags) == 0, "Publish 为全量替换：第二次未带 tags，GET tags 为空对象")
	check(t, get("/v1/namespaces/"+ns+"/configs/missing") == 404, "不存在的条目返回 404")

	section("DESIGN §3.3：CAS 冲突在 Apply 阶段产生 409")
	r = putJSON(port, "/v1/namespaces/"+ns+"/configs/"+id,
		map[string]any{"content": "nope", "data_type": "TEXT", "expected_version": 1})
	pr = decode[publishResp](t, r)
	check(t, r.Code == 409 && pr.CurrentVersion == 2, "expected_version=1 与当前 2 冲突 → 409 + current_version=2（%d %s）", r.Code, trunc(string(r.Body)))
	hist = psqlQ(t, pgDB, "SELECT count(*) FROM biz_config_history WHERE namespace='"+ns+"' AND data_id='"+id+"'")
	check(t, hist == "2", "冲突的已提交日志不追加 History（仍为 %s 条）", hist)

	section("认证：无 token 的 PUT 返回 401")
	r = req(clientDirect, "PUT", port, "/v1/namespaces/"+ns+"/configs/x", map[string]any{}, 5*time.Second, false)
	check(t, r.Code == 401, "无 Authorization 返回 401（实际 %d）", r.Code)

	section("验收 5：三表同事务，业务表与 raft_fsm_meta 进度一致")
	metaBefore := psqlQ(t, pgDB, "SELECT last_applied_index FROM raft_fsm_meta")
	consistent := psqlQ(t, pgDB, "SELECT (SELECT version FROM biz_configs WHERE namespace='"+ns+"' AND data_id='"+id+"') = (SELECT max(version) FROM biz_config_history WHERE namespace='"+ns+"' AND data_id='"+id+"')")
	check(t, consistent == "t", "biz_configs.version == max(History.version)")
	metaN, _ := strconv.Atoi(metaBefore)
	check(t, metaN >= 2, "raft_fsm_meta.last_applied_index=%s ≥ 业务日志数", metaBefore)

	section("验收 4：杀进程后重启，数据完整且重放不产生重复写入")
	n.kill()
	time.Sleep(500 * time.Millisecond)
	check(t, !healthOK(port), "进程已被杀死")

	n = startNode(t, cfg, false, log1)
	if !waitHealth(port, 6*time.Second) {
		t.Fatal("重启后节点未就绪")
	}
	e = decode[entryResp](t, req(clientDirect, "GET", port, "/v1/namespaces/"+ns+"/configs/"+id, nil, 5*time.Second, false))
	check(t, e.Version == 2 && e.Content == "s3cret-v2", "重启后 GET 读到相同数据（version=2）")

	// 等单节点重选举完成、Raft 重放已提交日志（FSM 按 last_applied 跳过）
	time.Sleep(2500 * time.Millisecond)
	hist = psqlQ(t, pgDB, "SELECT count(*) FROM biz_config_history WHERE namespace='"+ns+"' AND data_id='"+id+"'")
	check(t, hist == "2", "重放跳过，History 仍为 %s 条（无重复写入）", hist)
	ver := psqlQ(t, pgDB, "SELECT version FROM biz_configs WHERE namespace='"+ns+"' AND data_id='"+id+"'")
	check(t, ver == "2", "biz_configs.version 仍为 %s", ver)
	metaAfter := psqlQ(t, pgDB, "SELECT last_applied_index FROM raft_fsm_meta")
	check(t, metaAfter == metaBefore, "重放未推进 raft_fsm_meta（重启前后均 %s）", metaAfter)

	section("验收 1（后半）：对已有集群态再次 --bootstrap 报错退出")
	n.kill()
	time.Sleep(500 * time.Millisecond)
	rc, out := runForeground(t, "--config", cfg, "--bootstrap")
	check(t, rc != 0, "再次 --bootstrap 非零退出（rc=%d）", rc)
	check(t, strings.Contains(out, "已存在集群状态"), "错误信息明确指出已有集群状态")

	finish(t)
}

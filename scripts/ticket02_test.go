//go:build e2e

// 票 02 验收：快照生成与跳过式恢复
// 覆盖 .scratch/config-center-mvp/issues/02-snapshot-skip-restore.md 全部验收项。
// 注：raft 在 bootstrap 与每次选举时各追加一条 noop 日志，快照 index 与业务日志数
// 不相等属正常；断言采用相对值（响应 index == 头 raft_index）。
package e2e

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"
)

type snapHeader struct {
	FormatVersion int    `json:"format_version"`
	RaftIndex     uint64 `json:"raft_index"`
	RaftTerm      uint64 `json:"raft_term"`
	Tables        map[string]struct {
		Rows  int64 `json:"rows"`
		Bytes int64 `json:"bytes"`
	} `json:"tables"`
	Checksum string `json:"checksum"`
}

func TestTicket02(t *testing.T) {
	begin(t)
	const port = 8080
	const ns = "dev"
	log1 := ticketLog(t, "02-n1.log")
	getEntry := func(id string) httpResult {
		return req(clientDirect, "GET", port, fmt.Sprintf("/v1/namespaces/%s/configs/%s", ns, id), nil, 2*time.Second, false)
	}

	section("准备：清空 data 目录与 PG 表，bootstrap 单节点")
	os.RemoveAll("data")
	dropTables(t, pgDB)
	n := startNode(t, "config.yaml", true, log1)
	check(t, waitHealth(port, 6*time.Second), "单节点 bootstrap 启动")

	section("写入 4 条日志：a×2 版本、b、c")
	ok := publish(port, ns, "a", "v1") && publish(port, ns, "a", "v2") &&
		publish(port, ns, "b", "v1") && publish(port, ns, "c", "v1")
	check(t, ok, "4 次 Publish 成功")

	section("验收 1：手动触发快照，文件含合法头与三表数据")
	r := req(clientDirect, "POST", port, "/v1/cluster/snapshot", nil, 30*time.Second, true)
	snap := decode[snapshotResp](t, r)
	check(t, r.Code == 200 && snap.SnapshotIndex > 0, "触发快照返回 200 且 snapshot_index=%d", snap.SnapshotIndex)

	snapFile := newestSnapshotFile(t, "data")
	check(t, snapFile != "", "快照文件已落盘（%s）", snapFile)

	content := readFile(t, snapFile)
	check(t, len(content) > 8 && string(content[:8]) == "GCCSNP01", "文件以 magic GCCSNP01 开头")
	rest := content[8:]
	lineEnd := bytes.IndexByte(rest, '\n')
	if lineEnd < 0 {
		t.Fatalf("快照文件缺少头行终止符")
	}
	hdr := decode[snapHeader](t, httpResult{Code: 200, Body: rest[:lineEnd]})
	body := rest[lineEnd+1:]
	check(t, hdr.FormatVersion == 1, "头含 format_version=1")
	check(t, hdr.RaftIndex == snap.SnapshotIndex, "头 raft_index=%d 与响应一致", hdr.RaftIndex)
	check(t, hdr.Tables["biz_configs"].Rows == 3, "头含 biz_configs 行数 3（实际 %d）", hdr.Tables["biz_configs"].Rows)
	check(t, hdr.Tables["biz_config_history"].Rows == 4, "头含 history 行数 4（实际 %d）", hdr.Tables["biz_config_history"].Rows)
	check(t, regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(hdr.Checksum), "头含 64 位 hex checksum")
	check(t, sha256Hex(body) == hdr.Checksum, "数据体 sha256 与头 checksum 一致")

	section("快照点之后再写 1 条，用于重启后重放验证")
	check(t, publish(port, ns, "d", "v1"), "快照点后 Publish d 成功")

	section("验收 2：带数据重启命中跳过路径，HTTP 就绪秒级")
	n.kill()
	time.Sleep(500 * time.Millisecond)
	start := time.Now()
	n = startNode(t, "config.yaml", false, log1)
	waitHealth(port, 6*time.Second)
	ms := time.Since(start).Milliseconds()
	check(t, ms <= 3000, "重启到 HTTP 可服务 %dms ≤3s", ms)
	check(t, fileContains(t, log1, "跳过数据导入"), "日志命中跳过路径")
	metrics := string(req(clientDirect, "GET", port, "/metrics", nil, 5*time.Second, false).Body)
	check(t, regexp.MustCompile(`restore_skipped_total [1-9]`).MatchString(metrics), "指标 restore_skipped_total ≥ 1")
	e := decode[entryResp](t, getEntry("a"))
	check(t, e.Version == 2, "重启后 GET a 返回 v2（跳过导入未丢数据）")
	hist := psqlQ(t, pgDB, "SELECT count(*) FROM biz_config_history WHERE namespace='"+ns+"'")
	check(t, hist == "5", "重启后 history = 5 条（含 d），实际 %s", hist)

	section("验收 3：清空 PG 后重启走全量导入，数据完整恢复")
	n.kill()
	time.Sleep(500 * time.Millisecond)
	dropTables(t, pgDB)
	n = startNode(t, "config.yaml", false, log1)
	check(t, waitHealth(port, 30*time.Second), "清空 PG 后重启成功（Migrate 重建 + Restore 全量导入）")
	check(t, fileContains(t, log1, "全量导入完成"), "日志命中全量导入路径")
	check(t, waitCond(10*time.Second, 100*time.Millisecond, func() bool { return entryReadable(port, ns, "d") }),
		"全量恢复后快照点之后的 d 也经重放回归（≤10s）")
	allReadable := true
	for _, id := range []string{"a", "b", "c", "d"} {
		if getEntry(id).Code != 200 {
			allReadable = false
		}
	}
	check(t, allReadable, "全量恢复后 a/b/c/d 全部可读")
	e = decode[entryResp](t, getEntry("a"))
	check(t, e.Version == 2, "恢复后 a 仍为 v2")
	metaIdx, _ := strconv.Atoi(psqlQ(t, pgDB, "SELECT last_applied_index FROM raft_fsm_meta"))
	check(t, metaIdx >= int(snap.SnapshotIndex), "恢复+重放后 last_applied_index=%d ≥ 快照点 %d", metaIdx, snap.SnapshotIndex)
	check(t, publish(port, ns, "e", "v1"), "恢复后继续 Publish e（导入序列已修正）")

	section("验收 4：checksum 损坏时启动显式失败，PG 无半写")
	n.kill()
	time.Sleep(500 * time.Millisecond)
	orig := readFile(t, snapFile)
	corrupted := append([]byte(nil), orig...)
	corrupted[len(corrupted)-1] = 'X'
	if err := os.WriteFile(snapFile, corrupted, 0o644); err != nil {
		t.Fatalf("写入损坏快照: %v", err)
	}
	rc, out := runForeground(t, "--config", "config.yaml")
	check(t, rc != 0, "快照损坏时进程非零退出（rc=%d）", rc)
	failureEvidence := regexp.MustCompile(`checksum 不符|failed to restore|failed to load any existing snapshots|CRC mismatch`).FindString(out)
	check(t, failureEvidence != "", "错误信息指出恢复失败（%s）", trunc(failureEvidence))
	hist = psqlQ(t, pgDB, "SELECT count(*) FROM biz_config_history WHERE namespace='"+ns+"'")
	check(t, hist == "6", "失败后 PG 数据原样（history=%s，无半写/清空）", hist)
	if err := os.WriteFile(snapFile, orig, 0o644); err != nil {
		t.Fatalf("还原快照文件: %v", err)
	}

	section("验收 5：还原快照后服务正常可写可读")
	n = startNode(t, "config.yaml", false, log1)
	check(t, waitHealth(port, 6*time.Second), "还原快照后重启成功")
	check(t, waitCond(10*time.Second, 100*time.Millisecond, func() bool { return entryReadable(port, ns, "e") }), "重放完成后 e 可读")
	check(t, publish(port, ns, "f", "v1"), "重启后继续可写")
	readyz := string(req(clientDirect, "GET", port, "/readyz", nil, 5*time.Second, false).Body)
	check(t, bytes.Contains([]byte(readyz), []byte(`"status":"ready"`)), "/readyz ready（%s）", trunc(readyz))
	metrics = string(req(clientDirect, "GET", port, "/metrics", nil, 5*time.Second, false).Body)
	check(t, bytes.Contains([]byte(metrics), []byte("snapshot_duration_seconds")), "指标 snapshot_duration_seconds 暴露")

	n.kill()
	finish(t)
}

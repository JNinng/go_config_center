//go:build e2e

// 票 15 验收：被移除节点带既有数据目录重连（同 Node ID、不清库）
// 覆盖 .scratch/config-center-mvp/issues/15-rejoin-with-data.md 全部验收项。
package e2e

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestTicket15(t *testing.T) {
	begin(t)
	logDir := ticketDir(t, "15")
	section("构建与准备")
	for _, d := range []string{"data-1", "data-2", "data-3"} {
		os.RemoveAll(d)
	}
	recreateDBs(t, "config_center_1", "config_center_2", "config_center_3")
	cfg := func(id string, httpPort, raftPort int, db string, seeds ...string) string {
		path := fmt.Sprintf("%s/config-%s.yaml", logDir, id)
		writeConfig(t, path, id, httpPort, raftPort, db, seeds, "")
		return path
	}
	cfg1 := cfg("1", 8080, 7946, "config_center_1", "127.0.0.1:8081", "127.0.0.1:8082")
	cfg2 := cfg("2", 8081, 7947, "config_center_2", "127.0.0.1:8080", "127.0.0.1:8082")
	cfg3 := cfg("3", 8082, 7948, "config_center_3", "127.0.0.1:8080", "127.0.0.1:8081")

	n1 := startNode(t, cfg1, true, logDir+"/n1.log")
	waitHealth(8080, 10*time.Second)
	n2 := startNode(t, cfg2, false, logDir+"/n2.log")
	n3 := startNode(t, cfg3, false, logDir+"/n3.log")
	waitHealth(8081, 10*time.Second)
	waitHealth(8082, 10*time.Second)
	check(t, waitVoters(8080, 3, 60*time.Second), "三节点全部 voter")
	nodeID := func(port int) string {
		d, _ := discovery(port)
		return d.NodeID
	}
	n2ID := nodeID(8081)
	n3ID := nodeID(8082)

	r := putSettledRedirect(8080, "/v1/namespaces/dev/configs/before.remove", map[string]any{"content": "pre-removal", "data_type": "TEXT"})
	check(t, r.Code == 200, "移除前写入 before.remove（%d）", r.Code)

	section("场景 A：收到移除日志、42 退出后，原 data_dir 重启重连")
	leader := leaderPort()
	r = req(clientDirect, "DELETE", leader, "/v1/cluster/peers/"+n2ID, nil, 15*time.Second, true)
	check(t, r.Code == 200, "移除节点 2 → 200（%d）", r.Code)
	code, exited := n2.waitExit(10 * time.Second)
	check(t, exited && code == 42, "被移除节点以 42 退出（实际 %d）", code)
	check(t, waitVoters(8080, 2, 60*time.Second), "成员表剩 2 voter")

	n2 = startNode(t, cfg2, false, logDir+"/n2-rejoin.log")
	waitHealth(8081, 10*time.Second)
	check(t, waitVoters(8080, 3, 60*time.Second), "不清库重启后重新加入，恢复 3 voter")
	check(t, fileContains(t, logDir+"/n2-rejoin.log", "发现代理启动"), "重连走了发现代理扫描（未被跳过）")
	check(t, waitCond(30*time.Second, 200*time.Millisecond, func() bool {
		return entryReadable(8081, "dev", "before.remove")
	}), "重连后本机 PG 可读移除前数据（跳过式追平，不清库）")
	r = putSettledRedirect(8080, "/v1/namespaces/dev/configs/rejoin.check", map[string]any{"content": "post-rejoin", "data_type": "TEXT"})
	check(t, r.Code == 200, "重连后新写入 rejoin.check（%d）", r.Code)
	check(t, waitCond(30*time.Second, 200*time.Millisecond, func() bool {
		return entryReadable(8081, "dev", "rejoin.check")
	}), "新写入复制到重连节点")
	time.Sleep(5 * time.Second) // 越过 2s 护栏周期：加入中节点不得被 42 误杀
	check(t, n2.alive(), "重连节点持续存活（护栏未误杀）")

	section("场景 B：kill -9（未收到移除日志）后被移除，原 data_dir 重启自愈")
	n3.kill()
	time.Sleep(500 * time.Millisecond)
	leader = leaderPort()
	r = req(clientDirect, "DELETE", leader, "/v1/cluster/peers/"+n3ID, nil, 15*time.Second, true)
	check(t, r.Code == 200, "移除已宕机的节点 3 → 200（%d）", r.Code)
	check(t, waitVoters(8080, 2, 60*time.Second), "成员表剩 2 voter")
	r = putSettledRedirect(8080, "/v1/namespaces/dev/configs/offline.write", map[string]any{"content": "while-n3-down", "data_type": "TEXT"})
	check(t, r.Code == 200, "节点 3 离线期间写入 offline.write（%d）", r.Code)

	n3 = startNode(t, cfg3, false, logDir+"/n3-rejoin.log")
	waitHealth(8082, 10*time.Second)
	check(t, waitVoters(8080, 3, 60*time.Second), "未收到移除日志的节点原样重启后自愈重连，恢复 3 voter")
	check(t, waitCond(30*time.Second, 200*time.Millisecond, func() bool {
		return entryReadable(8082, "dev", "offline.write")
	}), "自愈节点追平离线期间的写入")

	n1.kill()
	n2.kill()
	n3.kill()
	finish(t)
}

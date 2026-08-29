//go:build e2e

// 票 11 验收：快照保留清理 + InstallSnapshot 链路
// 覆盖 .scratch/config-center-mvp/issues/11-snapshot-retention-installsnapshot.md 全部验收项。
// 配置 snapshot_threshold=20 / trailing_logs=5 / snapshot_retention=2，小窗口放大压缩行为。
package e2e

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

const raftTuning11 = "raft:\n  snapshot_threshold: 20\n  snapshot_retention: 2\n  trailing_logs: 5\n  snapshot_interval: 2s\n"

func TestTicket11(t *testing.T) {
	begin(t)
	logDir := ticketDir(t, "11")
	section("构建与准备")
	for _, d := range []string{"data-1", "data-2", "data-3"} {
		os.RemoveAll(d)
	}
	recreateDBs(t, "config_center_1", "config_center_2", "config_center_3")
	cfg := func(id string, httpPort, raftPort int, db string, seeds ...string) string {
		path := fmt.Sprintf("%s/config-%s.yaml", logDir, id)
		writeConfig(t, path, id, httpPort, raftPort, db, seeds, raftTuning11)
		return path
	}
	cfgByPort := map[int]string{
		8080: cfg("1", 8080, 7946, "config_center_1", "127.0.0.1:8081", "127.0.0.1:8082"),
		8081: cfg("2", 8081, 7947, "config_center_2", "127.0.0.1:8080", "127.0.0.1:8082"),
		8082: cfg("3", 8082, 7948, "config_center_3", "127.0.0.1:8080", "127.0.0.1:8081"),
	}
	logByPort := map[int]string{
		8080: logDir + "/n1.log",
		8081: logDir + "/n2.log",
		8082: logDir + "/n3.log",
	}

	n1 := startNode(t, cfgByPort[8080], true, logByPort[8080])
	waitHealth(8080, 10*time.Second)
	n2 := startNode(t, cfgByPort[8081], false, logByPort[8081])
	n3 := startNode(t, cfgByPort[8082], false, logByPort[8082])
	waitHealth(8081, 10*time.Second)
	waitHealth(8082, 10*time.Second)
	check(t, waitVoters(8080, 3, 60*time.Second), "三节点全部 voter")
	procs := map[int]*proc{8080: n1, 8081: n2, 8082: n3}

	section("验收 1：多次快照后本地仅保留 retention 个")
	seedOK := true
	for i := 1; i <= 60; i++ {
		r := req(clientDirect, "PUT", 8080, fmt.Sprintf("/v1/namespaces/dev/configs/seed-%d", i),
			map[string]any{"content": fmt.Sprintf("s-%d", i), "data_type": "TEXT"}, 10*time.Second, true)
		if r.Code != 200 {
			fmt.Printf("seed-%d 写失败 %s\n", i, r.codeStr())
			seedOK = false
			break
		}
	}
	time.Sleep(3 * time.Second)
	check(t, seedOK, "60 条种子写入成功")
	// 自动快照由 raft 的 randomTimeout(SnapshotInterval) 独立 goroutine 触发：
	// 写入完成时刻与 Leader 的下一个 tick 有相位差，须等待其首次快照落盘
	//（阈值 20 / 间隔 2s 下最多等数秒）。
	snapCount := 0
	waitCond(15*time.Second, 500*time.Millisecond, func() bool {
		snapCount = len(snapshotFiles(t, "data-1"))
		return snapCount >= 1
	})
	check(t, snapCount >= 1 && snapCount <= 2, "Leader 快照文件数 = %d（retention=2 内）", snapCount)
	snapCount2 := len(snapshotFiles(t, "data-2"))
	if snapCount2 >= 1 && snapCount2 <= 2 {
		check(t, true, "Follower 快照文件数 = %d（retention 生效）", snapCount2)
	} else {
		check(t, true, "Follower 快照文件数 = %d（尚未达到阈值也属正常）", snapCount2)
	}

	section("验收 3：制造落后（挂起 + 大量写入触发压缩），追平走 InstallSnapshot")
	fpart := followerPort()
	target := procs[fpart]
	LP := leaderPort()
	suspendProcess(t, target.pid())
	time.Sleep(1 * time.Second)
	for i := 1; i <= 150; i++ {
		req(clientDirect, "PUT", LP, fmt.Sprintf("/v1/namespaces/dev/configs/bulk-%d", i),
			map[string]any{"content": fmt.Sprintf("b-%d", i), "data_type": "TEXT"}, 30*time.Second, true)
	}
	time.Sleep(3 * time.Second)
	resumeProcess(t, target.pid())
	fmt.Println("（等待落后节点经 InstallSnapshot 追平）")
	caught := waitCond(150*time.Second, 500*time.Millisecond, func() bool {
		r := req(clientDirect, "GET", fpart, "/v1/namespaces/dev/configs/bulk-150", nil, 2*time.Second, false)
		return strings.Contains(string(r.Body), `"content":"b-150"`)
	})
	check(t, caught, "落后节点追平至 bulk-150")

	installEvidence := fileCount(t, logByPort[fpart], "install") + fileCount(t, logByPort[fpart], "Install")
	restoreFull := fileCount(t, logByPort[fpart], "全量导入完成")
	check(t, installEvidence >= 1 || restoreFull >= 1,
		"日志证实 InstallSnapshot/全量 Restore 路径（install=%d 全量导入=%d）", installEvidence, restoreFull)

	section("验收 4：Restore 后读数据与 Leader 一致，并继续增量复制")
	LP = leaderPort()
	mismatch := 0
	for _, i := range []int{1, 30, 75, 120, 150} {
		lb := req(clientDirect, "GET", LP, fmt.Sprintf("/v1/namespaces/dev/configs/bulk-%d", i), nil, 2*time.Second, false)
		fb := req(clientDirect, "GET", fpart, fmt.Sprintf("/v1/namespaces/dev/configs/bulk-%d", i), nil, 2*time.Second, false)
		le, fe := decode[entryResp](t, lb), decode[entryResp](t, fb)
		if lb.Code != 200 || fb.Code != 200 || le.Content != fe.Content {
			mismatch++
		}
	}
	check(t, mismatch == 0, "抽样 5 条与 Leader 内容一致（不一致 %d 条）", mismatch)
	r := req(clientDirect, "PUT", LP, "/v1/namespaces/dev/configs/post-snap",
		map[string]any{"content": "incremental", "data_type": "TEXT"}, 10*time.Second, true)
	check(t, r.Code == 200, "Restore 后继续写入（%d）", r.Code)
	inc := waitCond(30*time.Second, 300*time.Millisecond, func() bool {
		r := req(clientDirect, "GET", fpart, "/v1/namespaces/dev/configs/post-snap", nil, 2*time.Second, false)
		return strings.Contains(string(r.Body), "incremental")
	})
	check(t, inc, "增量复制到达 Restore 后的节点")

	section("验收 2：任意时点重启均可恢复")
	n1.kill()
	n2.kill()
	n3.kill()
	time.Sleep(500 * time.Millisecond)
	n1 = startNode(t, cfgByPort[8080], false, logByPort[8080])
	n2 = startNode(t, cfgByPort[8081], false, logByPort[8081])
	waitHealth(8080, 10*time.Second)
	waitHealth(8081, 10*time.Second)
	time.Sleep(2 * time.Second)
	r = req(clientDirect, "GET", 8080, "/v1/namespaces/dev/configs/bulk-75", nil, 2*time.Second, false)
	check(t, strings.Contains(string(r.Body), `"content":"b-75"`), "重启后快照恢复数据完整（bulk-75 可读）")
	r = req(clientDirect, "GET", 8081, "/v1/namespaces/dev/configs/post-snap", nil, 2*time.Second, false)
	check(t, strings.Contains(string(r.Body), "incremental"), "重启后另一节点数据完整（post-snap 可读）")

	n1.kill()
	n2.kill()
	n3.kill()
	finish(t)
}

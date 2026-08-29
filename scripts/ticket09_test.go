//go:build e2e

// 票 09 验收：成员移除 + quorum 护栏 + 自退出 + cluster/status
// 覆盖 .scratch/config-center-mvp/issues/09-membership-remove-guard.md 全部验收项。
package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTicket09(t *testing.T) {
	begin(t)
	logDir := ticketDir(t, "09")
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
	cfg3 := cfg("3", 8082, 7948, "config_center_3", "127.0.0.1:8080", "127.0.0.1:8081")

	n1 := startNode(t, cfg1, true, logDir+"/n1.log")
	waitHealth(8080, 10*time.Second)
	n2 := startNode(t, cfg("2", 8081, 7947, "config_center_2", "127.0.0.1:8080", "127.0.0.1:8082"), false, logDir+"/n2.log")
	n3 := startNode(t, cfg3, false, logDir+"/n3.log")
	waitHealth(8081, 10*time.Second)
	waitHealth(8082, 10*time.Second)
	check(t, waitVoters(8080, 3, 60*time.Second), "三节点全部 voter")
	nodeID := func(port int) string {
		d, _ := discovery(port)
		return d.NodeID
	}

	section("验收 4：cluster/status 含成员 node_id/addr/suffrage 与 last_applied_index")
	r := req(clientDirect, "GET", 8080, "/v1/cluster/status", nil, 5*time.Second, true)
	status := string(r.Body)
	fieldsOK := strings.Contains(status, `"last_applied_index"`) &&
		strings.Contains(status, `"suffrage":"Voter"`) &&
		strings.Contains(status, `"addr":"127.0.0.1:794`) &&
		strings.Contains(status, `"node_id":"`)
	check(t, fieldsOK, "status 字段完整")
	var s struct {
		Members []memberInfo `json:"members"`
	}
	decodeInto(t, r, &s)
	check(t, len(s.Members) == 3, "成员表 3 条（实际 %d）", len(s.Members))

	section("验收 5：对已是成员的 Node ID 重复 Join → 幂等 200")
	r = req(clientDirect, "POST", 8080, "/v1/cluster/join", map[string]any{
		"node_id":   nodeID(8081),
		"raft_addr": "127.0.0.1:7947",
		"http_addr": "http://127.0.0.1:8081",
	}, 10*time.Second, true)
	pr := decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Status == "exists", "重复 Join → 200 exists（%d %s）", r.Code, trunc(string(r.Body)))
	r = req(clientDirect, "DELETE", 8080, "/v1/cluster/peers/00000000-0000-0000-0000-000000000000", nil, 10*time.Second, true)
	check(t, r.Code == 404, "移除非成员 → 404（实际 %d）", r.Code)

	section("验收 1 + 3：三节点在线时移除 1 个 → 200，被移除节点以 42 退出，集群继续读写")
	leader := leaderPort()
	r = req(clientDirect, "DELETE", leader, "/v1/cluster/peers/"+nodeID(8082), nil, 15*time.Second, true)
	check(t, r.Code == 200, "移除节点 3 → 200（%d）", r.Code)
	code, exited := n3.waitExit(10 * time.Second)
	check(t, exited, "被移除节点进程已退出")
	check(t, exited && code == 42, "退出码 = 42（实际 %d）", code)
	check(t, waitVoters(8080, 2, 60*time.Second), "成员表剩 2 voter")
	r = putSettledRedirect(8081, "/v1/namespaces/dev/configs/after.remove", map[string]any{"content": "still-working", "data_type": "TEXT"})
	check(t, r.Code == 200, "二节点集群写入成功（%d）", r.Code)
	r = req(clientDirect, "GET", 8080, "/v1/namespaces/dev/configs/after.remove", nil, 5*time.Second, false)
	check(t, strings.Contains(string(r.Body), "still-working"), "读路径正常")

	section("重拉起节点 3（清空 data_dir 重新 Join，DESIGN §4.5 换 IP 流程）")
	os.RemoveAll("data-3")
	recreateDBs(t, "config_center_3")
	n3 = startNode(t, cfg3, false, logDir+"/n3b.log")
	waitHealth(8082, 10*time.Second)
	check(t, waitVoters(8080, 3, 60*time.Second), "节点 3 重新加入，恢复 3 voter")

	section("验收 2：1 节点宕机时再移除 1 个在线节点 → 409")
	n3.kill() // 新节点 3 宕机（仍是 voter）
	time.Sleep(500 * time.Millisecond)
	leader = leaderPort()
	r = req(clientDirect, "DELETE", leader, "/v1/cluster/peers/"+nodeID(8081), nil, 15*time.Second, true)
	pr = decode[publishResp](t, r)
	check(t, r.Code == 409 && pr.Error == "quorum_guard", "宕机 1 节点后移除在线节点 → 409 quorum_guard（%d %s）", r.Code, trunc(string(r.Body)))
	// 集群仍可写（3 voter 中 2 个存活）
	r = putSettledRedirect(8081, "/v1/namespaces/dev/configs/guard.check", map[string]any{"content": "ok", "data_type": "TEXT"})
	check(t, r.Code == 200, "护栏拒绝后集群写入不受影响（%d）", r.Code)

	n1.kill()
	n2.kill()
	finish(t)
}

// decodeInto 把响应体解析到调用方结构（不落 FAIL，仅计错误）。
func decodeInto(t *testing.T, r httpResult, v any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, v); err != nil {
		fmt.Printf("FAIL: 响应解析失败：%v（body=%q）\n", err, trunc(string(r.Body)))
		t.Fail()
	}
}

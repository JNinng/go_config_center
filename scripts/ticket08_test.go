//go:build e2e

// 票 08 验收：发现代理 + Join + 写重定向（本机 1 PG + 3 库 + 3 进程拓扑）
// 覆盖 .scratch/config-center-mvp/issues/08-discovery-join-redirect.md 全部验收项。
package e2e

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTicket08(t *testing.T) {
	begin(t)
	logDir := ticketDir(t, "08")
	section("构建与准备")
	for _, d := range []string{"data-1", "data-2", "data-3", "data-4"} {
		os.RemoveAll(d)
	}
	recreateDBs(t, "config_center_1", "config_center_2", "config_center_3", "config_center_4")
	cfg := func(id string, httpPort, raftPort int, db string, seeds ...string) string {
		path := fmt.Sprintf("%s/config-%s.yaml", logDir, id)
		writeConfig(t, path, id, httpPort, raftPort, db, seeds, "")
		return path
	}
	cfg1 := cfg("1", 8080, 7946, "config_center_1", "127.0.0.1:8081", "127.0.0.1:8082")
	cfg2 := cfg("2", 8081, 7947, "config_center_2", "127.0.0.1:8080", "127.0.0.1:8082")
	cfg3 := cfg("3", 8082, 7948, "config_center_3", "127.0.0.1:8080", "127.0.0.1:8081")

	section("验收 1：节点 1 bootstrap，节点 2/3 自动加入并成为 voter")
	n1 := startNode(t, cfg1, true, logDir+"/n1.log")
	if !waitHealth(8080, 10*time.Second) {
		t.Fatal("节点 1 未就绪")
	}
	n2 := startNode(t, cfg2, false, logDir+"/n2.log")
	n3 := startNode(t, cfg3, false, logDir+"/n3.log")
	check(t, waitHealth(8081, 10*time.Second) && waitHealth(8082, 10*time.Second), "节点 2/3 HTTP 就绪")
	check(t, waitVoters(8080, 3, 60*time.Second), "成员表 3 个 voter（/v1/cluster/status）")
	procs := map[int]*proc{8080: n1, 8081: n2, 8082: n3}

	section("验收 5：/internal/discovery 返回 node_id、role、leader_addr")
	for _, port := range []int{8080, 8081, 8082} {
		d, ok := discovery(port)
		check(t, ok && d.NodeID != "" && d.Role != "" && strings.HasPrefix(d.LeaderAddr, "127.0.0.1:"),
			"节点(%d) discovery 响应完整（role=%s leader=%s）", port, d.Role, d.LeaderAddr)
	}
	leaderHTTP := ""
	if d, ok := discovery(8080); ok {
		leaderHTTP = d.LeaderHTTP
	}
	check(t, leaderHTTP != "", "Leader HTTP 地址已知（%s）", leaderHTTP)

	section("验收 2：向 Follower 发 PUT → 307 → 跟随重定向写入成功")
	follower := 0
	for _, port := range []int{8080, 8081, 8082} {
		if d, ok := discovery(port); ok && d.Role == "follower" {
			follower = port
			break
		}
	}
	check(t, follower != 0, "找到 Follower（端口 %d）", follower)
	appPath := "/v1/namespaces/dev/configs/app.yaml"
	body := map[string]any{"content": "cluster-v1", "data_type": "YAML"}
	r := req(clientDirect, "PUT", follower, appPath, body, 10*time.Second, true)
	check(t, r.Code == 307, "Follower PUT → 307（实际 %d）", r.Code)
	r = req(clientFollow, "PUT", follower, appPath, body, 10*time.Second, true)
	pr := decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Version == 1, "跟随 307 重定向 → 写入成功（%d %s）", r.Code, trunc(string(r.Body)))
	time.Sleep(500 * time.Millisecond)
	for _, port := range []int{8080, 8081, 8082} {
		e := decode[entryResp](t, req(clientDirect, "GET", port, appPath, nil, 5*time.Second, false))
		check(t, e.Version == 1, "节点(%d) 读到复制数据", port)
	}
	// 307 保留查询串：DELETE expected_version 经 Follower
	r = req(clientFollow, "DELETE", follower, appPath+"?expected_version=1", nil, 10*time.Second, true)
	pr = decode[publishResp](t, r)
	check(t, r.Code == 200 && pr.Status == "deleted", "Follower DELETE 带 expected_version 经 307 → 删除成功（%d）", r.Code)

	section("验收 3：选举窗口内写请求 503 + Retry-After")
	LP := leaderPort()
	procs[LP].kill()
	saw503 := false
	retryAfter := ""
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && !saw503 {
		for _, port := range []int{8080, 8081, 8082} {
			if !healthOK(port) {
				continue
			}
			d, ok := discovery(port)
			if !ok {
				continue
			}
			if d.LeaderAddr == "" || d.Role == "candidate" {
				rr := req(clientDirect, "PUT", port, "/v1/namespaces/dev/configs/elec.test",
					map[string]any{"content": "x", "data_type": "TEXT"}, time.Second, true)
				if rr.Code == 503 {
					saw503 = true
					retryAfter = rr.Header.Get("Retry-After")
					break
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	check(t, saw503, "选举窗口内捕获 503")
	check(t, saw503 && retryAfter == "1", "503 响应带 Retry-After: 1（%q）", retryAfter)

	section("等待重选举，集群恢复可用")
	time.Sleep(4 * time.Second)
	LP2 := leaderPort()
	check(t, LP2 != 0 && LP2 != LP, "重选举完成，新 Leader 在端口 %d", LP2)
	r = putSettledRedirect(follower, "/v1/namespaces/dev/configs/elec.test", map[string]any{"content": "after-election", "data_type": "TEXT"})
	check(t, r.Code == 200, "新 Leader 下写入成功（%d）", r.Code)
	r = req(clientDirect, "GET", LP2, "/v1/namespaces/dev/configs/elec.test", nil, 5*time.Second, false)
	check(t, strings.Contains(string(r.Body), "after-election"), "重选举后数据可读")

	killAll(procs)

	section("验收 4：Seed 全不可达 → 持续退避重试，不自举、不退出")
	cfg4 := cfg("4", 8083, 7949, "config_center_4", "127.0.0.1:59991", "127.0.0.1:59992")
	n4 := startNode(t, cfg4, false, logDir+"/n4.log")
	check(t, waitHealth(8083, 10*time.Second), "节点 4 进程存活且 HTTP 可服务")
	time.Sleep(3 * time.Second)
	check(t, n4.alive(), "3 秒后节点 4 仍在运行（未退出）")
	check(t, fileContains(t, logDir+"/n4.log", "退避重试"), "日志显示退避重试（不自举）")
	n4log := string(readFile(t, logDir+"/n4.log"))
	selfBoot := strings.Contains(n4log, "BootstrapCluster") || strings.Contains(n4log, "已当选 Leader")
	check(t, !selfBoot, "节点 4 未自举（无 Bootstrap/当选记录）")
	n4.kill()

	finish(t)
}

// putSettledRedirect 跟随 307 重定向重复 PUT 直到非 503（选举期间按 503 重试）。
func putSettledRedirect(port int, path string, body any) httpResult {
	var r httpResult
	for range 100 {
		r = req(clientFollow, "PUT", port, path, body, 10*time.Second, true)
		if r.Code != http.StatusServiceUnavailable {
			return r
		}
		time.Sleep(100 * time.Millisecond)
	}
	return r
}

func killAll(procs map[int]*proc) {
	for _, p := range procs {
		p.kill()
	}
}

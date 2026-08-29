//go:build e2e

// 票 10 验收：混沌场景套件（DESIGN §8.2）
// 场景 A：写压测中杀 Leader——新 Leader 秒级产生、已提交写不丢、未提交写明确失败、
//
//	客户端仅凭 307/重试语义恢复。
//
// 场景 B：Follower 分区（进程挂起模拟）——期间其余节点读写正常，恢复后自动追平。
// 注：单机无网络分区手段，分区以进程挂起等价模拟（节点不响应任何请求）；
//
//	期间读旧数据不可观测，以恢复后追平断言替代（见票 10 验收记录）。
package e2e

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTicket10(t *testing.T) {
	begin(t)
	logDir := ticketDir(t, "10")
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

	section("场景 A：写压测中杀 Leader")
	fport := followerPort()
	type writeRec struct {
		id   string
		code string
	}
	var (
		mu   sync.Mutex
		recs []writeRec
		wg   sync.WaitGroup
	)
	writer := func(start, count, port int) {
		defer wg.Done()
		for i := start; i < start+count; i++ {
			id := fmt.Sprintf("chaos-%d", i)
			r := req(clientFollow, "PUT", port, "/v1/namespaces/dev/configs/"+id,
				map[string]any{"content": "c-" + strings.TrimPrefix(id, "chaos-"), "data_type": "TEXT"}, 3*time.Second, true)
			mu.Lock()
			recs = append(recs, writeRec{id, r.codeStr()})
			mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}
	wg.Add(2)
	go writer(1, 100, fport)
	go writer(101, 100, fport)
	time.Sleep(3 * time.Second)

	LP := leaderPort()
	procs[LP].kill()
	killAt := time.Now()
	check(t, true, "压测进行中杀掉 Leader（端口 %d）", LP)

	// 主动探测选举窗口：某节点尚未得知新 Leader 时写请求 → 503 + Retry-After
	saw503 := false
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
				r := req(clientDirect, "PUT", port, "/v1/namespaces/dev/configs/noleader-probe",
					map[string]any{"content": "x", "data_type": "TEXT"}, time.Second, true)
				if r.Code == 503 {
					saw503 = true
					break
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	newLP := 0
	for range 40 {
		if p := leaderPort(); p != 0 && p != LP {
			newLP = p
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Printf("（选举耗时约 %ds，新 Leader 端口 %d）\n", int(time.Since(killAt).Seconds()), newLP)
	check(t, newLP != 0 && newLP != LP, "新 Leader 秒级产生")
	wg.Wait()

	section("验收 1：已提交写全部可读；未提交写明确失败")
	committed, lost := 0, 0
	for _, rec := range recs {
		if rec.code != "200" {
			continue
		}
		committed++
		r := req(clientDirect, "GET", newLP, "/v1/namespaces/dev/configs/"+rec.id, nil, 2*time.Second, false)
		e := decode[entryResp](t, r)
		if r.Code != 200 || e.Content == "" {
			lost++
		}
	}
	check(t, committed > 50, "已提交写 %d 条", committed)
	check(t, lost == 0, "已提交写在新 Leader 上零丢失（丢失 %d 条）", lost)
	failCodes := map[string]bool{}
	for _, rec := range recs {
		if rec.code != "200" {
			failCodes[rec.code] = true
		}
	}
	if len(failCodes) == 0 {
		check(t, false, "压测期间应存在失败写以验证拒绝语义")
	} else {
		undefined := map[string]bool{}
		for c := range failCodes {
			if c != "503" && c != "000" && c != "307" {
				undefined[c] = true
			}
		}
		check(t, len(undefined) == 0, "失败写均为 503/超时/307 未送达（%v）", failCodes)
	}

	section("验收 2：选举期 503，选举后仅凭重试/307 恢复")
	check(t, saw503, "选举窗口写请求 503（主动探测捕获）")
	recovered := true
	for i := 901; i <= 905; i++ {
		r := req(clientFollow, "PUT", fport, fmt.Sprintf("/v1/namespaces/dev/configs/chaos-%d", i),
			map[string]any{"content": fmt.Sprintf("c-%d", i), "data_type": "TEXT"}, 3*time.Second, true)
		if r.Code != 200 {
			recovered = false
		}
	}
	check(t, recovered, "恢复后全新写入 5/5 成功（仅凭重试与 307）")

	section("场景 B：Follower 分区（进程挂起），恢复后自动追平")
	procs[LP] = startNode(t, cfgByPort[LP], false, logByPort[LP])
	check(t, waitVoters(8080, 3, 60*time.Second), "原 Leader 重启归队，恢复 3 voter")

	fpart := followerPort()
	target := procs[fpart]
	suspendProcess(t, target.pid())
	time.Sleep(1 * time.Second)
	check(t, !healthOK(fpart), "分区生效：Follower(端口 %d) 不响应", fpart)
	alive := alivePort()
	partOK := 0
	for i := 1; i <= 5; i++ {
		r := req(clientFollow, "PUT", alive, fmt.Sprintf("/v1/namespaces/dev/configs/partition-%d", i),
			map[string]any{"content": fmt.Sprintf("during-partition-%d", i), "data_type": "TEXT"}, 3*time.Second, true)
		if r.Code == 200 {
			partOK++
		}
		time.Sleep(1 * time.Second)
	}
	check(t, partOK == 5, "分区期间其余节点写入 5/5（quorum 2/3 存活），实际 %d", partOK)
	time.Sleep(25 * time.Second)
	resumeProcess(t, target.pid())
	caught := waitCond(75*time.Second, 500*time.Millisecond, func() bool {
		for i := 1; i <= 5; i++ {
			r := req(clientDirect, "GET", fpart, fmt.Sprintf("/v1/namespaces/dev/configs/partition-%d", i), nil, 2*time.Second, false)
			if !strings.Contains(string(r.Body), fmt.Sprintf("during-partition-%d", i)) {
				return false
			}
		}
		return true
	})
	check(t, caught, "恢复后 Follower 自动追平分区期间全部写入")

	killAll(procs)
	finish(t)
}

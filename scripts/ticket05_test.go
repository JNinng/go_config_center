//go:build e2e

// 票 05 验收：Watch 长轮询
// 覆盖 .scratch/config-center-mvp/issues/05-watch-long-polling.md 全部验收项。
package e2e

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestTicket05(t *testing.T) {
	begin(t)
	cfg := singleNodeConfig(t)
	const port = 8080
	const ns, id = "dev", "db.password"
	entryPath := fmt.Sprintf("/v1/namespaces/%s/configs/%s", ns, id)
	watchURL := func(dataID string, v int, timeout string) string {
		return fmt.Sprintf("%s/v1/namespaces/%s/configs/%s/watch?v=%d&timeout=%s", httpBase(port), ns, dataID, v, timeout)
	}

	section("构建与准备")
	os.RemoveAll("data")
	dropTables(t, pgDB)

	n := startNode(t, cfg, true, ticketLog(t, "05-n1.log"))
	if !waitHealth(port, 6*time.Second) {
		t.Fatal("节点未就绪")
	}
	check(t, healthOK(port), "服务就绪")

	r := putSettled(port, entryPath, map[string]any{"content": "watch-v1", "data_type": "TEXT"})
	check(t, r.Code == 200, "发布 v1（%d）", r.Code)

	section("验收 4（前半）：v 已是最新 → 挂起等待，至 timeout 收场；验收 3：timeout 回显 v 供原样重挂")
	start := time.Now()
	r = tryHTTP(clientDirect, "GET", watchURL(id, 1, "1s"), nil, 10*time.Second, false)
	ms := time.Since(start).Milliseconds()
	w := decode[watchResp](t, r)
	check(t, r.Code == 200 && w.Status == "timeout" && w.Version == 1,
		"v=1 最新 → timeout 状态 + version=1 回显（%dms %s）", ms, trunc(string(r.Body)))
	check(t, ms >= 900, "确实挂起了 timeout 时长（%dms ≥ 900ms）", ms)

	section("验收 4（后半）：v 落后 → 立即返回当前状态")
	start = time.Now()
	r = tryHTTP(clientDirect, "GET", watchURL(id, 0, "10s"), nil, 15*time.Second, false)
	ms = time.Since(start).Milliseconds()
	w = decode[watchResp](t, r)
	check(t, w.Status == "active" && w.Entry != nil && w.Entry.Content == "watch-v1" && w.Entry.Version == 1,
		"v=0 落后 → 立即 active + v1 条目（%dms）", ms)
	check(t, ms < 2000, "立即返回（%dms < 2s）", ms)

	section("验收 1：挂起期间 Publish → 立即返回 active + 新条目")
	start = time.Now()
	ch := make(chan httpResult, 1)
	go func() { ch <- tryHTTP(clientDirect, "GET", watchURL(id, 1, "10s"), nil, 15*time.Second, false) }()
	time.Sleep(400 * time.Millisecond)
	putJSON(port, entryPath, map[string]any{"content": "watch-v2", "data_type": "TEXT"})
	r = <-ch
	ms = time.Since(start).Milliseconds()
	w = decode[watchResp](t, r)
	check(t, w.Status == "active" && w.Entry != nil && w.Entry.Content == "watch-v2" && w.Entry.Version == 2,
		"挂起被 Publish 唤醒 → active + v2 条目（%s）", trunc(string(r.Body)))
	check(t, ms < 4000, "唤醒及时（%dms < 4s）", ms)

	section("验收 2：挂起期间 Delete → 立即返回 deleted + 墓碑版本")
	start = time.Now()
	ch = make(chan httpResult, 1)
	go func() { ch <- tryHTTP(clientDirect, "GET", watchURL(id, 2, "10s"), nil, 15*time.Second, false) }()
	time.Sleep(400 * time.Millisecond)
	req(clientDirect, "DELETE", port, entryPath, nil, 10*time.Second, true)
	r = <-ch
	ms = time.Since(start).Milliseconds()
	w = decode[watchResp](t, r)
	check(t, w.Status == "deleted" && w.Version == 3, "挂起被 Delete 唤醒 → deleted + 墓碑版本 3（%s）", trunc(string(r.Body)))
	check(t, ms < 4000, "唤醒及时（%dms < 4s）", ms)

	section("验收 3（补充）：不存在的条目挂起等待创建，超时收场")
	r = tryHTTP(clientDirect, "GET", watchURL("ghost.id", 0, "500ms"), nil, 10*time.Second, false)
	w = decode[watchResp](t, r)
	check(t, w.Status == "timeout", "ghost 条目 → timeout（%d）", r.Code)

	section("验收 5：服务端关闭时挂起连接断连收场，不悬挂")
	ch = make(chan httpResult, 1)
	go func() { ch <- tryHTTP(clientDirect, "GET", watchURL(id, 3, "30s"), nil, 10*time.Second, false) }()
	time.Sleep(500 * time.Millisecond)
	start = time.Now()
	n.kill()
	r = <-ch
	ms = time.Since(start).Milliseconds()
	check(t, r.Code == 0, "服务端被杀后挂起请求以错误收场（code=%s）", r.codeStr())
	check(t, ms < 5000, "断连及时（%dms < 5s），未悬挂到 30s timeout", ms)

	finish(t)
}

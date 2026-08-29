//go:build e2e

// 票 12 验收：10 万条规模化验证（DESIGN §1.1、§8.3、ADR-0007 量级承诺）
// 10 万条 Config Entry（含 History）下：快照生成时长/体积记录、冷重启 ≤10s 断言、
// 空节点经 InstallSnapshot 追平且读一致。测试失败 = 存在未通过项。
package e2e

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTicket12(t *testing.T) {
	begin(t)
	const (
		total   = 100000
		workers = 20
		per     = total / workers
		expect  = total + 1 // warmup 键占 1 条
	)
	section("构建与准备")
	os.RemoveAll("data-1")
	os.RemoveAll("data-2")
	recreateDBs(t, "config_center_1", "config_center_2")
	cfg1 := ticketLog(t, "12-config-1.yaml")
	cfg2 := ticketLog(t, "12-config-2.yaml")
	writeConfig(t, cfg1, "1", 8080, 7946, "config_center_1", nil, "")
	writeConfig(t, cfg2, "2", 8081, 7947, "config_center_2", []string{"127.0.0.1:8080"}, "")
	log1 := ticketLog(t, "12-n1.log")
	log2 := ticketLog(t, "12-n2.log")

	section("灌入 %d 条（%d 并发 worker × %d 条）", total, workers, per)
	n1 := startNode(t, cfg1, true, log1)
	if !waitHealth(8080, 10*time.Second) {
		t.Fatal("节点 1 未就绪")
	}
	warmOK := false
	for range 300 {
		r := req(clientDirect, "PUT", 8080, "/v1/namespaces/dev/configs/warmup",
			map[string]any{"content": "w", "data_type": "TEXT"}, 5*time.Second, true)
		if r.Code == 200 {
			warmOK = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	check(t, warmOK, "服务就绪")

	var (
		mu     sync.Mutex
		failed []string
		wg     sync.WaitGroup
	)
	startLoad := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w*per + 1; i <= (w+1)*per; i++ {
				id := "load-" + strconv.Itoa(i)
				r := req(clientDirect, "PUT", 8080, "/v1/namespaces/scale/configs/"+id,
					map[string]any{"content": "payload-" + strconv.Itoa(i), "data_type": "TEXT"}, 10*time.Second, true)
				if r.Code != 200 {
					mu.Lock()
					failed = append(failed, id+" "+r.codeStr())
					mu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("（灌数耗时 %dms）\n", time.Since(startLoad).Milliseconds())
	check(t, len(failed) == 0, "全部写入成功（失败 %d 条）", len(failed))

	count1 := psqlQ(t, "config_center_1", "SELECT count(*) FROM biz_configs")
	hist1 := psqlQ(t, "config_center_1", "SELECT count(*) FROM biz_config_history")
	check(t, count1 == strconv.Itoa(expect), "biz_configs = %d 条（实际 %s）", expect, count1)
	check(t, hist1 == strconv.Itoa(expect), "biz_config_history = %d 条（实际 %s）", expect, hist1)

	section("验收 1：快照生成成功，时长与体积记录")
	startSnap := time.Now()
	r := req(clientDirect, "POST", 8080, "/v1/cluster/snapshot", nil, 60*time.Second, true)
	snapMs := time.Since(startSnap).Milliseconds()
	snap := decode[snapshotResp](t, r)
	check(t, r.Code == 200 && snap.Status == "ok", "手动快照成功（%dms，%s）", snapMs, trunc(string(r.Body)))
	snapSize := 0
	for _, f := range snapshotFiles(t, "data-1") {
		if fi, err := os.Stat(f); err == nil && fi.Size() > int64(snapSize) {
			snapSize = int(fi.Size())
		}
	}
	fmt.Printf("SNAPSHOT_RECORD: duration_ms=%d size_bytes=%d size_mb=%dMB entries=%d\n",
		snapMs, snapSize, snapSize/1024/1024, total)

	section("验收 2：冷重启 ≤10s（10 万条数据）")
	n1.kill()
	time.Sleep(500 * time.Millisecond)
	startCold := time.Now()
	n1 = startNode(t, cfg1, false, log1)
	coldOK := waitCond(10*time.Second, 100*time.Millisecond, func() bool {
		if !healthOK(8080) {
			return false
		}
		r := req(clientDirect, "GET", 8080, "/v1/namespaces/scale/configs/load-50000", nil, 2*time.Second, false)
		return strings.Contains(string(r.Body), "payload-50000")
	})
	coldMs := time.Since(startCold).Milliseconds()
	check(t, coldOK && coldMs <= 10000, "冷重启到可读 %d ms ≤ 10s（ok=%v）", coldMs, coldOK)

	section("验收 3：空节点规模下经 InstallSnapshot 追平，读一致")
	n2 := startNode(t, cfg2, false, log2)
	waitHealth(8081, 10*time.Second)
	joined := waitCond(300*time.Second, 500*time.Millisecond, func() bool {
		c := psqlQ(t, "config_center_2", "SELECT count(*) FROM biz_configs")
		return c == strconv.Itoa(expect)
	})
	if joined {
		check(t, true, "新节点 PG 物化 %d 条", expect)
	} else {
		c := psqlQ(t, "config_center_2", "SELECT count(*) FROM biz_configs")
		check(t, false, "新节点物化 %s/%d", c, expect)
	}
	installEvidence := fileCount(t, log2, "install") + fileCount(t, log2, "Install")
	restoreFull := fileCount(t, log2, "全量导入完成")
	fmt.Printf("（新节点日志：install=%d 全量导入=%d）\n", installEvidence, restoreFull)

	// 抽样更密：每 100 条取一个 key，含首尾，共 1001 个样本点比对两节点。
	samples := []int{1}
	for i := 101; i <= 99901; i += 100 {
		samples = append(samples, i)
	}
	samples = append(samples, 100000)
	mismatch := 0
	for _, i := range samples {
		path := fmt.Sprintf("/v1/namespaces/scale/configs/load-%d", i)
		lb := decode[entryResp](t, req(clientDirect, "GET", 8080, path, nil, 5*time.Second, false))
		fb := decode[entryResp](t, req(clientDirect, "GET", 8081, path, nil, 5*time.Second, false))
		if lb.Content != fb.Content || lb.Version != fb.Version {
			mismatch++
		}
	}
	check(t, mismatch == 0, "抽样 %d 条两节点读一致（不一致 %d 条）", len(samples), mismatch)

	n1.kill()
	n2.kill()
	finish(t)
}

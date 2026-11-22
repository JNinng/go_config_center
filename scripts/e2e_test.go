//go:build e2e

// e2e 验收套件（DESIGN §8 验证策略的可执行部分）：每个 TestTicketNN 对应
// .scratch/config-center-mvp/issues/ 里的一张票，逐项断言该票的验收条件。
//
// 契约：
//   - 构建标签 e2e：无标签时本目录不参与任何编译，验收代码永不进入生产二进制。
//   - 黑盒：只经 HTTP API、psql 与被测二进制交互，不 import internal/*。
//   - TestMain 把工作目录切到仓库根并构建 ./config-center.exe；配置一律由套件
//     生成（singleNodeConfig/writeConfig），仓库根 config.yaml 不被读取或改动。
//   - 每张票结束（含失败）由 begin 注册的 clearFixtures 清空全部 data 目录与
//     测试库（config_center_1..4）；GCC_E2E_KEEP=1 跳过清理保留现场。
//
// 前置条件、运行命令与输出约定见 scripts/README.md。
package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	adminToken = "change-me"
	binName    = "config-center.exe"
	pgDB       = "config_center_1"
)

var (
	// clientDirect 不跟随重定向（观测 307 原始响应）。
	clientDirect = &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	// clientFollow 跟随重定向（与 clientDirect 相对，写 Follower 后取最终结果）。
	clientFollow = &http.Client{}
	logDir       = filepath.Join(os.TempDir(), "gcc-e2e-go")

	// fixtureDataDirs 与 fixtureDBs 是套件的全部持久化足迹：每张票结束清空，
	// 不让遗留 lineage 泄入下一次运行。
	fixtureDataDirs = []string{"data", "data-1", "data-2", "data-3", "data-4"}
	fixtureDBs      = []string{"config_center_1", "config_center_2", "config_center_3", "config_center_4"}
)

func TestMain(m *testing.M) {
	if err := os.Chdir(".."); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: 切换到仓库根: %v\n", err)
		os.Exit(1)
	}
	out, err := exec.Command("go", "build", "-o", binName, "./cmd/config-center").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: go build: %v\n%s", err, out)
		os.Exit(1)
	}
	fmt.Println("PASS: go build")
	os.Exit(m.Run())
}

// —— 断言与输出：每条断言一行 PASS:/FAIL:，结尾汇总 ——

func check(t *testing.T, ok bool, format string, args ...any) {
	t.Helper()
	msg := fmt.Sprintf(format, args...)
	if ok {
		fmt.Printf("PASS: %s\n", msg)
		return
	}
	fmt.Printf("FAIL: %s\n", msg)
	t.Fail()
}

func section(format string, args ...any) {
	fmt.Printf("=== %s ===\n", fmt.Sprintf(format, args...))
}

// begin 每张票的测试入口先调用：t.Setenv 使 go test 永不缓存本包结果；并注册
// 收场清理（clearFixtures）——t.Cleanup 为 LIFO，各 startProc 的进程强杀先执行，
// 再清 data 目录与 PG 库，票失败同样清场。GCC_E2E_KEEP=1 跳过清理保留现场排查。
func begin(t *testing.T) {
	t.Helper()
	t.Setenv("GCC_E2E_TICKET", t.Name())
	if os.Getenv("GCC_E2E_KEEP") == "" {
		t.Cleanup(func() { clearFixtures(t) })
	}
}

// clearFixtures 清空套件足迹：data 目录与测试库。Windows 下进程退出后句柄释放
// 可能滞后，目录删除带重试；清理失败仅告警不倒灌 FAIL——下次运行的准备阶段
// （RemoveAll/dropTables/recreateDBs）兜底。
func clearFixtures(t *testing.T) {
	t.Helper()
	for _, dir := range fixtureDataDirs {
		var err error
		for range 5 {
			if err = os.RemoveAll(dir); err == nil {
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		if err != nil {
			fmt.Printf("WARN: 清理 %s: %v\n", dir, err)
		}
	}
	for _, db := range fixtureDBs {
		dropFixtureDB(t, db)
	}
	section("收场清理：data 目录与测试库已清空")
}

// dropFixtureDB 删除单个测试库；不存在为幂等空操作。
func dropFixtureDB(t *testing.T, db string) {
	if _, err := psql("postgres", "DROP DATABASE IF EXISTS "+db+" WITH (FORCE)"); err != nil {
		fmt.Printf("WARN: 清理 PG 库 %s: %v\n", db, err)
	}
}

// singleNodeConfig 生成单节点组的临时配置（http 8080 / raft 7946 / data 目录 data/
// / 库 config_center_1），与多节点组同机制——套件不读写仓库根 config.yaml，
// 用户本地配置与测试足迹互不污染。
func singleNodeConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(logDir, "config-single.yaml")
	var b strings.Builder
	fmt.Fprintf(&b, "http_addr: \"127.0.0.1:8080\"\n")
	fmt.Fprintf(&b, "raft_addr: \"127.0.0.1:7946\"\n")
	fmt.Fprintf(&b, "data_dir: \"./data\"\n")
	fmt.Fprintf(&b, "admin_token: %q\n", adminToken)
	fmt.Fprintf(&b, "pg_dsn: \"host=127.0.0.1 port=5432 user=postgresql dbname=%s sslmode=disable\"\n", pgDB)
	fmt.Fprintf(&b, "http_advertise: \"http://127.0.0.1:8080\"\n")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("写配置 %s: %v", path, err)
	}
	return path
}

func finish(t *testing.T) {
	t.Helper()
	if t.Failed() {
		fmt.Println("存在失败项")
	} else {
		fmt.Println("全部验收项通过")
	}
}

func trunc(s string) string {
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}

// ticketDir 返回本票的子目录（如 "08/"），并确保存在。
func ticketDir(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(logDir, name)
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("创建目录 %s: %v", path, err)
	}
	return path
}

// ticketLog 返回本票某节点的日志文件路径（按票号前缀命名，互不覆盖），并确保父目录存在。
func ticketLog(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(logDir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("创建日志目录 %s: %v", filepath.Dir(path), err)
	}
	return path
}

func fileContains(t *testing.T, path, substr string) bool {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return strings.Contains(string(b), substr)
}

func fileCount(t *testing.T, path, substr string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return strings.Count(string(b), substr)
}

// —— psql ——

func psql(db, query string) (string, error) {
	out, err := exec.Command("psql", "-h", "127.0.0.1", "-U", "postgresql", "-d", db, "-tAc", query).Output()
	return strings.TrimSpace(string(out)), err
}

func psqlQ(t *testing.T, db, query string) string {
	t.Helper()
	out, err := psql(db, query)
	if err != nil {
		t.Fatalf("psql [%s] 失败: %v", query, err)
	}
	return out
}

// dropTables 清空单节点组的库表；库本身不存在（被上轮收场清理删去）则无事可清，
// 直接返回。
func dropTables(t *testing.T, db string) {
	t.Helper()
	if out, err := psql("postgres", "SELECT 1 FROM pg_database WHERE datname='"+db+"'"); err != nil || out != "1" {
		return
	}
	if _, err := psql(db, "DROP TABLE IF EXISTS biz_configs, biz_config_history, raft_fsm_meta CASCADE"); err != nil {
		t.Fatalf("清空 %s 表: %v", db, err)
	}
}

// recreateDBs 删并重建多节点组的库（DROP DATABASE IF EXISTS ... WITH (FORCE)，
// 保证无残留连接阻断）。
func recreateDBs(t *testing.T, dbs ...string) {
	t.Helper()
	for _, db := range dbs {
		if _, err := psql("postgres", "DROP DATABASE IF EXISTS "+db+" WITH (FORCE)"); err != nil {
			t.Fatalf("删库 %s: %v", db, err)
		}
		if _, err := psql("postgres", "CREATE DATABASE "+db); err != nil {
			t.Fatalf("建库 %s: %v", db, err)
		}
	}
}

// —— HTTP ——

type httpResult struct {
	Code   int // 0 = 连接失败/超时等传输层错误，未收到 HTTP 响应
	Body   []byte
	Header http.Header
	Err    error
}

func (r httpResult) codeStr() string {
	if r.Code == 0 {
		return "000"
	}
	return fmt.Sprintf("%d", r.Code)
}

func tryHTTP(client *http.Client, method, url string, body any, timeout time.Duration, authed bool) httpResult {
	var payload []byte
	if body != nil {
		if b, ok := body.([]byte); ok {
			payload = b
		} else {
			payload, _ = json.Marshal(body)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
	if err != nil {
		return httpResult{Err: err}
	}
	if authed {
		req.Header.Set("Authorization", "Bearer "+adminToken)
	}
	if len(payload) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return httpResult{Err: err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return httpResult{Code: resp.StatusCode, Err: err}
	}
	return httpResult{Code: resp.StatusCode, Body: data, Header: resp.Header}
}

func httpBase(port int) string { return fmt.Sprintf("http://127.0.0.1:%d", port) }

func req(client *http.Client, method string, port int, path string, body any, timeout time.Duration, authed bool) httpResult {
	return tryHTTP(client, method, httpBase(port)+path, body, timeout, authed)
}

// reqBearer 以任意 Authorization 头发请求（错 token 的 401 断言用）。
func reqBearer(method, url string, body any, timeout time.Duration, bearer string) httpResult {
	var payload []byte
	if body != nil {
		payload, _ = json.Marshal(body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
	if err != nil {
		return httpResult{Err: err}
	}
	hreq.Header.Set("Authorization", bearer)
	if len(payload) > 0 {
		hreq.Header.Set("Content-Type", "application/json")
	}
	resp, err := clientDirect.Do(hreq)
	if err != nil {
		return httpResult{Err: err}
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return httpResult{Code: resp.StatusCode, Body: data, Header: resp.Header}
}

// putJSON 无等待的带认证 PUT。
func putJSON(port int, path string, body any) httpResult {
	return req(clientDirect, "PUT", port, path, body, 10*time.Second, true)
}

// putSettled 重复 PUT 直到响应不再是 503（选举完成前写被拒且不产生日志）。
func putSettled(port int, path string, body any) httpResult {
	var r httpResult
	for range 100 {
		r = req(clientDirect, "PUT", port, path, body, 10*time.Second, true)
		if r.Code != http.StatusServiceUnavailable {
			return r
		}
		time.Sleep(100 * time.Millisecond)
	}
	return r
}

// publish 重复 PUT 直到 200（TEXT 类型，票 02 等单节点场景）。
func publish(port int, ns, id, content string) bool {
	for range 100 {
		r := req(clientDirect, "PUT", port, fmt.Sprintf("/v1/namespaces/%s/configs/%s", ns, id),
			map[string]any{"content": content, "data_type": "TEXT"}, 5*time.Second, true)
		if r.Code == 200 {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func entryReadable(port int, ns, id string) bool {
	return req(clientDirect, "GET", port, fmt.Sprintf("/v1/namespaces/%s/configs/%s", ns, id), nil, time.Second, false).Code == 200
}

// decode 把 JSON 响应体解析为 T；解析失败计一次 FAIL 并返回零值（后续断言自然落败）。
func decode[T any](t *testing.T, r httpResult) (v T) {
	t.Helper()
	if err := json.Unmarshal(r.Body, &v); err != nil {
		fmt.Printf("FAIL: 响应解析失败：%v（body=%q）\n", err, trunc(string(r.Body)))
		t.Fail()
	}
	return v
}

// —— 常用响应结构（键名与 internal/api 的渲染一致） ——

type publishResp struct {
	Version        int64  `json:"version"`
	Created        bool   `json:"created"`
	Status         string `json:"status"`
	Error          string `json:"error"`
	CurrentVersion int64  `json:"current_version"`
	Message        string `json:"message"`
}

type entryResp struct {
	Namespace string            `json:"namespace"`
	DataID    string            `json:"data_id"`
	DataType  string            `json:"data_type"`
	Content   string            `json:"content"`
	Version   int64             `json:"version"`
	Tags      map[string]string `json:"tags"`
}

type versionRecord struct {
	Version int64   `json:"version"`
	Op      string  `json:"op"`
	Content *string `json:"content"`
}

type watchResp struct {
	Status  string     `json:"status"`
	Version int64      `json:"version"`
	Entry   *entryResp `json:"entry"`
}

type discoveryInfo struct {
	NodeID     string `json:"node_id"`
	Role       string `json:"role"`
	LeaderAddr string `json:"leader_addr"`
	LeaderHTTP string `json:"leader_http_addr"`
}

type memberInfo struct {
	NodeID   string `json:"node_id"`
	Addr     string `json:"addr"`
	Suffrage string `json:"suffrage"`
}

type snapshotResp struct {
	Status        string `json:"status"`
	SnapshotIndex uint64 `json:"snapshot_index"`
}

// —— 就绪等待与集群视角 ——

func waitCond(timeout, step time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(step)
	}
	return cond()
}

func healthOK(port int) bool {
	return tryHTTP(clientDirect, "GET", httpBase(port)+"/healthz", nil, time.Second, false).Code == 200
}

func waitHealth(port int, timeout time.Duration) bool {
	return waitCond(timeout, 50*time.Millisecond, func() bool { return healthOK(port) })
}

func discovery(port int) (discoveryInfo, bool) {
	r := tryHTTP(clientDirect, "GET", httpBase(port)+"/internal/discovery", nil, time.Second, false)
	if r.Code != 200 {
		return discoveryInfo{}, false
	}
	var d discoveryInfo
	if json.Unmarshal(r.Body, &d) != nil {
		return discoveryInfo{}, false
	}
	return d, true
}

// voterCount 经 /v1/cluster/status 数 voter；请求失败返回 -1。
func voterCount(port int) int {
	r := req(clientDirect, "GET", port, "/v1/cluster/status", nil, 2*time.Second, true)
	if r.Code != 200 {
		return -1
	}
	var s struct {
		Members []memberInfo `json:"members"`
	}
	if json.Unmarshal(r.Body, &s) != nil {
		return -1
	}
	n := 0
	for _, m := range s.Members {
		if m.Suffrage == "Voter" {
			n++
		}
	}
	return n
}

func waitVoters(port, want int, timeout time.Duration) bool {
	return waitCond(timeout, 200*time.Millisecond, func() bool { return voterCount(port) == want })
}

func leaderPort() int   { return portWithRole("leader") }
func followerPort() int { return portWithRole("follower") }

// portWithRole 扫描多节点组端口 8080~8082，返回角色匹配且 /healthz 200 的节点
// HTTP 端口；找不到返回 0（leaderPort/followerPort 均基于此）。
func portWithRole(role string) int {
	for _, port := range []int{8080, 8081, 8082} {
		if healthOK(port) {
			if d, ok := discovery(port); ok && d.Role == role {
				return port
			}
		}
	}
	return 0
}

func alivePort() int {
	for _, port := range []int{8080, 8081, 8082} {
		if healthOK(port) {
			return port
		}
	}
	return 0
}

// —— 被测进程管理 ——

type proc struct {
	cmd    *exec.Cmd
	exited chan struct{}
	code   int
}

func startNode(t *testing.T, cfgPath string, bootstrap bool, logPath string) *proc {
	t.Helper()
	args := []string{"--config", cfgPath}
	if bootstrap {
		args = append(args, "--bootstrap")
	}
	return startProc(t, args, logPath)
}

func startProc(t *testing.T, args []string, logPath string) *proc {
	t.Helper()
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("打开日志 %s: %v", logPath, err)
	}
	cmd := exec.Command("./"+binName, args...)
	cmd.Stdout, cmd.Stderr = f, f
	p := &proc{cmd: cmd, exited: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		f.Close()
		t.Fatalf("启动 %v: %v", args, err)
	}
	go func() {
		p.code = exitCode(cmd.Wait())
		f.Close()
		close(p.exited)
	}()
	t.Cleanup(p.kill)
	return p
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
}

// kill 强杀进程并等待退出收场；进程已退出时为空操作。
func (p *proc) kill() {
	select {
	case <-p.exited:
		return
	default:
	}
	_ = p.cmd.Process.Kill()
	select {
	case <-p.exited:
	case <-time.After(10 * time.Second):
	}
}

func (p *proc) alive() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

// waitExit 等待进程自行退出，返回退出码；超时返回 (−1, false)。
func (p *proc) waitExit(timeout time.Duration) (int, bool) {
	select {
	case <-p.exited:
		return p.code, true
	case <-time.After(timeout):
		return -1, false
	}
}

func (p *proc) pid() int { return p.cmd.Process.Pid }

// runForeground 前台运行二进制直至退出，返回退出码与合并输出。
func runForeground(t *testing.T, args ...string) (int, string) {
	t.Helper()
	out, err := exec.Command("./"+binName, args...).CombinedOutput()
	return exitCode(err), string(out)
}

// writeConfig 生成节点配置文件；raftExtra 附加在文件尾（票 11 的小窗口调参），
// seeds 为空时省略。
func writeConfig(t *testing.T, path, id string, httpPort, raftPort int, db string, seeds []string, raftExtra string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "http_addr: \"127.0.0.1:%d\"\n", httpPort)
	fmt.Fprintf(&b, "raft_addr: \"127.0.0.1:%d\"\n", raftPort)
	fmt.Fprintf(&b, "data_dir: \"./data-%s\"\n", id)
	fmt.Fprintf(&b, "admin_token: %q\n", adminToken)
	fmt.Fprintf(&b, "pg_dsn: \"host=127.0.0.1 port=5432 user=postgresql dbname=%s sslmode=disable\"\n", db)
	fmt.Fprintf(&b, "http_advertise: \"http://127.0.0.1:%d\"\n", httpPort)
	if len(seeds) > 0 {
		b.WriteString("seeds:\n")
		for _, s := range seeds {
			fmt.Fprintf(&b, "  - %q\n", s)
		}
	}
	b.WriteString(raftExtra)
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("写配置 %s: %v", path, err)
	}
}

// —— 进程挂起/恢复（票 10/11 的分区模拟，Windows 专属路径） ——

func suspendProcess(t *testing.T, pid int) { ntProcess(t, "NtSuspendProcess", pid) }

func resumeProcess(t *testing.T, pid int) { ntProcess(t, "NtResumeProcess", pid) }

func ntProcess(t *testing.T, fn string, pid int) {
	t.Helper()
	script := fmt.Sprintf(`Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; public static class PS { [DllImport("ntdll.dll")] public static extern int %[1]s(IntPtr h); }'
$p = Get-Process -Id %[2]d
[PS]::%[1]s($p.Handle) | Out-Null`, fn, pid)
	if out, err := exec.Command("powershell", "-NoProfile", "-Command", script).CombinedOutput(); err != nil {
		t.Fatalf("%s(pid=%d): %v\n%s", fn, pid, err, out)
	}
}

// —— 快照文件与校验 ——

// snapshotFiles 返回 dataDir/raft/snapshots 下全部 state 文件（state.bin/state 后缀）。
func snapshotFiles(t *testing.T, dataDir string) []string {
	t.Helper()
	root := filepath.Join(dataDir, "raft", "snapshots")
	var out []string
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".state") || strings.HasSuffix(d.Name(), ".bin") {
			out = append(out, path)
		}
		return nil
	})
	return out
}

// newestSnapshotFile 取修改时间最新的快照 state 文件。
func newestSnapshotFile(t *testing.T, dataDir string) string {
	t.Helper()
	var newest string
	var newestAt time.Time
	for _, p := range snapshotFiles(t, dataDir) {
		fi, err := os.Stat(p)
		if err != nil {
			continue
		}
		if newest == "" || fi.ModTime().After(newestAt) {
			newest, newestAt = p, fi.ModTime()
		}
	}
	return newest
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s: %v", path, err)
	}
	return b
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

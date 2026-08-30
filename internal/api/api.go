// Package api 提供 HTTP 接口（DESIGN §5）：写走 Raft 提案并回传 Apply 结果，读本机 PG。
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
	"github.com/jninng/go_config_center/internal/watch"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// 提案失败原因，由 Proposer 实现方返回，映射为 DESIGN §4.1 的响应语义。
var (
	ErrNoLeader             = errors.New("当前无 Leader（选举中或已失主）")
	ErrRaftDown             = errors.New("Raft 已关闭")
	ErrProposeTimeout       = errors.New("提案未在期限内提交")
	ErrNothingNewToSnapshot = errors.New("快照点之后没有新日志，无需快照")
)

// Proposer 将一条日志信封经 Raft 提交，等待该日志应用完成后取回结果（DESIGN §3.3）。
type Proposer interface {
	Propose(ctx context.Context, cmd []byte) (raftlog.Result, error)
}

// SnapshotTrigger 手动触发 FSM 快照，返回快照点 index。
type SnapshotTrigger func() (uint64, error)

// ClusterInfo 是本节点集群视角的瞬时快照（DESIGN §5.3）。
type ClusterInfo struct {
	NodeID      string
	Role        string // leader | follower | candidate | shutdown
	Term        uint64
	LeaderID    string
	LeaderAddr  string // Leader 的 Raft 地址
	LeaderHTTP  string // 本节点已知的 Leader HTTP 基地址（可为空；自身为 Leader 时即通告地址）
	LastApplied uint64
	Restoring   bool

	// 本节点 Raft 指标（DESIGN §5.2 状态卡与 §5.3 成员自报共用）。
	LastLogIndex uint64 // Raft 日志末端索引
	CommitIndex  uint64 // 已提交索引
	// LastContactMS 是到 Leader 的最近心跳距今毫秒数；0=自身为 Leader，-1=从未收到心跳。
	LastContactMS int64
}

// MemberInfo 是成员表中的一条成员（DESIGN §5.2）。
type MemberInfo struct {
	NodeID   string `json:"node_id"`
	Addr     string `json:"addr"`
	Suffrage string `json:"suffrage"` // Voter | Staging | Nonvoter

	// 以下为尽力而为的自报信息：本节点行直接取本地指标，其余成员经
	// /internal/discovery 探测填充；字段缺省 = 本节点视角未知（地址未知或探测失败）。
	Role          string `json:"role,omitempty"` // leader | follower | candidate
	Term          uint64 `json:"term,omitempty"`
	LastLogIndex  uint64 `json:"last_log_index,omitempty"`
	CommitIndex   uint64 `json:"commit_index,omitempty"`
	AppliedIndex  uint64 `json:"applied_index,omitempty"`
	LastContactMS int64  `json:"last_contact_ms,omitempty"` // 语义同 ClusterInfo.LastContactMS
	Online        *bool  `json:"online,omitempty"`          // nil=未知；true=自报成功；false=探测无响应
}

// JoinFunc 在 Leader 上执行成员加入（AddVoter，DESIGN §4.5）。
// exists=true 表示该 Node ID 已是成员（幂等）。
type JoinFunc func(nodeID, raftAddr, httpAddr string) (exists bool, err error)

// RemoveFunc 在 Leader 上执行成员移除（DESIGN §4.5）。护栏不满足返回 ErrQuorumGuard，
// 目标非成员返回 ErrNotMember。
type RemoveFunc func(nodeID string) error

// 成员移除的失败语义（DESIGN §5.2）。
var (
	ErrNotMember   = errors.New("目标不是集群成员")
	ErrQuorumGuard = errors.New("移除后剩余在线 voter 不足 quorum，拒绝移除")
)

// Deps 聚合 Server 的外部依赖；可选依赖缺省时按单节点语义兜底。
type Deps struct {
	Proposer    Proposer
	Store       store.Store
	Hub         *watch.Hub
	AdminToken  string
	Ready       func() bool
	Snapshot    SnapshotTrigger
	ClusterInfo func() ClusterInfo
	Members     func() ([]MemberInfo, error)
	Join        JoinFunc
	Remove      RemoveFunc
	// Console 是控制台的静态资源 handler（ADR-0012），注册为 GET / 通配兜底；
	// nil 时不挂载（未匹配路径交由 ServeMux 默认 404）。
	Console http.Handler
}

// Watch 挂起时长边界（DESIGN §4.3：默认 30s，客户端原样重挂）。
const (
	defaultWatchTimeout = 30 * time.Second
	maxWatchTimeout     = 5 * time.Minute
)

type Server struct {
	proposer   Proposer
	store      store.Store
	hub        *watch.Hub
	adminToken string
	ready      func() bool
	snapshot   SnapshotTrigger
	cluster    func() ClusterInfo
	members    func() ([]MemberInfo, error)
	join       JoinFunc
	remove     RemoveFunc
	console    http.Handler
}

func New(d Deps) *Server {
	return &Server{
		proposer: d.Proposer, store: d.Store, hub: d.Hub,
		adminToken: d.AdminToken, ready: d.Ready, snapshot: d.Snapshot,
		cluster: d.ClusterInfo, members: d.Members, join: d.Join, remove: d.Remove,
		console: d.Console,
	}
}

// currentCluster 返回集群视角快照；未注入 ClusterInfo 时按单节点 Leader 兜底。
func (s *Server) currentCluster() ClusterInfo {
	if s.cluster == nil {
		return ClusterInfo{Role: "leader"}
	}
	return s.cluster()
}

// httpRequestDuration 按 ServeMux 路由模式记录 API 延迟（DESIGN §7）。
var httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "http_request_duration_seconds",
	Help:    "API 请求延迟",
	Buckets: prometheus.DefBuckets,
}, []string{"route"})

// withMetrics 记录每路由请求延迟；route 为匹配到的路由模式（含 {} 占位符）。
func withMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		httpRequestDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}

// withCORS 为全部响应附加受限 CORS 头（ADR-0012）：控制台可从任意节点加载，
// 其写请求经 307 跳 Leader 属跨源请求，浏览器要求目标响应带 CORS 头才放行携带
// Authorization 重发。token 经请求头携带而非 cookie，故通配来源不引入凭据语义；
// OPTIONS 预检在路由与认证之前短路返回。
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		h.Set("Access-Control-Max-Age", "600")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/namespaces/{ns}/configs/{data_id}", s.handlePublish)
	mux.HandleFunc("GET /v1/namespaces/{ns}/configs/{data_id}", s.handleGet)
	mux.HandleFunc("GET /v1/namespaces/{ns}/configs/{data_id}/versions", s.handleVersions)
	mux.HandleFunc("GET /v1/namespaces/{ns}/configs/{data_id}/watch", s.handleWatch)
	mux.HandleFunc("GET /v1/namespaces/{ns}/configs", s.handleListConfigs)
	mux.HandleFunc("GET /v1/namespaces", s.handleListNamespaces)
	mux.HandleFunc("DELETE /v1/namespaces/{ns}", s.handleDeleteNamespace)
	mux.HandleFunc("DELETE /v1/namespaces/{ns}/configs/{data_id}", s.handleDelete)
	mux.HandleFunc("POST /v1/namespaces/{ns}/configs/{data_id}/rollback", s.handleRollback)
	mux.HandleFunc("POST /v1/cluster/join", s.handleJoin)
	mux.HandleFunc("GET /v1/cluster/status", s.handleStatus)
	mux.HandleFunc("DELETE /v1/cluster/peers/{node_id}", s.handleRemovePeer)
	mux.HandleFunc("POST /v1/cluster/snapshot", s.handleSnapshot)
	mux.HandleFunc("GET /internal/discovery", s.handleDiscovery)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", promhttp.Handler())
	// GET / 是 ServeMux 通配兜底：精确模式按特异性优先，API 路径不受遮蔽（ADR-0012）。
	if s.console != nil {
		mux.Handle("GET /", s.console)
	}
	return withCORS(withMetrics(mux))
}

// writeGate 是写与集群管理接口的前置门（DESIGN §4.1）：Leader 放行；
// Follower 且已知 Leader HTTP 地址时 307（保留方法、路径与查询串，客户端原样重发）；
// 其余（candidate、地址未知）返回 503 + Retry-After。返回 true 表示放行。
func (s *Server) writeGate(w http.ResponseWriter, r *http.Request) bool {
	info := s.currentCluster()
	switch {
	case info.Role == "leader":
		return true
	case info.Role == "follower" && info.LeaderHTTP != "":
		location := info.LeaderHTTP + r.URL.Path
		if r.URL.RawQuery != "" {
			location += "?" + r.URL.RawQuery
		}
		http.Redirect(w, r, location, http.StatusTemporaryRedirect)
		return false
	}
	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "no_leader"})
	return false
}

type publishBody struct {
	Content         string            `json:"content"`
	DataType        string            `json:"data_type"`
	Tags            map[string]string `json:"tags"`
	ExpectedVersion *int64            `json:"expected_version"`
}

// handlePublish 处理 Publish（DESIGN §5.1）：
// 认证 → 参数校验 → Raft 提案 → 等待 Apply 取回结果 → 200/409/404。
func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	if !s.writeGate(w, r) {
		return
	}
	ns, dataId := r.PathValue("ns"), r.PathValue("data_id")

	r.Body = http.MaxBytesReader(w, r.Body, raftlog.MaxContentBytes+64*1024)
	var body publishBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		badRequest(w, err)
		return
	}

	cmd, err := raftlog.NewPublish(raftlog.PublishData{
		Namespace:       ns,
		DataId:          dataId,
		DataType:        body.DataType,
		Content:         body.Content,
		Tags:            body.Tags,
		ExpectedVersion: body.ExpectedVersion,
		Actor:           "admin",
	})
	if err != nil {
		badRequest(w, err)
		return
	}
	b, err := raftlog.Encode(cmd)
	if err != nil {
		badRequest(w, err)
		return
	}

	res, ok := s.propose(w, r, b)
	if !ok {
		return
	}

	switch res.Status {
	case raftlog.StatusOK:
		writeJSON(w, http.StatusOK, map[string]any{"version": res.Version, "created": res.Created})
	case raftlog.StatusVersionConflict:
		writeJSON(w, http.StatusConflict, map[string]any{"error": "version_conflict", "current_version": res.CurrentVersion})
	case raftlog.StatusNotFound:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
	default:
		s.applyFailed(w, res)
	}
}

// applyFailed 渲染未被业务语义覆盖的 Apply 结果（DESIGN §4.1）。
// replayed 说明本节点 PG 进度越过提案日志 index，两份持久化不是同一 lineage，
// 须按 DESIGN §6.4 重建集群；其余为 FSM 内部错误，透传 FSM 给出的原因。
func (s *Server) applyFailed(w http.ResponseWriter, res raftlog.Result) {
	msg := res.Err
	if res.Status == raftlog.StatusReplayed {
		msg = fmt.Sprintf("日志 index %d 不大于本节点 FSM 进度——PG 与 Raft 日志非同一 lineage，须重建集群", res.Index)
	}
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "apply_failed", "message": msg})
}

// propose 将命令提交 Raft 并等待该日志 Apply 完成；提案层失败（无 Leader、超时等）
// 直接写响应并返回 false（DESIGN §4.1）。
func (s *Server) propose(w http.ResponseWriter, r *http.Request, cmd []byte) (raftlog.Result, bool) {
	res, err := s.proposer.Propose(r.Context(), cmd)
	switch {
	case errors.Is(err, ErrNoLeader), errors.Is(err, ErrRaftDown):
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "no_leader"})
		return raftlog.Result{}, false
	case errors.Is(err, ErrProposeTimeout):
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "propose_timeout"})
		return raftlog.Result{}, false
	case err != nil:
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "propose_failed", "message": err.Error()})
		return raftlog.Result{}, false
	}
	return res, true
}

// handleDelete 处理删除（DESIGN §4.2/§5.1）：删除条目并写 Tombstone，版本号照常消耗。
// 404 = 条目不存在或已删除；409 = CAS 不匹配 + 当前版本。
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	if !s.writeGate(w, r) {
		return
	}
	var expected *int64
	if raw := r.URL.Query().Get("expected_version"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			badRequest(w, fmt.Errorf("expected_version 必须是非负整数，得到 %q", raw))
			return
		}
		expected = &v
	}

	cmd, err := raftlog.NewDelete(raftlog.DeleteData{
		Namespace:       r.PathValue("ns"),
		DataId:          r.PathValue("data_id"),
		ExpectedVersion: expected,
		Actor:           "admin",
	})
	if err != nil {
		badRequest(w, err)
		return
	}
	b, err := raftlog.Encode(cmd)
	if err != nil {
		badRequest(w, err)
		return
	}

	res, ok := s.propose(w, r, b)
	if !ok {
		return
	}
	switch res.Status {
	case raftlog.StatusOK:
		writeJSON(w, http.StatusOK, map[string]any{"version": res.Version, "status": "deleted"})
	case raftlog.StatusVersionConflict:
		writeJSON(w, http.StatusConflict, map[string]any{"error": "version_conflict", "current_version": res.CurrentVersion})
	case raftlog.StatusNotFound:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
	default:
		s.applyFailed(w, res)
	}
}

// handleRollback 处理回滚（DESIGN §4.2/§5.1）：以 to_version 从 History 取内容发布为新版本。
// to_version 的解析在 FSM Apply 阶段（本端点不预取，无 TOCTOU）。
// 404 = 条目从未存在或目标不在 History；409 = 目标指向 Tombstone 或 ≥ 当前版本。
func (s *Server) handleRollback(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	if !s.writeGate(w, r) {
		return
	}
	var body struct {
		ToVersion *int64 `json:"to_version"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4*1024)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		badRequest(w, fmt.Errorf("body 须为 {\"to_version\":N}: %w", err))
		return
	}
	if body.ToVersion == nil || *body.ToVersion <= 0 {
		badRequest(w, fmt.Errorf("to_version 必须为正整数"))
		return
	}

	cmd, err := raftlog.NewRollback(raftlog.RollbackData{
		Namespace: r.PathValue("ns"),
		DataId:    r.PathValue("data_id"),
		ToVersion: *body.ToVersion,
		Actor:     "admin",
	})
	if err != nil {
		badRequest(w, err)
		return
	}
	b, err := raftlog.Encode(cmd)
	if err != nil {
		badRequest(w, err)
		return
	}

	res, ok := s.propose(w, r, b)
	if !ok {
		return
	}
	switch res.Status {
	case raftlog.StatusOK:
		writeJSON(w, http.StatusOK, map[string]any{"version": res.Version})
	case raftlog.StatusNotFound:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
	case raftlog.StatusInvalidRollback:
		writeJSON(w, http.StatusConflict, map[string]any{"error": "invalid_rollback", "current_version": res.CurrentVersion})
	default:
		s.applyFailed(w, res)
	}
}

// handleVersions 返回 History 倒序分页（DESIGN §5.1），读本机 PG，含 Tombstone 记录。
func (s *Server) handleVersions(w http.ResponseWriter, r *http.Request) {
	if !s.ready() {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "restoring"})
		return
	}
	limit := store.DefaultHistoryLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			badRequest(w, fmt.Errorf("limit 必须是整数，得到 %q", raw))
			return
		}
		limit = v
	}
	var before *int64
	if raw := r.URL.Query().Get("before_version"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			badRequest(w, fmt.Errorf("before_version 必须是整数，得到 %q", raw))
			return
		}
		before = &v
	}

	records, err := s.store.ListHistory(r.Context(), r.PathValue("ns"), r.PathValue("data_id"), limit, before)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal", "message": err.Error()})
		return
	}
	out := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		tags := rec.Tags
		if tags == nil {
			tags = map[string]string{}
		}
		out = append(out, map[string]any{
			"version":    rec.Version,
			"op":         rec.Op,
			"data_type":  rec.DataType,
			"content":    rec.Content,
			"tags":       tags,
			"actor":      rec.Actor,
			"created_at": rec.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleListNamespaces 返回仍有存活条目的 Namespace 去重清单（DESIGN §5.1，读本机 PG）。
func (s *Server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	if !s.ready() {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "restoring"})
		return
	}
	namespaces, err := s.store.ListNamespaces(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal", "message": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"namespaces": namespaces})
}

// handleListConfigs 返回条目列表（DESIGN §5.1，读本机 PG）：仅存活条目，
// data_id 字典序 keyset 分页；响应为元数据（不含 content），next_after_id 为
// 下一页游标，本页不满 limit 时省略（已到末尾）。
func (s *Server) handleListConfigs(w http.ResponseWriter, r *http.Request) {
	if !s.ready() {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "restoring"})
		return
	}
	limit := store.DefaultListLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			badRequest(w, fmt.Errorf("limit 必须是整数，得到 %q", raw))
			return
		}
		// 游标判定须与存储层同一收敛结果，否则 limit=0/超上限时满页误判丢 next_after_id。
		limit = store.ClampListLimit(v)
	}
	var after *string
	if raw := r.URL.Query().Get("after_id"); raw != "" {
		after = &raw
	}

	entries, err := s.store.ListEntries(r.Context(), r.PathValue("ns"), limit, after)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal", "message": err.Error()})
		return
	}
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		tags := e.Tags
		if tags == nil {
			tags = map[string]string{}
		}
		out = append(out, map[string]any{
			"namespace":  e.Namespace,
			"data_id":    e.DataId,
			"data_type":  e.DataType,
			"version":    e.Version,
			"tags":       tags,
			"created_at": e.CreatedAt,
			"updated_at": e.UpdatedAt,
		})
	}
	resp := map[string]any{"entries": out}
	if len(entries) == limit && len(entries) > 0 {
		resp["next_after_id"] = entries[len(entries)-1].DataId
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleDeleteNamespace 处理删除 Namespace（DESIGN §5.1）：经 Raft 提案原子删除该
// Namespace 下全部存活 Config Entry（各写一条 Tombstone，History 保留可逐条回滚）。
// 幂等：无存活条目返回 200 entries:0。
func (s *Server) handleDeleteNamespace(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	if !s.writeGate(w, r) {
		return
	}
	cmd, err := raftlog.NewDeleteNamespace(raftlog.DeleteNamespaceData{
		Namespace: r.PathValue("ns"),
		Actor:     "admin",
	})
	if err != nil {
		badRequest(w, err)
		return
	}
	b, err := raftlog.Encode(cmd)
	if err != nil {
		badRequest(w, err)
		return
	}

	res, ok := s.propose(w, r, b)
	if !ok {
		return
	}
	switch res.Status {
	case raftlog.StatusOK:
		writeJSON(w, http.StatusOK, map[string]any{"status": "deleted", "entries": res.Entries})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "apply_failed", "message": res.Err})
	}
}

// handleGet 读当前条目（本机 PG，允许陈旧；DESIGN §5.1）。
// Serving 门闩关闭（全量恢复中）时 503 + Retry-After（DESIGN §4.4）。
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if !s.ready() {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "restoring"})
		return
	}
	entry, err := s.store.GetEntry(r.Context(), r.PathValue("ns"), r.PathValue("data_id"))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal", "message": err.Error()})
		return
	}
	if entry == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
		return
	}
	writeJSON(w, http.StatusOK, entryJSON(entry))
}

// entryJSON 渲染 Config Entry 的当前态（GET 与 Watch active 共用）。
func entryJSON(e *store.Entry) map[string]any {
	tags := e.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"namespace":  e.Namespace,
		"data_id":    e.DataId,
		"data_type":  e.DataType,
		"content":    e.Content,
		"version":    e.Version,
		"tags":       tags,
		"created_at": e.CreatedAt,
		"updated_at": e.UpdatedAt,
	}
}

// handleWatch 处理长轮询（DESIGN §4.3）：挂起至条目 version 越过 v、被删除或超时。
// 变更唤醒来自本机 FSM Apply（任意节点皆可挂起）；响应
// active（含最新条目）/ deleted（含墓碑版本）/ timeout（回显 v 供原样重挂）。
func (s *Server) handleWatch(w http.ResponseWriter, r *http.Request) {
	if !s.ready() {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "restoring"})
		return
	}
	ns, dataId := r.PathValue("ns"), r.PathValue("data_id")

	v := int64(0)
	if raw := r.URL.Query().Get("v"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			badRequest(w, fmt.Errorf("v 必须是非负整数，得到 %q", raw))
			return
		}
		v = parsed
	}
	timeout := defaultWatchTimeout
	if raw := r.URL.Query().Get("timeout"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			badRequest(w, fmt.Errorf("timeout 必须为正时长，得到 %q", raw))
			return
		}
		timeout = d
	}
	if timeout > maxWatchTimeout {
		timeout = maxWatchTimeout
	}

	// 先注册后检查：关闭"快路径判定与挂起之间错过通知"的竞态窗口。
	release, events := s.hub.Register(ns, dataId)
	defer release()

	entry, err := s.store.GetEntry(r.Context(), ns, dataId)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal", "message": err.Error()})
		return
	}
	if entry != nil {
		if entry.Version > v {
			writeJSON(w, http.StatusOK, map[string]any{"status": "active", "entry": entryJSON(entry)})
			return
		}
	} else if cur, _, err := s.store.CurrentVersion(r.Context(), ns, dataId); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal", "message": err.Error()})
		return
	} else if cur > v {
		writeJSON(w, http.StatusOK, map[string]any{"status": "deleted", "version": cur})
		return
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		writeJSON(w, http.StatusOK, map[string]any{"status": "timeout", "version": v})
	case ev := <-events:
		if ev.Deleted {
			writeJSON(w, http.StatusOK, map[string]any{"status": "deleted", "version": ev.Version})
			return
		}
		// 唤醒后重读本机 PG：事件只携带版本，条目体以 Apply 后的最新态为准。
		entry, err := s.store.GetEntry(r.Context(), ns, dataId)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal", "message": err.Error()})
			return
		}
		if entry == nil {
			// 事件后条目又被删除：以墓碑版本响应（版本单调，必越过 v）。
			cur, _, err := s.store.CurrentVersion(r.Context(), ns, dataId)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal", "message": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"status": "deleted", "version": cur})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "active", "entry": entryJSON(entry)})
	case <-r.Context().Done():
		// 客户端断开，连接已收场，无从写响应。
	}
}

// handleSnapshot 手动触发快照（DESIGN §4.4"达到阈值或手动触发"）；Leader 处理。
func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	if !s.writeGate(w, r) {
		return
	}
	if s.snapshot == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "snapshot_disabled"})
		return
	}
	index, err := s.snapshot()
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "snapshot_index": index})
	case errors.Is(err, ErrNothingNewToSnapshot):
		writeJSON(w, http.StatusOK, map[string]any{"status": "noop"})
	case errors.Is(err, ErrNoLeader), errors.Is(err, ErrRaftDown):
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "no_leader"})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "snapshot_failed", "message": err.Error()})
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "unhealthy", "pg": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.ready == nil || !s.ready() {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

func (s *Server) authorized(r *http.Request) bool {
	const prefix = "Bearer "
	auth := r.Header.Get("Authorization")
	if len(auth) <= len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(auth[len(prefix):]), []byte(s.adminToken)) == 1
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func badRequest(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_request", "message": err.Error()})
}

// handleRemovePeer 处理成员移除（DESIGN §4.5/§5.2）：Leader 执行前做 quorum 护栏校验。
// 200 移除成功；404 非成员；409 护栏拒绝；409/503 同写语义（非 Leader）。
func (s *Server) handleRemovePeer(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	if !s.writeGate(w, r) {
		return
	}
	if s.remove == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "remove_disabled"})
		return
	}
	err := s.remove(r.PathValue("node_id"))
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"status": "removed"})
	case errors.Is(err, ErrNotMember):
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_member"})
	case errors.Is(err, ErrQuorumGuard):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "quorum_guard"})
	case errors.Is(err, ErrNoLeader), errors.Is(err, ErrRaftDown):
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "no_leader"})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "remove_failed", "message": err.Error()})
	}
}

// handleDiscovery 供发现代理调用（DESIGN §5.3，不认证）：
// 返回本节点身份与角色、及其所知 Leader 的 Raft/HTTP 地址，附带本节点 Raft
// 指标自报（成员表状态/指标列经此合并，DESIGN §5.2）。
func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if !s.ready() {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "restoring"})
		return
	}
	info := s.currentCluster()
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":          info.NodeID,
		"role":             info.Role,
		"leader_addr":      info.LeaderAddr,
		"leader_http_addr": info.LeaderHTTP,
		"term":             info.Term,
		"last_log_index":   info.LastLogIndex,
		"commit_index":     info.CommitIndex,
		"applied_index":    info.LastApplied,
		"last_contact_ms":  info.LastContactMS,
	})
}

// handleJoin 处理成员加入（DESIGN §4.5/§5.2）：Leader 执行 AddVoter，加入即 voter
//（staging 自动转正在 hashicorp/raft 中未实现）。接口幂等：已是成员（地址相同）返回 200。
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	if !s.writeGate(w, r) {
		return
	}
	if s.join == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "join_disabled"})
		return
	}
	var body struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
		HTTPAddr string `json:"http_addr"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8*1024)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		badRequest(w, err)
		return
	}
	if body.NodeID == "" || body.RaftAddr == "" {
		badRequest(w, fmt.Errorf("node_id 与 raft_addr 必填"))
		return
	}

	exists, err := s.join(body.NodeID, body.RaftAddr, body.HTTPAddr)
	if err != nil {
		switch {
		case errors.Is(err, ErrNoLeader), errors.Is(err, ErrRaftDown):
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "no_leader"})
		default:
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "join_failed", "message": err.Error()})
		}
		return
	}
	status := "added"
	code := http.StatusAccepted
	if exists {
		status = "exists"
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]any{
		"status":           status,
		"leader_node_id":   s.currentCluster().NodeID,
		"leader_http_addr": s.currentCluster().LeaderHTTP,
	})
}

// handleStatus 返回本节点视角的集群视图（DESIGN §5.2，任意节点可调）。
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	info := s.currentCluster()
	members := []MemberInfo{}
	if s.members != nil {
		m, err := s.members()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal", "message": err.Error()})
			return
		}
		if m != nil {
			members = m
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":            info.NodeID,
		"role":               info.Role,
		"term":               info.Term,
		"leader_id":          info.LeaderID,
		"leader_addr":        info.LeaderAddr,
		"leader_http_addr":   info.LeaderHTTP,
		"last_applied_index": info.LastApplied,
		"last_log_index":     info.LastLogIndex,
		"commit_index":       info.CommitIndex,
		"last_contact_ms":    info.LastContactMS,
		"restoring":          info.Restoring,
		"members":            members,
	})
}

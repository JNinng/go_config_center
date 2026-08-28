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

type Server struct {
	proposer   Proposer
	store      *store.Store
	adminToken string
	ready      func() bool
	snapshot   SnapshotTrigger
}

func New(p Proposer, st *store.Store, adminToken string, ready func() bool, snapshot SnapshotTrigger) *Server {
	return &Server{proposer: p, store: st, adminToken: adminToken, ready: ready, snapshot: snapshot}
}

func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/namespaces/{ns}/configs/{data_id}", s.handlePublish)
	mux.HandleFunc("GET /v1/namespaces/{ns}/configs/{data_id}", s.handleGet)
	mux.HandleFunc("DELETE /v1/namespaces/{ns}/configs/{data_id}", s.handleDelete)
	mux.HandleFunc("GET /v1/namespaces/{ns}/configs/{data_id}/versions", s.handleVersions)
	mux.HandleFunc("POST /v1/cluster/snapshot", s.handleSnapshot)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
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
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "apply_failed", "message": res.Err})
	}
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
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "apply_failed", "message": res.Err})
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
	tags := entry.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"namespace":  entry.Namespace,
		"data_id":    entry.DataId,
		"data_type":  entry.DataType,
		"content":    entry.Content,
		"version":    entry.Version,
		"tags":       tags,
		"created_at": entry.CreatedAt,
		"updated_at": entry.UpdatedAt,
	})
}

// handleSnapshot 手动触发快照（DESIGN §4.4"达到阈值或手动触发"）；Leader 处理。
func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
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
	if err := s.store.Pool().Ping(ctx); err != nil {
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

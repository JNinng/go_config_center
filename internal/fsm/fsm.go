// Package fsm 实现 PostgresFSM：将已提交的 Raft 日志事务性写入本机 PG（DESIGN §3.3）。
package fsm

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// defaultResultKeep 是 logIndex → 结果 表保留的近期条目数（DESIGN §3.3）。
const defaultResultKeep = 4096

var metrics = struct {
	applyDuration    *prometheus.HistogramVec
	applyResults     *prometheus.CounterVec
	lastApplied      prometheus.Gauge
	snapshotDuration prometheus.Histogram
	snapshotSize     prometheus.Gauge
	restoreSkipped   prometheus.Counter
}{
	applyDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fsm_apply_duration_seconds",
		Help:    "FSM Apply 延迟",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	}, []string{"op"}),
	applyResults: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fsm_apply_results_total",
		Help: "写结果分布（DESIGN §7）",
	}, []string{"result"}),
	lastApplied: promauto.NewGauge(prometheus.GaugeOpts{
		Name: "fsm_last_applied_index",
		Help: "FSM 已应用的 Raft 日志 index",
	}),
	snapshotDuration: promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "snapshot_duration_seconds",
		Help:    "快照生成耗时（导出到写入快照存储）",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 16),
	}),
	snapshotSize: promauto.NewGauge(prometheus.GaugeOpts{
		Name: "snapshot_size_bytes",
		Help: "最近一次快照数据体字节数",
	}),
	restoreSkipped: promauto.NewCounter(prometheus.CounterOpts{
		Name: "restore_skipped_total",
		Help: "跳过式恢复命中次数（冷重启未触发全量导入）",
	}),
}

// FSM 是 hashicorp/raft 的 FSM 实现。Apply/Snapshot/Restore 由 Raft 单 goroutine
// 串行调度，内部不加锁保护 Apply 路径；互斥锁只保护结果表的并发读（Result）。
type FSM struct {
	store *store.Store
	log   *slog.Logger

	// serving 是 Serving 门闩（DESIGN §4.4）：全量恢复期间为 false，
	// 读 API 返回 503 + Retry-After，/readyz 为 not ready。
	serving atomic.Bool

	mu         sync.Mutex
	lastIndex  uint64
	lastTerm   uint64
	results    map[uint64]raftlog.Result
	resultKeep uint64
}

// New 构造 FSM 并从 raft_fsm_meta 恢复进度。
func New(ctx context.Context, st *store.Store) (*FSM, error) {
	index, term, err := st.LoadMeta(ctx)
	if err != nil {
		return nil, fmt.Errorf("恢复 FSM 进度: %w", err)
	}
	f := &FSM{
		store:      st,
		log:        slog.Default(),
		lastIndex:  index,
		lastTerm:   term,
		results:    make(map[uint64]raftlog.Result),
		resultKeep: defaultResultKeep,
	}
	f.serving.Store(true)
	return f, nil
}

// Apply 应用一条已提交日志，返回 raftlog.Result 经 ApplyFuture 回传给 API 层。
// 幂等：index ≤ last_applied_index 时跳过，不触碰 PG。
// apply-or-die：PG 写入失败直接 panic——吞错跳过会在进度上留下空洞，
// 后续日志的 meta 会越过失败日志，造成静默丢写；崩溃重启后从 last_applied 重放。
func (f *FSM) Apply(l *raft.Log) interface{} {
	if l.Type != raft.LogCommand {
		return nil
	}
	if l.Index <= f.lastIndex {
		f.mu.Lock()
		res, ok := f.results[l.Index]
		f.mu.Unlock()
		if ok {
			return res
		}
		return raftlog.Result{Status: raftlog.StatusReplayed}
	}

	started := time.Now()
	res, op := f.applyCommitted(l)
	metrics.applyDuration.WithLabelValues(string(op)).Observe(time.Since(started).Seconds())
	f.record(l.Index, l.Term, res)
	return res
}

func (f *FSM) applyCommitted(l *raft.Log) (raftlog.Result, raftlog.Op) {
	cmd, err := raftlog.Decode(l.Data)
	if err != nil {
		return f.advanceOnly(l, err, "日志信封损坏，仅推进进度"), "invalid"
	}

	switch cmd.Op {
	case raftlog.OpPublish:
		data, err := cmd.Publish()
		if err != nil {
			return f.advanceOnly(l, err, "publish 载荷非法，仅推进进度"), raftlog.OpPublish
		}
		res, err := f.store.ApplyPublish(context.Background(), data, l.Index, l.Term)
		if err != nil {
			panic(fmt.Sprintf("FSM Apply index=%d 写入 PG 失败: %v", l.Index, err))
		}
		metrics.applyResults.WithLabelValues(string(res.Status)).Inc()
		return res, raftlog.OpPublish

	case raftlog.OpDelete:
		data, err := cmd.Delete()
		if err != nil {
			return f.advanceOnly(l, err, "delete 载荷非法，仅推进进度"), raftlog.OpDelete
		}
		res, err := f.store.ApplyDelete(context.Background(), data, l.Index, l.Term)
		if err != nil {
			panic(fmt.Sprintf("FSM Apply index=%d 写入 PG 失败: %v", l.Index, err))
		}
		metrics.applyResults.WithLabelValues(string(res.Status)).Inc()
		return res, raftlog.OpDelete

	case raftlog.OpRollback:
		data, err := cmd.Rollback()
		if err != nil {
			return f.advanceOnly(l, err, "rollback 载荷非法，仅推进进度"), raftlog.OpRollback
		}
		res, err := f.store.ApplyRollback(context.Background(), data, l.Index, l.Term)
		if err != nil {
			panic(fmt.Sprintf("FSM Apply index=%d 写入 PG 失败: %v", l.Index, err))
		}
		metrics.applyResults.WithLabelValues(string(res.Status)).Inc()
		return res, raftlog.OpRollback

	default:
		// 未知 op：推进进度，避免毒日志卡死重放。
		return f.advanceOnly(l, fmt.Errorf("op %q 尚未实现", cmd.Op), "op 尚未实现，仅推进进度"), cmd.Op
	}
}

// advanceOnly 处理无法业务化但已提交的日志：推进 raft_fsm_meta 并返回错误结果，
// 保证 last_applied_index 无空洞。
func (f *FSM) advanceOnly(l *raft.Log, reason error, msg string) raftlog.Result {
	f.log.Error(msg, "index", l.Index, "err", reason)
	if err := f.store.AdvanceMeta(context.Background(), l.Index, l.Term); err != nil {
		panic(fmt.Sprintf("FSM Apply index=%d 推进 raft_fsm_meta 失败: %v", l.Index, err))
	}
	return raftlog.Result{Status: raftlog.StatusError, Err: reason.Error()}
}

func (f *FSM) record(index, term uint64, res raftlog.Result) {
	f.mu.Lock()
	f.lastIndex = index
	f.lastTerm = term
	f.results[index] = res
	if index > f.resultKeep {
		delete(f.results, index-f.resultKeep)
	}
	f.mu.Unlock()
	metrics.lastApplied.Set(float64(index))
}

// Result 查询 logIndex → 结果 表（仅保留近期条目）。
func (f *FSM) Result(index uint64) (raftlog.Result, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res, ok := f.results[index]
	return res, ok
}

// LastApplied 返回 FSM 当前进度。
func (f *FSM) LastApplied() (index, term uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastIndex, f.lastTerm
}

// Snapshot 与 Restore 见 snapshot.go（DESIGN §4.4）。

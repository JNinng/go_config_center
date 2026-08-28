// config-center：多节点 Raft 共识复制、每节点 PostgreSQL 物化的配置存储。
// 启动顺序（DESIGN §6.2）：读配置 → node-id → 连 PG 并迁移 schema → 打开 Raft → 服务。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/jninng/go_config_center/internal/api"
	"github.com/jninng/go_config_center/internal/config"
	"github.com/jninng/go_config_center/internal/discovery"
	"github.com/jninng/go_config_center/internal/fsm"
	"github.com/jninng/go_config_center/internal/nodeid"
	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
	"github.com/jninng/go_config_center/internal/watch"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func main() {
	var (
		configPath = flag.String("config", "config.yaml", "配置文件路径")
		bootstrap  = flag.Bool("bootstrap", false, "将空数据目录初始化为单节点新集群，仅首节点执行一次（ADR-0008）")
	)
	flag.Parse()
	// raft-boltdb 对已完成读事务的惯用回滚会经 stdlib log 打 "Rollback failed: tx closed"，
	// 属上游良性噪音，定向丢弃。
	base := slog.NewTextHandler(os.Stderr, nil)
	logger := slog.New(noiseFilter{base})
	slog.SetDefault(logger)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal(logger, "加载配置", err)
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		fatal(logger, "创建 data_dir", err)
	}
	nodeID, err := nodeid.LoadOrCreate(cfg.DataDir, cfg.NodeID)
	if err != nil {
		fatal(logger, "加载 Node ID", err)
	}

	ctx := context.Background()
	st, err := store.Open(ctx, cfg.PgDSN)
	if err != nil {
		fatal(logger, "连接 PostgreSQL", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		fatal(logger, "迁移 schema", err)
	}

	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		fatal(logger, "创建 raft 目录", err)
	}

	advertise, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		fatal(logger, "解析 raft_addr", err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, advertise, 3, 10*time.Second, os.Stderr)
	if err != nil {
		fatal(logger, "创建 Raft TCP 传输", err)
	}
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "logs.db"))
	if err != nil {
		fatal(logger, "打开 boltdb 日志存储", err)
	}
	snapStore, err := raft.NewFileSnapshotStore(raftDir, cfg.Raft.SnapshotRetention, os.Stderr)
	if err != nil {
		fatal(logger, "打开文件快照存储", err)
	}

	hub := watch.NewHub()
	machine, err := fsm.New(ctx, st, hub)
	if err != nil {
		fatal(logger, "构造 FSM", err)
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(nodeID)
	raftCfg.SnapshotThreshold = cfg.Raft.SnapshotThreshold
	raftCfg.TrailingLogs = cfg.Raft.TrailingLogs
	raftCfg.LogOutput = os.Stderr

	hasState, err := raft.HasExistingState(boltStore, boltStore, snapStore)
	if err != nil {
		fatal(logger, "检查既有集群状态", err)
	}
	if *bootstrap {
		// ADR-0008：本地已有集群态时再次 Bootstrap 必须报错退出。
		if hasState {
			fatal(logger, "--bootstrap 拒绝执行", fmt.Errorf("数据目录 %s 已存在集群状态，一个集群只允许一次 Bootstrap", raftDir))
		}
		// PG 已有 FSM 进度说明非空库；灾难恢复需先按 DESIGN §6.4 置零 raft_fsm_meta。
		if idx, _, err := st.LoadMeta(ctx); err != nil {
			fatal(logger, "读取 FSM 进度", err)
		} else if idx > 0 {
			fatal(logger, "--bootstrap 拒绝执行", fmt.Errorf("PG raft_fsm_meta.last_applied_index=%d > 0，要求空库（灾难恢复流程见 DESIGN §6.4）", idx))
		}
	}
	// 非 bootstrap 节点不再因无集群态而退出：由发现代理经 Seed List 加入（ADR-0008，
	// 永不自举）；已有状态的节点由 Raft 成员表自愈，发现代理仅跟踪 Leader 地址。

	r, err := raft.NewRaft(raftCfg, machine, boltStore, boltStore, snapStore, transport)
	if err != nil {
		fatal(logger, "启动 Raft", err)
	}
	if *bootstrap {
		f := r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{{
				ID:       raft.ServerID(nodeID),
				Address:  raft.ServerAddress(cfg.RaftAddr),
				Suffrage: raft.Voter,
			}},
		})
		if err := f.Error(); err != nil {
			fatal(logger, "Bootstrap 单节点集群", err)
		}
	}

	go func() {
		for isLeader := range r.LeaderCh() {
			if isLeader {
				logger.Info("已当选 Leader", "node_id", nodeID, "term", r.Stats()["term"])
			} else {
				logger.Info("失去或尚未获得 Leader 身份", "node_id", nodeID)
			}
		}
	}()

	// 共识状态指标（DESIGN §7）：周期采集 Raft 角色与进度。
	raftRole := promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "raft_role",
		Help: "当前 Raft 角色（命中角色为 1，其余为 0）",
	}, []string{"role"})
	raftTerm := promauto.NewGauge(prometheus.GaugeOpts{
		Name: "raft_term",
		Help: "当前 Raft term",
	})
	raftCommitIndex := promauto.NewGauge(prometheus.GaugeOpts{
		Name: "raft_commit_index",
		Help: "已提交日志 index",
	})
	go func() {
		update := func() {
			state := r.State().String()
			for _, role := range []string{"Leader", "Candidate", "Follower", "Shutdown"} {
				val := 0.0
				if strings.EqualFold(state, role) {
					val = 1
				}
				raftRole.WithLabelValues(strings.ToLower(role)).Set(val)
			}
			if term, err := strconv.ParseUint(r.Stats()["term"], 10, 64); err == nil {
				raftTerm.Set(float64(term))
			}
			if commit, err := strconv.ParseUint(r.Stats()["commit_index"], 10, 64); err == nil {
				raftCommitIndex.Set(float64(commit))
			}
		}
		update()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			update()
		}
	}()

	proposer := &raftProposer{raft: r}
	// /readyz 与读 API 的 Serving 判定：门闩（全量恢复中为 false）且 Raft 非 Shutdown。
	ready := func() bool { return machine.Serving() && r.State() != raft.Shutdown }

	// 集群视角：Leader HTTP 地址缓存放行 307 重定向；本节点为 Leader 时即自身通告地址。
	var leaderHTTPCache atomic.Value
	clusterInfo := func() api.ClusterInfo {
		role := strings.ToLower(r.State().String())
		leaderAddr, leaderID := r.LeaderWithID()
		term, _ := strconv.ParseUint(r.Stats()["term"], 10, 64)
		httpAddr := ""
		if role == "leader" {
			httpAddr = cfg.HTTPAdvertise
		} else if v, ok := leaderHTTPCache.Load().(string); ok {
			httpAddr = v
		}
		lastApplied, _ := machine.LastApplied()
		return api.ClusterInfo{
			NodeID:      nodeID,
			Role:        role,
			Term:        term,
			LeaderID:    string(leaderID),
			LeaderAddr:  string(leaderAddr),
			LeaderHTTP:  httpAddr,
			LastApplied: lastApplied,
			Restoring:   !machine.Serving(),
		}
	}
	members := func() ([]api.MemberInfo, error) {
		f := r.GetConfiguration()
		if err := f.Error(); err != nil {
			return nil, fmt.Errorf("读取成员表: %w", err)
		}
		out := make([]api.MemberInfo, 0, len(f.Configuration().Servers))
		for _, srv := range f.Configuration().Servers {
			out = append(out, api.MemberInfo{
				NodeID:   string(srv.ID),
				Addr:     string(srv.Address),
				Suffrage: srv.Suffrage.String(),
			})
		}
		return out, nil
	}
	// joinFn 在 Leader 上执行成员加入。同 Node ID 换地址时按新地址覆盖（幂等语义）。
	// 注：直接以 voter 加入——上游库的 staging 自动转正从未实现（membership.md 仅提案），
	// AddVoter 缺省即 Voter，与 Consul 等生产用法一致（DESIGN §4.5 注记）。
	joinFn := func(peerID, peerRaftAddr, _ string) (bool, error) {
		f := r.GetConfiguration()
		if err := f.Error(); err != nil {
			return false, fmt.Errorf("读取成员表: %w", err)
		}
		for _, srv := range f.Configuration().Servers {
			if srv.ID == raft.ServerID(peerID) && srv.Address == raft.ServerAddress(peerRaftAddr) {
				return true, nil
			}
		}
		addF := r.AddVoter(raft.ServerID(peerID), raft.ServerAddress(peerRaftAddr), 0, 10*time.Second)
		if err := addF.Error(); err != nil {
			switch {
			case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost):
				return false, api.ErrNoLeader
			case errors.Is(err, raft.ErrRaftShutdown):
				return false, api.ErrRaftDown
			default:
				return false, fmt.Errorf("AddVoter: %w", err)
			}
		}
		return false, nil
	}

	if !*bootstrap {
		seeds := make([]string, 0, len(cfg.Seeds))
		for _, s := range cfg.Seeds {
			if !strings.Contains(s, "://") {
				s = "http://" + s
			}
			seeds = append(seeds, s)
		}
		go discovery.Run(context.Background(), logger, discovery.Options{
			NodeID:        nodeID,
			RaftAddr:      cfg.RaftAddr,
			HTTPAdvertise: cfg.HTTPAdvertise,
			Seeds:         seeds,
			AdminToken:    cfg.AdminToken,
			OnLeader:      func(addr string) { leaderHTTPCache.Store(addr) },
		}, hasState)
	}

	snapshotTrigger := func() (uint64, error) {
		f := r.Snapshot()
		if err := f.Error(); err != nil {
			switch {
			case errors.Is(err, raft.ErrNothingNewToSnapshot):
				return 0, api.ErrNothingNewToSnapshot
			case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost):
				return 0, api.ErrNoLeader
			case errors.Is(err, raft.ErrRaftShutdown):
				return 0, api.ErrRaftDown
			default:
				return 0, fmt.Errorf("触发快照失败: %w", err)
			}
		}
		meta, rc, err := f.Open()
		if err != nil {
			return 0, fmt.Errorf("读取快照元信息: %w", err)
		}
		_ = rc.Close()
		return meta.Index, nil
	}
	mux := api.New(api.Deps{
		Proposer:    proposer,
		Store:       st,
		Hub:         hub,
		AdminToken:  cfg.AdminToken,
		Ready:       ready,
		Snapshot:    snapshotTrigger,
		ClusterInfo: clusterInfo,
		Members:     members,
		Join:        joinFn,
	}).Routes()

	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		fatal(logger, "监听 HTTP", err)
	}
	logger.Info("HTTP 服务就绪", "addr", cfg.HTTPAddr, "node_id", nodeID, "bootstrap", *bootstrap)
	go func() {
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP 服务异常", "err", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	logger.Info("收到退出信号，优雅关闭")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP 关闭异常", "err", err)
	}
	if f := r.Shutdown(); f.Error() != nil {
		logger.Error("Raft 关闭异常", "err", f.Error())
	}
	transport.Close()
}

// raftProposer 实现 api.Proposer：提案并等待该日志应用完成后取回结果（DESIGN §3.3）。
type raftProposer struct {
	raft *raft.Raft
}

func (p *raftProposer) Propose(ctx context.Context, cmd []byte) (raftlog.Result, error) {
	var zero raftlog.Result
	f := p.raft.Apply(cmd, 10*time.Second)
	if err := f.Error(); err != nil {
		switch {
		case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost):
			return zero, api.ErrNoLeader
		case errors.Is(err, raft.ErrRaftShutdown):
			return zero, api.ErrRaftDown
		case errors.Is(err, raft.ErrEnqueueTimeout):
			return zero, api.ErrProposeTimeout
		default:
			return zero, fmt.Errorf("提案失败: %w", err)
		}
	}
	res, ok := f.Response().(raftlog.Result)
	if !ok {
		return zero, fmt.Errorf("Apply 返回意外类型 %T", f.Response())
	}
	return res, nil
}

func fatal(logger *slog.Logger, msg string, err error) {
	logger.Error(msg, "err", err)
	os.Exit(1)
}

// noiseFilter 丢弃已知上游良性日志，其余透传。
type noiseFilter struct{ next slog.Handler }

func (f noiseFilter) Enabled(ctx context.Context, level slog.Level) bool {
	return f.next.Enabled(ctx, level)
}

func (f noiseFilter) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == "Rollback failed: tx closed" {
		return nil
	}
	return f.next.Handle(ctx, r)
}

func (f noiseFilter) WithAttrs(attrs []slog.Attr) slog.Handler {
	return noiseFilter{f.next.WithAttrs(attrs)}
}

func (f noiseFilter) WithGroup(name string) slog.Handler {
	return noiseFilter{f.next.WithGroup(name)}
}

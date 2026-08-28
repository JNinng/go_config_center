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
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/jninng/go_config_center/internal/api"
	"github.com/jninng/go_config_center/internal/config"
	"github.com/jninng/go_config_center/internal/fsm"
	"github.com/jninng/go_config_center/internal/nodeid"
	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
	"github.com/jninng/go_config_center/internal/watch"
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
	} else if !hasState {
		fatal(logger, "无法启动", fmt.Errorf("数据目录 %s 无集群状态：全新集群仅首节点执行一次 --bootstrap，其余节点走 Join（后续票实现）", raftDir))
	}

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

	proposer := &raftProposer{raft: r}
	// /readyz 与读 API 的 Serving 判定：门闩（全量恢复中为 false）且 Raft 非 Shutdown。
	ready := func() bool { return machine.Serving() && r.State() != raft.Shutdown }
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
	mux := api.New(proposer, st, hub, cfg.AdminToken, ready, snapshotTrigger).Routes()

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

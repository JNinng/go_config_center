# AGENTS.md

多节点 Raft 共识复制、每节点独立 PostgreSQL 物化的配置存储（Go 1.26，模块 `github.com/jninng/go_config_center`）。

## 目录

- `cmd/config-center/` — 单二进制入口：装配、自退出循环（退出码 42）、移除护栏、指标采集
- `internal/` — `api`（HTTP）、`store`（存储契约接口）、`store/pg`（PostgreSQL 实现）、`fsm`（Raft FSM + 快照）、`raftlog`（日志信封）、`discovery`（发现代理）、`watch`、`config`、`nodeid`、`testutil`
- `docs/DESIGN.md` — 设计契约，代码注释以 § 编号引用它；`docs/adr/` — 决策理由；`CONTEXT.md` — 术语表（**先读**）
- `scripts/e2e_ticketNN.sh` — 各票 e2e 验收；`.scratch/config-center-mvp/issues/` — 票档（过程记录归档）

## 命令

```bash
go build ./... && go vet ./...
go test ./...        # PG 集成测试需 GCC_TEST_PG_DSN（管理员 DSN），未设置时自动跳过
bash scripts/e2e_ticket09.sh   # e2e 需本地 PG（psql -U postgresql 可用）+ 已构建二进制
```

## 不变量（改代码前必读）

- **存储边界（ADR-0011）**：pgx 依赖仅允许出现在 `internal/store/pg`；fsm/api/cmd 只依赖 `store.Store` 接口。新存储后端实现该接口接入，快照分节顺序须遵守 `store.SnapshotTables`（格式 v1 契约）。
- **FSM apply-or-die**：Apply 写存储失败直接 panic，禁止吞错——吞错会让 `raft_fsm_meta` 越过失败日志留下进度空洞；崩溃后从 last_applied 重放。
- **单事务**：Apply 三方法的业务数据 + History + `raft_fsm_meta` 必须在同一事务内提交；CAS 失败等拒绝结果也推进 meta（不追加 History）。
- Apply/Snapshot/Restore 由 Raft 单 goroutine 串行调度，FSM Apply 路径不加锁（互斥锁仅保护结果表并发读）。
- **Rollback 的 to_version 在 FSM Apply 阶段解析**，API 层不预取（避免 TOCTOU）。
- History 只追加；Delete 写 Tombstone 并消耗版本号；版本按 `(namespace, data_id)` 单调。
- 写路径只能经 Raft 提案；读路径读本机 PG（允许陈旧，无 ReadIndex，见 ADR-0003）。

## 约定

- 注释、日志、文档全用中文；doc comment 引用 DESIGN §/ADR 编号（如 `DESIGN §4.5`）。
- **术语以 CONTEXT.md 为准**，尊重各词条 `_Avoid_` 列表：成员表（勿写"成员配置"，与业务 Config Entry 混淆）、Publish（勿写提交/commit）、Tombstone（勿写软删除）等。
- **活文档（DESIGN/ADR/README/代码注释）只写契约与不变量**：不写"已修复/日期戳/当前实现"式过程叙述；`TODO(#issue)` 保留事项可留。
- **断言 hashicorp/raft 行为前必须查模块缓存源码**（`go env GOMODCACHE` 下 `hashicorp/raft@v1.7.3`），禁止凭记忆——已发生过三次凭记忆写错（staging 转正、ShutdownOnRemove 触发条件、刷新周期）。
- `.scratch/` 是过程记录归档：不重写票的规格与验收历史；但其中以"已验证"口吻的失实技术断言应修正（完整机制只在 DESIGN 留一处，票档引用不双写）。

## 已知坑

- raft-boltdb 会经 stdlib log 打良性噪音 `Rollback failed: tx closed`，main.go 的 `noiseFilter` 定向丢弃——不要"修"它。
- `ShutdownOnRemove` 仅在本节点**作为 Leader 提交了不含自身的成员表**时触发（即移除 Leader 场景）；移除 Follower 依赖 main.go 的 2s `cluster/status` 核对循环以退出码 42 退出，e2e 断言此码。
- 开发环境为 Windows + Git Bash；e2e 脚本是 bash；仓库根的 `data*/`、`config-center.exe`、`config.yaml` 为本地运行产物，勿提交勿清理。
- Leader HTTP 地址缓存绑定获知时的 Leader Raft 地址（main.go `cachedForLeader`）：Leader 更换后旧缓存立即失效为 503，由发现代理 5s 刷新恢复 307——改动重定向逻辑前先理解此设计（DESIGN §4.1）。

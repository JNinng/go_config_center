# AGENTS.md

多节点 Raft 共识复制、每节点独立 PostgreSQL 物化的配置存储（Go 1.26，模块 `github.com/jninng/go_config_center`）。

## 目录

- `cmd/config-center/` — 单二进制入口：装配、自退出循环（退出码 42）、移除护栏、指标采集
- `internal/` — `api`（HTTP）、`store`（存储契约接口）、`store/pg`（PostgreSQL 实现）、`fsm`（Raft FSM + 快照）、`raftlog`（日志信封）、`discovery`（发现代理）、`watch`、`config`、`nodeid`、`testutil`
- `docs/DESIGN.md` — 设计契约，代码注释以 § 编号引用它；`docs/adr/` — 决策理由；`CONTEXT.md` — 术语表（**先读**）
- `scripts/` — e2e 验收套件（`_test.go` + `//go:build e2e`，每张 `TestTicketNN` 对应一张票；黑盒，不 import `internal/*`，永不进生产二进制）；`.scratch/config-center-mvp/issues/` — 票档（过程记录归档）

## 命令

```bash
go build ./... && go vet ./...
go test ./...        # PG 集成测试需 GCC_TEST_PG_DSN（管理员 DSN），未设置时自动跳过
go test -tags e2e ./scripts -run TestTicket09 -v -count=1   # e2e 需本地 PG（psql -U postgresql 可用），全量跑 -run TestTicket
bash scripts/build-console.sh   # 控制台：前端构建（需 node）+ go build；只改 Go 时直接 go build 即可
```

## 不变量（改代码前必读）

- **存储边界（ADR-0011）**：pgx 依赖仅允许出现在 `internal/store/pg`；fsm/api/cmd 只依赖 `store.Store` 接口。新存储后端实现该接口接入，快照分节顺序须遵守 `store.SnapshotTables`（格式 v1 契约）。
- **FSM apply-or-die**：Apply 写存储失败直接 panic，禁止吞错——吞错会让 `raft_fsm_meta` 越过失败日志留下进度空洞；崩溃后从 last_applied 重放。
- **单事务**：Apply 三方法的业务数据 + History + `raft_fsm_meta` 必须在同一事务内提交；CAS 失败等拒绝结果也推进 meta（不追加 History）。
- Apply/Snapshot/Restore 由 Raft 单 goroutine 串行调度，FSM Apply 路径不加锁（互斥锁仅保护结果表并发读）。
- **Rollback 的 to_version 在 FSM Apply 阶段解析**，API 层不预取（避免 TOCTOU）。
- History 只追加；Delete 写 Tombstone 并消耗版本号；版本按 `(namespace, data_id)` 单调。
- **行级时间戳随日志（DESIGN §3.1）**：`created_at/updated_at` 取 Raft 日志 `AppendedAt`（Leader 盖章随日志分发），存储写路径显式赋值——禁止本机时钟或 `now()` 盖章，否则新节点追平时时间与 Leader 漂移。
- 写路径只能经 Raft 提案；读路径读本机 PG（允许陈旧，无 ReadIndex，见 ADR-0003）。
- **lineage 护栏（DESIGN §6.2）**：本节点 PG 进度不得越过本地 Raft 日志末端，main.go 启动时校验，越界即拒绝启动（继续运行会静默吞写）；`pg_dsn` 指向的库不存在时由 `pg.Open` 自动创建。

## 约定

- 注释、日志、文档全用中文；doc comment 引用 DESIGN §/ADR 编号（如 `DESIGN §4.5`）。
- **术语以 CONTEXT.md 为准**，尊重各词条 `_Avoid_` 列表：成员表（勿写"成员配置"，与业务 Config Entry 混淆）、Publish（勿写提交/commit）、Tombstone（勿写软删除）等。
- **活文档（DESIGN/ADR/README/代码注释）只写契约与不变量**：不写"已修复/日期戳/当前实现"式过程叙述；`TODO(#issue)` 保留事项可留。
- **断言 hashicorp/raft 行为前必须查模块缓存源码**（`go env GOMODCACHE` 下 `hashicorp/raft@v1.7.3`），禁止凭记忆——已发生过三次凭记忆写错（staging 转正、ShutdownOnRemove 触发条件、刷新周期）。
- `.scratch/` 是过程记录归档：不重写票的规格与验收历史；但其中以"已验证"口吻的失实技术断言应修正（完整机制只在 DESIGN 留一处，票档引用不双写）。

## 已知坑

- raft-boltdb 会经 stdlib log 打良性噪音 `Rollback failed: tx closed`，main.go 的 `noiseFilter` 定向丢弃——不要"修"它。
- `ShutdownOnRemove` 仅在本节点**作为 Leader 提交了不含自身的成员表**时触发（即移除 Leader 场景）；移除 Follower 依赖 main.go 的 2s `cluster/status` 核对循环以退出码 42 退出，e2e 断言此码。
- 开发环境为 Windows + Git Bash；e2e 是 `scripts` 包的 Go 测试（`-tags e2e` 才参与编译；票 10/11 挂起进程经 PowerShell），每张票结束自动清空 data 目录与测试库（`GCC_E2E_KEEP=1` 保留现场），配置一律由套件生成、不读仓库根 config.yaml；`config-center.exe` 与 `config.yaml` 为本地运行产物，勿提交。仓库根的 `data*/` 为手动运行产物，勿提交勿清理。`internal/console/dist/` 内为控制台构建产物（`.keep` 除外，ADR-0012），重跑 `scripts/build-console.sh` 会整体覆盖；占位页在 `internal/console/placeholder.html`，构建不触碰它。
- Leader HTTP 地址缓存绑定获知时的 Leader Raft 地址（main.go `cachedForLeader`）：Leader 更换后旧缓存立即失效为 503，由发现代理 5s 刷新恢复 307——改动重定向逻辑前先理解此设计（DESIGN §4.1）。

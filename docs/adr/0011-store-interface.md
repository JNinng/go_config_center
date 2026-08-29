# 存储后端接口化：Store 契约 + PostgreSQL 默认实现，KV 后端留作待选

存储访问面是纯 KV 形状——`internal/store` 的全部操作（get/upsert/delete/前缀范围读/meta 推进）无 join、无 SQL 级聚合，业务数据仅三张表。为让后续可接入 etcd-io/bbolt 一类嵌入 KV（评估结论：技术可行、暂不做默认替换），把存储访问收拢为显式契约。

## Decision

- `internal/store` 只含 `store.Store` 接口与共享类型（`Entry`、`HistoryRecord`、快照分节类型）；**pgx 依赖不得出现在本包及上游（fsm/api/cmd）**，编译期保证抽象不泄漏。PostgreSQL 实现下沉至 `internal/store/pg`。
- 快照分层：格式骨架（magic、JSON 头、checksum 记账、跳过式恢复判定、Serving 门闩）归 fsm；数据体的分节编码与导入原子性归后端，接口为 `ExportTables`/`ImportTables`。分节顺序与命名（`biz_configs`/`biz_config_history`/`raft_fsm_meta`）是快照格式 v1 契约的一部分，唯一定义于 `store.SnapshotTables`。
- PG 仍是默认且唯一实现；新后端实现 `Store` 即可接入。`Pool()` 保留为 pg 包内测试断言/运维排查专用，不在接口上。

## Considered Options

- **接口携带 `Pool()`/pgx 类型**：否决——后端类型泄漏回上游，KV 后端无法实现；`/healthz` 改经接口的 `Ping`。
- **快照格式整体归后端**：否决——跳过式恢复要求头先于数据体可读（fsm 先比对 `raft_fsm_meta` 进度再决定是否消费数据体），且行数/checksum 校验必须发生在后端事务提交之前（否则校验失败时已产生半写状态），格式骨架与数据编码必须分层。

## Consequences

- 新增后端 = `internal/store/<name>` 实现 `store.Store` + 等价测试脚手架；票 06 幂等性属性测试参数化后端后可直接复用。
- bbolt 接入前须按票 12 场景实测冷重启打开耗时（bbolt 写模式打开需加载全部空闲页，库增大后打开时间上升）与 Compact 运维策略，达标后再讨论是否改默认（见 ADR-0002、ADR-0007）。

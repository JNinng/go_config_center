# 02 — 快照生成与跳过式恢复

**What to build:** 达到 `snapshot_threshold` 或手动触发时，FSM 在单个 REPEATABLE READ 事务内导出三表为自描述单文件流（magic + JSON 头 + COPY 数据体，DESIGN §4.4），经快照存储落盘。Restore 先读头中的 `raft_index` 与本机 `raft_fsm_meta.last_applied_index` 比对：已追平或超前 → 跳过数据导入仅推进 Raft 内部状态；否则全量导入（TRUNCATE + COPY）。全量恢复期间 Serving 门闩关闭，读 API 返回 503 + Retry-After。

依据：ADR-0007。hashicorp/raft 每次重启都会触发 Restore，跳过路径是"冷重启 ≤10s"目标的唯一保证。

**Blocked by:** 01 — 骨架 + 单节点 Publish/Get tracer

**Status:** ready-for-agent

- [x] 触发快照后，快照文件含合法头（format_version、raft index/term、各表行数、checksum）与三表 COPY 数据
- [x] 带数据重启：命中跳过路径（有可观测证据——日志或指标），HTTP 就绪为秒级
- [x] 清空 PG（保留 Raft 数据与快照文件）后重启：走全量导入路径，数据完整恢复
- [x] checksum 或 magic 不符时 Restore 显式失败，不产生半写状态
- [x] 全量恢复期间读请求收到 503 + Retry-After，完成后恢复正常

验收记录（scripts/e2e_ticket02.sh 31 项断言 + internal/fsm/snapshot_test.go 5 组集成测试，2026-08-27）：
手动触发经 `POST /v1/cluster/snapshot`（FileSnapshotStore 自建 `snapshots/` 子目录，载荷为 `state.bin`）；
带数据重启 537ms 可服务且 `restore_skipped_total`≥1；清空 PG 重启走全量导入，快照点之后的日志经选举完成后的
正常提交流程重放回归（实测 ~1.2s）；损坏场景被 FileSnapshotStore CRC 或 FSM sha256 双层拦截，进程显式退出、
PG 无半写；全量恢复期间 Serving 门闩关闭（Go 测试以阻塞读器中途观测，api 层 503+Retry-After 有单测）。
导入后 BIGSERIAL 序列修正到 MAX(id)，后续 Publish 不撞主键。

# 04 — Rollback

**What to build:** `POST .../rollback` body `{to_version}` 走 Raft 提交；FSM 在 Apply 阶段从 History 解析 `to_version` 的内容并作为一次 Publish 发布为新版本（版本序列延续，ADR-0006）。目标解析必须在 Apply 阶段而非 API 层预取，以消除与并发写的竞态。无效目标（History 中不存在 → 404；指向 Tombstone 或 ≥ 当前版本 → 409）经结果表回传。

**Blocked by:** 03 — CAS + Delete/Tombstone + History 查询

**Status:** done

- [x] v1→v2→v3 后 rollback to v1 → 产生 v4，内容与 v1 相同，History 追加 publish 记录
- [x] 回滚后可继续回滚到任何既存版本（链式回滚）
- [x] to_version 不存在 → 404；指向墓碑或 ≥ 当前版本 → 409
- [x] rollback 与并发 Publish 存在竞态时，结果与 Raft 日志顺序一致（无 TOCTOU）

验收记录（scripts/e2e_ticket04.sh 19 项断言 + store/fsm/api 三层集成测试，2026-08-29）：
目标解析与发布在 store.ApplyRollback 的单个 PG 事务内完成——同事务读 current 与目标行，
无 TOCTOU 窗口；无效目标（not_found / invalid_rollback）不追加 History、进度同事务推进。
删除后回滚到墓碑之前的任意版本可重建条目（重建版本 created=true，版本序列延续）。
e2e 并发段：8 个 Publish + 1 个 Rollback 同时打向单节点，终态满足
biz_configs.version == max(History.version)、max(version) == History 行数（无空洞）、
当前内容与最终版本 History 记录一致；随机化交错重放由票 06 属性测试覆盖。

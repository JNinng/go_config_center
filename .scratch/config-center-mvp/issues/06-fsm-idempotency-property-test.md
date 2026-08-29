# 06 — FSM 幂等性属性测试

**What to build:** 随机化属性测试（DESIGN §8.1）：混合 publish（含 CAS 失败项）/ delete / rollback 的随机日志序列，多次重放并与快照生成/恢复交错，PG 终态与结果表必须逐次一致。这是跳过式恢复（ADR-0007）正确性的直接证明，也是 Phase 1 DoD 的核心。

**Blocked by:** 02 — 快照生成与跳过式恢复；04 — Rollback

**Status:** done

- [x] 多种子随机序列（≥3）重复重放，PG 终态逐次一致
- [x] 与快照生成 / Restore 交错后终态一致
- [x] 失败操作（CAS 冲突、无效回滚）的判定在重放间一致
- [x] 测试纳入常规 `go test`（PG 依赖以容器或本地实例解决），CI 可跑

验收记录（internal/fsm/property_test.go，3 种子 × 60 操作，2026-08-29）：
序列由固定种子确定性生成（3 Namespace × 5 DataId，publish 含 CAS 冲突项与 expected=0，
rollback 目标随机含大量无效项，term 随 index 递增）。每个种子跑四种运行并逐项比对：
基准顺序应用；重复投递（同 index 两次投递，结果与首次一致且不重复写入）；
中途"重启"（新 FSM 实例重放全序列，前半段必须返回 replayed，后半段结果与基准一致）；
快照/Restore 交错（半程快照 → 全新 PG 全量导入，导入态与快照点一致 → 同快照二次 Restore
命中跳过路径且终态不变 → 继续应用后半段与基准一致）。终态摘要覆盖 biz_configs、
biz_config_history（不含自增 id 与时间戳）与 raft_fsm_meta。失败操作（CAS 冲突、
无效回滚、删除不存在条目）的 status/version/current_version 在所有运行间逐 index 相等。
运行方式：`GCC_TEST_PG_DSN=... go test ./internal/fsm`（未设置时跳过），纳入常规 go test。

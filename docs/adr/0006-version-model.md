# 版本模型：Rollback 产生新 Version，History 永久保留

每次 Publish（含 Rollback）追加一条不可变 History 记录并使 Version 单调 +1；Rollback 从不物理撤销，而是用 History 中的旧内容发布一个新版本（v5 回滚到 v3 产生 v6）。History 不设保留上限——配置变更频率低、真实体积可控，永久保留换取完整审计与任意深度回滚，避免"目标版本已被裁剪"的边界情况。

## Consequences

- 极端情形（高频写入 × 大配置）下 History 体积无上界；若真成问题，按条目加保留裁剪策略即可，不反转本决策。
- `biz_config_history` 与业务表同等参与 Raft 快照导出，否则恢复后回滚能力受损（见 ADR-0002）。

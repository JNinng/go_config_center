# 11 — 快照保留清理 + InstallSnapshot 链路

**What to build:** `snapshot_retention` 与 `trailing_logs` 生效：旧快照按保留数清理、快照点之后的日志按窗口截断。落后超过日志保留窗口的 Follower 由 Leader 触发标准 InstallSnapshot，接收方走全量 Restore（DESIGN §4.4）后继续正常复制。

**Blocked by:** 08 — 发现代理 + Join + 写重定向

**Status:** done

- [x] 多次快照后本地仅保留 retention 个快照；trailing_logs 窗口外的日志被截断
- [x] retention 清理不触碰当前有效快照，任意时点重启均可恢复
- [x] 人为制造落后（长分区或日志截断）后追平走 InstallSnapshot 路径
- [x] Restore 完成后该节点读数据与 Leader 一致，并继续增量复制新日志

验收记录（scripts/e2e_ticket11.sh 11 项断言，2026-08-29）：
配置 snapshot_threshold=20 / trailing_logs=5 / snapshot_retention=2 / snapshot_interval=2s
（新增 `raft.snapshot_interval` 配置并接线 raftCfg——库默认 120s，测试以 2s 放大压缩行为）。
60 写后 Leader/Follower 快照目录各保留 1 个文件（retention=2 内）；挂起 Follower 后灌 150 写，
Leader 多轮快照压缩日志窗口，恢复后该 Follower 追平至 bulk-150，节点日志同时出现
InstallSnapshot（raft install=1）与 FSM 全量导入（恢复快照：全量导入完成=1）双证据——
证明窗口外日志已截断、必须走快照路径。Restore 后抽样 5 条（含首条/压缩边界/末条）与
Leader 内容一致，post-snap 增量写到达该节点（增量复制继续）；全部节点 kill 后重启，
快照恢复数据完整（bulk-75 / post-snap 可读）。

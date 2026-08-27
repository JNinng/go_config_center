# 快照为单文件 [元信息头][数据体]，Restore 按本机进度跳过

Raft 快照内容为一个自描述文件：元信息头（raft index/term、格式版本、checksum）+ PG COPY 文本数据体，经标准 InstallSnapshot 流式传输给 Follower。Restore 先读头，与本机 `raft_fsm_meta.last_applied_index` 比对——本机已追平或超前则跳过数据导入，仅推进 Raft 内部状态。

关键动机：hashicorp/raft **每次重启都会从最新本地快照恢复 FSM**，若无此跳过逻辑，每次重启等于 TRUNCATE + 全量重灌 PG（GB 级数据为分钟级），与快速启动目标（冷重启 ≤10s @10 万条）直接冲突。跳过式恢复将重启成本降为重放快照点之后的少量日志。

## Considered Options

- **引用传递**（分片导出到本地/S3，快照仅存 SnapshotMeta.json 引用，Follower 自行下载导入）：推迟——当前量级（≤10 万条 / ≈1GB）下 LAN 流式传输数十秒可完成，分片管理、chunk 下载器、Restoring 下载状态机均属过度设计。保留为 `SnapshotStore` 接口之后的可替换实现，数据量到千万级再启用。

## Consequences

- Restore 期间本机 PG 数据不完整，读路径需门闩（HTTP 503 + Retry-After，进程内原子布尔）。
- FSM 的 `Apply`/`Snapshot`/`Restore` 由 raft 单 goroutine 串行调用，无需自建"快照期间暂停应用"机制。

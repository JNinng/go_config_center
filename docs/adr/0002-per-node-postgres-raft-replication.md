# 每节点独立 PostgreSQL，Raft 复制操作而非数据

每个节点各持一个独立 PG 实例作为唯一业务持久化存储；Raft Log 只含操作指令，由各节点 FSM 幂等地应用到自己的 PG。

## Considered Options

- **三节点共用一个 PG**：被否决——此时 Raft 多数派复制是多余的，PG 自身就是单点真相源，整个共识层没有存在意义。
- **嵌入式 KV（BoltDB/Pebble 等）**被否决——放弃 SQL 查询、备份与既有 PG 运维生态。

## Consequences

- 新节点/落后节点的 PG 需要通过 Raft 快照或日志重放灌数据，这是快照机制存在的原因。
- History 表（版本历史）与业务表同等参与快照导出，否则恢复后回滚能力受损。

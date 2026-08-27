# Raft 库选用 hashicorp/raft，而非 go.etcd.io/raft/v3

选 hashicorp/raft（MPL-2.0，v1.7.x）：全家桶交付（TCP 传输、FileSnapshotStore、FSM/LogStore 接口，配 raft-boltdb 日志存储），MVP 工程量远小于 etcd raft（纯状态机库，传输、WAL、快照存储全部自建，WAL 还需引入 etcd/server 模块）。etcd raft 的优势——ReadIndex 线性一致读、joint consensus、learner——均为本项目明确不需要的能力（见 ADR-0003；集群规模 3~5 节点）。

## Considered Options

- **go.etcd.io/raft/v3**（Apache-2.0，活跃维护）：否决——等价工程量约 ×3，换来的特性用不上。若日后确需线性一致读，可基于 hashicorp/raft 已暴露的 `CommitIndex()` 按 Raft 论文自实现 ReadIndex，不构成锁定死局。

## Consequences

- 成员变更仅支持单服务器变更（无 joint consensus）——≤5 节点足够。
- 库维护低频（v1.7.3，2025-03）但为 Consul/Nomad 的生产引擎；共识核心代码已冻结，低频不构成实质风险。
- 许可证注记：hashicorp/raft 未转入 BUSL（2023 年 HashiCorp 的 BUSL 重新授权仅覆盖产品仓库），仍为 MPL-2.0。

# 全新集群仅允许单节点显式 Bootstrap，其余节点只能 Join

全新集群只在首个节点上执行一次 `--bootstrap`（`raft.BootstrapCluster` 幂等保护：本地已有集群态时再次执行直接报错）。其余节点永久禁止自举：启动后通过 Seed List 寻找 Leader，调用 Join API 加入（`AddVoter` 直接加入为 voter；staging 自动转正在 hashicorp/raft 中未实现，见 DESIGN §4.5），失败则退避无限重试。原设计中 `--force-bootstrap`（全员探测、全无响应则自举）被删除——两节点同时自举或分区期间自举会造成永久脑裂。

## Consequences

- 整集群丢失后的容灾重建是运维手动操作，程序不保留任何自举后门。
- Seed List 仅是发现线索；成员真相源是 Raft 自身的成员表，二者不得混用。

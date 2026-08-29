# go_config_center

多节点 Raft 共识复制、每节点 PostgreSQL 物化的配置存储，提供版本化配置的发布、回滚与变更通知。

## Language

**Namespace**:
配置的名字空间，用于环境/团队隔离（如 dev、prod）。与 DataId 共同唯一标识一条配置。隐式创建（首次 Publish 生效）；可整体删除——删除其下全部存活条目并各写 Tombstone，History 保留可逐条回滚重建。
_Avoid_: 环境、tenant、group

**DataId**:
配置项在 Namespace 内的唯一业务标识（如 db.password）。
_Avoid_: key、name

**Config Entry**:
一条配置的当前态：Namespace + DataId + 内容 + 数据类型 + Version + 标签。
_Avoid_: 配置项、item

**Version**:
单条 Config Entry 的单调递增整数，每次成功 Publish（含 Rollback）加一。
_Avoid_: revision、generation

**History**:
每次写入追加的不可变历史版本记录，是 Rollback 的数据来源。
_Avoid_: 快照（勿与 Raft Snapshot 混淆）、日志

**Rollback**:
用 History 中的旧内容 Publish 一个新 Version 的操作；从不物理撤销已发生的版本。
_Avoid_: 回退、revert、undo

**Expected Version**:
客户端写入时携带的 CAS 条件——期望目标 Config Entry 当前的 Version，不匹配则拒绝写入。
_Avoid_: if-match、乐观锁版本

**Publish**:
一次成功通过共识的配置写入（创建、更新或回滚）。
_Avoid_: 提交、commit、deploy

**Watch**:
客户端按 Namespace + DataId + 已见 Version 订阅变更通知的机制。
_Avoid_: 订阅、listener、push

**Node ID**:
集群成员的持久唯一标识（UUID），生命周期内不随地址变化。
_Avoid_: server、peer、IP

**成员表**:
Raft 集群的成员清单（各成员的 Node ID、地址、投票权），以日志条目复制并提交，是集群成员的唯一真相源。hashicorp/raft 称 Configuration，与业务的 Config Entry 无关。
_Avoid_: 成员配置、集群配置、peers

**Seed List**:
新节点启动时用于寻找 Leader 的候选地址列表，仅服务发现，不是集群成员的真相源。
_Avoid_: peers、成员表

**发现代理**:
节点内寻找 Leader 的后台任务：启动时扫描 Seed List 并执行 Join；加入成功后周期刷新 Leader 的 HTTP 地址（供 307 重定向与被移除自退出核对）。
_Avoid_: 服务发现、registry、proxy

**Bootstrap**:
将空数据目录的节点初始化为单节点新集群的一次性操作，仅允许在一个节点上执行。
_Avoid_: force-bootstrap、自举

**Tombstone**:
History 中 op=delete 的记录，不含配置内容；Rollback 到其之前的任意版本可重建条目。
_Avoid_: 软删除、删除标记

**控制台**:
内嵌于服务二进制的 Web 管理界面：浏览 Config Entry、执行 Publish/Delete/Rollback、查看 History 与成员表。
_Avoid_: 管理页面、Admin UI、Dashboard

# 09 — 成员移除 + quorum 护栏 + 自退出 + cluster/status

**What to build:** `DELETE /v1/cluster/peers/{node_id}` 在 Leader 执行移除前做护栏校验：移除后所需 quorum 必须不大于剩余在线 voter 数，不满足返回 409（DESIGN §4.5，Raft 库不提供此保护）。被移除节点观察到自身 Raft 进入 Shutdown 后以专用退出码 42 退出进程。`GET /v1/cluster/status` 返回完整集群视图（成员、role、suffrage、本节点 applied 进度）。Join 接口幂等。

**Blocked by:** 08 — 发现代理 + Join + 写重定向

**Status:** done

- [x] 3 节点全在线：移除 1 个成功，集群继续读写
- [x] 1 节点宕机时再移除 1 个在线节点 → 409
- [x] 被移除节点进程以退出码 42 退出
- [x] cluster/status 含各成员 node_id / addr / suffrage 与本节点 last_applied_index
- [x] 对已是成员的 Node ID 重复 Join → 幂等 200

验收记录（scripts/e2e_ticket09.sh 15 项断言 + api 单测，2026-08-29）：
护栏实现：Leader 移除前计算移除后 voter 数与 quorum，剩余 voter 以 HTTP 探活
（/internal/discovery，地址取 Join 时登记表；地址未知按离线处理——护栏失效方向为拒绝），
本 Leader 计入在线；online < quorum → 409 quorum_guard。status 端点票 08 已交付。
设计勘误（实现要点）：hashicorp/raft 的 ShutdownOnRemove 仅在节点收到不含自身的新配置时
触发——Leader 不变更时被移除节点收不到任何 raft 消息，永远不会自行退出。补齐主动核对：
各节点周期向 Leader 的 cluster/status 核对自身是否在成员表，缺席即退出 42；查询失败
不退出（保守方向）。e2e 实测：三节点在线移除 → 200 + 被移除节点 ~2s 内以 42 退出 +
二节点集群读写正常；清空 data_dir 重新 Join 回 3 voter（DESIGN §4.5 换 IP 流程）；
宕机一节点后移除在线节点 → 409 且集群写入不受影响；重复 Join 幂等 200 exists。

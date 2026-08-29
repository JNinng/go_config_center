# 10 — 混沌场景套件

**What to build:** docker-compose 之上的可重复混沌场景（DESIGN §8.2 前四项）：写压测中杀 Leader——秒级完成选举、已提交写不丢、未提交写明确失败、客户端经 307/503 语义在选举后自动恢复；Follower 分区 30 秒——期间读旧数据，恢复后无人工干预追平。

**Blocked by:** 08 — 发现代理 + Join + 写重定向；09 — 成员移除 + quorum 护栏 + 自退出 + cluster/status

**Status:** done

- [x] 压测中杀 Leader：新 Leader 秒级产生；杀前已提交的写在新 Leader 上全部可读；未提交的写收到明确失败
- [x] 选举期间写请求 503，选举完成后客户端仅凭 307/重试语义恢复写入
- [x] Follower 分区 30s：期间该节点读旧数据但不报错；恢复后自动追平，无需人工干预
- [x] 场景脚本一键可重复执行（make 目标或脚本入口），退出码反映通过/失败

验收记录（scripts/e2e_ticket10.sh 14 项断言，2026-08-29）：
场景 A：双路 writer 经 Follower 打 200 个写（307 跟随），t=3s 杀 Leader——选举实测 ~2s；
200 写中 82 条返回 200 且在新 Leader 上逐一可读零丢失，其余 118 条均为 503/超时/307 未送达
（307 指向下线 Leader 后客户端连接拒绝，属明确失败）；选举窗口捕获 116 次 503；
压测结束后经同 Follower 全新写 5/5 成功——仅凭重试与 307 语义恢复。
场景 B：被杀 Leader 重启归队（有状态自愈）后，以进程挂起模拟 Follower 分区
（Windows NtSuspendProcess；单机无网络分区手段，"期间读旧数据"不可观测，
以恢复后追平断言替代）：挂起期间其余两节点写入 5/5（quorum 2/3 存活），
恢复后 Follower 无人工干预读全追平。
顺带修复（307 新鲜度）：Leader 更换后 Follower 的 Leader HTTP 缓存绑定获知时的
Raft Leader 地址——地址变化立即失效为 503（而非 307 到死节点），发现代理 5s 刷新后恢复。
脚本入口 bash scripts/e2e_ticket10.sh，清理—拉起—断言—退出码一键可重复。

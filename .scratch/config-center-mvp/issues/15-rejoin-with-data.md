# 15 - 被移除节点带既有数据目录重连

## What to build

被移除节点（以退出码 42 退出或未经退出即下线）以**同一 data_dir、同一 Node ID** 重新启动后，不经清库即可重新加入集群并追平数据（DESIGN §4.5 重连节点）。发现代理每次进程启动都尝试 Join（幂等）——本地有 Raft 状态只说明有持久化数据，不说明仍在成员表：曾被移除节点的本地成员表可能已不含自身（移除日志送达后进程退出），也可能仍含自身（下线早于移除日志复制）。自退出护栏的条件 2 仅在本进程曾观察到自身在成员表后才生效，避免误杀正在加入的节点。

## Blocked by

09 — 成员移除 + quorum 护栏 + 自退出 + cluster/status

## Status: done

- [x] 场景 A：收到移除日志、以 42 退出的节点，原 data_dir 重启后重新加入，恢复 3 voter
- [x] 场景 A：重连后本机 PG 可读移除前数据（跳过式恢复追平，不清库）
- [x] 场景 A：重连后新写入可复制到该节点；护栏未把加入中的节点误杀（进程持续存活）
- [x] 场景 B：被 kill -9（未收到移除日志）后被集群移除的节点，原 data_dir 重启后同样自愈重连
- [x] 场景 B：重连后追平离线期间的写入

验收记录（scripts/ticket15_test.go，2026-08-31）：
根因：main.go 曾以 raft.HasExistingState（v1.7.3 api.go：currentTerm>0 / 有日志 / 有快照
任一即 true，只回答"本地有无持久化数据"）作为发现代理 alreadyMember 依据，被移除节点
重启后永远跳过 Join，随后被自退出护栏以 42 杀掉，重连不可能成立。修复：发现代理去掉
alreadyMember，每次进程启动都 Join（joinFn 对已是成员返回幂等 200）；OnLeader（Leader
地址缓存，即护栏条件 2 的查询依据）从 Join 发起前挪到提交成功后上报——缓存的建立即
"本进程曾为成员"，护栏查到缺席即为移除，加入/重连进行中的节点不持有缓存、不发起查询，
不会被误杀（原实现 OnLeader 先于 Join 上报，存在加入中节点缺席即被 42 误杀的窗口）。
重连追平安全性依据：hashicorp/raft v1.7.3 fsm.go runFSM 仅对 LogCommand 调 fsm.Apply，
成员表变更日志（LogConfiguration）不进 FSM，PG 以 raft_fsm_meta 跳过式追平（DESIGN
§4.4/§4.5）。e2e 实测：场景 A 移除 → 42 退出 → 原 data_dir 重启 → 重新加入恢复 3 voter、
移除前数据可读、新写入复制到位；场景 B kill -9 后移除 → 原样重启 → 自愈重连并追平
离线期间写入。

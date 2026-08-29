# 01 — 骨架 + 单节点 Publish/Get tracer

**What to build:** 全新集群的首个节点以 `--bootstrap` 启动（单 voter），进程连接本机 PG 完成三表迁移（DESIGN §3.1），打开 Raft（boltdb 日志存储 + 文件快照存储 + 内置 TCP 传输）。客户端 PUT 一条配置，请求经 Raft 提交、FSM Apply 在单事务内写 `biz_configs` + History + `raft_fsm_meta`，响应返回分配的 Version；GET 读回条目。进程重启后数据完整。logIndex → 结果 的 Apply 结果回传机制在本张建立（DESIGN §3.3）。

`FSM.Snapshot` 本张允许返回未实现错误：默认 `snapshot_threshold=10000`，本张测试不触达；票 02 落地真实现。

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [x] `--bootstrap` 拉起单节点成功；对已有集群态再次 `--bootstrap` 报错退出
- [x] 同一 DataId 连续 PUT 两次 → version 依次为 1、2，History 各追加一条 publish 记录
- [x] GET 返回条目及 version；不存在的条目返回 404
- [x] 杀进程后重启，GET 读到相同数据；重放通过 `index ≤ last_applied_index` 跳过，不产生重复写入
- [x] 三表更新在同事务内：业务表与 `raft_fsm_meta` 进度恒一致
- [x] 空库从启动到 HTTP 可服务 ≤3s

验收记录（scripts/e2e_ticket01.sh，2026-08-27）：21 项断言全过；空库启动实测 52–554ms；
CAS 冲突 409 在 Apply 阶段产生且不追加 History；重启前后 `raft_fsm_meta.last_applied_index` 不变（重放全部跳过）。

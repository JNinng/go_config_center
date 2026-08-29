# 08 — 发现代理 + Join + 写重定向

**What to build:** 非 bootstrap 节点启动后扫描 Seed List（排除自身地址），经 `/internal/discovery` 找到 Leader 并调用 `POST /v1/cluster/join`；Leader 执行 AddVoter（staging 身份进入，追平后自动转正，ADR-0008/0009）。Seed 全部不可达时指数退避重试——永不自举、不退出。Follower 收到写请求返回 307（Location 指向 Leader）；无 Leader（选举中）返回 503 + Retry-After。docker-compose 拉起 3 节点 + 3 PG 的开发拓扑。

**Blocked by:** 02 — 快照生成与跳过式恢复；07 — Admin Token + healthz + metrics

**Status:** done

- [x] 节点 1 `--bootstrap` 后，节点 2/3 启动自动加入并转正为 voter，`/v1/cluster/status` 可见
- [x] 向 Follower 发 PUT → 307 Location 指向 Leader，跟随重定向写入成功
- [x] 选举窗口内写请求 503 + Retry-After: 1
- [x] Seed 全不可达时节点持续退避重试，不自举、不退出
- [x] /internal/discovery 返回 node_id、role、leader_addr
- [x] docker-compose 一键拉起三节点拓扑并可用

验收记录（scripts/e2e_ticket08.sh 22 项断言 + api/discovery 单测，2026-08-29）：
本机 1 PG + 4 库 + 4 进程等价拓扑验证（用户环境约定；docker 本机不可用，compose 三节点
描述以 Dockerfile + docker-compose.yml + deploy/config-node{1,2,3}.yaml 交付）。
实现要点：internal/discovery 扫描 seeds（值改为对端 HTTP 地址，DESIGN §6.1 注记同步）——
Leader seed 直接 Join，Follower seed 经其 leader_http_addr 缓存再试；加入成功后按周期
刷新 Leader HTTP 地址缓存（供 307 与 discovery 使用），刷新期 Join 幂等不重复。api 层
writeGate：Leader 放行 / Follower 已知 Leader 307（保留方法、路径与查询串）/ 其余 503+Retry-After；
cluster/status 返回 members（node_id/addr/suffrage）与本节点 applied 进度。
设计勘误：hashicorp/raft v1.7.3 的 staging 自动转正从未实现（membership.md 仅提案，
全库无 Promote 调用），AddVoter 缺省即 Voter——改为直接 voter 加入，与 Consul 等生产
用法一致；DESIGN §4.5 已同步注记。e2e 实测：节点 2/3 秒级入列 3 voter；Follower PUT 307
跟随写入后三节点可读；kill Leader 后选举窗口捕获 503+Retry-After:1，重选举后新 Leader
可写、数据完整；bogus seeds 节点存活、退避重试、未自举未当选。

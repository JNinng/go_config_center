# 07 — Admin Token + healthz + metrics

**What to build:** 写接口与集群管理接口要求 `Authorization: Bearer <admin_token>`（DESIGN §5），未认证或错误 token 返回 401；读接口与内部发现接口不认证。`/healthz` 反映进程存活与 PG 连通；`/readyz` 反映 Serving 门闩与 Raft 非 Shutdown；`/metrics` 暴露 DESIGN §7 的 Prometheus 指标最低集合。

**Blocked by:** 05 — Watch 长轮询

**Status:** done

- [x] 无 token / 错 token 的写请求 401；正确 token 通过
- [x] /healthz 在 PG 断连时转为不健康
- [x] 全量恢复期间 /readyz 为 not ready（与票 02 的门闩联动），恢复后转 ready
- [x] /metrics 含 §7 最低集合；`fsm_apply_results_total` 按 result 标签区分 ok / version_conflict / invalid_rollback / not_found

验收记录（scripts/e2e_ticket07.sh 20 项断言 + api/fsm 验收测试，2026-08-29）：
认证（PUT/DELETE/rollback/snapshot 四写端点的无 token 与错 token 401、读接口免认证）；
/healthz 以连接池 Ping 判定（单测模拟池关闭 → 503）；/readyz 读取 Serving 门闩 + Raft
非 Shutdown（单测断言门闩开合两态；恢复期 not ready 由票 02 的门闩与 api 层 503 单测覆盖）；
§7 最低集合在真实进程 /metrics 全部可见——本票补齐 raft_role（GaugeVec 按 leader/candidate/
follower/shutdown 各 0|1）、raft_term、raft_commit_index（周期采集 r.Stats()）与
http_request_duration_seconds{route}（中间件以 Go 1.22+ r.Pattern 作 route 标签）；
fsm_apply_results_total 四个 result 标签序列在制造对应写结果后逐一存在。

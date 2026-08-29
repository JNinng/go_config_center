# 03 — CAS + Delete/Tombstone + History 查询

**What to build:** Publish 支持 Expected Version 三档语义（DESIGN §4.1）：省略 = LWW；`0` = 条目不存在或已删除时通过；`N > 0` = 当前 version 必须为 N。不匹配返回 409 + 当前版本。判定发生在已提交日志的 Apply 阶段，经结果表回传（拒绝的不是提案，而是提案的不生效）。Delete 删除条目并写入 content 为 NULL 的 Tombstone（ADR-0010），版本号照常消耗；已删除条目 GET 返回 404。History 查询接口倒序分页，含墓碑记录。

**Blocked by:** 01 — 骨架 + 单节点 Publish/Get tracer

**Status:** done

- [x] expected_version 省略时并发写最后写赢，版本序列无空洞
- [x] expected_version=0：对不存在/已删除条目通过，对存在条目 409
- [x] expected_version=N：匹配通过；不匹配 409 且响应含 current_version
- [x] Delete 后 GET 404；History 存在 op=delete、content NULL 的记录且占用一个版本号
- [x] versions 接口按 version 倒序，支持 limit / before_version 分页

验收记录（scripts/e2e_ticket03.sh 24 项断言 + store/fsm/api 三层集成测试，2026-08-29）：
Publish 的 CAS 判定票 01 已落在 Apply 阶段（store.ApplyPublish），本票补齐 Delete 全链路：
`DELETE ...?expected_version=N` → raftlog.DeleteData → FSM OpDelete 分派 → store.ApplyDelete
单事务（删 biz_configs 行 + 追加 op=delete/content/tags/data_type 全 NULL 的 Tombstone + 推进 meta）。
语义决策：Delete 对"不存在或已删除"一律 404（无物可删，进度照常推进）；expected=0 与删除互斥
（走到 CAS 时条目必存活，恒 409）；删除后重建延续版本序列（墓碑占版本号不回收）。
History 查询 `GET .../versions?limit=&before_version=` 读本机 PG，受 Serving 门闩约束（恢复期 503），
limit ≤0 取默认 50、上限 1000 收敛。

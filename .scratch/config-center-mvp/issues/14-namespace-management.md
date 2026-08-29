# 14 - Namespace 管理（控制台新增/删除命名空间）

## 需求

控制台初始启动无 Namespace 时无法进行任何新增/修改（列表页入口在 Namespace 下）。增加：
- 新增 Namespace（解决空态死锁）；
- 删除 Namespace。

## 规格

- **新增**：Namespace 维持隐式创建契约（DESIGN §5.1，首次 Publish 生效，无独立创建 API）——控制台"新建 Namespace"仅校验命名（非空、≤64、不含空白与路径分隔符）并跳转到新列表页，空态引导创建首条配置。命名非法由后端校验兜底（raftlog validateNamespace）。
- **删除**：新增 Raft 日志 op `delete_namespace`（DESIGN §3.2 op 枚举扩展，向后兼容：未知 op 仍走 advanceOnly），原子删除该 Namespace 下全部存活 Config Entry，各追加一条 Tombstone（墓碑版本 = 删除前版本 + 1，版本序列延续），History 保留可逐条回滚；单事务（业务 + Tombstone + raft_fsm_meta）。无存活条目为空操作（幂等，200 entries:0）。
- 新端点 `DELETE /v1/namespaces/{ns}`：认证 + 307/503 写语义 + Raft 提案。响应 200 `{status:"deleted", entries:N}`。
- Store 接口新增 `ApplyDeleteNamespace`（pg 实现：单条 CTE 完成 DELETE...RETURNING + 逐行 INSERT Tombstone，结果携带被删条目与墓碑版本）；Result 增 `Entries`/`Deleted` 字段。
- FSM Apply 到 delete_namespace 成功后逐条唤醒该 Namespace 下挂起的 Watch（Deleted 语义与单条删除一致）。
- 控制台：侧栏 Namespace 区块"＋新建"（行内输入）；每个 Namespace hover 显示删除按钮，输入 Namespace 名二次确认；删除后当前页是该 Namespace 时跳回首页并刷新清单。

## 验收（scripts/ticket14_test.go）

1. 向新 Namespace 发布首条即出现于清单（隐式创建）。
2. 删除 Namespace = 200 deleted entries:3；biz_configs 清空；History 3 条 delete Tombstone（content NULL、版本 = 删前+1）；清单不再列出；GET 单条 404。
3. 再次删除（空）200 entries:0 幂等。
4. 无 token 401。
5. 删除后回滚到墓碑前版本可重建条目。

单测覆盖：raftlog 编解码/校验/白名单、pg 事务语义与幂等、fsm 分发 + Watch 逐条通知、api 端点映射（200/401/307/500）。

## 验收记录

- 单测：raftlog（delete_namespace 编解码/校验/Decode 白名单）、pg（单事务 CTE：3 条删除 + 墓碑版本 = 删前+1 + meta 推进 + 空 ns 幂等 + 删除后回滚重建）、fsm（op 分发 + 结果携带被删条目 + 逐条唤醒 Watch）、api（200 entries / 401 / 307 / 500）全绿。
- e2e 票 14 全部验收项通过（含全量套件 01~14 一次通过）。
- 控制台：侧栏"＋新建 Namespace"跳转引导创建首条；hover 删除按钮 + 输入 Namespace 名二次确认；删除后跳回首页并刷新。

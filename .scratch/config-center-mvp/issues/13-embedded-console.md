# 13 - 内嵌控制台（可视化管理界面）

决策记录：ADR-0012（构建期 node 前端 + go:embed 内嵌，运行期零外部依赖）。

## 需求

为存储新增控制台：配置列表、Publish/Delete/Rollback、History 与版本对比、集群实例只读展示。约束：部署产物仍是单二进制；部署环境无外部依赖；构建期允许 node。

## 规格

- 技术栈 Vite + Vue 3 + TypeScript（源码 `web/console/`），产物经 `scripts/build-console.sh` 拷入 `internal/console/dist/` 内嵌；dist 整体不入库（仅 `.keep` 保证 embed 目录存在），占位页 `internal/console/placeholder.html` 保证纯 `go build` 通过且构建永不触碰被跟踪文件。
- 根路径挂载：`GET /` ServeMux 通配兜底 + SPA fallback；精确 API 路径不受遮蔽。
- API 附受限 CORS 头：控制台写请求经 307 跳 Leader 属跨源请求（ADR-0012）。
- 后端契约新增（DESIGN §5.1）：
  - `GET /v1/namespaces`：仍有存活条目的 Namespace 清单；
  - `GET /v1/namespaces/{ns}/configs?limit=&after_id=`：仅存活条目、data_id 字典序 keyset 分页、元数据不含 content。
- Store 接口新增 `ListEntries` / `ListNamespaces`（pg 实现，遵守 ADR-0011 存储边界）。
- 控制台：token 存 localStorage；发布/删除自动携带 Expected Version；历史任选两版本 diff（jsdiff）；回滚确认后发 `to_version`；集群页只读展示成员表/Leader/进度。
- v1 范围外：集群操作进 UI（join/remove/snapshot）、实时刷新。

## 验收（scripts/ticket13_test.go）

1. `GET /` 200 且为 text/html 控制台入口。
2. 未知路径 SPA fallback 回退入口页，非 404。
3. `/healthz`、`/v1/namespaces` 不被兜底遮蔽；未注册路径 POST 405（挂载后语义，原 404）。
4. 全部响应带 `Access-Control-Allow-Origin: *`；OPTIONS 预检 204 + 许可头。
5. Namespace 清单去重字典序；条目列表仅存活条目、keyset 分页、元数据不含 content、`next_after_id` 末页省略；`limit=abc` 400。

控制台前端交互（表单 CAS、diff、回滚）不在 e2e 覆盖内，由单测（internal/console handler、API 端点）与人工验收承担。

## 验收记录

- 单测：`internal/console`（SPA fallback、入口页）、`internal/api`（列表/Namespace/CORS/挂载不遮蔽）、`internal/store/pg`（keyset 分页、Tombstone 不出现、Namespace 去重）全绿（GCC_TEST_PG_DSN 下）。
- e2e 票 13 全部验收项通过（纯 go build 与前端产物已构建两种二进制均验）。
- 全量 e2e：12 张旧票 + 票 13 通过。票 10（混沌）在本机存在与本变更无关的时序抖动：同一二进制多次运行结果混合、失败断言漂移（选举后恢复写/分区期写/Follower 追平），隔离复跑与全量复跑均可通过；本变更不触及复制与写路径（Raft 走 TCP）。
- 冒烟：根路径返回 Vue 入口、`/assets/*` 以正确 Content-Type 服务、未知路径回退入口页、`/healthz` JSON 不被遮蔽。

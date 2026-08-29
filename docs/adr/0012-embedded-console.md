# 控制台：构建期 node 前端 + go:embed 内嵌，运行期零外部依赖

部署产物是单二进制、部署环境无任何外部依赖（无 CDN、无 node）是硬约束；可视化管理（配置列表、Publish/Delete/Rollback、History、集群状态）要求引入 Web 前端。构建期允许使用 node 工具链。

## Decision

- 前端为 Vite + Vue 3 + TypeScript（源码 `web/console/`），构建产物经 `scripts/build-console.sh` 拷入 `internal/console/dist/`，由 `go:embed` 内嵌，同一 mux 服务——部署形态仍是单二进制，运行期零外部依赖。
- 挂载于根路径：`GET /` 以 ServeMux 通配模式注册，精确注册的 `/v1/*`、`/healthz`、`/readyz`、`/metrics`、`/internal/discovery` 按模式特异性优先，不受遮蔽；dist 内不存在的路径回退 index.html（SPA fallback）。
- dist 不入库：仓库内 dist 仅含 `.keep`（保证 go:embed 目录存在），构建产物永不触碰被跟踪文件；占位页内置于 Go 侧 `internal/console/placeholder.html`——未构建时直接 `go build` 仍通过，访问 `/` 得占位页（"未构建"是可观测状态而非故障）。
- API 附带受限 CORS 头（`Access-Control-Allow-Origin: *`、允许 `Authorization`/`Content-Type`、OPTIONS 预检 204）：控制台可从任意节点加载，写请求经 307 跳 Leader 属跨源请求，浏览器要求目标带 CORS 头才放行携带 `Authorization` 重发。token 经请求头携带而非 cookie，不引入凭据语义，内网信任模型（读接口不认证）不变。

## Considered Options

- **零构建 vanilla TS**：否决——列表/编辑/diff/集群多视图的客户端状态管理手写维护性差，且构建期 node 已被允许。
- **React / Svelte**：否决——团队栈为 Vue；差异不触及任何契约。
- **dist 入库**：否决——每次前端改动产生大量产物 diff，且产物与源码形成双真相源。
- **`/ui` 前缀挂载**：否决——根路径"打开即用"，通配模式的特异性规则已保证 API 不被遮蔽，前缀只增加 URL 噪音。
- **UI 检测非 Leader 后引导跳转 Leader 页面**（替代 CORS）：否决——Leader 漂移后跳转地址失效，且无法覆盖 307 已返回的窗口；CORS 使任意节点都是控制台入口，写请求按 307 语义自动重发。

## Consequences

- 控制台改动需 node 工具链重跑构建脚本再编译；仅 Go 侧改动不受影响。
- 纯 `go build` 的二进制访问 `/` 得占位页——"未构建"是可观测状态而非故障。
- CORS 对所有来源开放：内网信任模型下成立；若未来暴露到不受信网络，须收紧为来源白名单（与读接口鉴权同批重估）。

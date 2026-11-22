# go_config_center

多节点 Raft 共识复制、每节点 PostgreSQL 物化的配置存储。Go 单二进制，提供版本化配置的发布、回滚与变更通知。

## 功能特性

- **版本化存储**：每次发布追加不可变历史（History），支持任意深度回滚；删除产生 Tombstone，回滚可重建条目
- **CAS 条件写入**：`expected_version` 省略时为 LWW，`0` 为 create-if-absent，`N > 0` 精确匹配当前版本
- **高可用**：3~5 节点 Leader-Follower 集群（hashicorp/raft），自动选举与故障转移；成员变更带 quorum 护栏
- **变更通知**：HTTP 长轮询 Watch，客户端携带已见版本挂起，变更或超时返回
- **快速启动**：空库首启 ≤3s；10 万条配置冷重启 ≤10s（跳过式快照恢复）
- **轻量部署**：单二进制 + 每节点一个独立 PostgreSQL，无其他外部依赖
- **可观测**：Prometheus 指标（共识状态、Apply 延迟、Watch 挂起、快照健康）与 `/healthz` `/readyz` 健康端点
- **内嵌控制台**：单二进制内嵌 Web 管理界面（配置浏览、发布、回滚、删除与 Namespace 管理），构建期 node 构建、运行期零依赖

## 快速开始

前置要求：Go 1.26+，PostgreSQL 12+（每节点一个库）。

### 构建与单节点启动

```bash
go build -o config-center ./cmd/config-center

cp config.example.yaml config.yaml
# 编辑 pg_dsn 与 admin_token
./config-center --config config.yaml --bootstrap   # 全新集群仅首节点执行一次
```

`pg_dsn` 指向的库不存在时会自动创建（要求连接账号具备 CREATEDB 权限）；若本节点 PG 进度与 Raft 日志不属于同一 lineage，启动时护栏会拒绝并指路 DESIGN §6.4。

### 三节点集群（docker compose）

```bash
docker compose up -d
```

仅 node1 执行 `--bootstrap`，node2/3 经 Seed List 自动加入并追平数据。节点映射：`8080/8081/8082`。

### 写入与读取

```bash
TOKEN=change-me
# 发布（CAS：携带 expected_version 做条件写）
curl -X PUT "http://127.0.0.1:8080/v1/namespaces/dev/configs/db.password" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"content":"s3cret","data_type":"TEXT","expected_version":0}'
# → {"created":true,"version":1}

# 读取（任意节点，允许毫秒级陈旧）
curl "http://127.0.0.1:8080/v1/namespaces/dev/configs/db.password"

# 回滚到版本 1（产生新版本，从不物理撤销）
curl -X POST "http://127.0.0.1:8080/v1/namespaces/dev/configs/db.password/rollback" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"to_version":1}'

# 长轮询 Watch：挂起至版本越过 1、被删除或超时
curl "http://127.0.0.1:8080/v1/namespaces/dev/configs/db.password/watch?v=1&timeout=30s"
```

写请求发到 Follower 时返回 307 重定向到 Leader（`curl -L` 可直接跟随）；选举窗口内返回 503 + `Retry-After: 1`。

## API 一览

| 方法与路径 | 语义 |
|---|---|
| `PUT /v1/namespaces/{ns}/configs/{data_id}` | 发布。200 `{version, created}`；409 版本冲突 |
| `GET /v1/namespaces/{ns}/configs/{data_id}` | 读当前条目。404 = 不存在或已删除 |
| `DELETE /v1/namespaces/{ns}/configs/{data_id}?expected_version=N` | 删除并写 Tombstone |
| `GET .../versions?limit=50&before_version=` | 历史版本倒序分页 |
| `POST .../rollback` | body `{to_version}`，发布为新版本 |
| `GET /v1/namespaces` | 存活 Namespace 清单（字典序） |
| `DELETE /v1/namespaces/{ns}` | 原子删除该 Namespace 下全部存活条目并各写 Tombstone，幂等 |
| `GET .../watch?v=N&timeout=30s` | 长轮询变更通知 |
| `GET /v1/cluster/status` | 集群视图（角色、Leader、成员表、Apply 进度） |
| `POST /v1/cluster/join` · `DELETE /v1/cluster/peers/{node_id}` | 成员加入 / 移除（移除带 quorum 护栏） |
| `POST /v1/cluster/snapshot` | 手动触发快照 |
| `/healthz` · `/readyz` · `/metrics` | 存活 / 就绪 / Prometheus 指标 |

写接口与集群管理接口要求 `Authorization: Bearer <admin_token>`；读接口不认证（面向可信内网）。完整请求/响应语义见 [docs/DESIGN.md](docs/DESIGN.md) §5。

## 架构概要

- **写**：客户端 → Leader 提案 → Raft 多数派提交 → 各节点 FSM 将操作事务性地写入本机 PostgreSQL
- **读**：客户端 → 任意节点 → 本机 PG（Follower 允许落后数条已提交日志）
- **快照**：单文件自描述流（JSON 头 + PG COPY 数据体）；恢复时本机进度已追平则跳过导入，是冷重启 ≤10s 的依据
- **成员**：Node ID 为持久 UUID，成员真相源是 Raft 成员表；被移除节点以退出码 42 自行退出

术语以 [CONTEXT.md](CONTEXT.md) 为准（Namespace、DataId、Publish、Tombstone 等）；设计契约与核心机制见 [docs/DESIGN.md](docs/DESIGN.md)，关键决策（数据库布局、Raft 选型、版本模型等）见 [docs/adr/](docs/adr/)。

## 部署

- 集群拓扑：奇数 voter（3 或 5），每节点 1 进程 + 1 独立 PostgreSQL，跨机分布以容故障域
- 配置项说明见 `config.example.yaml` 内注释与 DESIGN §6.1；多机部署须显式配置 `http_advertise`
- 灾难恢复（quorum 永久丢失）为人工流程，见 DESIGN §6.4

## License

[MIT](LICENSE)

# go_config_center 设计文档

术语以 [CONTEXT.md](../CONTEXT.md) 为准（Namespace、DataId、Publish、Rollback、Tombstone 等）。关键决策的理由与被否方案记录于 [docs/adr/](./adr/)，本文只陈述设计本身。

## 1. 目标与非目标

### 1.1 目标

- **版本化配置存储**：每次 Publish 追加不可变 History，支持任意深度 Rollback（ADR-0006）。
- **高可用**：3~5 节点 Leader-Follower 集群，基于 Raft 多数派复制，自动选举与故障转移（ADR-0002、ADR-0005）。
- **快速启动**：空库首启 ≤3s；10 万条配置冷重启 ≤10s（ADR-0007）。
- **变更通知**：HTTP 长轮询 Watch（ADR-0004）。
- **轻量部署**：Go 单二进制，每节点一个独立 PostgreSQL，无其他外部依赖。

### 1.2 非目标

以下能力明确不在范围内，接口不得为其预留隐式语义：

- 配置内容加密存储（以 PG 访问控制为安全边界）
- 多租户与细粒度权限（仅有单一 Admin Token）
- 灰度发布、配置对比合并
- 客户端 SDK、gRPC
- 引用式/分片快照（数据存 S3 等外部存储，快照文件仅存引用）。`SnapshotStore` 是预留的替换点，数据量达千万级时另行实现（ADR-0007）

### 1.3 量级假设

≤10 万条 Config Entry、单条 ≤1MB、写 ≤100 QPS、读 ≤1 万 QPS。超出该量级的设计压力点只有一个：快照体积——处理方式见非目标最后一条。

## 2. 总体架构

组件：

| 组件 | 职责 |
|---|---|
| API 层 | net/http，配置与集群 API、307 重定向、Watch 长轮询 |
| Raft Core | hashicorp/raft + raft-boltdb（日志）+ FileSnapshotStore（快照落盘）+ 内置 TCP 传输 |
| PostgresFSM | 将已提交日志事务性写入本机 PG；快照导出/导入 |
| 发现代理 | 启动时扫描 Seed List 寻找 Leader 并 Join；加入后周期刷新 Leader 的 HTTP 地址（供 307 重定向） |

数据流向：

- **写**：Client → Leader → Raft 日志 → 多数派提交 → 各节点 FSM Apply → 各自 PG。已提交的写在 Leader 任期内保证不丢。
- **读**：Client → 任意节点 → 本机 PG。Follower 读允许落后 Leader 数条已提交日志（正常毫秒级）；客户端要读到自己的写（read-your-writes），以 Publish 返回的 Version 为基准，用 Watch 的 `v` 参数或 GET 结果比对版本即可（ADR-0003）。
- **Watch**：Client 挂在任意节点；FSM Apply 到 Publish/删除时唤醒本机挂起的 Watch。
- **快照**：Leader/Follower 各自本地生成；落后节点通过标准 InstallSnapshot 接收。

集群不依赖节点间时钟同步（无 lease read）；事件先后以 Raft term/index 判定。

## 3. 数据模型

### 3.1 PostgreSQL Schema

每节点独立 PG，含三张表，同库同事务操作：

```sql
CREATE TABLE biz_configs (
    id          BIGSERIAL PRIMARY KEY,
    namespace   VARCHAR(64)  NOT NULL,
    data_id     VARCHAR(255) NOT NULL,
    data_type   VARCHAR(50)  NOT NULL,            -- JSON | YAML | TEXT
    content     TEXT         NOT NULL,
    version     BIGINT       NOT NULL,
    tags        JSONB,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (namespace, data_id)
);

CREATE TABLE biz_config_history (
    id          BIGSERIAL PRIMARY KEY,
    namespace   VARCHAR(64)  NOT NULL,
    data_id     VARCHAR(255) NOT NULL,
    version     BIGINT       NOT NULL,
    op          VARCHAR(16)  NOT NULL,            -- publish | delete
    data_type   VARCHAR(50),
    content     TEXT,                             -- delete 时为 NULL（Tombstone）
    tags        JSONB,
    actor       VARCHAR(255),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (namespace, data_id, version)
);

CREATE TABLE raft_fsm_meta (
    id                  INT  PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_applied_index  BIGINT NOT NULL,
    last_applied_term   BIGINT NOT NULL
);
```

不变量：

- `biz_configs.version` 恒等于该条目 History 中的最大 version。
- 版本号按 `(namespace, data_id)` 单调分配，Publish 与 Delete 均消耗版本号；删除后重建延续同一序列。
- History 只追加，永不 UPDATE/DELETE。
- `raft_fsm_meta` 与业务表同事务更新，二者进度恒一致——这是 §4.4 跳过式恢复（本机进度不落后于快照点时跳过数据导入）的依据。

### 3.2 Raft 日志格式

一条日志一个操作，JSON 信封，带格式版本号：

```json
{"v": 1, "op": "publish",  "data": {"namespace": "...", "data_id": "...", "data_type": "JSON", "content": "...", "tags": {}, "expected_version": 3, "actor": "user-a"}}
{"v": 1, "op": "delete",   "data": {"namespace": "...", "data_id": "...", "expected_version": null, "actor": "user-a"}}
{"v": 1, "op": "rollback", "data": {"namespace": "...", "data_id": "...", "to_version": 3, "actor": "user-a"}}
{"v": 1, "op": "delete_namespace", "data": {"namespace": "...", "actor": "user-a"}}
```

`v` 为日志格式版本，升级编解码时递增并兼容读取。`op=delete_namespace` 原子删除该 Namespace 下全部存活条目并各写 Tombstone（History 保留可逐条回滚，见 §5.1）；旧版本节点解码不到该 op 时按 §3.3 的 advanceOnly 推进进度不崩溃。多操作批量 Publish 是后续扩展（`op: "batch"`），不改变单操作语义。

### 3.3 FSM 不变量

- **Apply 幂等**：`index ≤ last_applied_index` 时跳过（重放安全）；同一 index 重复应用结果一致。
- **Apply 原子**：业务表 + History + `raft_fsm_meta` 在单个 PG 事务内提交。
- **Apply 与 Snapshot/Restore 串行**：由 Raft 单 goroutine 调度保证，FSM 内不建锁。
- **Apply 结果回传**：CAS 失败、Rollback 目标无效等发生在**已提交日志的 Apply 阶段**——不是拒绝提案。FSM 维护 `logIndex → 结果` 表；API 处理方在提案应用完成后取回结果，据此返回 409/404。该表仅保留近期条目。

## 4. 核心机制

### 4.1 写入与一致性

1. Client 向任意节点发写请求。
2. Follower/候选者不处理写：有 Leader 时返回 **307**（`Location` 指向 Leader，方法与 body 由客户端按 307 语义重发）；无 Leader（选举中）返回 **503 + Retry-After: 1**。
3. Leader `Propose`，多数派提交，FSM Apply。
4. 响应：成功 200 + 新 Version；CAS 不匹配 409 + 当前 Version；条目不存在 404。其余 Apply 结果为 500 `apply_failed` + 原因：`replayed`（日志 index 不大于本节点 FSM 进度）说明本节点 PG 与 Raft 日志不是同一 lineage，须按 §6.4 重建集群。

Expected Version（CAS）规则，`expected_version` 省略时为 LWW（最后写赢）：

| 取值 | 通过条件 |
|---|---|
| 省略 | 恒通过 |
| `0` | 条目不存在**或**已删除（create-if-absent） |
| `N > 0` | 条目当前 version = N（含墓碑版本） |

### 4.2 版本、删除与回滚

- **Publish**：upsert `biz_configs`（version+1），History 追加 `op=publish`。
- **Delete**：删除 `biz_configs` 行，History 追加 `op=delete`、content 为 NULL 的 **Tombstone**（ADR-0010）。
- **Rollback**：以 `to_version` 从 History 取内容，作为一次 Publish 发布为**新版本**；从不物理撤销。`to_version` 的解析发生在 FSM Apply 阶段（而非 API 层预取），以消除与并发写的竞态。`to_version` 无效（History 中不存在、指向 Tombstone、≥ 当前版本）时该日志 Apply 结果为失败。

### 4.3 Watch（长轮询）

`GET .../{data_id}/watch?v=N&timeout=30s`：

- 挂起至该条目 version > N、条目被删除、或超时。
- 响应：变更 → 200 `{status: "active", entry: {...}}` 或 `{status: "deleted", version: V}`；超时 → 200 `{status: "timeout", version: N}`，客户端原样重挂。
- 挂起可在任意节点受理（Follower 同样 Apply 日志并在本地唤醒）；节点落后或分区只表现为超时，由客户端重试兜底。

### 4.4 快照与恢复（ADR-0007）

**格式**：单文件自描述流：

```
[magic 8B "GCCSNP01"][JSON 头一行][数据体]
```

头：`{format_version, raft_index, raft_term, tables: {<表名>: {rows, bytes}}, checksum}`。`checksum` 为数据体 sha256；数据体为三表（含 `raft_fsm_meta`）按固定表序排列的 PG `COPY TO STDOUT` 文本流，各分节以头中声明的字节长度分界（分节内容含 COPY 结束标记）。

**生成**（`FSM.Snapshot`）：单个 REPEATABLE READ 事务导出三表，导出事务内读到的 `last_applied_index/term` 即快照点。达到 `snapshot_threshold` 条日志或手动触发。

**恢复**（`FSM.Restore`）——Restore 在每次重启与 InstallSnapshot 时都会触发，必须满足：

1. 校验 magic/格式版本/checksum，不符即失败（触发全量路径，见下）。
2. 读头中 `raft_index`，与本机 `raft_fsm_meta.last_applied_index`（FSM 启动时装载、
   随 Apply/Restore 同步推进的内存进度）比对：**本机已追平或超前 → 跳过数据导入**，仅推进 Raft 内部状态。这是冷重启 ≤10s 的依据。
3. 否则全量导入：置本机 Serving 门闩为 false → 单事务 TRUNCATE 三表 + COPY 导入 → 更新内存 Raft 状态 → 门闩恢复 true。

Serving 门闩为进程内原子布尔；false 期间所有读 API 返回 **503 + Retry-After**，`/readyz` 为 not ready。

### 4.5 成员生命周期

**身份**：Node ID 为 UUID，首次启动生成并持久化于 `<data_dir>/node-id`，生命周期内不变；成员表以 Node ID 为键，`raft_addr` 只是属性（ADR-0009）。

**Bootstrap**（ADR-0008）：全新集群仅在首节点执行一次 `--bootstrap`（单 voter 集群）；本地已有集群态时该命令必须报错退出。其余节点永久禁止自举——不存在"探测失败则自举"的路径。

**Join**：新节点/重连节点启动后扫描 Seed List（排除自身地址），逐个调 `GET /internal/discovery`：对方为 Leader → 向其 `POST /v1/cluster/join`；对方为 Follower → 从响应取 Leader 地址再试。全部不可达 → 指数退避重试，永不自举、不退出。Leader 收到 Join 后 `AddVoter`，接口幂等（已是成员 → 200）。注：加入即 voter。hashicorp/raft 的设计文档（membership.md）提出过 staging——先以非投票身份加入、复制追平后自动转为 voter——但库从未实现，`AddVoter` 直接赋予投票权；Consul 等生产用法相同。

**Remove**：`DELETE /v1/cluster/peers/{node_id}`，执行前护栏校验——**移除后所需 quorum 不得超过剩余在线 voter 数**（在线以对该节点 HTTP 探活判定，执行方 Leader 自身视为在线），不满足返回 409（Raft 库不提供此保护）。

Raft 没有"通知节点已被移除"的机制。成员表（hashicorp/raft 称 Configuration，与业务的 Config Entry 无关）以日志形式复制：每次成员变更，Leader 追加一条日志，内容是变更后的完整成员表，并立即停止向被移除节点复制（心跳随之停止）——此后它收不到任何 Raft 消息。`ShutdownOnRemove`（库默认开启）只覆盖一种情形——**本节点作为 Leader，提交的成员表已不含自身**——此时自动关闭 Raft，即移除 Leader 自身的场景（该条日志提交后立即触发，集群随后重新选举）。移除 Follower 时该机制不会生效：Follower 应用不含自身的成员表时，库不做任何自检；而 v1.7.3 对不在成员表中的节点发来的投票请求直接拒绝（在处理 term 之前），它无法借重新当选走到上述关闭点。

因此被移除节点的退出由本服务自己保证：自退出循环每 2s 检查一次，命中任一条件即以专用退出码 `EXIT_REMOVED = 42` 终止进程——

1. Raft 已关闭（状态 Shutdown，含上述库内自关闭）；
2. 非 Leader 节点向发现代理缓存的 Leader 地址查 `GET /v1/cluster/status`，响应成员表已不含自身 Node ID。

条件 2 查询失败（无 Leader、网络分区、地址未知）不视为被移除，不退出——宁可多留一个进程，不误杀在线节点；人工优雅停机不触发退出。部署单元应将退出码 42 配置为不自动重启。

**换 IP**：运维流程 = Leader 上移除旧成员（护栏校验）→ 节点清空 `data_dir`、以新 Node ID 重新 Join（快照 + 日志追平，分钟级）。

## 5. API 规范

所有响应为 JSON。写接口与集群管理接口要求 `Authorization: Bearer <admin_token>`；读接口与内部接口不认证（部署于可信内网）。全部响应附带受限 CORS 头（`Access-Control-Allow-Origin: *`、允许 `Authorization`/`Content-Type`、OPTIONS 预检 204）——控制台（§5.5）从任意节点加载时，写请求经 307 跳 Leader 属跨源请求；token 经请求头携带而非 cookie，通配来源不引入凭据语义（ADR-0012）。

### 5.1 配置管理

| 方法与路径 | 语义 |
|---|---|
| `PUT /v1/namespaces/{ns}/configs/{data_id}` | Publish。body: `{content, data_type, tags?, expected_version?}`。200 `{version, created}`；409 `{error:"version_conflict", current_version}` |
| `GET /v1/namespaces/{ns}/configs/{data_id}` | 读当前条目（本机 PG，允许陈旧）。404 = 不存在或已删除 |
| `DELETE /v1/namespaces/{ns}/configs/{data_id}?expected_version=N` | 删除并写 Tombstone。200 `{version, status:"deleted"}`；404/409 同上 |
| `GET /v1/namespaces/{ns}/configs/{data_id}/versions?limit=50&before_version=` | History 倒序分页 |
| `POST /v1/namespaces/{ns}/configs/{data_id}/rollback` | body `{to_version}`。200 `{version}`；404 条目从未存在；409 目标无效 |
| `GET /v1/namespaces/{ns}/configs/{data_id}/watch?v=N&timeout=30s` | 见 4.3 |
| `GET /v1/namespaces` | 仍有存活条目的 Namespace 去重清单（字典序） |
| `DELETE /v1/namespaces/{ns}` | 原子删除该 Namespace 下全部存活 Config Entry，各写一条 Tombstone（墓碑版本 = 删前版本 + 1，版本序列延续）；History 保留，可对任一条目回滚重建。200 `{status:"deleted", entries:N}`；无存活条目幂等 200 `entries:0` |
| `GET /v1/namespaces/{ns}/configs?limit=100&after_id=` | 条目列表：仅存活条目，data_id 字典序 keyset 分页；响应 `{entries: [<元数据，不含 content>], next_after_id?}`，本页不满 limit 时省略 `next_after_id`（已到末尾） |

Namespace 隐式创建（首次 Publish 即生效），无独立创建接口——"新增 Namespace"由控制台引导向新 Namespace 发布首条配置完成。

### 5.2 集群管理（Leader 处理，否则 307/503）

| 方法与路径 | 语义 |
|---|---|
| `GET /v1/cluster/status` | 本节点视角：`{node_id, role, term, leader_id, leader_addr, leader_http_addr, last_applied_index, last_log_index, commit_index, last_contact_ms, restoring, members: [{node_id, addr, suffrage, role?, term?, last_log_index?, commit_index?, applied_index?, last_contact_ms?, online?}]}`。任意节点可调。`last_contact_ms`：到 Leader 的最近心跳毫秒数，0=自身为 Leader，-1=从未收到心跳。成员行自报字段尽力而为：本节点行必有；其余成员经 `/internal/discovery` 探测（HTTP 地址需本节点 Join 时登记过）填充，缺省 = 本节点视角未知，`online` 仅区分已知在线/无响应，不臆断离线成员已下线 |
| `POST /v1/cluster/join` | body `{node_id, raft_addr, http_addr?}`（http_addr 登记后供移除护栏探活）。202 已受理；幂等 200 |
| `POST /v1/cluster/snapshot` | 手动触发 FSM 快照（§4.4"达到阈值或手动触发"）。200 `{status:"ok", snapshot_index}`；快照点之后无新日志时再触发 200 `{status:"noop"}` |
| `DELETE /v1/cluster/peers/{node_id}` | 移除成员。200；409 护栏拒绝；404 非成员 |

### 5.3 内部接口（不认证）

`GET /internal/discovery` → `{node_id, role, leader_addr, leader_http_addr, term, last_log_index, commit_index, applied_index, last_contact_ms}`（role ∈ leader/follower/candidate）。调用方：发现代理（寻找 Leader）、移除护栏（探活）与 `/v1/cluster/status` 的成员自报合并（§5.2）。

### 5.4 运维端点

| 路径 | 语义 |
|---|---|
| `/healthz` | 进程存活 + PG 可连 |
| `/readyz` | Serving 门闩为 true 且 Raft 非 Shutdown；LB 据此摘流 |
| `/metrics` | Prometheus 格式，见 §7 |

### 5.5 控制台

控制台（CONTEXT.md「控制台」）是内嵌于二进制的 Web 管理界面（ADR-0012）：

- 构建期以 Vite + Vue 3 产出静态资源（`web/console/` → `scripts/build-console.sh` → `internal/console/dist/`），`go:embed` 内嵌，运行期零外部依赖；dist 不入库，未构建时 `/` 返回占位页。
- 挂载于根路径：`GET /` 为 ServeMux 通配兜底，精确注册的 API 路径按特异性优先不受遮蔽；dist 内不存在的路径回退 index.html（SPA fallback）。挂载后未注册路径的 POST 由 404 变为 405（命中 `GET /` 模式但方法不符，ServeMux 语义）。
- 控制台静态资源不鉴权（与读接口同属内网信任模型）；写操作由使用者在页面内提供 admin_token，经 `Authorization` 头携带，存于浏览器 localStorage。
- 页面发布/删除均自动携带 Expected Version（CAS），冲突时展示当前版本并要求刷新。
- Namespace 管理：侧栏可新建（引导向新 Namespace 发布首条，隐式创建）与删除（调 `DELETE /v1/namespaces/{ns}`，输入 Namespace 名二次确认；删除后当前页是该 Namespace 时跳回首页）。

## 6. 配置与部署

### 6.1 配置文件（config.yaml）

```yaml
# 留空则首次启动生成 UUID 并持久化到 <data_dir>/node-id
node_id: ""
http_addr: ":8080"
# 对外通告的 HTTP 基地址（307 重定向与 Join 响应携带）；
# 留空按 raft_addr 的主机 + http_addr 的端口推导，跨机部署时须显式配置
http_advertise: "http://192.168.1.10:8080"
# Raft 对外通告地址（成员间复制流量）
raft_addr: "192.168.1.10:7946"
data_dir: "./data"
admin_token: "change-me"

pg_dsn: "host=127.0.0.1 user=config dbname=config_center sslmode=disable password=..."

# Seed List：仅用于启动时寻找 Leader，不是集群成员清单；
# 值为对端节点的 HTTP 地址（发现代理经 /internal/discovery 寻找 Leader）；
# 成员真相源是 Raft 自身成员表（ADR-0008）
seeds:
  - "192.168.1.10:8080"
  - "192.168.1.11:8080"
  - "192.168.1.12:8080"

raft:
  snapshot_threshold: 10000   # 已应用日志达到该条数触发快照
  snapshot_retention: 2       # 本地保留快照个数
  trailing_logs: 10240        # 快照点之后保留的日志条数
  snapshot_interval: 120s     # 周期性触发快照的间隔
```

选举/心跳超时采用库默认值；若故障转移实测不达标再调。

### 6.2 启动

```bash
# 全新集群：仅首节点、仅一次
config-center --config config.yaml --bootstrap

# 其余节点及一切日常启停
config-center --config config.yaml
```

启动顺序：读配置 → 加载/生成 node-id → 连 PG 并确保 schema → 打开 Raft（boltdb 日志 + 文件快照 + TCP 传输）→ 非 bootstrap 节点执行发现与 Join → Serving。

- `pg_dsn` 指向的库不存在时经维护库 postgres 自动创建（要求连接账号具备 CREATEDB 权限），schema 随后照常迁移。
- **lineage 护栏**：本节点 `raft_fsm_meta.last_applied_index` 越过本地 Raft 日志末端 index 时拒绝启动——两份持久化不是同一 lineage，继续运行会静默丢弃全部新写；恢复须按 §6.4。

### 6.3 部署拓扑

奇数 voter（3 或 5）；每节点 1 进程 + 1 独立 PG（可同机不同实例）。节点跨机分布以容故障域；客户端经 LB 或直连任意节点（写自动 307 到 Leader）。

### 6.4 灾难恢复（quorum 永久丢失时的人工流程）

1. 停全部节点。
2. 选 PG 数据最新的节点：将其 `raft_fsm_meta` 置零（`UPDATE raft_fsm_meta SET last_applied_index=0, last_applied_term=0`），以此数据执行 `--bootstrap` 建新集群。
3. 其余节点清空 `data_dir`，正常启动走 Join 流程。

## 7. 可观测性

`/metrics`（Prometheus）最低集合：

| 指标 | 用途 |
|---|---|
| `raft_role` / `raft_term` / `raft_commit_index` | 共识状态 |
| `fsm_last_applied_index` / `fsm_apply_duration_seconds` | Apply 延迟与进度 |
| `fsm_apply_results_total{result=ok\|version_conflict\|invalid_rollback\|not_found}` | 写结果分布 |
| `watch_waiters` / `watch_notifications_total` | Watch 挂起与命中 |
| `snapshot_duration_seconds` / `snapshot_size_bytes` / `restore_skipped_total` | 快照健康 |
| `http_request_duration_seconds{route}` | API 延迟 |

告警基线建议：Leader 缺失 >15s；`fsm_apply_duration_seconds` p99 > 1s；节点 `/readyz` 持续 not ready > 1min。

## 8. 验证策略（各阶段完成定义）

1. **FSM 幂等性属性测试**（单元，随机化）：任意日志序列（publish/delete/rollback 混合，含 CAS 失败项）重放多次、与 Snapshot/Restore 交错，PG 终态与结果表必须逐次一致。这是跳过式恢复正确性的直接依据。
2. **混沌场景**（docker-compose：3 节点 + 3 PG + 场景脚本）：
   - 写压测中杀 Leader：秒级完成选举，已提交写不丢，307/503 按语义返回；
   - Follower 分区 30s 后恢复：无人工干预追平；
   - 带数据节点重启：断言 ≤10s 就绪（跳过式恢复生效）；
   - 新节点 Join：经快照与日志追平数据（加入即 voter，无转正阶段）；
   - 移除节点：护栏 409 路径与正常路径均验证；被移除节点以 42 退出；
   - 落后 Follower 触发 InstallSnapshot：Restore 后读数据一致。
3. **启动时间断言**（自动化防回归）：空库 ≤3s；10 万条（含 History）冷启 ≤10s。

## 9. 实施阶段

### Phase 1 — 单节点 MVP
- PG 三表 schema 与启动迁移
- Raft 集成：boltdb LogStore + FileSnapshotStore + 单节点 Bootstrap
- PostgresFSM：Apply（三操作 + 幂等跳过 + 结果表）、快照导出、跳过式 Restore、Serving 门闩
- 配置 API：PUT/GET/DELETE + CAS；History/Rollback API；Watch 长轮询
- Admin Token 认证、/healthz、/readyz、/metrics
- **DoD**：幂等性属性测试通过；空库启动 ≤3s 断言；单节点全 API 集成测试

### Phase 2 — 集群
- 发现代理（Seed 扫描 + 退避重试）与 Join API
- 307/503 写重定向语义
- 成员移除 + quorum 护栏 + 被移除节点自退出（码 42）
- /v1/cluster/status
- **DoD**：§8.2 前四项混沌场景通过

### Phase 3 — 快照与规模化
- 快照保留清理（retention/trailing_logs 生效）
- InstallSnapshot → Restore 全链路场景
- 10 万条数据量下：快照生成、落后节点恢复、冷重启时间
- **DoD**：§8.2 全部场景 + §8.3 全部断言通过

## 10. 技术栈

| 项 | 选型 |
|---|---|
| 语言 | Go 1.26+ |
| 共识 | hashicorp/raft v1.7.x（ADR-0005） |
| 日志存储 | hashicorp/raft-boltdb |
| 数据库/驱动 | PostgreSQL + pgx（连接池） |
| HTTP | net/http 标准库（Go 1.22+ 路由） |
| 控制台 | Vue 3 + Vite + TypeScript（构建期 node）；`go:embed` 内嵌，运行期零依赖（ADR-0012） |
| 序列化 | JSON（日志信封带格式版本号） |
| 指标 | Prometheus client_golang |

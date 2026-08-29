# e2e 验收套件（Go）

本目录是 [DESIGN §8 验证策略](../docs/DESIGN.md) 的可执行部分：每个 `TestTicketNN`
对应 [.scratch/config-center-mvp/issues/](../.scratch/config-center-mvp/issues/) 里的一张票，
逐项断言该票的验收条件。全部测试可重复执行——开始前自行清场，结束后（含失败）
自动清空 data 目录与测试库，不在仓库与 PG 里留任何足迹。

## 形态与构建隔离

- 套件是 `scripts` 包的 `_test.go` 文件（`//go:build e2e` 构建标签）：无标签时本目录
  **不参与任何编译**——`go build ./...`、`go vet ./...`、`go test ./...` 均不受影响，
  验收代码不会进入任何生产二进制。
- 黑盒测试：只经 HTTP API、`psql` 与被测二进制交互，不 import `internal/*`。
- `TestMain` 把工作目录切到仓库根并构建 `./config-center.exe`；单节点组
  （票 01~05、07）与多节点组一样使用套件生成的临时配置（`%TMP%/gcc-e2e-go/`），
  data 目录为仓库根 `data/`、库 `config_center_1`。仓库根 config.yaml 不被套件
  读取或改动，用户本地配置与测试足迹互不污染。

## 前置条件

- **Go**（`TestMain` 自动执行 `go build -o config-center.exe ./cmd/config-center`）。
- **本机 PostgreSQL**（127.0.0.1:5432）：`psql` 能以免密方式（trust）连超级用户
  `postgresql`——套件经 `psql -h 127.0.0.1 -U postgresql -tAc` 建删库。
- Windows 下票 10/11 用 **PowerShell**（`NtSuspendProcess`/`NtResumeProcess`）挂起/恢复
  进程模拟分区。
- **空闲端口**：HTTP 8080~8083、Raft 7946~7949。
- 写接口的 `admin_token` 在套件内写死为 `change-me`。

## 运行

```bash
go test -tags e2e ./scripts -run TestTicket01 -v -count=1   # 单张票
go test -tags e2e ./scripts -run TestTicket -v -count=1     # 全部票按序执行
```

- `-count=1` 为双保险：`begin()` 里的 `t.Setenv` 已禁用 go test 对本包的结果缓存，
  这些副作用测试每次都必须真跑。
- **收场清理**：每张票结束（含失败）自动删除 `data/`、`data-1/2/3` 目录并
  `DROP DATABASE` 测试库（`config_center_1..4`）。要保留现场排查时用
  `GCC_E2E_KEEP=1` 前缀跳过清理。
- 多节点组为脚本生成的临时配置（`%TMP%/gcc-e2e-go/`）、数据目录 `data-1/2/3`、
  库 `config_center_1/2/3`。多节点拓扑是本机 1 PG + 多库 + 多进程的等价拓扑
  （DESIGN §6.3 的 docker-compose 三节点版本见仓库根，套件不依赖 Docker）。

输出约定：每条断言打一行 `PASS:` / `FAIL:`，结尾汇总"全部验收项通过"或"存在失败项"；
**go test 退出码 0 = 全部通过**。节点日志在 `%TMP%/gcc-e2e-go/`，按"票号-节点"命名。

## 票清单

| 测试 | 内容 | 看点 |
|---|---|---|
| `TestTicket01` | 单节点 Publish/Get 冒烟 | 重复 `--bootstrap` 必须报错退出（ADR-0008） |
| `TestTicket02` | 快照生成与跳过式恢复 | 触发快照后**解析 state.bin 文件**：magic/JSON 头/三表行数/checksum 重算；带数据重启 ≤3s；清空 PG 后全量导入恢复 |
| `TestTicket03` | CAS + 删除 + History | `expected_version` 冲突 409、Delete 写 Tombstone、History 倒序分页 |
| `TestTicket04` | Rollback | 回滚产生新版本；无效目标 404/409；并发写与回滚竞态无 TOCTOU |
| `TestTicket05` | Watch 长轮询 | active/deleted/timeout 三种响应语义；服务端关闭时挂起连接及时断连 |
| `TestTicket07` | 认证 + 健康检查 + 指标 | 401、/healthz、/readyz、/metrics |
| `TestTicket08` | 发现代理 + Join + 写重定向 | 三节点自动入列；Follower 写 307 跟随；选举窗口 503 + Retry-After；Seed 全不可达不自举 |
| `TestTicket09` | 成员移除 + quorum 护栏 | 护栏 409；被移除节点 ~2s 内以退出码 42 自行退出；清空 data_dir 重新 Join 回 3 voter |
| `TestTicket10` | 混沌场景（DESIGN §8.2 前两项） | 杀 Leader 秒级切主、已提交写不丢；挂起进程模拟分区，恢复后自动追平 |
| `TestTicket11` | 快照保留清理 + InstallSnapshot | 小阈值（threshold=20/interval=2s/retention=2）放大压缩行为；挂起 Follower 制造落后，恢复后经 InstallSnapshot 追平 |
| `TestTicket12` | 10 万条规模化验证 | 两节点、20 并发写 10 万条；快照时长/体积记录；冷重启 ≤10s；新节点 InstallSnapshot 追平。**最耗时的一张**（10 万次 HTTP 写入，分钟级） |

票 06（FSM 幂等性属性测试）没有 e2e 测试，对应单元测试：

```bash
go test ./internal/fsm   # TestFSMPropertyReplayDeterminism：随机日志序列重放/重启/快照交错终态一致
```

## 想直接看 raft 快照文件

票 02/11 会在 PASS 行里解析并校验快照结构（magic/JSON 头/三表行数/checksum）。
要肉眼检查文件本体，用 `GCC_E2E_KEEP=1` 保留现场（以票 02 为例）：

```bash
GCC_E2E_KEEP=1 go test -tags e2e ./scripts -run TestTicket02 -v -count=1
ls -lt data/raft/snapshots/                            # 每个快照一个目录
cat data/raft/snapshots/*/meta.json                    # raft 层元数据（Term/Index/Configuration 成员表）
tail -c +9 data/raft/snapshots/*/state.bin | head -1   # FSM 快照 JSON 头（跳过 8B magic）
```

`state.bin` 是 GCCSNP01 格式的 FSM 快照（三张业务表数据体，格式见 DESIGN §4.4）；
成员表不在其中——它在 `meta.json` 的 `Configuration` 字段，属 raft 自带状态。
检查完毕删掉 `data/` 即可（或留着，下次运行开始前的清场会处理）。

## 排错

- **端口占用 / 行为怪异**：先确认没有上次中断遗留的进程——
  `ps -W | grep config-center`（Git Bash），结束用 `taskkill //F //IM config-center.exe`。
  测试失败中断时已注册的清理会强杀本票拉起的进程，但更早的遗留进程需手动清。
- **psql 免密不通**：为 `postgresql` 用户配置 trust（本机开发库）或 `~/.pgpass`。
- **多节点票库残留**：套件启动时 `DROP DATABASE IF EXISTS ... WITH (FORCE)` 自清理；
  若 PG 端有连接挂死，先杀掉遗留进程再重跑。
- `data/`、`data-1/2/3` 目录可随时整目录删除，不影响下次运行。

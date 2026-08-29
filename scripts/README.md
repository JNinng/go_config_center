# e2e 验收脚本

本目录是 [DESIGN §8 验证策略](../docs/DESIGN.md) 的可执行部分：每张脚本对应
[.scratch/config-center-mvp/issues/](../.scratch/config-center-mvp/issues/) 里的一张票，
逐项断言该票的验收条件。全部脚本可重复执行——启动时自行清理数据目录并重建库/表。

## 前置条件

- **Go**：脚本第一步执行 `go build -o ./config-center.exe ./cmd/config-center`。
- **本机 PostgreSQL**（127.0.0.1:5432）：`psql` 能以免密方式（trust）连超级用户
  `postgresql`——脚本经 `psql -h 127.0.0.1 -U postgresql -tAc` 建删库。
- **curl**；Windows 下另需 **Git Bash**（脚本使用 `ps -W`，票 11 用 PowerShell
  挂起/恢复进程模拟分区）。
- **空闲端口**：HTTP 8080~8082、Raft 7946~7948。
- 写接口的 `admin_token` 在脚本内写死为 `change-me`。

## 运行

```bash
bash scripts/e2e_ticket01.sh   # 脚本自行 cd 到仓库根，无需先 build
```

输出约定：每条断言打一行 `PASS:` / `FAIL:`，结尾汇总"全部验收项通过"或"存在失败项"；
**退出码 0 = 全部通过**。运行日志在 `/tmp/gcc-e2e*.log`（多节点脚本为
`/tmp/gcc-e2eNN/nN.log`，按节点分文件）。

## 脚本清单

单节点组使用仓库根 [config.yaml](../config.yaml) 与 `./data` 数据目录、库
`config_center_1`；多节点组为脚本生成的临时配置（`/tmp/gcc-e2eNN/`）、数据目录
`data-1/2/3`、库 `config_center_1/2/3`。多节点拓扑是本机 1 PG + 多库 + 多进程的
等价拓扑（DESIGN §6.3 的 docker-compose 三节点版本见仓库根，脚本不依赖 Docker）。

| 脚本 | 内容 | 看点 |
|---|---|---|
| `e2e_ticket01.sh` | 单节点 Publish/Get 冒烟 | 重复 `--bootstrap` 必须报错退出（ADR-0008） |
| `e2e_ticket02.sh` | 快照生成与跳过式恢复 | 触发快照后**解析 state.bin 文件**：magic/JSON 头/三表行数/checksum 重算；带数据重启 ≤3s；清空 PG 后全量导入恢复 |
| `e2e_ticket03.sh` | CAS + 删除 + History | `expected_version` 冲突 409、Delete 写 Tombstone、History 倒序分页 |
| `e2e_ticket04.sh` | Rollback | 回滚产生新版本；无效目标 404/409 |
| `e2e_ticket05.sh` | Watch 长轮询 | active/deleted/timeout 三种响应语义 |
| `e2e_ticket07.sh` | 认证 + 健康检查 + 指标 | 401、/healthz、/readyz、/metrics |
| `e2e_ticket08.sh` | 发现代理 + Join + 写重定向 | 三节点自动入列；Follower 写 307 跟随；选举窗口 503 + Retry-After |
| `e2e_ticket09.sh` | 成员移除 + quorum 护栏 | 护栏 409；被移除节点 ~2s 内以退出码 42 自行退出；清空 data_dir 重新 Join 回 3 voter |
| `e2e_ticket10.sh` | 混沌场景（DESIGN §8.2 前两项） | 杀 Leader 秒级切主、已提交写不丢；挂起进程模拟分区，恢复后自动追平 |
| `e2e_ticket11.sh` | 快照保留清理 + InstallSnapshot | 小阈值（threshold=20/interval=2s/retention=2）放大压缩行为；挂起 Follower 制造落后，恢复后经 InstallSnapshot 追平 |
| `e2e_ticket12.sh` | 10 万条规模化验证 | 两节点、20 并发写 10 万条；快照时长/体积记录；冷重启 ≤10s；新节点 InstallSnapshot 追平。**最耗时的一张**（10 万次 HTTP 写入，分钟级） |

票 06（FSM 幂等性属性测试）没有 e2e 脚本，对应单元测试：

```bash
go test ./internal/fsm   # TestFSMPropertyReplayDeterminism：随机日志序列重放/重启/快照交错终态一致
```

## 想直接看 raft 快照文件

- **票 02** 展示快照文件的内部结构并独立校验 checksum；
- **票 11** 展示快照的保留清理与 InstallSnapshot 传输链路。

脚本跑完后可肉眼检查（以票 02 留下的 `data/` 为例）：

```bash
ls -lt data/raft/snapshots/                            # 每个快照一个目录
cat data/raft/snapshots/*/meta.json                    # raft 层元数据（Term/Index/Configuration 成员表）
tail -c +9 data/raft/snapshots/*/state.bin | head -1   # FSM 快照 JSON 头（跳过 8B magic）
```

`state.bin` 是 GCCSNP01 格式的 FSM 快照（三张业务表数据体，格式见 DESIGN §4.4）；
成员表不在其中——它在 `meta.json` 的 `Configuration` 字段，属 raft 自带状态。

## 排错

- **端口占用 / 行为怪异**：先确认没有上次中断遗留的进程——
  `ps -W | grep config-center`（Git Bash），结束用 `taskkill //F //IM config-center.exe`。
- **psql 免密不通**：为 `postgresql` 用户配置 trust（本机开发库）或 `~/.pgpass`。
- **多节点脚本库残留**：脚本启动时 `DROP DATABASE IF EXISTS ... WITH (FORCE)` 自清理；
  若 PG 端有连接挂死，先杀掉遗留进程再重跑。
- `data/`、`data-1/2/3` 目录可随时整目录删除，不影响下次运行。

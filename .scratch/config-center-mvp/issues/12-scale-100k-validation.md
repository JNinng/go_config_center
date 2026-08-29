# 12 — 10 万条规模化验证

**What to build:** 灌入 10 万条 Config Entry（含 History），在真实量级下验证：快照生成可行（时长/体积有记录）、落后节点经快照追平、冷重启 ≤10s 断言自动化防回归（DESIGN §1.1、§8.3，ADR-0007 的量级承诺）。本张收口全部 Phase DoD 回归。

**Blocked by:** 10 — 混沌场景套件；11 — 快照保留清理 + InstallSnapshot 链路

**Status:** done

- [x] 10 万条（含 History）数据下快照生成成功，时长与体积记录在案
- [x] 冷重启 ≤10s 断言自动化（10 万条数据），纳入防回归测试
- [x] 落后节点在 10 万条规模下经 InstallSnapshot 追平，读数据一致
- [x] DESIGN §8 全部场景（混沌 + 快照 + 启动时间断言）最终回归通过

验收记录（scripts/e2e_ticket12.sh 10 项断言，2026-08-29）：
10 万条经 20 并发 worker 写入（bash curl 逐条 spawn，实测 140s——吞吐瓶颈在测试脚本
进程开销而非服务端）。实测记录：**快照生成 342ms / 18MB（10 万条 + History）**；
**冷重启到可读 575ms（阈值 10s，跳过式恢复 ADR-0007 生效）**；空节点加入经
InstallSnapshot 追平（节点日志 install=1 + FSM 全量导入=1），PG 物化 100001 条
（含 warmup），抽样 5 条（首/25k/50k/75k/末）与 Leader 内容一致。
最终回归：GCC_TEST_PG_DSN 下 go test 全绿（含票 06 属性测试）+
scripts/e2e_ticket01~12.sh 逐脚本回归通过（各票验收记录附当日结果）。

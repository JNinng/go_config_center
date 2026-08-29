# 05 — Watch 长轮询

**What to build:** `GET .../{data_id}/watch?v=N&timeout=30s` 挂起至该条目 version > N、被删除或超时（DESIGN §4.3）。变更返回 `{status: active, entry}` 或 `{status: deleted, version}`；超时返回 `{status: timeout}` 供客户端原样重挂。唤醒由本机 FSM Apply 触发——任何节点（含 Follower）Apply 到 Publish/删除时通知本机挂起的 Watch（ADR-0004）。

**Blocked by:** 03 — CAS + Delete/Tombstone + History 查询

**Status:** done

- [x] 挂起期间 Publish → 立即返回 active + 新条目
- [x] 挂起期间 Delete → 立即返回 deleted + 墓碑版本
- [x] 无变更时按 timeout 返回 timeout 状态，客户端可原样重挂
- [x] v 已是最新 → 挂起等待；v 落后 → 立即返回当前状态
- [x] 服务端关闭/重启时挂起连接以超时或断连收场，不悬挂

验收记录（scripts/e2e_ticket05.sh 12 项断言 + watch/fsm/api 集成测试，2026-08-29）：
internal/watch.Hub 为进程内挂起表（先注册后检查，关闭快路径与挂起间错过通知的竞态窗口）；
FSM 在 Publish/Delete/Rollback 的 Apply 成功路径调 hub.Notify（CAS 冲突等不变更状态的操作不唤醒）。
事件只携带版本，唤醒后重读本机 PG 取最新条目；事件缓冲 1 非阻塞投递，错过的事件不补发
（waiter 侧状态单调，重读即最新）。参数边界：timeout ≤5min 收敛，非法 v/timeout 400。
e2e 实测：v 落后 47ms 立即 active；挂起被 Publish/Delete 唤醒约 450ms（含人为 400ms 间隔）；
服务端 kill -9 后挂起连接 21ms 断连退出，不悬挂。Rollback 唤醒与 Publish 同路径，由单测覆盖。

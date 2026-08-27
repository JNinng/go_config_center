# 读最终一致：Follower 可读旧数据，不做 ReadIndex

配置消费天然容忍秒级延迟。写经 Raft 多数派线性一致，Publish 响应返回新 Version，客户端以 Version 协商实现读己之写；读路径不做 ReadIndex / VerifyLeader（如日后需要强读，仅对 Leader 补 VerifyLeader 即可，不影响 API）。

## Consequences

- 直接放宽 Raft 库选型：hashicorp/raft 无内置 ReadIndex 不构成缺陷。
- Follower 读到的数据最多落后 Leader 若干条已提交日志（正常情况下毫秒级）。

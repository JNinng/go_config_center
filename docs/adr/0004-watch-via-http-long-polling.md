# Watch 采用 HTTP 长轮询

变更通知用 HTTP 长轮询实现（客户端携带已见 Version 挂起约 30 秒，变更或超时返回），MVP 不做 gRPC stream / SSE：客户端零 SDK 依赖、可穿透代理、服务端实现最小。通知语义与传输解耦，后续可增加 gRPC watch 而不破坏现有 API。

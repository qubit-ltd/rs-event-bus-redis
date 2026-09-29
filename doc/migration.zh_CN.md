# 迁移到 Redis provider 0.5

[English](migration.md) · [用户指南](user_guide.zh_CN.md)

将 `qubit-event-bus-redis` 从 0.4 升级到 0.5 时，须同时采用
`qubit-event-bus` 0.17。一起更新直接依赖、下游 fixture 和 lockfile，不能混用
不同 SPI minor。`decode(&EncodedPayload)`、`PublishFailure` 和
`PayloadLimits` 的迁移见[核心迁移指南](https://github.com/qubit-ltd/rs-event-bus/blob/main/doc/migration.zh_CN.md)。
codec 应精确验证元数据；需要读取历史 schema 时，明确记录并实现允许的版本集合。

wire 版本 1 继续支持。部署前测试真实保留的 stream 数据；Rust API 升级不需要
删除 stream 或消费组。限额内格式错误的版本 1 记录仍采用既有隔离并确认路径；
合法但不支持的版本保留 pending 状态，等待兼容 consumer。

新增正数 provider options，默认 `redis.max_wire_bytes=8388608`、
`redis.max_payload_bytes=1048576`、`redis.max_headers_bytes=65536`。
facade 另有默认各 1 MiB 的编码发布/接收限额，两层应一起配置。
发布在复制 payload 前检查，headers/wire 序列化在 `XADD` 前有界执行；接收在
复制字符串前检查 wire 字节，并在字段解码过程中限制增长。这不限制 Redis
客户端首次 RESP 分配，也不是进程总内存预算。

接收 wire、payload 或 headers 超限会返回 `receive_limit_exceeded` 并停止
facade 订阅，源 PEL 记录保留，不执行 `XACK`、`XDEL` 或隔离。
查看 `terminal_failure()`，修复限额或 codec，再以同一 group 创建新的持久
订阅；用 `XPENDING` 验证恢复，不能为消除错误清空 pending 数据。

SPI 发布失败现在声明 `PublishEffect`。提交前打开连接失败及明确 server 拒绝
是 `NotAccepted`；query 开始后的断连、超时或响应转换失败是
`MayHaveBeenAccepted`。默认 `DuplicateRiskPolicy::Forbid` 阻止未知接纳的自动
重发，自定义重试规则不能绕过；`AllowDuplicates` 只允许原策略继续判断。
前序未知效果保留在最终失败中，后来成功的 `duplicate_possible()` 也报告风险。
RetryPolicy 是软预算，不是所有执行中命令的硬超时。取消已启动发布后，记录仍
可能在 Redis 中，应保留 EventId 并核对业务副作用。

Redis 不按 EventId 自动去重，`XADD` 接纳也不证明 fsync 或 handler 完成。
facade 死信转发与源 `XACK` 是两个操作，消费者必须容忍重复逻辑死信。
部署前针对实际 Redis 版本运行 provider feature matrix、conformance、
有界解码、回复丢失和持久恢复测试。

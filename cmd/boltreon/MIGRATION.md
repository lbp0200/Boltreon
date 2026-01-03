# 日志系统迁移说明

## 从自定义日志迁移到 zerolog

项目已从自定义日志系统迁移到 **zerolog**，这是一个高性能、零分配的 JSON 日志库。

## 安装依赖

首先需要安装 zerolog 依赖：

```bash
go mod tidy
```

或者手动添加：

```bash
go get github.com/rs/zerolog
```

## API 兼容性

为了保持向后兼容，我们保留了原有的简单 API：

```go
// 这些函数仍然可用
logger.Debug("消息: %s", value)
logger.Info("消息: %s", value)
logger.Warning("消息: %s", value)
logger.Error("消息: %s", value)
```

## 新的结构化日志 API

zerolog 提供了更强大的结构化日志功能：

```go
// 使用结构化字段
logger.Logger.Debug().
    Str("remote_addr", remoteAddr).
    Str("command", cmd).
    Int("arg_count", len(args)-1).
    Msg("执行命令")

// 带错误信息
logger.Logger.Warn().
    Str("remote_addr", remoteAddr).
    Err(err).
    Msg("写入响应失败")
```

## 优势

1. **性能**: zerolog 是零分配的，性能比标准库和大多数日志库更好
2. **结构化**: 支持 JSON 格式，便于日志分析
3. **字段丰富**: 可以添加任意类型的字段（Str, Int, Bool, Err 等）
4. **社区支持**: 广泛使用，维护良好

## 日志级别映射

| 旧级别 | 新级别 | zerolog 级别 |
|--------|--------|--------------|
| DEBUG  | DEBUG  | DebugLevel   |
| INFO   | INFO   | InfoLevel    |
| WARNING| WARN   | WarnLevel     |
| ERROR  | ERROR  | ErrorLevel   |

## 下一步

运行 `go mod tidy` 安装依赖，然后就可以正常使用了！


# 待修复的集成测试

## 测试状态统计
- **通过**: 139 个测试
- **失败/跳过**: 38 个测试

---

## 1. DUMP / RESTORE 测试

**文件**: `key_advanced_test.go`
**问题**: go-redis Do() 返回类型与 `[]byte` 断言不兼容
**错误**: `ERR invalid serialized data` - RESTORE 参数处理问题

### 问题详情
- `DUMP` 命令使用 go-redis `Do()` 方法返回 `string` 类型而非 `[]byte`
- `RESTORE` 命令的参数顺序需要进一步调查（ttl 与序列化数据）
- 序列化格式与 Redis RDB 格式不完全兼容

### 待修复
- `TestDump`
- `TestRestore`

---

## 2. 订阅/取消订阅测试

**文件**: `pubsub_test.go`
**问题**: SUBSCRIBE 是阻塞命令，测试无法正常返回
**错误**: 测试超时或无法验证

### 待修复
- `TestSubscribe`
- `TestUnsubscribe`
- `TestPSubscribe`
- `TestPUnsubscribe`
- `TestUnsubscribeAll`
- `TestTimeoutUnsubscribe`

### 解决方案建议
- 使用 goroutine + channel 进行异步测试
- 或使用 `UNSUBSCRIBE` 主动取消阻塞

---

## 3. 复制相关测试

**文件**: `replication_test.go`
**问题**: 需要主从配置才能运行
**错误**: `ERR not a replica` 或连接失败

### 待修复
- `TestReplicationSlave`
- `TestReplicationSync`
- `TestReplicationRoleInfo`
- `TestReplicaOf`
- `TestReplConf`
- `TestPSync`

### 解决方案建议
- 测试启动时创建主从配置
- 或跳过这些测试（需要在集成测试环境中运行）

---

## 4. CLIENT 命令测试

**文件**: `server_advanced_test.go`
**问题**: 需要完整客户端连接管理
**错误**: 各种客户端命令失败

### 待修复
- `TestConfigSet`
- `TestClientGetName`
- `TestClientSetName`
- `TestClientKill`
- `TestClientInfo`
- `TestClientNoEvict`
- `TestClientTracking`

### 解决方案建议
- 需要实现完整的 CLIENT 命令集
- 或模拟客户端连接进行测试

---

## 5. 其他测试

| 测试 | 文件 | 问题 |
|------|------|------|
| `TestSortBy` | `key_advanced_test.go` | 排序权重功能 |
| `TestSortStore` | `key_advanced_test.go` | SORT + STORE |
| `TestObjectEncoding` | `key_advanced_test.go` | OBJECT ENCODING |
| `TestObjectFreq` | `key_advanced_test.go` | OBJECT FREQ |
| `TestBgSave` | `server_advanced_test.go` | BGSAVE 命令 |
| `TestLastSave` | `server_advanced_test.go` | LASTSAVE 命令 |
| `TestZRangeByScore` | `sortedset_advanced_test.go` | ZRANGEBYSCORE |
| `TestZRemRangeByScore` | `sortedset_advanced_test.go` | ZREMRANGEBYSCORE |
| `TestZLex` | `sortedset_advanced_test.go` | ZLEX 相关命令 |
| `TestZScan` | `sortedset_advanced_test.go` | ZSCAN |
| `TestZRangeByRankWithScores` | `sortedset_advanced_test.go` | ZRANGE with scores |
| Stream 相关测试 | `stream_advanced_test.go` | 多个 Stream 命令 |
| `TestModuleList` | `server_advanced_test.go` | MODULE LIST |

---

## 修复优先级

### 高优先级
1. [ ] DUMP/RESTORE - 修复序列化格式和参数处理
2. [ ] SUBSCRIBE 阻塞问题 - 异步测试方法

### 中优先级
3. [ ] CLIENT 命令实现
4. [ ] SORT BY/STORE 功能完善

### 低优先级
5. [ ] Stream 命令完善
6. [ ] 复制测试环境配置

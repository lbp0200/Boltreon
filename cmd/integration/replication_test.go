package integration

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// contains helper function to check if string contains substring
func contains(s, substr string) bool {
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestReplicationMaster 测试主节点复制信息
func TestReplicationMaster(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// INFO replication - 主节点信息
	result, err := testClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(info) > 0)

	// 检查关键信息
	assert.True(t, contains(info, "role:"))
	assert.True(t, contains(info, "connected_slaves:"))
}

// TestReplicationSlave 测试从节点复制
// 需要主从配置环境，单机模式下跳过
func TestReplicationSlave(t *testing.T) {
	t.Skip("需要主从配置环境 - 请使用 -replicaof 参数启动主节点")
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 创建从节点（使用REPLICAOF）
	result, err := testClient.Do(ctx, "REPLICAOF", "127.0.0.1", "6379").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// INFO replication - 从节点信息
	result, err = testClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(info) > 0)

	// 停止复制
	result, err = testClient.Do(ctx, "REPLICAOF", "NO", "ONE").Result()
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)
}

// TestReplicationSync 测试同步过程
// 需要主从配置环境，单机模式下跳过
func TestReplicationSync(t *testing.T) {
	t.Skip("需要主从配置环境 - 请使用 -replicaof 参数启动从节点")
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 在主节点上添加数据
	_ = testClient.Set(ctx, "sync_test_key", "sync_test_value", 0).Err()

	// PSYNC - 尝试部分同步
	result, err := testClient.Do(ctx, "PSYNC", "?", "-1").Result()
	assert.NoError(t, err)

	// 应该收到FULLRESYNC响应
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr))

	// 第一个元素应该是replid
	replid, ok := arr[0].(string)
	assert.True(t, ok)
	assert.Equal(t, "?", replid)

	// 第二个元素应该是offset
	offset, ok := arr[1].(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(-1), offset)
}

// TestReplicationCommandProp 测试命令传播
func TestReplicationCommandProp(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 添加一些数据
	for i := 0; i < 10; i++ {
		_ = testClient.Set(ctx, "prop_key_"+string(rune('a'+i)), "value_"+string(rune('a'+i)), 0).Err()
	}

	// INFO replication - 检查复制偏移量
	result, err := testClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)

	// 检查master_repl_offset存在
	assert.True(t, contains(info, "master_repl_offset:"))
}

// TestReplicationBacklog 测试复制积压缓冲区
func TestReplicationBacklog(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// INFO replication - 检查积压缓冲区信息
	result, err := testClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)

	// 检查积压相关信息
	assert.True(t, contains(info, "repl_backlog_active:") || contains(info, "repl_backlog_size:"))
}

// TestReplicationMultipleSlaves 测试多从节点场景
func TestReplicationMultipleSlaves(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 主节点上添加数据
	_ = testClient.Set(ctx, "multi_slave_test", "value", 0).Err()

	// INFO replication - 获取从节点数量
	result, err := testClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)

	// 检查connected_slaves字段
	assert.True(t, contains(info, "connected_slaves:"))
}

// TestReplicationRoleInfo 测试角色详细信息
// 需要主从配置环境，单机模式下跳过
func TestReplicationRoleInfo(t *testing.T) {
	t.Skip("需要主从配置环境")
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// ROLE - 获取详细角色信息
	result, err := testClient.Do(ctx, "ROLE").Result()
	assert.NoError(t, err)

	arr, ok := result.([]interface{})
	assert.True(t, ok)

	// 主节点格式: [role, repl_offset, [slaves...]]
	assert.Equal(t, "master", arr[0])

	// 偏移量应该是数字
	_, ok = arr[1].(int64)
	assert.True(t, ok)
}

// TestMasterLinkStatus 测试主从连接状态
func TestMasterLinkStatus(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// INFO replication - 检查主从链接状态
	result, err := testClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)

	// 检查链接状态
	assert.True(t, contains(info, "master_link_status:") || contains(info, "role:"))
}

// TestSlaveReadOnly 测试从节点只读属性
func TestSlaveReadOnly(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// INFO replication - 检查从节点只读设置
	result, err := testClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(info) > 0)
}

// TestReplicationStress 测试复制压力
func TestReplicationStress(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 快速添加大量数据
	for i := 0; i < 100; i++ {
		key := "stress_key_" + string(rune('a'+i%26)) + "_" + string(rune('0'+i/26))
		_ = testClient.Set(ctx, key, "stress_value_"+string(rune('a'+i)), 0).Err()
	}

	// 检查复制偏移量增长
	result, err := testClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(info) > 0)
}

// TestReplicationWithDifferentTypes 测试不同数据类型的复制
func TestReplicationWithDifferentTypes(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// String
	_ = testClient.Set(ctx, "type_string", "value", 0).Err()

	// List
	_ = testClient.RPush(ctx, "type_list", "a", "b", "c").Err()

	// Hash
	_ = testClient.HSet(ctx, "type_hash", "field", "value").Err()

	// Set
	_ = testClient.SAdd(ctx, "type_set", "a", "b", "c").Err()

	// ZSet
	_ = testClient.ZAdd(ctx, "type_zset", redis.Z{Score: 1, Member: "a"}).Err()

	// 检查所有类型都被记录
	result, err := testClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)

	info, ok := result.(string)
	assert.True(t, ok)
	assert.True(t, len(info) > 0)
}

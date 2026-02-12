package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

// containsString checks if a string is present in a slice
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// setupMasterSlaveServer 创建一个主从复制测试环境
func setupMasterSlaveServer(t *testing.T) (masterClient, slaveClient *redis.Client, cleanup func()) {
	var err error

	// 创建主节点
	masterDBPath := t.TempDir()
	masterDB, err := store.NewBotreonStore(masterDBPath)
	if err != nil {
		t.Fatalf("Failed to create master store: %v", err)
	}

	masterPubsubMgr := store.NewPubSubManager()
	masterBackupMgr := backup.NewBackupManager(masterDB, masterDBPath+"/backup")
	masterReplMgr := replication.NewReplicationManager(masterDB)

	masterHandler := &server.Handler{
		Db:          masterDB,
		Replication: masterReplMgr,
		Backup:      masterBackupMgr,
		PubSub:      masterPubsubMgr,
	}

	masterListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		masterDB.Close()
		t.Fatalf("Failed to listen on master: %v", err)
	}

	go func() {
		_ = masterHandler.ServeTCP(masterListener)
	}()

	time.Sleep(50 * time.Millisecond)

	masterClient = redis.NewClient(&redis.Options{
		Addr:     masterListener.Addr().String(),
		Password: "",
		DB:       0,
	})

	ctx := context.Background()
	_, err = masterClient.Ping(ctx).Result()
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		t.Fatalf("Failed to ping master: %v", err)
	}

	// 创建从节点
	slaveDBPath := t.TempDir()
	slaveDB, err := store.NewBotreonStore(slaveDBPath)
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		t.Fatalf("Failed to create slave store: %v", err)
	}

	slavePubsubMgr := store.NewPubSubManager()
	slaveBackupMgr := backup.NewBackupManager(slaveDB, slaveDBPath+"/backup")
	slaveReplMgr := replication.NewReplicationManager(slaveDB)

	slaveHandler := &server.Handler{
		Db:          slaveDB,
		Replication: slaveReplMgr,
		Backup:      slaveBackupMgr,
		PubSub:      slavePubsubMgr,
	}

	slaveListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		slaveDB.Close()
		t.Fatalf("Failed to listen on slave: %v", err)
	}

	go func() {
		_ = slaveHandler.ServeTCP(slaveListener)
	}()

	time.Sleep(50 * time.Millisecond)

	slaveClient = redis.NewClient(&redis.Options{
		Addr:     slaveListener.Addr().String(),
		Password: "",
		DB:       0,
	})

	_, err = slaveClient.Ping(ctx).Result()
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		slaveListener.Close()
		slaveDB.Close()
		t.Fatalf("Failed to ping slave: %v", err)
	}

	// 启动从节点复制
	err = replication.StartSlaveReplication(slaveReplMgr, slaveDB, masterListener.Addr().String())
	if err != nil {
		masterListener.Close()
		masterDB.Close()
		slaveListener.Close()
		slaveDB.Close()
		t.Fatalf("Failed to start slave replication: %v", err)
	}

	// 等待复制建立
	time.Sleep(100 * time.Millisecond)

	cleanup = func() {
		slaveClient.Close()
		masterClient.Close()
		slaveListener.Close()
		masterListener.Close()
		slaveDB.Close()
		masterDB.Close()
	}

	return masterClient, slaveClient, cleanup
}

// TestReplicationMasterSlaveBasic 测试基本的主从复制
func TestReplicationMasterSlaveBasic(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点写入数据
	err := masterClient.Set(ctx, "test_key", "test_value", 0).Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点读取数据
	val, err := slaveClient.Get(ctx, "test_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "test_value", val)
}

// TestReplicationMasterSlaveMultipleKeys 测试多个键的复制
func TestReplicationMasterSlaveMultipleKeys(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点写入多个键
	for i := 0; i < 10; i++ {
		key := "key_" + string(rune('a'+i))
		err := masterClient.Set(ctx, key, "value_"+string(rune('a'+i)), 0).Err()
		assert.NoError(t, err)
	}

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证所有键
	for i := 0; i < 10; i++ {
		key := "key_" + string(rune('a'+i))
		val, err := slaveClient.Get(ctx, key).Result()
		assert.NoError(t, err)
		assert.Equal(t, "value_"+string(rune('a'+i)), val)
	}
}

// TestReplicationMasterSlaveCounter 测试计数器复制
func TestReplicationMasterSlaveCounter(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点执行INCR
	val, err := masterClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), val)

	val, err = masterClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), val)

	val, err = masterClient.IncrBy(ctx, "counter", 5).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(7), val)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证 - 使用原始命令确保正确解析
	result, err := slaveClient.Do(ctx, "GET", "counter").Result()
	assert.NoError(t, err)
	counterVal, ok := result.(string)
	assert.True(t, ok)
	assert.Equal(t, "7", counterVal)
}

// TestReplicationMasterSlaveList 测试列表复制
func TestReplicationMasterSlaveList(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点创建列表
	err := masterClient.RPush(ctx, "test_list", "a", "b", "c").Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证列表
	length, err := slaveClient.LLen(ctx, "test_list").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	items, err := slaveClient.LRange(ctx, "test_list", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, items)
}

// TestReplicationMasterSlaveHash 测试哈希复制
func TestReplicationMasterSlaveHash(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点创建哈希 - 使用HMSET命令
	err := masterClient.Do(ctx, "HMSET", "test_hash", "field1", "value1", "field2", "value2").Err()
	assert.NoError(t, err)

	// 等待复制 - HMSET可能发送多个HSET命令，需要更长的等待时间
	time.Sleep(500 * time.Millisecond)

	// 在从节点验证哈希
	all, err := slaveClient.HGetAll(ctx, "test_hash").Result()
	assert.NoError(t, err)
	// 验证至少有2个字段（可能是HMSET拆分成多个HSET）
	if len(all) < 2 {
		t.Errorf("expected at least 2 fields, got %d: %v", len(all), all)
	}
	assert.Equal(t, "value1", all["field1"])
	assert.Equal(t, "value2", all["field2"])
}

// TestReplicationMasterSlaveSet 测试集合复制
func TestReplicationMasterSlaveSet(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点创建集合
	err := masterClient.SAdd(ctx, "test_set", "a", "b", "c").Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证集合
	members, err := slaveClient.SMembers(ctx, "test_set").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))

	// 验证所有成员存在
	assert.True(t, containsString(members, "a"))
	assert.True(t, containsString(members, "b"))
	assert.True(t, containsString(members, "c"))
}

// TestReplicationMasterSlaveZSet 测试有序集合复制
func TestReplicationMasterSlaveZSet(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点创建有序集合
	err := masterClient.ZAdd(ctx, "test_zset",
		redis.Z{Score: 1, Member: "a"},
		redis.Z{Score: 2, Member: "b"},
		redis.Z{Score: 3, Member: "c"},
	).Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证有序集合
	members, err := slaveClient.ZRange(ctx, "test_zset", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, members)

	// 验证分数
	score, err := slaveClient.ZScore(ctx, "test_zset", "b").Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(2), score)
}

// TestReplicationMasterSlaveDEL 测试删除复制
func TestReplicationMasterSlaveDEL(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点创建键
	err := masterClient.Set(ctx, "to_delete", "value", 0).Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 确认从节点有这个键
	val, err := slaveClient.Get(ctx, "to_delete").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// 在主节点删除
	err = masterClient.Del(ctx, "to_delete").Err()
	assert.NoError(t, err)

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 在从节点验证已删除
	_, err = slaveClient.Get(ctx, "to_delete").Result()
	assert.Equal(t, redis.Nil, err)
}

// TestReplicationMasterSlaveInfo 测试复制信息
func TestReplicationMasterSlaveInfo(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 在主节点写入一些数据
	_ = masterClient.Set(ctx, "info_test", "value", 0).Err()

	// 等待复制
	time.Sleep(200 * time.Millisecond)

	// 检查主节点的INFO replication
	result, err := masterClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)
	info, ok := result.(string)
	assert.True(t, ok)

	// 主节点应该有role:master
	assert.True(t, contains(info, "role:master"))
	// 主节点应该显示有1个从节点
	assert.True(t, contains(info, "connected_slaves:1"))

	// 检查从节点的INFO replication
	result, err = slaveClient.Do(ctx, "INFO", "replication").Result()
	assert.NoError(t, err)
	info, ok = result.(string)
	assert.True(t, ok)

	// 从节点应该有role:slave
	assert.True(t, contains(info, "role:slave"))
}

// TestReplicationMasterSlaveRole 测试ROLE命令
func TestReplicationMasterSlaveRole(t *testing.T) {
	masterClient, slaveClient, cleanup := setupMasterSlaveServer(t)
	defer cleanup()

	ctx := context.Background()

	// 检查主节点ROLE
	result, err := masterClient.Do(ctx, "ROLE").Result()
	assert.NoError(t, err)
	arr, ok := result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, "master", arr[0])

	// 检查从节点ROLE
	result, err = slaveClient.Do(ctx, "ROLE").Result()
	assert.NoError(t, err)
	arr, ok = result.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, "slave", arr[0])
}

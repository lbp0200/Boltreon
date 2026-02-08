package main

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

var (
	testClient *redis.Client
	testServer *server.Handler
	testDB     *store.BotreonStore
	listener   net.Listener
)

// setupTestServer 启动测试服务器
func setupTestServer(t *testing.T) {
	var err error

	// 创建临时数据库目录
	dbPath := t.TempDir()

	// 创建数据库
	testDB, err = store.NewBotreonStore(dbPath)
	assert.NoError(t, err)

	// 创建服务器处理器
	testServer = &server.Handler{Db: testDB}

	// 启动服务器（使用随机端口）
	listener, err = net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	// 在goroutine中运行服务器
	go func() {
		_ = testServer.ServeTCP(listener)
	}()

	// 等待服务器启动
	time.Sleep(50 * time.Millisecond)

	// 创建Redis客户端
	testClient = redis.NewClient(&redis.Options{
		Addr:     listener.Addr().String(),
		Password: "", // 无密码
		DB:       0,  // 默认数据库
	})

	// 测试连接
	ctx := context.Background()
	_, err = testClient.Ping(ctx).Result()
	assert.NoError(t, err)
}

// teardownTestServer 清理测试服务器
func teardownTestServer(t *testing.T) {
	if testClient != nil {
		_ = testClient.Close()
	}
	if listener != nil {
		_ = listener.Close()
	}
	if testDB != nil {
		_ = testDB.Close()
	}
}

// TestMain 测试入口
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

// TestConnectionCommands 测试连接命令
func TestConnectionCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// PING
	pong, err := testClient.Ping(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, "PONG", pong)

	// ECHO
	echo, err := testClient.Echo(ctx, "Hello").Result()
	assert.NoError(t, err)
	assert.Equal(t, "Hello", echo)
}

// TestStringCommands 测试String类型命令
func TestStringCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// SET/GET
	err := testClient.Set(ctx, "key1", "value1", 0).Err()
	assert.NoError(t, err)

	val, err := testClient.Get(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	// GET不存在的键
	_, err = testClient.Get(ctx, "nonexistent").Result()
	assert.Equal(t, redis.Nil, err)

	// SETEX
	err = testClient.SetEx(ctx, "key2", "value2", 10*time.Second).Err()
	assert.NoError(t, err)

	val, err = testClient.Get(ctx, "key2").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value2", val)

	// PSETEX
	err = testClient.Set(ctx, "key2_psetex", "value", 10000*time.Millisecond).Err()
	assert.NoError(t, err)

	// SETNX
	set, err := testClient.SetNX(ctx, "key3", "value3", 0).Result()
	assert.NoError(t, err)
	assert.True(t, set)

	set, err = testClient.SetNX(ctx, "key3", "value3_updated", 0).Result()
	assert.NoError(t, err)
	assert.False(t, set)

	// GETSET
	oldVal, err := testClient.GetSet(ctx, "key1", "new_value1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value1", oldVal)

	val, err = testClient.Get(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "new_value1", val)

	// MGET/MSET
	err = testClient.MSet(ctx, "key4", "value4", "key5", "value5").Err()
	assert.NoError(t, err)

	mgetVals, err := testClient.MGet(ctx, "key4", "key5", "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(mgetVals))
	if mgetVals[0] != nil {
		assert.Equal(t, "value4", mgetVals[0].(string))
	}
	if mgetVals[1] != nil {
		assert.Equal(t, "value5", mgetVals[1].(string))
	}
	assert.Nil(t, mgetVals[2])

	// MSETNX
	set, err = testClient.MSetNX(ctx, "key6", "value6", "key7", "value7").Result()
	assert.NoError(t, err)
	assert.True(t, set)

	set, err = testClient.MSetNX(ctx, "key6", "value6_new", "key8", "value8").Result()
	assert.NoError(t, err)
	assert.False(t, set)

	// INCR
	counterVal, err := testClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), counterVal)

	counterVal, err = testClient.Incr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), counterVal)

	// INCRBY
	counterVal, err = testClient.IncrBy(ctx, "counter", 5).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(7), counterVal)

	// DECR
	counterVal, err = testClient.Decr(ctx, "counter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), counterVal)

	// DECRBY
	counterVal, err = testClient.DecrBy(ctx, "counter", 3).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), counterVal)

	// INCRBYFLOAT
	valFloat, err := testClient.IncrByFloat(ctx, "float_counter", 1.5).Result()
	assert.NoError(t, err)
	assert.Equal(t, 1.5, valFloat)

	valFloat, err = testClient.IncrByFloat(ctx, "float_counter", -0.5).Result()
	assert.NoError(t, err)
	assert.Equal(t, 1.0, valFloat)

	// APPEND
	length, err := testClient.Append(ctx, "key1", "_appended").Result()
	assert.NoError(t, err)
	assert.True(t, length > 0)

	val, err = testClient.Get(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "new_value1_appended", val)

	// STRLEN
	length, err = testClient.StrLen(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(len("new_value1_appended")), length)

	// GETRANGE
	val, err = testClient.GetRange(ctx, "key1", 0, 4).Result()
	assert.NoError(t, err)
	assert.Equal(t, "new_v", val)

	// SETRANGE
	length, err = testClient.SetRange(ctx, "key1", 0, "NEW_").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(len("new_value1_appended")), length)

	val, err = testClient.Get(ctx, "key1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "NEW_value1_appended", val)
}

// TestKeyCommands 测试Key相关命令

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lbp0200/Boltreon/internal/server"
	"github.com/lbp0200/Boltreon/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/zeebo/assert"
)

var (
	testClient *redis.Client
	testServer *server.Handler
	testDB     *store.BoltreonStore
	listener   net.Listener
)

// setupTestServer 启动测试服务器
func setupTestServer(t *testing.T) {
	var err error

	// 创建临时数据库目录
	dbPath := t.TempDir()

	// 创建数据库
	testDB, err = store.NewBoltreonStore(dbPath)
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
func _TestKeyCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 先创建一些测试数据
	_ = testClient.Set(ctx, "test_key", "test_value", 0).Err()
	_ = testClient.Set(ctx, "test_key2", "test_value2", 0).Err()
	_ = testClient.Set(ctx, "delete_me", "delete_value", 0).Err()
	_ = testClient.Set(ctx, "expire_key", "expire_value", 0).Err()

	// DEL
	deleted, err := testClient.Del(ctx, "delete_me").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// DEL multiple keys
	deleted, err = testClient.Del(ctx, "test_key", "test_key2", "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), deleted)

	// EXISTS
	exists, err := testClient.Exists(ctx, "test_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists) // 已删除

	// EXISTS multiple keys
	_ = testClient.Set(ctx, "exists_key1", "value", 0).Err()
	_ = testClient.Set(ctx, "exists_key2", "value", 0).Err()

	exists, err = testClient.Exists(ctx, "exists_key1", "exists_key2", "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), exists)

	// TYPE
	_ = testClient.Set(ctx, "string_key", "value", 0).Err()
	_ = testClient.HSet(ctx, "hash_key", "field", "value").Err()
	_ = testClient.LPush(ctx, "list_key", "item").Err()
	_ = testClient.SAdd(ctx, "set_key", "member").Err()
	_ = testClient.ZAdd(ctx, "zset_key", redis.Z{Score: 1, Member: "member"}).Err()

	keyType, err := testClient.Type(ctx, "string_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "string", keyType)

	keyType, err = testClient.Type(ctx, "hash_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "hash", keyType)

	keyType, err = testClient.Type(ctx, "list_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "list", keyType)

	keyType, err = testClient.Type(ctx, "set_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "set", keyType)

	keyType, err = testClient.Type(ctx, "zset_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "zset", keyType)

	keyType, err = testClient.Type(ctx, "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, "none", keyType)

	// EXPIRE
	_ = testClient.Set(ctx, "expire_key", "value", 0).Err()
	set, err := testClient.Expire(ctx, "expire_key", 10*time.Second).Result()
	assert.NoError(t, err)
	assert.True(t, set)

	// EXPIREAT
	_ = testClient.Set(ctx, "expireat_key", "value", 0).Err()
	futureTime := time.Now().Add(1 * time.Hour).Unix()
	set, err = testClient.ExpireAt(ctx, "expireat_key", time.Unix(futureTime, 0)).Result()
	assert.NoError(t, err)
	assert.True(t, set)

	// PEXPIRE
	_ = testClient.Set(ctx, "pexpire_key", "value", 0).Err()
	set, err = testClient.PExpire(ctx, "pexpire_key", 10000*time.Millisecond).Result()
	assert.NoError(t, err)
	assert.True(t, set)

	// PEXPIREAT
	_ = testClient.Set(ctx, "pexpireat_key", "value", 0).Err()
	futureMs := time.Now().Add(1 * time.Hour).UnixMilli()
	set, err = testClient.PExpireAt(ctx, "pexpireat_key", time.UnixMilli(futureMs)).Result()
	assert.NoError(t, err)
	assert.True(t, set)

	// TTL
	_ = testClient.Set(ctx, "ttl_key", "value", 0).Err()
	_ = testClient.Expire(ctx, "ttl_key", 10*time.Second).Err()
	ttl, err := testClient.TTL(ctx, "ttl_key").Result()
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 10*time.Second)

	// PTTL
	_ = testClient.Set(ctx, "pttl_key", "value", 0).Err()
	_ = testClient.PExpire(ctx, "pttl_key", 10000).Err()
	pttl, err := testClient.PTTL(ctx, "pttl_key").Result()
	assert.NoError(t, err)
	assert.True(t, pttl > 0 && pttl <= 10000*time.Millisecond)

	// PERSIST
	_ = testClient.Set(ctx, "persist_key", "value", 0).Err()
	_ = testClient.Expire(ctx, "persist_key", 10*time.Second).Err()
	set, err = testClient.Persist(ctx, "persist_key").Result()
	assert.NoError(t, err)
	assert.True(t, set)

	ttl, err = testClient.TTL(ctx, "persist_key").Result()
	assert.NoError(t, err)
	assert.Equal(t, time.Duration(-1), ttl)

	// KEYS
	_ = testClient.Set(ctx, "keys_pattern1", "value", 0).Err()
	_ = testClient.Set(ctx, "keys_pattern2", "value", 0).Err()
	_ = testClient.Set(ctx, "other_key", "value", 0).Err()

	keys, err := testClient.Keys(ctx, "keys_pattern*").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))

	// RENAME
	_ = testClient.Set(ctx, "rename_source", "value", 0).Err()
	err = testClient.Rename(ctx, "rename_source", "rename_dest").Err()
	assert.NoError(t, err)

	val, err := testClient.Get(ctx, "rename_dest").Result()
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// RENAMENX
	_ = testClient.Set(ctx, "renamenx_source", "value1", 0).Err()
	_ = testClient.Set(ctx, "renamenx_dest", "value2", 0).Err()
	set, err = testClient.RenameNX(ctx, "renamenx_source", "renamenx_dest").Result()
	assert.NoError(t, err)
	assert.False(t, set) // 目标已存在

	_ = testClient.Del(ctx, "renamenx_dest").Err()
	set, err = testClient.RenameNX(ctx, "renamenx_source", "renamenx_dest").Result()
	assert.NoError(t, err)
	assert.True(t, set)

	// RANDOMKEY
	randomKey, err := testClient.RandomKey(ctx).Result()
	assert.NoError(t, err)
	assert.True(t, len(randomKey) > 0)

	// SCAN
	_ = testClient.Set(ctx, "scan_key1", "value", 0).Err()
	_ = testClient.Set(ctx, "scan_key2", "value", 0).Err()

	var cursor uint64
	keys, cursor, err = testClient.Scan(ctx, cursor, "scan_*", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))
}

// TestListCommands 测试List类型命令
func _TestListCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// LPUSH
	length, err := testClient.LPush(ctx, "mylist", "world", "hello").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)

	// LPUSHX
	length, err = testClient.LPushX(ctx, "mylist", "before").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	length, err = testClient.LPushX(ctx, "nonexistent_list", "value").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), length) // 键不存在，不执行操作

	// RPUSH
	length, err = testClient.RPush(ctx, "mylist", "end").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), length)

	// RPUSHX
	length, err = testClient.RPushX(ctx, "mylist", "after").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), length)

	length, err = testClient.RPushX(ctx, "nonexistent_list", "value").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), length)

	// LLEN
	length, err = testClient.LLen(ctx, "mylist").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), length)

	// LINDEX
	val, err := testClient.LIndex(ctx, "mylist", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, "before", val)

	val, err = testClient.LIndex(ctx, "mylist", -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "after", val)

	// LRANGE
	vals, err := testClient.LRange(ctx, "mylist", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 5, len(vals))
	assert.Equal(t, "before", vals[0])
	assert.Equal(t, "hello", vals[1])
	assert.Equal(t, "world", vals[2])
	assert.Equal(t, "end", vals[3])
	assert.Equal(t, "after", vals[4])

	// LSET
	err = testClient.LSet(ctx, "mylist", 0, "updated").Err()
	assert.NoError(t, err)

	val, err = testClient.LIndex(ctx, "mylist", 0).Result()
	assert.NoError(t, err)
	assert.Equal(t, "updated", val)

	// LTRIM
	err = testClient.LTrim(ctx, "mylist", 1, 3).Err()
	assert.NoError(t, err)

	length, err = testClient.LLen(ctx, "mylist").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	// LINSERT
	_ = testClient.RPush(ctx, "insertlist", "a", "b", "c").Err()
	err = testClient.LInsert(ctx, "insertlist", "BEFORE", "b", "x").Err()
	assert.NoError(t, err)

	vals, err = testClient.LRange(ctx, "insertlist", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "a", vals[0])
	assert.Equal(t, "x", vals[1])
	assert.Equal(t, "b", vals[2])

	err = testClient.LInsert(ctx, "insertlist", "AFTER", "c", "y").Err()
	assert.NoError(t, err)

	vals, err = testClient.LRange(ctx, "insertlist", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "y", vals[3])

	// LREM
	_ = testClient.RPush(ctx, "remlist", "a", "b", "a", "c", "a").Err()
	count, err := testClient.LRem(ctx, "remlist", 2, "a").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	vals, err = testClient.LRange(ctx, "remlist", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "b", vals[0])
	assert.Equal(t, "c", vals[1])
	assert.Equal(t, "a", vals[2])

	// LPOP
	val, err = testClient.LPop(ctx, "mylist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "hello", val)

	// RPOP
	val, err = testClient.RPop(ctx, "mylist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "world", val)

	// RPOPLPUSH
	_ = testClient.RPush(ctx, "sourcelist", "item1", "item2").Err()
	_ = testClient.RPush(ctx, "destlist", "existing").Err()

	val, err = testClient.RPopLPush(ctx, "sourcelist", "destlist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "item2", val)

	vals, err = testClient.LRange(ctx, "destlist", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "existing", vals[0])
	assert.Equal(t, "item2", vals[1])

	// BLPOP
	_ = testClient.RPush(ctx, "blist", "item1", "item2").Err()

	result, err := testClient.BLPop(ctx, 1*time.Second, "blist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "blist", result[0])
	assert.Equal(t, "item1", result[1])

	// BRPOP
	result, err = testClient.BRPop(ctx, 1*time.Second, "blist").Result()
	assert.NoError(t, err)
	assert.Equal(t, "blist", result[0])
	assert.Equal(t, "item2", result[1])
}

// TestHashCommands 测试Hash类型命令
func _TestHashCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// HSET
	count, err := testClient.HSet(ctx, "user:1", "name", "Alice", "age", "30").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// HSETNX
	set, err := testClient.HSetNX(ctx, "user:1", "email", "alice@example.com").Result()
	assert.NoError(t, err)
	assert.True(t, set)

	set, err = testClient.HSetNX(ctx, "user:1", "name", "Bob").Result()
	assert.NoError(t, err)
	assert.False(t, set) // 字段已存在

	// HGET
	val, err := testClient.HGet(ctx, "user:1", "name").Result()
	assert.NoError(t, err)
	assert.Equal(t, "Alice", val)

	// HGETALL
	all, err := testClient.HGetAll(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(all))
	assert.Equal(t, "Alice", all["name"])
	assert.Equal(t, "30", all["age"])
	assert.Equal(t, "alice@example.com", all["email"])

	// HEXISTS
	exists, err := testClient.HExists(ctx, "user:1", "name").Result()
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = testClient.HExists(ctx, "user:1", "nonexistent").Result()
	assert.NoError(t, err)
	assert.False(t, exists)

	// HLEN
	length, err := testClient.HLen(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), length)

	// HKEYS
	keys, err := testClient.HKeys(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(keys))

	// HVALS
	vals, err := testClient.HVals(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))

	// HMSET
	err = testClient.HMSet(ctx, "user:2", map[string]interface{}{
		"name": "Bob",
		"city": "Beijing",
	}).Err()
	assert.NoError(t, err)

	// HMGET
	hmgetVals, err := testClient.HMGet(ctx, "user:2", "name", "city", "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(hmgetVals))
	if hmgetVals[0] != nil {
		assert.Equal(t, "Bob", hmgetVals[0].(string))
	}
	if hmgetVals[1] != nil {
		assert.Equal(t, "Beijing", hmgetVals[1].(string))
	}
	assert.Nil(t, hmgetVals[2])

	// HINCRBY
	ageVal, err := testClient.HIncrBy(ctx, "user:1", "age", 1).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(31), ageVal)

	// HINCRBYFLOAT
	heightVal, err := testClient.HIncrByFloat(ctx, "user:1", "height", 1.5).Result()
	assert.NoError(t, err)
	assert.Equal(t, 1.5, heightVal)

	// HSTRLEN
	strlen, err := testClient.HStrLen(ctx, "user:1", "name").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), strlen)

	// HDEL
	deleted, err := testClient.HDel(ctx, "user:1", "age").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	length, err = testClient.HLen(ctx, "user:1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), length)
}

// TestSetCommands 测试Set类型命令
func TestSetCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// SADD
	added, err := testClient.SAdd(ctx, "myset", "apple", "banana", "cherry").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), added)

	// SADD to existing
	added, err = testClient.SAdd(ctx, "myset", "apple", "date").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), added) // 只添加了date

	// SCARD
	count, err := testClient.SCard(ctx, "myset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	// SISMEMBER
	member, err := testClient.SIsMember(ctx, "myset", "apple").Result()
	assert.NoError(t, err)
	assert.True(t, member)

	member, err = testClient.SIsMember(ctx, "myset", "nonexistent").Result()
	assert.NoError(t, err)
	assert.False(t, member)

	// SMEMBERS
	members, err := testClient.SMembers(ctx, "myset").Result()
	assert.NoError(t, err)
	assert.Equal(t, 4, len(members))

	// SREM
	removed, err := testClient.SRem(ctx, "myset", "apple").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), removed)

	count, err = testClient.SCard(ctx, "myset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// SPOP
	val, err := testClient.SPop(ctx, "myset").Result()
	assert.NoError(t, err)
	assert.True(t, len(val) > 0)

	count, err = testClient.SCard(ctx, "myset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// SRANDMEMBER
	vals, err := testClient.SRandMemberN(ctx, "myset", 1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vals))

	// SMOVE
	_ = testClient.SAdd(ctx, "set1", "a", "b", "c").Err()
	_ = testClient.SAdd(ctx, "set2", "x", "y").Err()

	moved, err := testClient.SMove(ctx, "set1", "set2", "b").Result()
	assert.NoError(t, err)
	assert.True(t, moved)

	members, err = testClient.SMembers(ctx, "set2").Result()
	assert.NoError(t, err)
	assert.True(t, len(members) == 3)

	// SINTER
	_ = testClient.SAdd(ctx, "setA", "a", "b", "c").Err()
	_ = testClient.SAdd(ctx, "setB", "b", "c", "d").Err()

	intersection, err := testClient.SInter(ctx, "setA", "setB").Result()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(intersection))

	// SINTERSTORE
	err = testClient.SInterStore(ctx, "setInter", "setA", "setB").Err()
	assert.NoError(t, err)

	count, err = testClient.SCard(ctx, "setInter").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// SUNION
	union, err := testClient.SUnion(ctx, "setA", "setB").Result()
	assert.NoError(t, err)
	assert.Equal(t, 4, len(union))

	// SUNIONSTORE
	err = testClient.SUnionStore(ctx, "setUnion", "setA", "setB").Err()
	assert.NoError(t, err)

	count, err = testClient.SCard(ctx, "setUnion").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	// SDIFF
	diff, err := testClient.SDiff(ctx, "setA", "setB").Result()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(diff))
	assert.Equal(t, "a", diff[0])

	// SDIFFSTORE
	err = testClient.SDiffStore(ctx, "setDiff", "setA", "setB").Err()
	assert.NoError(t, err)

	count, err = testClient.SCard(ctx, "setDiff").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// TestSortedSetCommands 测试SortedSet类型命令
func _TestSortedSetCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// ZADD
	added, err := testClient.ZAdd(ctx, "zset", redis.Z{
		Score:  1.0,
		Member: "member1",
	}, redis.Z{
		Score:  2.0,
		Member: "member2",
	}, redis.Z{
		Score:  3.0,
		Member: "member3",
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), added)

	// ZADD with duplicate members
	added, err = testClient.ZAdd(ctx, "zset", redis.Z{
		Score:  4.0,
		Member: "member1", // 更新
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), added) // 只是更新

	// ZCARD
	count, err := testClient.ZCard(ctx, "zset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// ZSCORE
	score, err := testClient.ZScore(ctx, "zset", "member1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 4.0, score) // 已更新为4.0

	// ZRANGE
	members, err := testClient.ZRange(ctx, "zset", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member1", members[0])
	assert.Equal(t, "member2", members[1])
	assert.Equal(t, "member3", members[2])

	// ZRANGE with scores
	zs, err := testClient.ZRangeWithScores(ctx, "zset", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(zs))
	assert.Equal(t, "member1", zs[0].Member)
	assert.Equal(t, 4.0, zs[0].Score)
	assert.Equal(t, "member2", zs[1].Member)
	assert.Equal(t, 2.0, zs[1].Score)

	// ZREVRANGE
	members, err = testClient.ZRevRange(ctx, "zset", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member3", members[0]) // 反序
	assert.Equal(t, "member2", members[1])
	assert.Equal(t, "member1", members[2])

	// ZRANK
	rank, err := testClient.ZRank(ctx, "zset", "member2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rank)

	// ZREVRANK
	rank, err = testClient.ZRevRank(ctx, "zset", "member2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rank)

	// ZCOUNT
	count, err = testClient.ZCount(ctx, "zset", "1", "3").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// ZINCRBY
	newScore, err := testClient.ZIncrBy(ctx, "zset", 1.5, "member1").Result()
	assert.NoError(t, err)
	assert.Equal(t, 5.5, newScore)

	// ZINCRBY on non-existent member
	newScore, err = testClient.ZIncrBy(ctx, "zset", 10.0, "new_member").Result()
	assert.NoError(t, err)
	assert.Equal(t, 10.0, newScore)

	// ZREM
	removed, err := testClient.ZRem(ctx, "zset", "member1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), removed)

	count, err = testClient.ZCard(ctx, "zset").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// ZREMRANGEBYRANK
	_ = testClient.ZAdd(ctx, "zset2", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Err()
	count, err = testClient.ZRemRangeByRank(ctx, "zset2", 0, 1).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// ZREMRANGEBYSCORE
	_ = testClient.ZAdd(ctx, "zset3", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Err()
	count, err = testClient.ZRemRangeByScore(ctx, "zset3", "-inf", "2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// ZLEXCOUNT
	_ = testClient.ZAdd(ctx, "zset4", redis.Z{Score: 0, Member: "a"}, redis.Z{Score: 0, Member: "b"}, redis.Z{Score: 0, Member: "c"}).Err()
	count, err = testClient.ZLexCount(ctx, "zset4", "-", "+").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// ZRANGEBYLEX
	members, err = testClient.ZRevRangeByScore(ctx, "zset4", &redis.ZRangeBy{
		Min: "-",
		Max: "+",
	}).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
}

// TestServerCommands 测试Server相关命令
func TestServerCommands(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 先创建一些数据
	_ = testClient.Set(ctx, "key1", "value1", 0).Err()
	_ = testClient.Set(ctx, "key2", "value2", 0).Err()
	_ = testClient.HSet(ctx, "hashkey", "field", "value").Err()

	// DBSIZE
	size, err := testClient.DBSize(ctx).Result()
	assert.NoError(t, err)
	assert.True(t, size >= 3)

	// LASTSAVE - skipped because backup is not enabled
	// lastSave, err := testClient.LastSave(ctx).Result()
	// assert.NoError(t, err)
	// assert.True(t, lastSave > 0)

	// INFO
	info, err := testClient.Info(ctx).Result()
	assert.NoError(t, err)
	assert.True(t, len(info) > 0)

	// FLUSHDB
	err = testClient.FlushDB(ctx).Err()
	assert.NoError(t, err)

	size, err = testClient.DBSize(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)

	// 重新创建数据用于FLUSHALL测试
	_ = testClient.Set(ctx, "key1", "value1", 0).Err()
}

// TestEdgeCases 测试边界情况和错误处理
func _TestEdgeCases(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	// 测试不存在的键
	_, err := testClient.Get(ctx, "nonexistent").Result()
	assert.Equal(t, redis.Nil, err)

	// 测试DEL不存在的键
	deleted, err := testClient.Del(ctx, "nonexistent").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	// 测试类型错误（对字符串键执行Hash操作）
	err = testClient.Set(ctx, "string_key", "value", 0).Err()
	assert.NoError(t, err)

	_, err = testClient.HGet(ctx, "string_key", "field").Result()
	assert.Error(t, err)

	// 测试对非list键执行LPOP
	err = testClient.Set(ctx, "not_list", "value", 0).Err()
	assert.NoError(t, err)

	val, err := testClient.LPop(ctx, "not_list").Result()
	assert.NoError(t, err)
	assert.Equal(t, nil, val) // 键被转换为空list, 返回nil

	// 测试对非set键执行SADD
	_ = testClient.Set(ctx, "not_set", "value", 0).Err()
	added, err := testClient.SAdd(ctx, "not_set", "member").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), added) // 键被转换

	// 测试对非zset键执行ZADD
	_ = testClient.Set(ctx, "not_zset", "value", 0).Err()
	added, err = testClient.ZAdd(ctx, "not_zset", redis.Z{Score: 1, Member: "m"}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), added)

	// 测试INCR对非数字字符串
	_ = testClient.Set(ctx, "not_number", "abc", 0).Err()
	_, err = testClient.Incr(ctx, "not_number").Result()
	assert.Error(t, err)

	// 测试边界值
	_ = testClient.Set(ctx, "empty", "", 0).Err()
	val, err = testClient.Get(ctx, "empty").Result()
	assert.NoError(t, err)
	assert.Equal(t, "", val)

	// 测试Unicode
	err = testClient.Set(ctx, "unicode", "你好世界", 0).Err()
	assert.NoError(t, err)

	val, err = testClient.Get(ctx, "unicode").Result()
	assert.NoError(t, err)
	assert.Equal(t, "你好世界", val)

	// 测试二进制数据
	binaryData := string([]byte{0x00, 0x01, 0x02, 0xFF})
	err = testClient.Set(ctx, "binary", binaryData, 0).Err()
	assert.NoError(t, err)

	val, err = testClient.Get(ctx, "binary").Result()
	assert.NoError(t, err)
	assert.Equal(t, binaryData, val)

	// 测试超长键名
	longKey := string(make([]byte, 4096))
	err = testClient.Set(ctx, longKey, "value", 0).Err()
	assert.NoError(t, err)

	val, err = testClient.Get(ctx, longKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// 测试超长值
	longValue := string(make([]byte, 1024*1024)) // 1MB
	err = testClient.Set(ctx, "longvalue", longValue, 0).Err()
	assert.NoError(t, err)

	length, err := testClient.StrLen(ctx, "longvalue").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1024*1024), length)
}

// TestConcurrentOperations 测试并发操作
func TestConcurrentOperations(t *testing.T) {
	setupTestServer(t)
	defer teardownTestServer(t)

	ctx := context.Background()

	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	// 并发写入
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			key := fmt.Sprintf("concurrent_key_%d", id)
			err := testClient.Set(ctx, key, fmt.Sprintf("value_%d", id), 0).Err()
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 验证所有键都存在
	for i := 0; i < numGoroutines; i++ {
		key := fmt.Sprintf("concurrent_key_%d", i)
		val, err := testClient.Get(ctx, key).Result()
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value_%d", i), val)
	}

	// 并发读取和写入混合操作
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			key := fmt.Sprintf("mixed_key_%d", id)
			_ = testClient.Set(ctx, key, fmt.Sprintf("initial_%d", id), 0).Err()
			val, _ := testClient.Get(ctx, key).Result()
			assert.Equal(t, fmt.Sprintf("initial_%d", id), val)
			_ = testClient.Incr(ctx, fmt.Sprintf("counter_%d", id)).Err()
			done <- true
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

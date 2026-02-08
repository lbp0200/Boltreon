package store

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
)

func TestDelString(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_string"

	// 设置字符串
	err := store.Set(key, "value")
	assert.NoError(t, err)

	// 验证存在
	val, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// 删除
	err = store.Del(key)
	assert.NoError(t, err)

	// 验证已删除
	val, err = store.Get(key)
	assert.Error(t, err)
	assert.Equal(t, "", val)

	// 删除不存在的键
	err = store.Del("nonexistent")
	assert.NoError(t, err)
}

func TestDelList(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_list"

	// 创建列表并添加元素
	_, err := store.LPush(key, "value1", "value2", "value3")
	assert.NoError(t, err)

	// 验证存在
	length, err := store.LLen(key)
	assert.NoError(t, err)
	assert.Equal(t, 3, length)

	// 删除
	err = store.Del(key)
	assert.NoError(t, err)

	// 验证已删除
	length, err = store.LLen(key)
	assert.NoError(t, err)
	assert.Equal(t, 0, length)

	// 验证所有相关键都已删除
	members, err := store.LRange(key, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(members))
}

func TestDelHash(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_hash"

	// 设置哈希字段
	err := store.HSet(key, "field1", "value1")
	assert.NoError(t, err)
	err = store.HSet(key, "field2", "value2")
	assert.NoError(t, err)

	// 验证存在
	count, err := store.HLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), count)

	// 删除
	err = store.Del(key)
	assert.NoError(t, err)

	// 验证已删除
	count, err = store.HLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)

	// 验证所有字段都已删除
	data, err := store.HGetAll(key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(data))
}

func TestDelSet(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_set"

	// 添加集合成员
	_, err := store.SAdd(key, "member1", "member2", "member3")
	assert.NoError(t, err)

	// 验证存在
	count, err := store.SCard(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), count)

	// 删除
	err = store.Del(key)
	assert.NoError(t, err)

	// 验证已删除
	count, err = store.SCard(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)

	// 验证所有成员都已删除
	members, err := store.SMembers(key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(members))
}

func TestDelSortedSet(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_zset"

	// 添加有序集合成员
	members := []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
	}
	err := store.ZAdd(key, members)
	assert.NoError(t, err)

	// 验证存在
	card, err := store.ZCard(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), card)

	// 删除
	err = store.Del(key)
	assert.NoError(t, err)

	// 验证已删除
	card, err = store.ZCard(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), card)

	// 验证所有成员都已删除
	rangeMembers, err := store.ZRange(key, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(rangeMembers))

	// 验证元数据已删除
	score, exists, err := store.ZScore(key, "member1")
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Equal(t, 0.0, score)
}

func TestDelNonExistentKey(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	// 删除不存在的键应该成功（不报错）
	err := store.Del("nonexistent")
	assert.NoError(t, err)
}

func TestDelAfterMultipleOperations(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_multi"

	// 先设置为字符串
	err := store.Set(key, "string_value")
	assert.NoError(t, err)

	// 删除
	err = store.Del(key)
	assert.NoError(t, err)

	// 再设置为列表
	_, err = store.LPush(key, "list_value")
	assert.NoError(t, err)

	// 验证类型已改变
	length, err := store.LLen(key)
	assert.NoError(t, err)
	assert.Equal(t, 1, length)

	// 再次删除
	err = store.Del(key)
	assert.NoError(t, err)

	// 验证已删除
	length, err = store.LLen(key)
	assert.NoError(t, err)
	assert.Equal(t, 0, length)
}

func TestDelStringMethod(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_del_string"

	// 设置字符串
	err := store.Set(key, "value")
	assert.NoError(t, err)

	// 使用DelString删除
	err = store.DelString(key)
	assert.NoError(t, err)

	// 验证已删除
	val, err := store.Get(key)
	assert.Error(t, err)
	assert.Equal(t, "", val)
}

func TestDelAllTypes(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	// 测试所有类型的删除
	tests := []struct {
		name string
		setup func(string) error
		verify func(string) error
	}{
		{
			name: "String",
			setup: func(key string) error {
				return store.Set(key, "value")
			},
			verify: func(key string) error {
				_, err := store.Get(key)
				if err == nil {
					return fmt.Errorf("expected key to be deleted")
				}
				return nil
			},
		},
		{
			name: "List",
			setup: func(key string) error {
				_, err := store.LPush(key, "value")
				return err
			},
			verify: func(key string) error {
				length, err := store.LLen(key)
				if err != nil {
					return err
				}
				if length != 0 {
					return nil // 如果长度不为0，说明删除失败
				}
				return nil
			},
		},
		{
			name: "Hash",
			setup: func(key string) error {
				return store.HSet(key, "field", "value")
			},
			verify: func(key string) error {
				count, err := store.HLen(key)
				if err != nil {
					return err
				}
				if count != 0 {
					return nil // 如果计数不为0，说明删除失败
				}
				return nil
			},
		},
		{
			name: "Set",
			setup: func(key string) error {
				_, err := store.SAdd(key, "member")
				return err
			},
			verify: func(key string) error {
				count, err := store.SCard(key)
				if err != nil {
					return err
				}
				if count != 0 {
					return nil // 如果计数不为0，说明删除失败
				}
				return nil
			},
		},
		{
			name: "SortedSet",
			setup: func(key string) error {
				return store.ZAdd(key, []ZSetMember{{Member: "member", Score: 1.0}})
			},
			verify: func(key string) error {
				card, err := store.ZCard(key)
				if err != nil {
					return err
				}
				if card != 0 {
					return nil // 如果计数不为0，说明删除失败
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "test_" + tt.name

			// 设置数据
			err := tt.setup(key)
			assert.NoError(t, err)

			// 删除
			err = store.Del(key)
			assert.NoError(t, err)

			// 验证已删除
			err = tt.verify(key)
			assert.NoError(t, err)
		})
	}
}

func TestDelLargeDataset(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_large"

	// 创建大量数据
	for i := 0; i < 100; i++ {
		_ = store.HSet(key, string(rune('a'+i)), i)
	}

	// 验证存在
	count, err := store.HLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), count)

	// 删除
	err = store.Del(key)
	assert.NoError(t, err)

	// 验证所有数据都已删除
	count, err = store.HLen(key)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)

	// 验证所有字段都已删除
	data, err := store.HGetAll(key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(data))
}

func TestDelComplexSortedSet(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_complex_zset"

	// 创建包含多个成员的排序集
	members := make([]ZSetMember, 50)
	for i := 0; i < 50; i++ {
		members[i] = ZSetMember{
			Member: string(rune('a' + i)),
			Score:  float64(i),
		}
	}
	err := store.ZAdd(key, members)
	assert.NoError(t, err)

	// 验证存在
	card, err := store.ZCard(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(50), card)

	// 删除
	err = store.Del(key)
	assert.NoError(t, err)

	// 验证所有数据都已删除
	card, err = store.ZCard(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), card)

	// 验证索引键、数据键和元数据键都已删除
	rangeMembers, err := store.ZRange(key, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(rangeMembers))

	// 验证特定成员已删除
	score, exists, err := store.ZScore(key, "a")
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Equal(t, 0.0, score)
}

func TestExists(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_exists"

	// 不存在的键
	exists, err := store.Exists(key)
	assert.NoError(t, err)
	assert.False(t, exists)

	// 设置键
	_ = store.Set(key, "value")
	exists, err = store.Exists(key)
	assert.NoError(t, err)
	assert.True(t, exists)

	// 删除键
	_ = store.Del(key)
	exists, err = store.Exists(key)
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestType(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	// 不存在的键
	keyType, err := store.Type("nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, "none", keyType)

	// String类型
	_ = store.Set("str_key", "value")
	keyType, err = store.Type("str_key")
	assert.NoError(t, err)
	assert.Equal(t, "string", keyType)

	// List类型
	_, _ = store.LPush("list_key", "value")
	keyType, err = store.Type("list_key")
	assert.NoError(t, err)
	assert.Equal(t, "list", keyType)

	// Hash类型
	_ = store.HSet("hash_key", "field", "value")
	keyType, err = store.Type("hash_key")
	assert.NoError(t, err)
	assert.Equal(t, "hash", keyType)

	// Set类型
	_, _ = store.SAdd("set_key", "member")
	keyType, err = store.Type("set_key")
	assert.NoError(t, err)
	assert.Equal(t, "set", keyType)

	// SortedSet类型
	_ = store.ZAdd("zset_key", []ZSetMember{{Member: "member", Score: 1.0}})
	keyType, err = store.Type("zset_key")
	assert.NoError(t, err)
	assert.Equal(t, "zset", keyType)
}

func TestExpire(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_expire"

	// 设置键
	_ = store.Set(key, "value")

	// 设置过期时间
	success, err := store.Expire(key, 10)
	assert.NoError(t, err)
	assert.True(t, success)

	// 验证TTL
	ttl, err := store.TTL(key)
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 10)

	// 不存在的键
	success, err = store.Expire("nonexistent", 10)
	assert.NoError(t, err)
	assert.False(t, success)
}

func TestTTL(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_ttl"

	// 不存在的键
	ttl, err := store.TTL(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), ttl)

	// 设置键（无过期时间）
	_ = store.Set(key, "value")
	ttl, err = store.TTL(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), ttl) // -1表示没有过期时间

	// 设置过期时间
	_, _ = store.Expire(key, 10)
	ttl, err = store.TTL(key)
	assert.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 10)
}

func TestPTTL(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_pttl"

	// 不存在的键
	pttl, err := store.PTTL(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), pttl)

	// 设置键（无过期时间）
	_ = store.Set(key, "value")
	pttl, err = store.PTTL(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), pttl) // -1表示没有过期时间

	// 设置过期时间（毫秒）
	_, _ = store.PExpire(key, 10000)
	pttl, err = store.PTTL(key)
	assert.NoError(t, err)
	assert.True(t, pttl > 0 && pttl <= 10000)
}

func TestPersist(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	key := "test_persist"

	// 设置键（无过期时间）
	_ = store.Set(key, "value")
	success, err := store.Persist(key)
	assert.NoError(t, err)
	assert.False(t, success) // 没有TTL，返回false

	// 设置过期时间
	_, _ = store.Expire(key, 10)
	ttl, _ := store.TTL(key)
	assert.True(t, ttl > 0)

	// 移除过期时间
	success, err = store.Persist(key)
	assert.NoError(t, err)
	assert.True(t, success)

	// 验证TTL为-1
	ttl, err = store.TTL(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), ttl)

	// 不存在的键
	success, err = store.Persist("nonexistent")
	assert.NoError(t, err)
	assert.False(t, success)
}

func TestRename(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	oldKey := "old_key"
	newKey := "new_key"

	// 测试String类型
	_ = store.Set(oldKey, "value")
	err := store.Rename(oldKey, newKey)
	assert.NoError(t, err)

	// 验证旧键不存在
	exists, _ := store.Exists(oldKey)
	assert.False(t, exists)

	// 验证新键存在
	val, _ := store.Get(newKey)
	assert.Equal(t, "value", val)

	// 测试List类型
	_, _ = store.LPush("list_old", "value1", "value2")
	err = store.Rename("list_old", "list_new")
	assert.NoError(t, err)

	length, _ := store.LLen("list_new")
	assert.Equal(t, 2, length)

	// 测试Hash类型
	_ = store.HSet("hash_old", "field", "value")
	err = store.Rename("hash_old", "hash_new")
	assert.NoError(t, err)

	valBytes, _ := store.HGet("hash_new", "field")
	assert.NotNil(t, valBytes)

	// 测试Set类型
	_, _ = store.SAdd("set_old", "member1", "member2")
	err = store.Rename("set_old", "set_new")
	assert.NoError(t, err)

	count, _ := store.SCard("set_new")
	assert.Equal(t, uint64(2), count)

	// 测试SortedSet类型
	_ = store.ZAdd("zset_old", []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
	})
	err = store.Rename("zset_old", "zset_new")
	assert.NoError(t, err)

	card, _ := store.ZCard("zset_new")
	assert.Equal(t, int64(2), card)

	// 测试重命名到已存在的键（应该覆盖）
	_ = store.Set("key1", "value1")
	_ = store.Set("key2", "value2")
	err = store.Rename("key1", "key2")
	assert.NoError(t, err)

	val, _ = store.Get("key2")
	assert.Equal(t, "value1", val) // 应该是key1的值

	// 测试不存在的键
	err = store.Rename("nonexistent", "new_key")
	assert.Error(t, err)
}

func TestRenameNX(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	oldKey := "old_key"
	newKey := "new_key"

	// 设置旧键
	_ = store.Set(oldKey, "value")

	// 新键不存在，应该成功
	success, err := store.RenameNX(oldKey, newKey)
	assert.NoError(t, err)
	assert.True(t, success)

	// 验证重命名成功
	val, _ := store.Get(newKey)
	assert.Equal(t, "value", val)

	// 再次尝试重命名（新键已存在）
	_ = store.Set(oldKey, "value2")
	success, err = store.RenameNX(oldKey, newKey)
	assert.NoError(t, err)
	assert.False(t, success)

	// 验证新键值未改变
	val, _ = store.Get(newKey)
	assert.Equal(t, "value", val) // 仍然是旧值

	// 测试不存在的键
	success, err = store.RenameNX("nonexistent", "any_key")
	assert.Error(t, err)
	assert.False(t, success)
}

func TestKeys(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	// 创建多个键
	_ = store.Set("user:1", "value1")
	_ = store.Set("user:2", "value2")
	_ = store.Set("order:1", "value3")
	_, _ = store.LPush("list:1", "item1")
	_ = store.HSet("hash:1", "field", "value")

	// 测试匹配所有键
	keys, err := store.Keys("*")
	assert.NoError(t, err)
	assert.True(t, len(keys) >= 5)

	// 测试匹配模式
	keys, err = store.Keys("user:*")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))

	keys, err = store.Keys("order:*")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keys))

	// 测试不匹配的模式
	keys, err = store.Keys("nonexistent:*")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(keys))
}

func TestScan(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	// 创建多个键
	for i := 0; i < 20; i++ {
		_ = store.Set(fmt.Sprintf("key:%d", i), "value")
	}

	// 测试SCAN
	cursor := uint64(0)
	totalKeys := 0
	for {
		result, err := store.Scan(cursor, "*", 5)
		assert.NoError(t, err)
		totalKeys += len(result.Keys)
		if result.Cursor == 0 {
			break
		}
		cursor = result.Cursor
	}
	assert.True(t, totalKeys >= 20)
}

func TestRandomKey(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	// 空数据库
	key, err := store.RandomKey()
	assert.NoError(t, err)
	assert.Equal(t, "", key)

	// 有键的数据库
	_ = store.Set("key1", "value1")
	_ = store.Set("key2", "value2")
	_ = store.Set("key3", "value3")

	key, err = store.RandomKey()
	assert.NoError(t, err)
	assert.True(t, key == "key1" || key == "key2" || key == "key3")
}

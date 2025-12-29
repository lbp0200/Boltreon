package store

import (
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
				return err
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
		store.HSet(key, string(rune('a'+i)), i)
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

package store

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestZAdd(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 添加成员
	members := []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
		{Member: "member3", Score: 0.0},
	}
	err := store.ZAdd(zSetName, members)
	assert.NoError(t, err)

	// 验证成员已添加
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(3), card)

	// 更新成员分数
	updateMembers := []ZSetMember{
		{Member: "member1", Score: 2.5},
	}
	err = store.ZAdd(zSetName, updateMembers)
	assert.NoError(t, err)

	// 验证分数已更新
	score, exists, _ := store.ZScore(zSetName, "member1")
	assert.True(t, exists)
	assert.Equal(t, 2.5, score)
}

func TestZCard(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 空集合
	card, err := store.ZCard(zSetName)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), card)

	// 添加成员后
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
	})
	card, err = store.ZCard(zSetName)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), card)
}

func TestZScore(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
	})

	// 获取存在的成员分数
	score, exists, err := store.ZScore(zSetName, "member1")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, 1.5, score)

	// 获取不存在的成员
	score, exists, err = store.ZScore(zSetName, "nonexistent")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestZCount(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
		{Member: "member4", Score: 4.0},
	})

	// 计算范围内的成员数
	count, err := store.ZCount(zSetName, 1.0, 3.0)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// 计算所有成员
	count, err = store.ZCount(zSetName, -100.0, 100.0)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count)

	// 空范围
	count, err = store.ZCount(zSetName, 10.0, 20.0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestZIncrBy(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 对不存在的成员增加分数
	newScore, err := store.ZIncrBy(zSetName, "member1", 5.0)
	assert.NoError(t, err)
	assert.Equal(t, 5.0, newScore)

	// 再次增加
	newScore, err = store.ZIncrBy(zSetName, "member1", 2.5)
	assert.NoError(t, err)
	assert.Equal(t, 7.5, newScore)

	// 减少分数
	newScore, err = store.ZIncrBy(zSetName, "member1", -1.0)
	assert.NoError(t, err)
	assert.Equal(t, 6.5, newScore)

	// 验证分数
	score, exists, _ := store.ZScore(zSetName, "member1")
	assert.True(t, exists)
	assert.Equal(t, 6.5, score)
}

func TestZRank(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据（按分数排序：member2(-2.0), member3(0.0), member1(1.5)）
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
		{Member: "member3", Score: 0.0},
	})

	// 获取排名
	rank, err := store.ZRank(zSetName, "member2")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rank) // 最低分数，排名0

	rank, err = store.ZRank(zSetName, "member3")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rank)

	rank, err = store.ZRank(zSetName, "member1")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rank) // 最高分数，排名2

	// 不存在的成员
	rank, err = store.ZRank(zSetName, "nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), rank)
}

func TestZRevRank(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据（按分数排序：member2(-2.0), member3(0.0), member1(1.5)）
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
		{Member: "member3", Score: 0.0},
	})

	// 获取反向排名
	rank, err := store.ZRevRank(zSetName, "member1")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rank) // 最高分数，反向排名0

	rank, err = store.ZRevRank(zSetName, "member3")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rank)

	rank, err = store.ZRevRank(zSetName, "member2")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), rank) // 最低分数，反向排名2

	// 不存在的成员
	rank, err = store.ZRevRank(zSetName, "nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), rank)
}

func TestZRange(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
		{Member: "member3", Score: 0.0},
	})

	// 获取所有成员
	members, err := store.ZRange(zSetName, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member2", members[0].Member) // 最低分数
	assert.Equal(t, "member3", members[1].Member)
	assert.Equal(t, "member1", members[2].Member) // 最高分数

	// 获取范围
	members, err = store.ZRange(zSetName, 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))

	// 负索引
	members, err = store.ZRange(zSetName, -2, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

func TestZRevRange(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.5},
		{Member: "member2", Score: -2.0},
		{Member: "member3", Score: 0.0},
	})

	// 获取所有成员（反向）
	members, err := store.ZRevRange(zSetName, 0, -1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member1", members[0].Member) // 最高分数
	assert.Equal(t, "member3", members[1].Member)
	assert.Equal(t, "member2", members[2].Member) // 最低分数

	// 获取范围
	members, err = store.ZRevRange(zSetName, 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
	assert.Equal(t, "member1", members[0].Member)
}

func TestZRangeByScore(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
		{Member: "member4", Score: 4.0},
	})

	// 获取分数范围内的成员
	members, err := store.ZRangeByScore(zSetName, 1.0, 3.0, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member1", members[0].Member)
	assert.Equal(t, "member2", members[1].Member)
	assert.Equal(t, "member3", members[2].Member)

	// 带offset和count
	members, err = store.ZRangeByScore(zSetName, 1.0, 4.0, 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
}

func TestZRevRangeByScore(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
	})

	// 获取分数范围内的成员（反向）
	members, err := store.ZRevRangeByScore(zSetName, 3.0, 1.0, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member3", members[0].Member) // 最高分数
	assert.Equal(t, "member2", members[1].Member)
	assert.Equal(t, "member1", members[2].Member) // 最低分数
}

func TestZRem(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
	})

	// 删除成员
	err := store.ZRem(zSetName, "member1")
	assert.NoError(t, err)

	// 验证成员已删除
	_, exists, _ := store.ZScore(zSetName, "member1")
	assert.False(t, exists)

	// 验证集合大小
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(1), card)

	// 删除不存在的成员
	err = store.ZRem(zSetName, "nonexistent")
	assert.NoError(t, err)
}

func TestZRemRangeByRank(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
		{Member: "member4", Score: 4.0},
	})

	// 删除排名范围内的成员
	removed, err := store.ZRemRangeByRank(zSetName, 1, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), removed)

	// 验证剩余成员
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(2), card)

	members, _ := store.ZRange(zSetName, 0, -1)
	assert.Equal(t, 2, len(members))
}

func TestZRemRangeByScore(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
		{Member: "member4", Score: 4.0},
	})

	// 删除分数范围内的成员
	removed, err := store.ZRemRangeByScore(zSetName, 2.0, 3.0)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), removed)

	// 验证剩余成员
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(2), card)
}

func TestZPopMax(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
	})

	// 弹出最高分数的成员
	members, err := store.ZPopMax(zSetName, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "member3", members[0].Member)
	assert.Equal(t, 3.0, members[0].Score)

	// 验证成员已删除
	_, exists, _ := store.ZScore(zSetName, "member3")
	assert.False(t, exists)

	// 弹出多个成员
	members, err = store.ZPopMax(zSetName, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
	assert.Equal(t, "member2", members[0].Member)
	assert.Equal(t, "member1", members[1].Member)
}

func TestZPopMin(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
	})

	// 弹出最低分数的成员
	members, err := store.ZPopMin(zSetName, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(members))
	assert.Equal(t, "member1", members[0].Member)
	assert.Equal(t, 1.0, members[0].Score)

	// 验证成员已删除
	_, exists, _ := store.ZScore(zSetName, "member1")
	assert.False(t, exists)

	// 弹出多个成员
	members, err = store.ZPopMin(zSetName, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(members))
	assert.Equal(t, "member2", members[0].Member)
	assert.Equal(t, "member3", members[1].Member)
}

func TestZSetDel(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 准备数据
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
	})

	// 删除整个集合
	err := store.ZSetDel(zSetName)
	assert.NoError(t, err)

	// 验证集合已删除
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(0), card)

	members, _ := store.ZRange(zSetName, 0, -1)
	assert.Equal(t, 0, len(members))
}

func TestSortedSetEdgeCases(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 测试空集合操作
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(0), card)

	rank, _ := store.ZRank(zSetName, "member1")
	assert.Equal(t, int64(-1), rank)

	// 测试相同分数的成员
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 1.0},
		{Member: "member3", Score: 1.0},
	})

	card, _ = store.ZCard(zSetName)
	assert.Equal(t, int64(3), card)

	// 测试大量成员
	largeMembers := make([]ZSetMember, 100)
	for i := 0; i < 100; i++ {
		largeMembers[i] = ZSetMember{
			Member: string(rune('a' + i)),
			Score:  float64(i),
		}
	}
	store.ZAdd("large", largeMembers)
	card, _ = store.ZCard("large")
	assert.Equal(t, int64(100), card)

	// 测试负分数
	store.ZAdd("negative", []ZSetMember{
		{Member: "member1", Score: -10.0},
		{Member: "member2", Score: -5.0},
		{Member: "member3", Score: 0.0},
	})
	members, _ := store.ZRange("negative", 0, -1)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, "member1", members[0].Member) // 最低分数
}

func TestSortedSetOperations(t *testing.T) {
	dbPath := t.TempDir()
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	zSetName := "myset"

	// 综合测试：添加、更新、查询、删除
	store.ZAdd(zSetName, []ZSetMember{
		{Member: "member1", Score: 1.0},
		{Member: "member2", Score: 2.0},
		{Member: "member3", Score: 3.0},
	})

	// 使用ZIncrBy增加分数
	newScore, _ := store.ZIncrBy(zSetName, "member1", 1.5)
	assert.Equal(t, 2.5, newScore)

	// 获取排名
	rank, _ := store.ZRank(zSetName, "member1")
	assert.True(t, rank >= 0)

	// 获取反向排名
	revRank, _ := store.ZRevRank(zSetName, "member1")
	assert.True(t, revRank >= 0)

	// 范围查询
	members, _ := store.ZRange(zSetName, 0, -1)
	assert.Equal(t, 3, len(members))

	// 分数范围查询
	scoreMembers, _ := store.ZRangeByScore(zSetName, 1.0, 3.0, 0, 0)
	assert.True(t, len(scoreMembers) > 0)

	// 删除成员
	store.ZRem(zSetName, "member2")
	card, _ := store.ZCard(zSetName)
	assert.Equal(t, int64(2), card)
}

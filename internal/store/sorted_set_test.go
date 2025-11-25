package store

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupTestStore(t *testing.T) *BadgerStore {
	dbPath := t.TempDir()
	fmt.Println("dbPath:", dbPath)
	store, err := NewBadgerStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create BadgerStore: %v", err)
	}
	return store
}

func TestZSetAll(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	t.Run("ZAdd and ZScore", func(t *testing.T) {
		members := []ZSetMember{
			{Member: "member1", Score: 1.5},
			{Member: "member2", Score: -2.0},
			{Member: "member3", Score: 0.0},
		}
		err := store.ZAdd("myset", members)
		assert.NoError(t, err, "ZAdd should succeed")

		for _, m := range members {
			score, exists, err := store.ZScore("myset", m.Member)
			assert.NoError(t, err, "ZScore should succeed for %s", m.Member)
			assert.True(t, exists, "Member %s should exist", m.Member)
			assert.Equal(t, m.Score, score, "Score for %s should match", m.Member)
		}

		_, exists, err := store.ZScore("myset", "nonexistent")
		assert.NoError(t, err, "ZScore should succeed for nonexistent member")
		assert.False(t, exists, "Nonexistent member should not exist")
	})

	t.Run("ZRange", func(t *testing.T) {
		entries, err := store.ZRange("myset", 0, -1)
		assert.NoError(t, err, "ZRange should succeed")
		expected := []ZSetMember{
			{Member: "member2", Score: -2.0},
			{Member: "member3", Score: 0.0},
			{Member: "member1", Score: 1.5},
		}
		assert.Equal(t, len(expected), len(entries), "ZRange length should match")
		for i, entry := range entries {
			assert.Equal(t, expected[i].Member, entry.Member, "Member %d should match", i)
			assert.Equal(t, expected[i].Score, entry.Score, "Score %d should match", i)
		}

		entries, err = store.ZRange("myset", -2, -1)
		assert.NoError(t, err, "ZRange with negative indices should succeed")
		if assert.Len(t, entries, 2, "ZRange length should be 2") {
			assert.Equal(t, "member3", entries[0].Member, "First member should match")
			assert.Equal(t, "member1", entries[1].Member, "Second member should match")
		}

		entries, err = store.ZRange("nonexistent", 0, -1)
		assert.NoError(t, err, "ZRange on nonexistent set should succeed")
		assert.Empty(t, entries, "ZRange on nonexistent set should return empty")
	})

	t.Run("ZRangeByScore", func(t *testing.T) {
		entries, err := store.ZRangeByScore("myset", -3.0, 1.0, 0, 0)
		assert.NoError(t, err, "ZRangeByScore should succeed")
		expected := []ZSetMember{
			{Member: "member2", Score: -2.0},
			{Member: "member3", Score: 0.0},
		}
		assert.Equal(t, len(expected), len(entries), "ZRangeByScore length should match")
		for i, entry := range entries {
			assert.Equal(t, expected[i].Member, entry.Member, "Member %d should match", i)
			assert.Equal(t, expected[i].Score, entry.Score, "Score %d should match", i)
		}

		entries, err = store.ZRangeByScore("myset", -3.0, 1.5, 1, 1)
		assert.NoError(t, err, "ZRangeByScore with offset should succeed")
		if assert.Len(t, entries, 1, "ZRangeByScore length should be 1") {
			assert.Equal(t, "member3", entries[0].Member, "Member should match")
		}
	})

	t.Run("ZRem", func(t *testing.T) {
		err := store.ZRem("myset", "member1")
		assert.NoError(t, err, "ZRem should succeed")
		_, exists, err := store.ZScore("myset", "member1")
		assert.NoError(t, err, "ZScore should succeed")
		assert.False(t, exists, "Removed member should not exist")

		entries, err := store.ZRange("myset", 0, -1)
		assert.NoError(t, err, "ZRange should succeed")
		assert.Equal(t, 2, len(entries), "ZRange length should be 2")

		err = store.ZRem("myset", "nonexistent")
		assert.NoError(t, err, "ZRem nonexistent member should succeed")
	})

	t.Run("ZSetDel", func(t *testing.T) {
		err := store.ZSetDel("myset")
		assert.NoError(t, err, "ZSetDel should succeed")
		entries, err := store.ZRange("myset", 0, -1)
		assert.NoError(t, err, "ZRange should succeed")
		assert.Empty(t, entries, "ZRange on deleted set should return empty")
	})

	t.Run("Concurrent ZAdd", func(t *testing.T) {
		store := setupTestStore(t)
		defer store.Close()

		var wg sync.WaitGroup
		numGoroutines := 10
		membersPerGoroutine := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				members := make([]ZSetMember, membersPerGoroutine)
				for j := 0; j < membersPerGoroutine; j++ {
					members[j] = ZSetMember{
						Member: fmt.Sprintf("member_%d_%d", id, j),
						Score:  float64(id*100 + j),
					}
				}
				err := store.ZAdd("concurrent_set", members)
				assert.NoError(t, err, "Concurrent ZAdd should succeed")
			}(i)
		}
		wg.Wait()

		entries, err := store.ZRange("concurrent_set", 0, -1)
		assert.NoError(t, err, "ZRange should succeed")
		assert.Equal(t, numGoroutines*membersPerGoroutine, len(entries), "Concurrent ZAdd count should match")
	})

	t.Run("Large Dataset", func(t *testing.T) {
		store := setupTestStore(t)
		defer store.Close()

		const numMembers = 1000
		members := make([]ZSetMember, numMembers)
		for i := 0; i < numMembers; i++ {
			members[i] = ZSetMember{
				Member: fmt.Sprintf("member_%d", i),
				Score:  float64(i) - 500.0,
			}
		}
		err := store.ZAdd("large_set", members)
		assert.NoError(t, err, "ZAdd large dataset should succeed")

		entries, err := store.ZRange("large_set", 0, -1)
		assert.NoError(t, err, "ZRange large dataset should succeed")
		assert.Equal(t, numMembers, len(entries), "ZRange length should match")
		for i := 1; i < len(entries); i++ {
			assert.LessOrEqual(t, entries[i-1].Score, entries[i].Score, "Scores should be sorted")
		}
	})

	t.Run("Edge Cases", func(t *testing.T) {
		store := setupTestStore(t)
		defer store.Close()

		err := store.ZAdd("empty_set", []ZSetMember{})
		assert.NoError(t, err, "ZAdd empty input should succeed")

		entries, err := store.ZRange("empty_set", 10, 5)
		assert.NoError(t, err, "ZRange invalid indices should succeed")
		assert.Empty(t, entries, "ZRange invalid indices should return empty")

		members := []ZSetMember{
			{Member: "member1", Score: 1.0},
			{Member: "member1", Score: 2.0},
		}
		err = store.ZAdd("duplicate_set", members)
		assert.NoError(t, err, "ZAdd duplicate members should succeed")
		score, exists, err := store.ZScore("duplicate_set", "member1")
		assert.NoError(t, err, "ZScore should succeed")
		assert.True(t, exists, "Member should exist")
		assert.Equal(t, 2.0, score, "Score should be updated to latest")
	})
}

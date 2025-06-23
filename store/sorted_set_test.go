package store

import (
	"fmt"
	"testing"
)

func TestZsetAll(t *testing.T) {
	dbPath := t.TempDir()
	t.Log("dbPath:", dbPath)
	store, _ := NewBadgerStore(dbPath)

	// 添加元素到 Sorted Set
	err := store.ZAdd("myset", []ZSetMember{{Member: "member1", Score: 1.5}})
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Added Count:")
	}

	// 获取范围内的元素
	entries, err := store.ZRange([]byte("myset"), 0, -1)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		for _, entry := range entries {
			fmt.Printf("Member: %s, Score: %.2f\n", entry.Member, entry.Score)
		}
	}
}

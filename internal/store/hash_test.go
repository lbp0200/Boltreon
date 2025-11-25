package store

import (
	"fmt"
	"testing"
)

func TestHashAuto(t *testing.T) {
	dbPath := t.TempDir()
	t.Log("dbPath:", dbPath)
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	// 设置哈希字段
	err := store.HSet("user:1", "name", "Alice")
	if err != nil {
		t.Fatal(err)
	}
	err = store.HSet("user:1", "age", 30)
	if err != nil {
		t.Fatal(err)
	}
	// 获取所有字段
	data, err := store.HGetAll("user:1")
	if err != nil {
		t.Fatal(err)
	}
	// 输出: map[name:[65 108 105 99 101] age:[30]]
	t.Log(data)
	// 删除字段
	deleted, _ := store.HDel("user:1", "age")
	fmt.Println(deleted) // 1

	// 获取字段数量
	count, _ := store.HLen("user:1")
	fmt.Println(count) // 1

}

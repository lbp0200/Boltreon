package store

import (
	"fmt"
	"testing"
)

func TestSetAuto(t *testing.T) {
	dbPath := t.TempDir()
	t.Log("dbPath:", dbPath)
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()

	// 添加成员
	added, _ := store.SAdd([]byte("myset"), []byte("apple"), []byte("banana"))
	fmt.Println(added) // 输出 2

	// 判断成员存在性
	exists, _ := store.SIsMember([]byte("myset"), []byte("apple"))
	fmt.Println(exists) // true

	// 获取集合大小
	count, _ := store.SCard([]byte("myset"))
	fmt.Println(count) // 2

	// 删除成员
	removed, _ := store.SRem([]byte("myset"), []byte("banana"))
	fmt.Println(removed) // 1

}

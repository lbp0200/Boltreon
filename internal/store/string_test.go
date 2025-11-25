package store

import (
	"fmt"
	"testing"
)

func TestStringAuto(t *testing.T) {
	dbPath := t.TempDir()
	t.Log("dbPath:", dbPath)
	store, _ := NewBadgerStore(dbPath)
	defer store.Close()
	// 设置键值
	err := store.Set([]byte("mykey"), []byte("Hello World"))
	if err != nil {
		panic(err)
	}

	// 获取键值
	value, err := store.Get([]byte("mykey"))
	fmt.Println(string(value)) // 输出 "Hello World"

	// 删除键
	err = store.Del("mykey")
	if err != nil {
		panic(err)
	}

}

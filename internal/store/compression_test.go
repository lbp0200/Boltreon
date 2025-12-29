package store

import (
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

func TestCompressionLZ4(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBoltreonStoreWithCompression(dbPath, CompressionLZ4)
	assert.NoError(t, err)
	defer store.Close()

	// 测试大字符串压缩
	largeValue := strings.Repeat("This is a test string that will be compressed. ", 100)
	key := "large_key"

	// 写入
	err = store.Set(key, largeValue)
	assert.NoError(t, err)

	// 读取
	value, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, largeValue, value)
}

func TestCompressionZSTD(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBoltreonStoreWithCompression(dbPath, CompressionZSTD)
	assert.NoError(t, err)
	defer store.Close()

	// 测试大字符串压缩
	largeValue := strings.Repeat("This is a test string that will be compressed. ", 100)
	key := "large_key"

	// 写入
	err = store.Set(key, largeValue)
	assert.NoError(t, err)

	// 读取
	value, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, largeValue, value)
}

func TestCompressionNone(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBoltreonStoreWithCompression(dbPath, CompressionNone)
	assert.NoError(t, err)
	defer store.Close()

	// 测试不压缩
	largeValue := strings.Repeat("This is a test string. ", 100)
	key := "large_key"

	// 写入
	err = store.Set(key, largeValue)
	assert.NoError(t, err)

	// 读取
	value, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, largeValue, value)
}

func TestCompressionSmallData(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBoltreonStoreWithCompression(dbPath, CompressionLZ4)
	assert.NoError(t, err)
	defer store.Close()

	// 小数据（小于64字节）不应该被压缩
	smallValue := "small"
	key := "small_key"

	// 写入
	err = store.Set(key, smallValue)
	assert.NoError(t, err)

	// 读取
	value, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, smallValue, value)
}

func TestCompressionHash(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBoltreonStoreWithCompression(dbPath, CompressionLZ4)
	assert.NoError(t, err)
	defer store.Close()

	// 测试Hash压缩
	key := "user:1"
	largeValue := strings.Repeat("This is a large hash field value. ", 50)

	err = store.HSet(key, "description", largeValue)
	assert.NoError(t, err)

	value, err := store.HGet(key, "description")
	assert.NoError(t, err)
	assert.Equal(t, largeValue, string(value))
}

func TestCompressionBackwardCompatibility(t *testing.T) {
	dbPath := t.TempDir()
	
	// 先不使用压缩写入数据
	store1, err := NewBoltreonStoreWithCompression(dbPath, CompressionNone)
	assert.NoError(t, err)
	err = store1.Set("key1", "value1")
	assert.NoError(t, err)
	store1.Close()

	// 使用压缩读取旧数据
	store2, err := NewBoltreonStoreWithCompression(dbPath, CompressionLZ4)
	assert.NoError(t, err)
	defer store2.Close()

	value, err := store2.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)

	// 写入新数据（会被压缩）
	err = store2.Set("key2", strings.Repeat("large value ", 100))
	assert.NoError(t, err)

	// 读取新数据
	value2, err := store2.Get("key2")
	assert.NoError(t, err)
	assert.True(t, len(value2) > 0)
}

func TestCompressionSwitch(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBoltreonStoreWithCompression(dbPath, CompressionLZ4)
	assert.NoError(t, err)
	defer store.Close()

	// 使用LZ4写入
	largeValue := strings.Repeat("test ", 100)
	err = store.Set("key1", largeValue)
	assert.NoError(t, err)

	// 切换到ZSTD
	store.SetCompression(CompressionZSTD)
	assert.Equal(t, CompressionZSTD, store.GetCompression())

	// 使用ZSTD写入
	err = store.Set("key2", largeValue)
	assert.NoError(t, err)

	// 读取两个键都应该正常
	value1, err := store.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, largeValue, value1)

	value2, err := store.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, largeValue, value2)
}


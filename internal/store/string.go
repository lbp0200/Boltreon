package store

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// stringKey 方法用于生成存储在 Badger 数据库中的键
func (s *BoltreonStore) stringKey(key []byte) []byte {
	return []byte(fmt.Sprintf("%s:%s", KeyTypeString, string(key)))
}

// Set 实现 Redis SET 命令
func (s *BoltreonStore) Set(key []byte, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(TypeOfKeyGet(string(key)), []byte(KeyTypeString)); err != nil {
			return err
		}
		strKey := s.stringKey(key)
		return txn.Set(strKey, value)
	})
}

// SetWithTTL 字符串操作
func (s *BoltreonStore) SetWithTTL(key, value []byte, ttl time.Duration) error {
	return s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, value).WithTTL(ttl)
		return txn.SetEntry(e)
	})
}

// Get 实现 Redis GET 命令
func (s *BoltreonStore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		strKey := s.stringKey(key)
		item, err := txn.Get(strKey)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil // 返回 nil 表示键不存在
			}
			return err
		}
		v, _ := item.ValueCopy(nil)
		val = v
		return nil
	})
	return val, err
}

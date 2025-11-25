package store

import (
	"errors"
	"fmt"
	"github.com/lbp0200/Boltreon/internal/helper"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// 哈希操作
//func (s *BoltreonStore) HSet(key, field string, value interface{}) error {
//	logFuncTag := "BoltreonStoreHSet"
//	baggerTypeKey := TypeOfKeyGet(key)
//	bValue, err := helper.InterfaceToBytes(value)
//	if err != nil {
//		return fmt.Errorf("%s,%v", logFuncTag, err)
//	}
//	hkey := s.hashKey(key, field)
//	return s.db.Update(func(txn *badger.Txn) error {
//		err := txn.Set(hkey, bValue)
//		if err != nil {
//			return err
//		}
//		return txn.Set(baggerTypeKey, []byte(KeyTypeHash))
//	})
//}

// 修改 HSet 维护计数器
func (s *BoltreonStore) HSet(key, field string, value interface{}) error {
	logFuncTag := "BoltreonStoreHSet"
	bValue, err := helper.InterfaceToBytes(value)
	if err != nil {
		return fmt.Errorf("%s,%v", logFuncTag, err)
	}
	hkey := s.hashKey(key, field)
	return s.db.Update(func(txn *badger.Txn) error {
		// 检查字段是否存在
		exists := false
		if _, err := txn.Get(hkey); err == nil {
			exists = true
		}

		// 写入字段值
		if err := txn.Set(hkey, bValue); err != nil {
			return err
		}

		// 更新计数器
		countKey := s.hashCountKey(key)
		var currentCount uint64
		countItem, err := txn.Get(countKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			currentCount = 0
		} else {
			val, err := countItem.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("HSet: failed to get count value: %v", err)
			}
			currentCount = helper.BytesToUint64(val)
		}

		if !exists {
			currentCount++
		}

		return txn.Set(countKey, helper.Uint64ToBytes(currentCount))
	})
}
func (s *BoltreonStore) HGet(key, field string) ([]byte, error) {
	hkey := s.hashKey(key, field)
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(hkey)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return val, err
}

func (s *BoltreonStore) hashKey(key, field string) []byte {
	return []byte(fmt.Sprintf("%s:%s:%s", KeyTypeHash, key, field))
}

// hashCountKey 方法用于生成哈希表计数器键
func (s *BoltreonStore) hashCountKey(key string) []byte {
	return []byte(fmt.Sprintf("%s:%s:count", KeyTypeHash, key))
}

// HDel 实现 Redis HDEL 命令
func (s *BoltreonStore) HDel(key string, fields ...string) (int, error) {
	deletedCount := 0
	return 1, s.db.Update(func(txn *badger.Txn) error {
		countKey := s.hashCountKey(key)
		var currentCount uint64
		countItem, err := txn.Get(countKey)
		if err == nil {
			val, err := countItem.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("HDel: failed to get count value: %v", err)
			}
			currentCount = helper.BytesToUint64(val)
		}

		for _, field := range fields {
			hkey := s.hashKey(key, field)
			// 检查是否存在
			_, err := txn.Get(hkey)
			if err == nil {
				// 存在则删除
				if err := txn.Delete(hkey); err != nil {
					return err
				}
				deletedCount++
				currentCount--
			}
		}

		// 更新计数器（即使计数器为0也要保留）
		if deletedCount > 0 {
			return txn.Set(countKey, helper.Uint64ToBytes(currentCount))
		}
		return nil
	})
}

// HLen 实现 Redis HLEN 命令
func (s *BoltreonStore) HLen(key string) (uint64, error) {
	var count uint64
	err := s.db.View(func(txn *badger.Txn) error {
		countKey := s.hashCountKey(key)
		item, err := txn.Get(countKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			count = 0
		} else if err != nil {
			return err
		} else {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("HLen: failed to get count value: %v", err)
			}
			count = helper.BytesToUint64(val)
		}
		return nil
	})
	return count, err
}

// HGetAll 实现 Redis HGETALL 命令
func (s *BoltreonStore) HGetAll(key string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	prefix := fmt.Sprintf("%s:%s:", KeyTypeHash, key)
	err := s.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		prefixBytes := []byte(prefix)
		for iter.Seek(prefixBytes); iter.Valid(); iter.Next() {
			k := iter.Item().Key()
			kStr := string(k)
			if !strings.HasPrefix(kStr, prefix) {
				break
			}
			// 提取字段名
			_, field := splitHashKey(k)
			val, _ := iter.Item().ValueCopy(nil)
			result[field] = val
		}
		return nil
	})
	return result, err
}

// splitHashKey 从哈希键中解析出字段名
func splitHashKey(key []byte) (string, string) {
	parts := strings.SplitN(string(key), ":", 3)
	if len(parts) >= 3 {
		return parts[1], parts[2]
	}
	return "", ""
}

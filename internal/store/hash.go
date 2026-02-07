package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/Boltreon/internal/helper"

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
	// 将值转换为字符串（与Redis一致，Hash值都是字符串）
	var bValue []byte
	switch v := value.(type) {
	case string:
		bValue = []byte(v)
	case []byte:
		bValue = v
	case int, int8, int16, int32, int64:
		bValue = []byte(fmt.Sprintf("%d", v))
	case uint, uint8, uint16, uint32, uint64:
		bValue = []byte(fmt.Sprintf("%d", v))
	case float32, float64:
		bValue = []byte(fmt.Sprintf("%g", v))
	case bool:
		if v {
			bValue = []byte("1")
		} else {
			bValue = []byte("0")
		}
	default:
		// 对于其他类型，使用gob编码（向后兼容）
		var err error
		bValue, err = helper.InterfaceToBytes(value)
		if err != nil {
			return fmt.Errorf("%s,%v", logFuncTag, err)
		}
	}
	hkey := s.hashKey(key, field)
	typeKey := TypeOfKeyGet(key)

	// 先在View事务中获取当前计数
	var currentCount uint64
	err := s.db.View(func(txn *badger.Txn) error {
		countKey := s.hashCountKey(key)
		countItem, err := txn.Get(countKey)
		if err == nil {
			val, err := countItem.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("HSet: failed to get count value: %v", err)
			}
			currentCount = helper.BytesToUint64(val)
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 然后在Update事务中写入
	return s.db.Update(func(txn *badger.Txn) error {
		// 检查字段是否存在
		exists := false
		if _, err := txn.Get(hkey); err == nil {
			exists = true
		}

		if err := txn.Set(typeKey, []byte(KeyTypeHash)); err != nil {
			return err
		}

		// 写入字段值（带压缩）
		if err := s.setValueWithCompression(txn, hkey, bValue); err != nil {
			return err
		}

		// 更新计数器
		countKey := s.hashCountKey(key)
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
		val, err = s.getValueWithDecompression(item)
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
	err := s.db.Update(func(txn *badger.Txn) error {
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
				if currentCount > 0 {
					currentCount--
				}
			}
		}

		// 更新计数器（即使计数器为0也要保留）
		if deletedCount > 0 {
			return txn.Set(countKey, helper.Uint64ToBytes(currentCount))
		}
		return nil
	})
	return deletedCount, err
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
			if field == "count" {
				continue
			}
			item := iter.Item()
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			result[field] = val
		}
		return nil
	})
	return result, err
}

// splitHashKey 从哈希键中解析出key和field
// 键格式: HASH:key:field
// 例如: HASH:user:1:name -> key="user:1", field="name"
func splitHashKey(key []byte) (string, string) {
	keyStr := string(key)
	// 去掉前缀 "HASH:"
	prefix := KeyTypeHash + ":"
	if !strings.HasPrefix(keyStr, prefix) {
		return "", ""
	}
	// 获取剩余部分: "user:1:name"
	remainder := keyStr[len(prefix):]
	// 找到最后一个冒号，前面是key，后面是field
	lastColon := strings.LastIndex(remainder, ":")
	if lastColon == -1 {
		return "", ""
	}
	hashKey := remainder[:lastColon]
	field := remainder[lastColon+1:]
	return hashKey, field
}

// getAllHashFields 获取哈希表中的所有字段
func (s *BoltreonStore) getAllHashFields(txn *badger.Txn, key string) ([]string, error) {
	var fields []string
	prefix := fmt.Sprintf("%s:%s:", KeyTypeHash, key)
	prefixBytes := []byte(prefix)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	for iter.Seek(prefixBytes); iter.Valid(); iter.Next() {
		k := iter.Item().Key()
		kStr := string(k)
		if !strings.HasPrefix(kStr, prefix) {
			break
		}
		_, field := splitHashKey(k)
		if field != "count" {
			fields = append(fields, field)
		}
	}
	return fields, nil
}

// HExists 实现 Redis HEXISTS 命令，检查字段是否存在
func (s *BoltreonStore) HExists(key, field string) (bool, error) {
	exists := false
	err := s.db.View(func(txn *badger.Txn) error {
		hkey := s.hashKey(key, field)
		_, err := txn.Get(hkey)
		if err == nil {
			exists = true
			return nil
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return err
	})
	return exists, err
}

// HKeys 实现 Redis HKEYS 命令，获取所有字段名
func (s *BoltreonStore) HKeys(key string) ([]string, error) {
	var fields []string
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		fields, err = s.getAllHashFields(txn, key)
		return err
	})
	return fields, err
}

// HVals 实现 Redis HVALS 命令，获取所有字段值
func (s *BoltreonStore) HVals(key string) ([][]byte, error) {
	var values [][]byte
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
			_, field := splitHashKey(k)
			if field == "count" {
				continue
			}
			item := iter.Item()
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			values = append(values, val)
		}
		return nil
	})
	return values, err
}

// HMSet 实现 Redis HMSET 命令，批量设置多个字段
func (s *BoltreonStore) HMSet(key string, fieldValues map[string]interface{}) error {
	typeKey := TypeOfKeyGet(key)
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(typeKey, []byte(KeyTypeHash)); err != nil {
			return err
		}

		countKey := s.hashCountKey(key)
		var currentCount uint64
		countItem, err := txn.Get(countKey)
		if err == nil {
			val, err := countItem.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("HMSet: failed to get count value: %v", err)
			}
			currentCount = helper.BytesToUint64(val)
		}

		newFields := 0
		for field, value := range fieldValues {
			// 将值转换为字符串（与Redis一致）
			var bValue []byte
			switch v := value.(type) {
			case string:
				bValue = []byte(v)
			case []byte:
				bValue = v
			case int, int8, int16, int32, int64:
				bValue = []byte(fmt.Sprintf("%d", v))
			case uint, uint8, uint16, uint32, uint64:
				bValue = []byte(fmt.Sprintf("%d", v))
			case float32, float64:
				bValue = []byte(fmt.Sprintf("%g", v))
			case bool:
				if v {
					bValue = []byte("1")
				} else {
					bValue = []byte("0")
				}
			default:
				// 对于其他类型，使用gob编码（向后兼容）
				var err error
				bValue, err = helper.InterfaceToBytes(value)
				if err != nil {
					return fmt.Errorf("HMSet: failed to convert value for field %s: %v", field, err)
				}
			}
			hkey := s.hashKey(key, field)

			// 检查字段是否存在
			exists := false
			if _, err := txn.Get(hkey); err == nil {
				exists = true
			}

			// 写入字段值（带压缩）
			if err := s.setValueWithCompression(txn, hkey, bValue); err != nil {
				return err
			}

			if !exists {
				newFields++
			}
		}

		// 更新计数器
		if newFields > 0 {
			currentCount += uint64(newFields)
			return txn.Set(countKey, helper.Uint64ToBytes(currentCount))
		}
		return nil
	})
}

// HMGet 实现 Redis HMGET 命令，批量获取多个字段值
func (s *BoltreonStore) HMGet(key string, fields ...string) ([][]byte, error) {
	values := make([][]byte, len(fields))
	err := s.db.View(func(txn *badger.Txn) error {
		for i, field := range fields {
			hkey := s.hashKey(key, field)
			item, err := txn.Get(hkey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				values[i] = nil
				continue
			}
			if err != nil {
				return err
			}
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			values[i] = val
		}
		return nil
	})
	return values, err
}

// HSetNX 实现 Redis HSETNX 命令，仅当字段不存在时设置
func (s *BoltreonStore) HSetNX(key, field string, value interface{}) (bool, error) {
	success := false
	typeKey := TypeOfKeyGet(key)
	// 将值转换为字符串（与Redis一致）
	var bValue []byte
	switch v := value.(type) {
	case string:
		bValue = []byte(v)
	case []byte:
		bValue = v
	case int, int8, int16, int32, int64:
		bValue = []byte(fmt.Sprintf("%d", v))
	case uint, uint8, uint16, uint32, uint64:
		bValue = []byte(fmt.Sprintf("%d", v))
	case float32, float64:
		bValue = []byte(fmt.Sprintf("%g", v))
	case bool:
		if v {
			bValue = []byte("1")
		} else {
			bValue = []byte("0")
		}
	default:
		// 对于其他类型，使用gob编码（向后兼容）
		var err error
		bValue, err = helper.InterfaceToBytes(value)
		if err != nil {
			return false, fmt.Errorf("HSetNX: failed to convert value: %v", err)
		}
	}
	hkey := s.hashKey(key, field)
	err := s.db.Update(func(txn *badger.Txn) error {
		// 检查字段是否存在
		_, getErr := txn.Get(hkey)
		if getErr == nil {
			// 字段已存在，不设置
			return nil
		}
		if !errors.Is(getErr, badger.ErrKeyNotFound) {
			return getErr
		}

		// 字段不存在，设置它（带压缩）
		if err := txn.Set(typeKey, []byte(KeyTypeHash)); err != nil {
			return err
		}
		if err := s.setValueWithCompression(txn, hkey, bValue); err != nil {
			return err
		}

		// 更新计数器
		countKey := s.hashCountKey(key)
		var currentCount uint64
		countItem, err := txn.Get(countKey)
		if err == nil {
			val, err := countItem.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("HSetNX: failed to get count value: %v", err)
			}
			currentCount = helper.BytesToUint64(val)
		}
		currentCount++
		if err := txn.Set(countKey, helper.Uint64ToBytes(currentCount)); err != nil {
			return err
		}

		success = true
		return nil
	})
	return success, err
}

// HIncrBy 实现 Redis HINCRBY 命令，将字段值增加整数
func (s *BoltreonStore) HIncrBy(key, field string, increment int64) (int64, error) {
	var result int64
	typeKey := TypeOfKeyGet(key)
	err := s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(typeKey, []byte(KeyTypeHash)); err != nil {
			return err
		}

		hkey := s.hashKey(key, field)
		var currentValue int64
		fieldExists := false

		// 获取当前值
		item, err := txn.Get(hkey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			currentValue = 0
		} else if err != nil {
			return err
		} else {
			fieldExists = true
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			// 尝试解析为整数（支持字符串格式的整数）
			intVal, err := strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				return fmt.Errorf("HIncrBy: value is not an integer or out of range")
			}
			currentValue = intVal
		}

		// 计算新值
		result = currentValue + increment

		// 保存新值（存储为字符串，与Redis一致，带压缩）
		bValue := []byte(strconv.FormatInt(result, 10))
		if err := s.setValueWithCompression(txn, hkey, bValue); err != nil {
			return err
		}

		// 更新计数器（如果字段是新创建的）
		if !fieldExists {
			countKey := s.hashCountKey(key)
			var currentCount uint64
			countItem, err := txn.Get(countKey)
			if err == nil {
				val, err := countItem.ValueCopy(nil)
				if err != nil {
					return fmt.Errorf("HIncrBy: failed to get count value: %v", err)
				}
				currentCount = helper.BytesToUint64(val)
			}
			currentCount++
			if err := txn.Set(countKey, helper.Uint64ToBytes(currentCount)); err != nil {
				return err
			}
		}

		return nil
	})
	return result, err
}

// HIncrByFloat 实现 Redis HINCRBYFLOAT 命令，将字段值增加浮点数
func (s *BoltreonStore) HIncrByFloat(key, field string, increment float64) (float64, error) {
	var result float64
	typeKey := TypeOfKeyGet(key)
	err := s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(typeKey, []byte(KeyTypeHash)); err != nil {
			return err
		}

		hkey := s.hashKey(key, field)
		var currentValue float64
		fieldExists := false

		// 获取当前值
		item, err := txn.Get(hkey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			currentValue = 0
		} else if err != nil {
			return err
		} else {
			fieldExists = true
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			// 尝试解析为浮点数（支持字符串格式的浮点数）
			floatVal, err := strconv.ParseFloat(string(val), 64)
			if err != nil {
				return fmt.Errorf("HIncrByFloat: value is not a valid float")
			}
			currentValue = floatVal
		}

		// 计算新值
		result = currentValue + increment

		// 保存新值（存储为字符串，与Redis一致，带压缩）
		bValue := []byte(strconv.FormatFloat(result, 'f', -1, 64))
		if err := s.setValueWithCompression(txn, hkey, bValue); err != nil {
			return err
		}

		// 更新计数器（如果字段是新创建的）
		if !fieldExists {
			countKey := s.hashCountKey(key)
			var currentCount uint64
			countItem, err := txn.Get(countKey)
			if err == nil {
				val, err := countItem.ValueCopy(nil)
				if err != nil {
					return fmt.Errorf("HIncrByFloat: failed to get count value: %v", err)
				}
				currentCount = helper.BytesToUint64(val)
			}
			currentCount++
			if err := txn.Set(countKey, helper.Uint64ToBytes(currentCount)); err != nil {
				return err
			}
		}

		return nil
	})
	return result, err
}

// HStrLen 实现 Redis HSTRLEN 命令，获取字段值的字符串长度
func (s *BoltreonStore) HStrLen(key, field string) (int, error) {
	var length int
	err := s.db.View(func(txn *badger.Txn) error {
		hkey := s.hashKey(key, field)
		item, err := txn.Get(hkey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			length = 0
			return nil
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		length = len(val)
		return nil
	})
	return length, err
}

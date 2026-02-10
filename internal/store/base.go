package store

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func (s *BotreonStore) Del(key string) error {
	typeKey := TypeOfKeyGet(key)
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(valCopy)
		
		// 清除读缓存
		if s.readCache != nil {
			s.readCache.Delete(key)
		}

		switch keyType {
		case KeyTypeString:
			if err := txn.Delete(typeKey); err != nil {
				return err
			}
			return txn.Delete([]byte(s.stringKey(key)))
		case KeyTypeList:
			if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeList, key))); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeHash:
			if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeHash, key))); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeSet:
			if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeSet, key))); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeSortedSet:
			// SortedSet的键格式是: zset:key:meta, zset:key:data:member, zset:key:index:...
			if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, key))); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		default:
			return txn.Delete(typeKey)
		}
	})
}

func (s *BotreonStore) DelString(key string) error {
	logFuncTag := "BotreonStoreDelString"
	bKey := []byte(key)
	badgerTypeKey := TypeOfKeyGet(key)
	badgerValueKey := s.stringKey(string(bKey))
	
	// 清除读缓存
	if s.readCache != nil {
		s.readCache.Delete(key)
	}
	
	return s.db.Update(func(txn *badger.Txn) error {
		errDel := txn.Delete(badgerTypeKey)
		if errDel != nil {
			return fmt.Errorf("%s,Del Badger Type Key:%v", logFuncTag, errDel)
		}
		errDel = txn.Delete([]byte(badgerValueKey))
		if errDel != nil {
			return fmt.Errorf("%s,Del Badger Value Key:%v", logFuncTag, errDel)
		}
		return nil
	})
}

func deleteByPrefix(txn *badger.Txn, prefix []byte) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	iter := txn.NewIterator(opts)
	defer iter.Close()

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		if err := txn.Delete(iter.Item().KeyCopy(nil)); err != nil {
			return err
		}
	}
	return nil
}

// getKeyValueKey 根据键类型获取值键
func (s *BotreonStore) getKeyValueKey(key string, keyType string) ([]byte, error) {
	switch keyType {
	case KeyTypeString:
		return []byte(s.stringKey(key)), nil
	case KeyTypeList:
		// List的主键是length键
		return []byte(s.listKey(key, "length")), nil
	case KeyTypeHash:
		// Hash的主键是count键
		return []byte(fmt.Sprintf("%s:%s:count", KeyTypeHash, key)), nil
	case KeyTypeSet:
		// Set的主键是count键
		return []byte(s.setKey(key, "count")), nil
	case KeyTypeSortedSet:
		// SortedSet的主键是meta键
		return sortedSetKeyMeta(key), nil
	default:
		return nil, fmt.Errorf("unknown key type: %s", keyType)
	}
}

// EXISTS 实现 Redis EXISTS 命令，检查键是否存在
func (s *BotreonStore) Exists(key string) (bool, error) {
	exists := false
	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
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

// Type 实现 Redis TYPE 命令，返回键的类型
func (s *BotreonStore) Type(key string) (string, error) {
	var keyType string
	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			keyType = "none"
			return nil
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType = string(val)
		// 将内部类型转换为Redis类型
		switch keyType {
		case KeyTypeString:
			keyType = "string"
		case KeyTypeList:
			keyType = "list"
		case KeyTypeHash:
			keyType = "hash"
		case KeyTypeSet:
			keyType = "set"
		case KeyTypeSortedSet:
			keyType = "zset"
		default:
			keyType = "none"
		}
		return nil
	})
	return keyType, err
}

// EXPIRE 实现 Redis EXPIRE 命令，设置键的过期时间（秒）
func (s *BotreonStore) Expire(key string, seconds int) (bool, error) {
	success := false
	err := s.db.Update(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在，返回false
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)

		// 获取值键并设置TTL
		valueKey, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}

		// 获取当前值
		valueItem, err := txn.Get(valueKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		valBytes, err := valueItem.ValueCopy(nil)
		if err != nil {
			return err
		}

		// 设置新TTL
		e := badger.NewEntry(valueKey, valBytes).WithTTL(time.Duration(seconds) * time.Second)
		if err := txn.SetEntry(e); err != nil {
			return err
		}

		success = true
		return nil
	})
	return success, err
}

// EXPIREAT 实现 Redis EXPIREAT 命令，设置键的过期时间（Unix时间戳，秒）
func (s *BotreonStore) ExpireAt(key string, timestamp int64) (bool, error) {
	now := time.Now().Unix()
	ttl := timestamp - now
	if ttl <= 0 {
		// 时间戳已过期，删除键
		return false, s.Del(key)
	}
	return s.Expire(key, int(ttl))
}

// PEXPIRE 实现 Redis PEXPIRE 命令，设置键的过期时间（毫秒）
func (s *BotreonStore) PExpire(key string, milliseconds int64) (bool, error) {
	success := false
	err := s.db.Update(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在，返回false
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)

		// 获取值键并设置TTL
		valueKey, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}

		// 获取当前值
		valueItem, err := txn.Get(valueKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		valBytes, err := valueItem.ValueCopy(nil)
		if err != nil {
			return err
		}

		// 设置新TTL
		e := badger.NewEntry(valueKey, valBytes).WithTTL(time.Duration(milliseconds) * time.Millisecond)
		if err := txn.SetEntry(e); err != nil {
			return err
		}

		success = true
		return nil
	})
	return success, err
}

// PEXPIREAT 实现 Redis PEXPIREAT 命令，设置键的过期时间（Unix时间戳，毫秒）
func (s *BotreonStore) PExpireAt(key string, timestampMillis int64) (bool, error) {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	ttl := timestampMillis - now
	if ttl <= 0 {
		// 时间戳已过期，删除键
		return false, s.Del(key)
	}
	return s.PExpire(key, ttl)
}

// TTL 实现 Redis TTL 命令，获取键的剩余生存时间（秒）
func (s *BotreonStore) TTL(key string) (int64, error) {
	var ttl int64 = -2 // -2表示键不存在
	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在，返回-2
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)

		// 获取值键
		valueKey, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}

		// 获取值键的TTL
		valueItem, err := txn.Get(valueKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			ttl = -2
			return nil
		}
		if err != nil {
			return err
		}

		expiresAt := valueItem.ExpiresAt()
		if expiresAt == 0 {
			ttl = -1 // -1表示键存在但没有设置过期时间
			return nil
		}

		now := time.Now().Unix()
		// #nosec G115 - expiresAt is a valid Unix timestamp within int64 range
		ttl = int64(expiresAt) - now
		if ttl < 0 {
			ttl = -2 // 已过期
		}
		return nil
	})
	return ttl, err
}

// PTTL 实现 Redis PTTL 命令，获取键的剩余生存时间（毫秒）
func (s *BotreonStore) PTTL(key string) (int64, error) {
	var ttl int64 = -2 // -2表示键不存在
	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 键不存在，返回-2
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)

		// 获取值键
		valueKey, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}

		// 获取值键的TTL
		valueItem, err := txn.Get(valueKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			ttl = -2
			return nil
		}
		if err != nil {
			return err
		}

		expiresAt := valueItem.ExpiresAt()
		if expiresAt == 0 {
			ttl = -1 // -1表示键存在但没有设置过期时间
			return nil
		}

		now := time.Now().UnixNano() / int64(time.Millisecond)
		// #nosec G115 - expiresAt is a valid Unix timestamp within int64 range
		ttl = (int64(expiresAt) * 1000) - now
		if ttl < 0 {
			ttl = -2 // 已过期
		}
		return nil
	})
	return ttl, err
}

// PERSIST 实现 Redis PERSIST 命令，移除键的过期时间
func (s *BotreonStore) Persist(key string) (bool, error) {
	// 先读取键的类型和值键（在 View 事务中）
	var valueKey []byte
	var hasTTL bool
	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound // 键不存在
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)

		// 获取值键
		vk, err := s.getKeyValueKey(key, keyType)
		if err != nil {
			return err
		}
		valueKey = vk

		// 检查是否有TTL
		valueItem, err := txn.Get(valueKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}
		hasTTL = valueItem.ExpiresAt() != 0
		return nil
	})
	if err != nil {
		return false, nil // 键不存在或没有TTL
	}
	if !hasTTL {
		return false, nil
	}

	// 重新读取值并写入无TTL副本（在 Update 事务中）
	var valBytes []byte
	err = s.db.Update(func(txn *badger.Txn) error {
		valueItem, err := txn.Get(valueKey)
		if err != nil {
			return err
		}
		valBytes, err = valueItem.ValueCopy(nil)
		return err
	})
	if err != nil {
		return false, err
	}

	// 写入无TTL的值
	return true, s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(valueKey, valBytes)
	})
}

// RENAME 实现 Redis RENAME 命令，重命名键
func (s *BotreonStore) Rename(key, newKey string) error {
	// 清除读缓存
	if s.readCache != nil {
		s.readCache.Delete(key)
		s.readCache.Delete(newKey)
	}
	return s.db.Update(func(txn *badger.Txn) error {
		// 检查旧键是否存在
		typeKey := TypeOfKeyGet(key)
		item, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("no such key")
		}
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyType := string(val)

		// 如果新键存在，先删除它（在同一事务中）
		newTypeKey := TypeOfKeyGet(newKey)
		newItem, err := txn.Get(newTypeKey)
		if err == nil {
			// 新键存在，需要删除
			newVal, err := newItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			newKeyType := string(newVal)
			// 删除新键的所有相关数据
			switch newKeyType {
			case KeyTypeString:
				_ = txn.Delete(newTypeKey)
				_ = txn.Delete([]byte(s.stringKey(newKey)))
			case KeyTypeList:
				if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeList, newKey))); err != nil {
					return err
				}
				_ = txn.Delete(newTypeKey)
			case KeyTypeHash:
				if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeHash, newKey))); err != nil {
					return err
				}
				_ = txn.Delete(newTypeKey)
			case KeyTypeSet:
				if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s:%s:", KeyTypeSet, newKey))); err != nil {
					return err
				}
				_ = txn.Delete(newTypeKey)
			case KeyTypeSortedSet:
				if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, newKey))); err != nil {
					return err
				}
				_ = txn.Delete(newTypeKey)
			default:
				_ = txn.Delete(newTypeKey)
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		// 根据类型复制所有相关键
		switch keyType {
		case KeyTypeString:
			oldValueKey := []byte(s.stringKey(key))
			oldValue, err := txn.Get(oldValueKey)
			if err != nil {
				return err
			}
			valueBytes, err := oldValue.ValueCopy(nil)
			if err != nil {
				return err
			}
			// 设置新键
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			newValueKey := []byte(s.stringKey(newKey))
			// 保持TTL
			expiresAt := oldValue.ExpiresAt()
			if expiresAt > 0 {
				// #nosec G115 - expiresAt is a valid Unix timestamp within int64 range
				ttl := time.Until(time.Unix(int64(expiresAt), 0))
				if ttl > 0 {
					e := badger.NewEntry(newValueKey, valueBytes).WithTTL(ttl)
					if err := txn.SetEntry(e); err != nil {
						return err
					}
				} else {
					// TTL已过期，不设置
					if err := txn.Set(newValueKey, valueBytes); err != nil {
						return err
					}
				}
			} else {
				if err := txn.Set(newValueKey, valueBytes); err != nil {
					return err
				}
			}
			// 删除旧键
			_ = txn.Delete(typeKey)
			_ = txn.Delete(oldValueKey)
			return nil
		case KeyTypeList:
			// 复制所有LIST键
			prefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeList, key))
			if err := copyKeysByPrefix(txn, prefix, key, newKey, KeyTypeList); err != nil {
				return err
			}
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			// 删除旧键
			if err := deleteByPrefix(txn, prefix); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeHash:
			prefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeHash, key))
			if err := copyKeysByPrefix(txn, prefix, key, newKey, KeyTypeHash); err != nil {
				return err
			}
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			// 删除旧键
			if err := deleteByPrefix(txn, prefix); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeSet:
			prefix := []byte(fmt.Sprintf("%s:%s:", KeyTypeSet, key))
			if err := copyKeysByPrefix(txn, prefix, key, newKey, KeyTypeSet); err != nil {
				return err
			}
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			// 删除旧键
			if err := deleteByPrefix(txn, prefix); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		case KeyTypeSortedSet:
			prefix := []byte(fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, key))
			if err := copyKeysByPrefix(txn, prefix, key, newKey, KeyTypeSortedSet); err != nil {
				return err
			}
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			// 删除旧键
			if err := deleteByPrefix(txn, prefix); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		default:
			if err := txn.Set(newTypeKey, []byte(keyType)); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		}
	})
}

// copyKeysByPrefix 复制所有匹配前缀的键
func copyKeysByPrefix(txn *badger.Txn, oldPrefix []byte, oldKey, newKey, keyType string) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	iter := txn.NewIterator(opts)
	defer iter.Close()

	for iter.Seek(oldPrefix); iter.ValidForPrefix(oldPrefix); iter.Next() {
		item := iter.Item()
		oldKeyBytes := item.KeyCopy(nil)
		oldKeyStr := string(oldKeyBytes)

		// 生成新键
		var newKeyStr string
		if keyType == KeyTypeSortedSet {
			// SortedSet使用zset:前缀，格式是zset:oldKey:...
			// 需要替换为zset:newKey:...
			oldKeyPrefix := fmt.Sprintf("%s%s:", prefixKeySortedSetBytes, oldKey)
			newKeyStr = fmt.Sprintf("%s%s:%s", prefixKeySortedSetBytes, newKey, oldKeyStr[len(oldKeyPrefix):])
		} else {
			// 其他类型使用TYPE:oldKey:...格式
			// 需要替换为TYPE:newKey:...
			oldKeyPrefix := fmt.Sprintf("%s:%s:", keyType, oldKey)
			newKeyStr = fmt.Sprintf("%s:%s:%s", keyType, newKey, oldKeyStr[len(oldKeyPrefix):])
		}

		// 复制值
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		// 设置新键（保持TTL）
		expiresAt := item.ExpiresAt()
		if expiresAt > 0 {
			// #nosec G115 - expiresAt is a valid Unix timestamp within int64 range
			ttl := time.Until(time.Unix(int64(expiresAt), 0))
			if ttl > 0 {
				e := badger.NewEntry([]byte(newKeyStr), val).WithTTL(ttl)
				if err := txn.SetEntry(e); err != nil {
					return err
				}
			} else {
				// TTL已过期，跳过
				if err := txn.Set([]byte(newKeyStr), val); err != nil {
					return err
				}
			}
		} else {
			if err := txn.Set([]byte(newKeyStr), val); err != nil {
				return err
			}
		}
	}
	return nil
}

// RENAMENX 实现 Redis RENAMENX 命令，仅当新键不存在时重命名
func (s *BotreonStore) RenameNX(key, newKey string) (bool, error) {
	success := false
	err := s.db.Update(func(txn *badger.Txn) error {
		// 检查新键是否已存在
		newTypeKey := TypeOfKeyGet(newKey)
		_, err := txn.Get(newTypeKey)
		if err == nil {
			// 新键已存在，返回false
			return nil
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		// 新键不存在，执行重命名
		if err := s.Rename(key, newKey); err != nil {
			return err
		}
		success = true
		return nil
	})
	return success, err
}

// matchPattern 检查键是否匹配模式（支持*和?通配符）
func matchPattern(key, pattern string) bool {
	// 简单的通配符匹配实现
	if pattern == "*" {
		return true
	}
	// 使用简单的字符串匹配（可以改进为正则表达式）
	keyRunes := []rune(key)
	patternRunes := []rune(pattern)

	keyIdx := 0
	patternIdx := 0
	keyStar := -1
	patternStar := -1

	for keyIdx < len(keyRunes) || patternIdx < len(patternRunes) {
		if patternIdx < len(patternRunes) && patternRunes[patternIdx] == '*' {
			keyStar = keyIdx
			patternStar = patternIdx
			patternIdx++
			continue
		}
		if keyIdx < len(keyRunes) && patternIdx < len(patternRunes) &&
			(patternRunes[patternIdx] == '?' || patternRunes[patternIdx] == keyRunes[keyIdx]) {
			keyIdx++
			patternIdx++
			continue
		}
		if keyStar >= 0 {
			keyStar++
			keyIdx = keyStar
			patternIdx = patternStar + 1
			continue
		}
		return false
	}

	// 处理pattern末尾的*
	for patternIdx < len(patternRunes) && patternRunes[patternIdx] == '*' {
		patternIdx++
	}

	return patternIdx == len(patternRunes)
}

// Keys 实现 Redis KEYS 命令，查找所有匹配给定模式的键
func (s *BotreonStore) Keys(pattern string) ([]string, error) {
	var keys []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		// 查找所有TYPE_前缀的键
		prefix := prefixKeyTypeBytes
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			// 提取实际键名（去掉TYPE_前缀）
			key := string(keyBytes[len(prefixKeyTypeBytes):])
			if matchPattern(key, pattern) {
				keys = append(keys, key)
			}
		}
		return nil
	})
	return keys, err
}

// ScanResult 表示SCAN命令的返回结果
type ScanResult struct {
	Cursor uint64
	Keys   []string
}

// Scan 实现 Redis SCAN 命令，增量迭代键空间
func (s *BotreonStore) Scan(cursor uint64, pattern string, count int) (ScanResult, error) {
	var result ScanResult
	result.Cursor = 0
	result.Keys = []string{}

	if count <= 0 {
		count = 10 // 默认值
	}

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := prefixKeyTypeBytes
		currentPos := uint64(0)
		collected := 0

		// 如果cursor不为0，需要跳过前面的键
		if cursor > 0 {
			// 简单实现：从头开始迭代，跳过cursor个键
			for iter.Seek(prefix); iter.ValidForPrefix(prefix) && currentPos < cursor; iter.Next() {
				currentPos++
			}
		} else {
			iter.Seek(prefix)
		}

		// 收集匹配的键
		for iter.ValidForPrefix(prefix) && collected < count {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			key := string(keyBytes[len(prefixKeyTypeBytes):])

			if pattern == "" || pattern == "*" || matchPattern(key, pattern) {
				result.Keys = append(result.Keys, key)
				collected++
			}

			currentPos++
			iter.Next()
		}

		// 检查是否还有更多键
		if iter.ValidForPrefix(prefix) {
			result.Cursor = currentPos
		} else {
			result.Cursor = 0 // 0表示迭代完成
		}

		return nil
	})
	return result, err
}

// RandomKey 实现 Redis RANDOMKEY 命令，随机返回一个键
func (s *BotreonStore) RandomKey() (string, error) {
	var key string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := prefixKeyTypeBytes

		// 先计算总数
		count := 0
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			count++
		}

		if count == 0 {
			return nil // 没有键
		}

		// 随机选择一个位置
		randPos := 0
		if count > 1 {
			// 使用简单的伪随机（实际应该使用crypto/rand）
			randPos = int(time.Now().UnixNano() % int64(count))
		}

		// 迭代到随机位置
		iter.Rewind()
		current := 0
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			if current == randPos {
				item := iter.Item()
				keyBytes := item.KeyCopy(nil)
				key = string(keyBytes[len(prefixKeyTypeBytes):])
				return nil
			}
			current++
		}

		return nil
	})
	return key, err
}

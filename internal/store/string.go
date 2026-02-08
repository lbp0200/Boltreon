package store

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// ErrKeyNotFound 表示键不存在
var ErrKeyNotFound = errors.New("key not found")

// stringKey 方法用于生成存储在 Badger 数据库中的键
func (s *BotreonStore) stringKey(key string) string {
	return fmt.Sprintf("%s:%s", KeyTypeString, key)
}

// Set 实现 Redis SET 命令
func (s *BotreonStore) Set(key string, value string) error {
	// 先更新写缓存
	if s.writeCache != nil {
		s.writeCache.Set(key, []byte(value))
	}
	// 同时更新读缓存（避免后续读取时缓存未命中）
	if s.readCache != nil {
		s.readCache.Set(key, []byte(value))
	}

	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
			return err
		}
		strKey := s.stringKey(key)
		return s.setValueWithCompression(txn, []byte(strKey), []byte(value))
	})
}

// SetWithTTL 字符串操作，设置键值对并设置过期时间
func (s *BotreonStore) SetWithTTL(key, value string, ttl time.Duration) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
			return err
		}
		strKey := s.stringKey(key)
		return s.setEntryWithCompression(txn, []byte(strKey), []byte(value), ttl)
	})
}

// SetEX 实现 Redis SETEX 命令，设置键值对并设置过期时间（秒）
func (s *BotreonStore) SetEX(key string, value string, seconds int) error {
	return s.SetWithTTL(key, value, time.Duration(seconds)*time.Second)
}

// PSETEX 实现 Redis PSETEX 命令，设置键值对并设置过期时间（毫秒）
func (s *BotreonStore) PSETEX(key string, value string, milliseconds int64) error {
	return s.SetWithTTL(key, value, time.Duration(milliseconds)*time.Millisecond)
}

// SetNX 实现 Redis SETNX 命令，仅当键不存在时设置
func (s *BotreonStore) SetNX(key string, value string) (bool, error) {
	success := false
	err := s.db.Update(func(txn *badger.Txn) error {
		strKey := s.stringKey(key)
		_, err := txn.Get([]byte(strKey))
		if err == nil {
			// 键已存在
			return nil
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		// 键不存在，可以设置
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
			return err
		}
		if err := s.setValueWithCompression(txn, []byte(strKey), []byte(value)); err != nil {
			return err
		}
		success = true
		return nil
	})
	return success, err
}

// GetSet 实现 Redis GETSET 命令，设置新值并返回旧值
func (s *BotreonStore) GetSet(key string, value string) (string, error) {
	// 先读取旧值（在 View 事务中）
	var oldValue string
	err := s.db.View(func(txn *badger.Txn) error {
		strKey := s.stringKey(key)
		item, err := txn.Get([]byte(strKey))
		if err == nil {
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			oldValue = string(val)
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	// 然后写入新值（在单独的 Update 事务中）
	return oldValue, s.Set(key, value)
}

// MGet 实现 Redis MGET 命令，获取多个键的值
func (s *BotreonStore) MGet(keys ...string) ([]string, error) {
	values := make([]string, len(keys))
	err := s.db.View(func(txn *badger.Txn) error {
		for i, key := range keys {
			strKey := s.stringKey(key)
			item, err := txn.Get([]byte(strKey))
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					values[i] = "" // 键不存在返回空字符串
					continue
				}
				return err
			}
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			values[i] = string(val)
		}
		return nil
	})
	return values, err
}

// MSet 实现 Redis MSET 命令，设置多个键值对
func (s *BotreonStore) MSet(keyValues ...string) error {
	if len(keyValues)%2 != 0 {
		return errors.New("MSET requires an even number of arguments")
	}
	return s.db.Update(func(txn *badger.Txn) error {
		for i := 0; i < len(keyValues); i += 2 {
			key := keyValues[i]
			value := keyValues[i+1]
			if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
				return err
			}
			strKey := s.stringKey(key)
			if err := txn.Set([]byte(strKey), []byte(value)); err != nil {
				return err
			}
		}
		return nil
	})
}

// MSetNX 实现 Redis MSETNX 命令，仅当所有键都不存在时设置多个键值对
func (s *BotreonStore) MSetNX(keyValues ...string) (bool, error) {
	if len(keyValues)%2 != 0 {
		return false, errors.New("MSETNX requires an even number of arguments")
	}
	success := false
	err := s.db.Update(func(txn *badger.Txn) error {
		// 先检查所有键是否都不存在
		for i := 0; i < len(keyValues); i += 2 {
			key := keyValues[i]
			strKey := s.stringKey(key)
			_, err := txn.Get([]byte(strKey))
			if err == nil {
				// 至少有一个键存在，不能设置
				return nil
			}
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		// 所有键都不存在，可以设置
		for i := 0; i < len(keyValues); i += 2 {
			key := keyValues[i]
			value := keyValues[i+1]
			if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
				return err
			}
			strKey := s.stringKey(key)
			if err := txn.Set([]byte(strKey), []byte(value)); err != nil {
				return err
			}
		}
		success = true
		return nil
	})
	return success, err
}

// Get 实现 Redis GET 命令
func (s *BotreonStore) Get(key string) (string, error) {
	// 先检查读缓存
	if s.readCache != nil {
		if cachedValue, found := s.readCache.Get(key); found {
			return string(cachedValue), nil
		}
	}

	var val string
	err := s.db.View(func(txn *badger.Txn) error {
		strKey := s.stringKey(key)
		item, err := txn.Get([]byte(strKey))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrKeyNotFound // 返回特定错误表示键不存在
			}
			return err
		}
		valBytes, err := s.getValueWithDecompression(item)
		if err != nil {
			return err
		}
		val = string(valBytes)
		return nil
	})

	// 如果成功，更新读缓存
	if err == nil && s.readCache != nil {
		s.readCache.Set(key, []byte(val))
	}

	if errors.Is(err, ErrKeyNotFound) {
		return "", ErrKeyNotFound
	}
	return val, err
}

// getIntValue 获取整数值，如果键不存在或不是整数，返回0和错误
func (s *BotreonStore) getIntValue(txn *badger.Txn, key string) (int64, error) {
	strKey := s.stringKey(key)
	item, err := txn.Get([]byte(strKey))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return 0, nil // 键不存在时返回0
		}
		return 0, err
	}
	val, err := s.getValueWithDecompression(item)
	if err != nil {
		return 0, err
	}
	intVal, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not an integer")
	}
	return intVal, nil
}

// setIntValue 设置整数值
func (s *BotreonStore) setIntValue(txn *badger.Txn, key string, value int64) error {
	if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
		return err
	}
	strKey := s.stringKey(key)
	return txn.Set([]byte(strKey), []byte(strconv.FormatInt(value, 10)))
}

// INCR 实现 Redis INCR 命令，将键的值加1
func (s *BotreonStore) INCR(key string) (int64, error) {
	var newValue int64
	err := s.db.Update(func(txn *badger.Txn) error {
		oldValue, err := s.getIntValue(txn, key)
		if err != nil {
			return err
		}
		newValue = oldValue + 1
		return s.setIntValue(txn, key, newValue)
	})
	return newValue, err
}

// INCRBY 实现 Redis INCRBY 命令，将键的值增加指定整数
func (s *BotreonStore) INCRBY(key string, increment int64) (int64, error) {
	var newValue int64
	err := s.db.Update(func(txn *badger.Txn) error {
		oldValue, err := s.getIntValue(txn, key)
		if err != nil {
			return err
		}
		newValue = oldValue + increment
		return s.setIntValue(txn, key, newValue)
	})
	return newValue, err
}

// DECR 实现 Redis DECR 命令，将键的值减1
func (s *BotreonStore) DECR(key string) (int64, error) {
	return s.INCRBY(key, -1)
}

// DECRBY 实现 Redis DECRBY 命令，将键的值减少指定整数
func (s *BotreonStore) DECRBY(key string, decrement int64) (int64, error) {
	return s.INCRBY(key, -decrement)
}

// INCRBYFLOAT 实现 Redis INCRBYFLOAT 命令，将键的值增加指定浮点数
func (s *BotreonStore) INCRBYFLOAT(key string, increment float64) (float64, error) {
	// 先读取旧值（在 View 事务中）
	oldValue, err := s.Get(key)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	oldFloat, _ := strconv.ParseFloat(oldValue, 64)
	newValue := oldFloat + increment
	// 检查溢出
	if math.IsInf(newValue, 0) || math.IsNaN(newValue) {
		return 0, fmt.Errorf("increment would produce NaN or Infinity")
	}
	// 写入新值
	newValueStr := strconv.FormatFloat(newValue, 'f', -1, 64)
	return newValue, s.Set(key, newValueStr)
}

// APPEND 实现 Redis APPEND 命令，追加字符串
func (s *BotreonStore) APPEND(key string, value string) (int, error) {
	// 先读取旧值（在 View 事务中）
	var existingValue string
	err := s.db.View(func(txn *badger.Txn) error {
		strKey := s.stringKey(key)
		item, err := txn.Get([]byte(strKey))
		if err == nil {
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			existingValue = string(val)
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	// 然后写入新值
	newValue := existingValue + value
	newLength := len(newValue)
	return newLength, s.Set(key, newValue)
}

// StrLen 实现 Redis STRLEN 命令，获取字符串长度
func (s *BotreonStore) StrLen(key string) (int, error) {
	var length int
	err := s.db.View(func(txn *badger.Txn) error {
		strKey := s.stringKey(key)
		item, err := txn.Get([]byte(strKey))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				length = 0
				return nil
			}
			return err
		}
		val, err := s.getValueWithDecompression(item)
		if err != nil {
			return err
		}
		length = len(string(val))
		return nil
	})
	return length, err
}

// GetRange 实现 Redis GETRANGE 命令，获取字符串的子串
func (s *BotreonStore) GetRange(key string, start, end int) (string, error) {
	var result string
	err := s.db.View(func(txn *badger.Txn) error {
		strKey := s.stringKey(key)
		item, err := txn.Get([]byte(strKey))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				result = ""
				return nil
			}
			return err
		}
		val, err := s.getValueWithDecompression(item)
		if err != nil {
			return err
		}
		str := string(val)
		strLen := len(str)
		// 处理负数索引
		if start < 0 {
			start = strLen + start
			if start < 0 {
				start = 0
			}
		}
		if end < 0 {
			end = strLen + end
			if end < 0 {
				end = -1
			}
		}
		if start > strLen {
			result = ""
			return nil
		}
		if end >= strLen {
			end = strLen - 1
		}
		if start > end {
			result = ""
			return nil
		}
		result = str[start : end+1]
		return nil
	})
	return result, err
}

// SetRange 实现 Redis SETRANGE 命令，设置字符串的子串
func (s *BotreonStore) SetRange(key string, offset int, value string) (int, error) {
	// 先读取旧值（在 View 事务中）
	var existingValue string
	err := s.db.View(func(txn *badger.Txn) error {
		strKey := s.stringKey(key)
		item, err := txn.Get([]byte(strKey))
		if err == nil {
			val, err := s.getValueWithDecompression(item)
			if err != nil {
				return err
			}
			existingValue = string(val)
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	// 如果offset超出当前长度，用null字节填充
	if offset > len(existingValue) {
		existingValue += string(make([]byte, offset-len(existingValue)))
	}
	// 构建新字符串
	var newValue string
	if offset > 0 {
		newValue = existingValue[:offset]
	}
	newValue += value
	if offset+len(value) < len(existingValue) {
		newValue += existingValue[offset+len(value):]
	}
	newLength := len(newValue)
	// 写入新值
	return newLength, s.Set(key, newValue)
}

// getStringBytes 获取字符串的字节数组
func (s *BotreonStore) getStringBytes(txn *badger.Txn, key string) ([]byte, error) {
	strKey := s.stringKey(key)
	item, err := txn.Get([]byte(strKey))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return []byte{}, nil
		}
		return nil, err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return decompressData(val)
}

// GetBit 实现 Redis GETBIT 命令，获取指定位的值
func (s *BotreonStore) GetBit(key string, offset int) (int, error) {
	var bit int
	err := s.db.View(func(txn *badger.Txn) error {
		data, err := s.getStringBytes(txn, key)
		if err != nil {
			return err
		}
		byteIndex := offset / 8
		bitIndex := offset % 8
		if byteIndex >= len(data) {
			bit = 0
			return nil
		}
		if (data[byteIndex] & (1 << (7 - bitIndex))) != 0 {
			bit = 1
		} else {
			bit = 0
		}
		return nil
	})
	return bit, err
}

// SetBit 实现 Redis SETBIT 命令，设置指定位的值
func (s *BotreonStore) SetBit(key string, offset int, value int) (int, error) {
	// 清除读缓存
	if s.readCache != nil {
		s.readCache.Delete(key)
	}
	var oldBit int
	err := s.db.Update(func(txn *badger.Txn) error {
		data, err := s.getStringBytes(txn, key)
		if err != nil {
			return err
		}
		byteIndex := offset / 8
		bitIndex := offset % 8
		// 扩展数据如果需要
		if byteIndex >= len(data) {
			newData := make([]byte, byteIndex+1)
			copy(newData, data)
			data = newData
		}
		// 获取旧位值
		if (data[byteIndex] & (1 << (7 - bitIndex))) != 0 {
			oldBit = 1
		} else {
			oldBit = 0
		}
		// 设置新位值
		if value == 1 {
			data[byteIndex] |= (1 << (7 - bitIndex))
		} else {
			data[byteIndex] &^= (1 << (7 - bitIndex))
		}
		// 保存（带压缩）
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeString)); err != nil {
			return err
		}
		strKey := s.stringKey(key)
		return s.setValueWithCompression(txn, []byte(strKey), data)
	})
	return oldBit, err
}

// BitCount 实现 Redis BITCOUNT 命令，计算字符串中1的位数
func (s *BotreonStore) BitCount(key string, start, end int) (int, error) {
	var count int
	err := s.db.View(func(txn *badger.Txn) error {
		data, err := s.getStringBytes(txn, key)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			return nil
		}
		// 处理负数索引
		if start < 0 {
			start = len(data) + start
			if start < 0 {
				start = 0
			}
		}
		if end < 0 {
			end = len(data) + end
			if end < 0 {
				end = -1
			}
		}
		if start > len(data) {
			return nil
		}
		if end >= len(data) {
			end = len(data) - 1
		}
		if start > end {
			return nil
		}
		// 计算指定范围内的1的位数
		for i := start; i <= end; i++ {
			for j := 0; j < 8; j++ {
				if (data[i] & (1 << (7 - j))) != 0 {
					count++
				}
			}
		}
		return nil
	})
	return count, err
}

// BitOp 实现 Redis BITOP 命令，位操作
func (s *BotreonStore) BitOp(op string, destKey string, keys ...string) (int, error) {
	// 清除读缓存
	if s.readCache != nil {
		s.readCache.Delete(destKey)
	}
	var resultLength int
	err := s.db.Update(func(txn *badger.Txn) error {
		if len(keys) == 0 {
			return errors.New("BITOP requires at least one source key")
		}
		// 获取所有源键的数据
		sources := make([][]byte, len(keys))
		maxLen := 0
		for i, key := range keys {
			data, err := s.getStringBytes(txn, key)
			if err != nil {
				return err
			}
			sources[i] = data
			if len(data) > maxLen {
				maxLen = len(data)
			}
		}
		if maxLen == 0 {
			// 所有键都为空，结果也为空
			if err := txn.Set(TypeOfKeyGet(destKey), []byte(KeyTypeString)); err != nil {
				return err
			}
			strKey := s.stringKey(destKey)
			return txn.Set([]byte(strKey), []byte{})
		}
		// 执行位操作
		result := make([]byte, maxLen)
		for i := 0; i < maxLen; i++ {
			var byteVal byte
			switch op {
			case "AND":
				byteVal = 0xFF
				for _, src := range sources {
					var b byte
					if i < len(src) {
						b = src[i]
					}
					byteVal &= b
				}
			case "OR":
				for _, src := range sources {
					var b byte
					if i < len(src) {
						b = src[i]
					}
					byteVal |= b
				}
			case "XOR":
				for _, src := range sources {
					var b byte
					if i < len(src) {
						b = src[i]
					}
					byteVal ^= b
				}
			case "NOT":
				if len(keys) != 1 {
					return errors.New("BITOP NOT requires exactly one source key")
				}
				var b byte
				if i < len(sources[0]) {
					b = sources[0][i]
				}
				byteVal = ^b
			default:
				return fmt.Errorf("unknown bitop operation: %s", op)
			}
			result[i] = byteVal
		}
		resultLength = len(result)
		// 保存结果
		if err := txn.Set(TypeOfKeyGet(destKey), []byte(KeyTypeString)); err != nil {
			return err
		}
		strKey := s.stringKey(destKey)
		return txn.Set([]byte(strKey), result)
	})
	return resultLength, err
}

// BitPos 实现 Redis BITPOS 命令，查找第一个设置或清除的位
func (s *BotreonStore) BitPos(key string, bit int, start, end int) (int, error) {
	var pos int = -1
	err := s.db.View(func(txn *badger.Txn) error {
		data, err := s.getStringBytes(txn, key)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			if bit == 1 {
				pos = -1
			} else {
				pos = 0
			}
			return nil
		}
		// 处理负数索引
		if start < 0 {
			start = len(data) + start
			if start < 0 {
				start = 0
			}
		}
		if end < 0 {
			end = len(data) + end
			if end < 0 {
				end = -1
			}
		}
		if start > len(data) {
			pos = -1
			return nil
		}
		if end >= len(data) {
			end = len(data) - 1
		}
		if start > end {
			pos = -1
			return nil
		}
		// 查找指定位
		for i := start; i <= end; i++ {
			for j := 0; j < 8; j++ {
				bitOffset := i*8 + j
				currentBit := 0
				if (data[i] & (1 << (7 - j))) != 0 {
					currentBit = 1
				}
				if currentBit == bit {
					pos = bitOffset
					return nil
				}
			}
		}
		// 未找到
		pos = -1
		return nil
	})
	return pos, err
}

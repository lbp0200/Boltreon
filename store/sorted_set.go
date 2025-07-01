package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/dgraph-io/badger/v4"
)

// ZSetMember 定义排序集成员结构体
type ZSetMember struct {
	Member string
	Score  float64
}

// encodeScore 改进的编码函数，确保负分数正确排序
func encodeScore(score float64) []byte {
	bits := math.Float64bits(score)
	b := make([]byte, 8)
	// 对于正数，翻转符号位（0x8000000000000000）以确保负数排在前面
	if score >= 0 {
		bits = bits ^ 0x8000000000000000
	} else {
		// 对于负数，翻转所有位并保留符号位
		bits = ^bits
	}
	binary.BigEndian.PutUint64(b, bits)
	return b
}

// decodeScore 从字节解码回 float64
func decodeScore(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if bits&0x8000000000000000 == 0 {
		bits = ^bits
	} else {
		bits = bits ^ 0x8000000000000000
	}
	return math.Float64frombits(bits)
}

func sortedSetKeyIndex(zSetName, member string) []byte {
	return keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex+member))
}

func sortedSetKeyMember(zSetName, member string) []byte {
	return keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetData+member))
}

// ZAdd 添加或更新成员分数
func (s *BadgerStore) ZAdd(zSetName string, members []ZSetMember) error {
	if len(members) == 0 {
		return nil
	}
	return s.db.Update(func(txn *badger.Txn) error {
		badgerTypeKey := TypeOfKeyGet(zSetName)
		if err := txn.Set(badgerTypeKey, []byte(KeyTypeSortedSet)); err != nil {
			return err
		}

		for _, m := range members {
			member := m.Member
			score := m.Score
			dataKey := sortedSetKeyMember(zSetName, member)

			// 检查旧分数
			item, err := txn.Get(dataKey)
			if err == nil {
				var oldScoreBytes []byte
				err = item.Value(func(val []byte) error {
					oldScoreBytes = val
					return nil
				})
				if err != nil {
					return err
				}
				oldScore := decodeScore(oldScoreBytes)

				// 删除旧索引
				oldIndexKey := []byte(zSetName + sortedSetIndex)
				oldIndexKey = append(oldIndexKey, encodeScore(oldScore)...)
				oldIndexKey = append(oldIndexKey, []byte(UnderScore+member)...)
				if err := txn.Delete(oldIndexKey); err != nil {
					return err
				}
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}

			// 写入新数据
			if err := txn.Set(dataKey, encodeScore(score)); err != nil {
				return err
			}

			// 写入新索引
			newIndexKey := []byte(zSetName + sortedSetIndex)
			newIndexKey = append(newIndexKey, encodeScore(score)...)
			newIndexKey = append(newIndexKey, []byte(UnderScore+member)...)
			if err := txn.Set(newIndexKey, nil); err != nil {
				return err
			}
		}
		return nil
	})
}

// ZRangeByScore 获取分数范围内的成员
func (s *BadgerStore) ZRangeByScore(zSetName string, minScore, maxScore float64, offset, count int) ([]ZSetMember, error) {
	var results []ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := []byte(zSetName + sortedSetIndex)
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := append(prefix, encodeScore(minScore)...)
		current := 0
		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			// 提取分数和成员
			parts := bytes.Split(key, []byte(UnderScore))
			if len(parts) < 4 {
				continue
			}
			member := string(parts[3])
			scoreBytes := key[len(prefix) : len(key)-len(UnderScore+member)]
			score := decodeScore(scoreBytes)

			if score > maxScore {
				break
			}
			if current < offset {
				current++
				continue
			}
			if count > 0 && len(results) >= count {
				break
			}

			results = append(results, ZSetMember{Member: member, Score: score})
		}
		return nil
	})
	return results, err
}

// ZRem 删除成员
func (s *BadgerStore) ZRem(zSetName, member string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		dataKey := sortedSetKeyMember(zSetName, member)
		item, err := txn.Get(dataKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var scoreBytes []byte
		err = item.Value(func(val []byte) error {
			scoreBytes = val
			return nil
		})
		if err != nil {
			return err
		}
		score := decodeScore(scoreBytes)

		if err := txn.Delete(dataKey); err != nil {
			return err
		}

		indexKey := []byte(zSetName + sortedSetIndex)
		indexKey = append(indexKey, encodeScore(score)...)
		indexKey = append(indexKey, []byte(UnderScore+member)...)
		return txn.Delete(indexKey)
	})
}

// ZScore 获取成员分数
func (s *BadgerStore) ZScore(zSetName, member string) (float64, bool, error) {
	var score float64
	dataKey := sortedSetKeyMember(zSetName, member)

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(dataKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			score = decodeScore(val)
			return nil
		})
	})

	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, false, nil
	}
	return score, true, err
}

// ZRange 获取指定排名范围的成员
func (s *BadgerStore) ZRange(zSetName string, start, stop int64) ([]*ZSetMember, error) {
	var results []*ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := []byte(zSetName + sortedSetIndex)
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		// 计算总数以处理负索引
		totalCount := int64(0)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			totalCount++
		}

		// 处理负索引
		if start < 0 {
			start = totalCount + start
		}
		if stop < 0 {
			stop = totalCount + stop
		}
		if start < 0 {
			start = 0
		}
		if stop >= totalCount {
			stop = totalCount - 1
		}
		if start > stop {
			return nil
		}

		currentIndex := int64(0)
		it.Rewind()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if currentIndex < start {
				currentIndex++
				continue
			}
			if currentIndex > stop {
				break
			}

			item := it.Item()
			key := item.Key()

			parts := bytes.Split(key, []byte(UnderScore))
			if len(parts) < 4 {
				continue
			}
			member := string(parts[3])
			scoreBytes := key[len(prefix) : len(key)-len(UnderScore+member)]
			score := decodeScore(scoreBytes)

			results = append(results, &ZSetMember{Member: member, Score: score})
			currentIndex++
		}
		return nil
	})
	return results, err
}

// ZSetDel 删除整个排序集
func (s *BadgerStore) ZSetDel(zSetName string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		dataPrefix := []byte(zSetName + sortedSetData)
		indexPrefix := []byte(zSetName + sortedSetIndex)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		// 删除数据键
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(dataPrefix); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				return err
			}
		}

		// 删除索引键
		it = txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(indexPrefix); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				return err
			}
		}

		return nil
	})
}

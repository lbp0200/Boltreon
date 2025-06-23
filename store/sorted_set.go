package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/dgraph-io/badger/v4"
)

// ZSetMember 添加ZSetMember结构体
type ZSetMember struct {
	Member string
	Score  float64
}

// encodeScore 对 float64 进行编码以用于排序
func encodeScore(score float64) []byte {
	bits := math.Float64bits(score)
	if score < 0 {
		bits = ^bits
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, bits)
	return b
}

// decodeScore 从字节解码回 float64
func decodeScore(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if bits&0x8000000000000000 == 0 {
		bits = ^bits
	}
	return math.Float64frombits(bits)
}

func sortedSetKeyIndex(zSetName, member string) []byte {
	return []byte(sortedSetPrefix + zSetName + sortedSetData + member)
}

func sortedSetKeyMember(zSetName, member string) []byte {
	return []byte(sortedSetPrefix + zSetName + sortedSetData + member)
}

// ZAdd 添加或更新成员分数
func (s *BadgerStore) ZAdd(zSetName string, members []ZSetMember) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// 遍历所有待添加的成员
		for _, m := range members {
			member := m.Member
			score := m.Score
			dataKey := sortedSetKeyMember(zSetName, member)

			// 1. 检查旧 score（逻辑与原单个成员处理一致）
			item, err := txn.Get(dataKey)
			if err == nil { // 成员已存在
				oldScoreBytes, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				oldScore := decodeScore(oldScoreBytes)

				// 2. 删除旧的索引 Key
				oldIndexKey := []byte(zSetName + sortedSetIndex)
				oldIndexKey = append(oldIndexKey, encodeScore(oldScore)...)
				oldIndexKey = append(oldIndexKey, []byte("|"+member)...)
				if err := txn.Delete(oldIndexKey); err != nil {
					return err
				}
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}

			// 3. 写入新数据和索引（逻辑与原单个成员处理一致）
			// 写入数据 Key
			if err := txn.Set(dataKey, encodeScore(score)); err != nil {
				return err
			}

			// 写入索引 Key
			newIndexKey := []byte(zSetName + sortedSetIndex)
			newIndexKey = append(newIndexKey, encodeScore(score)...)
			newIndexKey = append(newIndexKey, []byte("|"+member)...)
			if err := txn.Set(newIndexKey, nil); err != nil {
				return err
			}
		}
		return nil
	})
}

// ZRangeByScore 获取分数范围内的成员
func (s *BadgerStore) ZRangeByScore(zSetName string, start, stop int) ([]ZSetMember, error) {
	var results []ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := []byte(zSetName + "|index|")
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		current := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if current < start {
				current++
				continue
			}
			if stop != -1 && current > stop {
				break
			}

			item := it.Item()
			key := item.Key()

			// 解析 member 和 score
			parts := bytes.Split(key, []byte("|"))
			member := string(parts[3])

			// 从 key 中提取 score 的字节部分
			scorePrefix := append(prefix, parts[2]...)
			scoreBytes := key[len(prefix):len(scorePrefix)]
			score := decodeScore(scoreBytes)

			results = append(results, ZSetMember{Member: member, Score: score})
			current++
		}
		return nil
	})
	return results, err
}

// ZRem 删除成员
func (s *BadgerStore) ZRem(zSetName, member string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		dataKey := []byte(zSetName + "|data|" + member)

		item, err := txn.Get(dataKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 成员不存在，无需操作
		}
		if err != nil {
			return err
		}

		// 获取 score 以便删除索引
		scoreBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		score := decodeScore(scoreBytes)

		// 1. 删除数据 Key
		if err := txn.Delete(dataKey); err != nil {
			return err
		}

		// 2. 删除索引 Key
		indexKey := []byte(zSetName + "|index|")
		indexKey = append(indexKey, encodeScore(score)...)
		indexKey = append(indexKey, []byte("|"+member)...)
		if err := txn.Delete(indexKey); err != nil {
			return err
		}

		return nil
	})
}

// ZScore 获取成员分数
func (s *BadgerStore) ZScore(zSetName, member string) (float64, error) {
	var score float64
	dataKey := []byte(zSetName + "|data|" + member)

	// 使用只读事务查询数据
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(dataKey)
		if err != nil {
			return err // 可能返回 badger.ErrKeyNotFound 或其他错误
		}

		// 读取并解码分数
		scoreBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		score = decodeScore(scoreBytes)
		return nil
	})

	// 处理键不存在的情况
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, errors.New("member not found in zset")
	}
	return score, err
}

// ZRange 获取指定范围内的成员（按索引）
func (s *BadgerStore) ZRange(zSetName []byte, start, stop int64) ([]*ZSetMember, error) {
	var results []*ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := append([]byte{}, zSetName...)
		prefix = append(prefix, []byte("|index|")...)
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		// 处理负索引
		totalCount := int64(0)
		// 先计算元素总数
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			totalCount++
		}
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

			// 解析 member 和 score
			parts := bytes.Split(key, []byte("|"))
			member := string(parts[3])

			// 从 key 中提取 score 的字节部分
			scorePrefix := append(prefix, parts[2]...)
			scoreBytes := key[len(prefix):len(scorePrefix)]
			score := decodeScore(scoreBytes)

			results = append(results, &ZSetMember{Member: member, Score: score})
			currentIndex++
		}
		return nil
	})
	return results, err
}

// ZSetDel 删除整个有序集合
func (s *BadgerStore) ZSetDel(zSetName string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// 删除数据相关的键
		dataPrefix := []byte(zSetName + "|data|")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = dataPrefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		// 删除索引相关的键
		indexPrefix := []byte(zSetName + "|index|")
		opts.Prefix = indexPrefix
		it = txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})
}

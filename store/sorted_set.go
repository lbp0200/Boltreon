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

// ZSetsMetaValue 定义元数据结构体，存储成员数量和版本
type ZSetsMetaValue struct {
	Card    int64
	Version uint32
}

// encodeScore 优化分数编码，确保负分数正确排序
func encodeScore(score float64) []byte {
	bits := math.Float64bits(score)
	b := make([]byte, 8)
	// 翻转符号位以确保负分数排在正分数前
	if score >= 0 {
		bits = bits ^ 0x8000000000000000
	} else {
		// 负分数翻转所有位以保持绝对值排序
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

// encodeMeta 编码元数据
func encodeMeta(meta ZSetsMetaValue) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b[:8], uint64(meta.Card))
	binary.BigEndian.PutUint32(b[8:], meta.Version)
	return b
}

// decodeMeta 解码元数据
func decodeMeta(b []byte) (ZSetsMetaValue, error) {
	if len(b) != 12 {
		return ZSetsMetaValue{}, errors.New("invalid meta data")
	}
	card := int64(binary.BigEndian.Uint64(b[:8]))
	version := binary.BigEndian.Uint32(b[8:])
	return ZSetsMetaValue{Card: card, Version: version}, nil
}

func sortedSetKeyMeta(zSetName string) []byte {
	return keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+"meta"))
}

func sortedSetKeyIndex(zSetName string, score float64, member string, version uint32) []byte {
	key := []byte(zSetName + sortedSetIndex)
	key = append(key, encodeScore(score)...)
	key = append(key, []byte(UnderScore+member)...)
	key = append(key, encodeVersion(version)...)
	return keyBadgerGet(prefixKeySortedSetBytes, key)
}

func sortedSetKeyMember(zSetName, member string) []byte {
	return keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetData+member))
}

func encodeVersion(version uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, version)
	return b
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

		// 获取元数据
		metaKey := sortedSetKeyMeta(zSetName)
		var meta ZSetsMetaValue
		item, err := txn.Get(metaKey)
		if err == nil {
			err = item.Value(func(val []byte) error {
				meta, err = decodeMeta(val)
				return err
			})
			if err != nil {
				return err
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		newMembers := int64(len(members))
		meta.Version++

		// 批量收集操作
		type operation struct {
			dataKey     []byte
			indexKey    []byte
			oldIndexKey []byte
			score       []byte
		}
		ops := make([]operation, 0, len(members))

		for _, m := range members {
			member := m.Member
			score := m.Score
			dataKey := sortedSetKeyMember(zSetName, member)

			// 检查旧分数
			var oldScore float64
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
				oldScore = decodeScore(oldScoreBytes)
				newMembers-- // 替换现有成员，计数不变
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}

			// 准备操作
			op := operation{
				dataKey:  dataKey,
				indexKey: sortedSetKeyIndex(zSetName, score, member, meta.Version),
				score:    encodeScore(score),
			}
			if err == nil {
				op.oldIndexKey = sortedSetKeyIndex(zSetName, oldScore, member, meta.Version-1)
			}
			ops = append(ops, op)
		}

		// 更新元数据计数
		meta.Card += newMembers
		if err := txn.Set(metaKey, encodeMeta(meta)); err != nil {
			return err
		}

		// 批量执行操作
		for _, op := range ops {
			if op.oldIndexKey != nil {
				if err := txn.Delete(op.oldIndexKey); err != nil {
					return err
				}
			}
			if err := txn.Set(op.dataKey, op.score); err != nil {
				return err
			}
			if err := txn.Set(op.indexKey, nil); err != nil {
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
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := append(prefix, encodeScore(minScore)...)
		current := 0
		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			parts := bytes.Split(key, []byte(UnderScore))
			if len(parts) < 4 {
				continue
			}
			member := string(parts[3])
			scoreBytes := key[len(prefix) : len(key)-len(UnderScore+member)-4]
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

		// 获取元数据
		metaKey := sortedSetKeyMeta(zSetName)
		var meta ZSetsMetaValue
		item, err = txn.Get(metaKey)
		if err == nil {
			err = item.Value(func(val []byte) error {
				meta, err = decodeMeta(val)
				return err
			})
			if err != nil {
				return err
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
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

		indexKey := sortedSetKeyIndex(zSetName, score, member, meta.Version)
		if err := txn.Delete(indexKey); err != nil {
			return err
		}

		// 更新元数据
		meta.Card--
		if meta.Card <= 0 {
			return txn.Delete(metaKey)
		}
		return txn.Set(metaKey, encodeMeta(meta))
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
		opts.PrefetchValues = false

		// 获取元数据
		metaKey := sortedSetKeyMeta(zSetName)
		var totalCount int64
		item, err := txn.Get(metaKey)
		if err == nil {
			var meta ZSetsMetaValue
			err = item.Value(func(val []byte) error {
				meta, err = decodeMeta(val)
				return err
			})
			if err != nil {
				return err
			}
			totalCount = meta.Card
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
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

		it := txn.NewIterator(opts)
		defer it.Close()

		currentIndex := int64(0)
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
			scoreBytes := key[len(prefix) : len(key)-len(UnderScore+member)-4]
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

		// 删除元数据
		return txn.Delete(sortedSetKeyMeta(zSetName))
	})
}

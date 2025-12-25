package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"math"

	"github.com/dgraph-io/badger/v4"
)

const (
	prefixKeySortedSetBytes = "zset:"
	sortedSetIndex          = ":index:"
	sortedSetData           = ":data:"
	UnderScore              = "_"
	KeyTypeSortedSet        = "zset"
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
	if score >= 0 {
		bits = bits ^ 0x8000000000000000
	} else {
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
	return keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+":meta"))
}

func sortedSetKeyIndex(zSetName string, score float64, member string, version uint32) []byte {
	key := []byte(zSetName + sortedSetIndex)
	key = append(key, encodeScore(score)...)
	key = append(key, []byte(":"+member+":")...)
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

func keyBadgerGet(prefix string, key []byte) []byte {
	return append([]byte(prefix), key...)
}

// ZAdd 添加或更新成员分数
func (s *BoltreonStore) ZAdd(zSetName string, members []ZSetMember) error {
	if len(members) == 0 {
		return nil
	}
	return s.db.Update(func(txn *badger.Txn) error {
		badgerTypeKey := TypeOfKeyGet(zSetName)
		if err := txn.Set(badgerTypeKey, []byte(KeyTypeSortedSet)); err != nil {
			log.Printf("ZAdd: Failed to set type key: %v", err)
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
				log.Printf("ZAdd: Failed to decode meta: %v", err)
				return err
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			log.Printf("ZAdd: Failed to get meta: %v", err)
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
					log.Printf("ZAdd: Failed to get old score for %s: %v", member, err)
					return err
				}
				oldScore = decodeScore(oldScoreBytes)
				newMembers-- // 替换现有成员，计数不变
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				log.Printf("ZAdd: Failed to check member %s: %v", member, err)
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
			log.Printf("ZAdd: Failed to set meta: %v", err)
			return err
		}

		// 批量执行操作
		for _, op := range ops {
			if op.oldIndexKey != nil {
				if err := txn.Delete(op.oldIndexKey); err != nil {
					log.Printf("ZAdd: Failed to delete old index: %v", err)
					return err
				}
			}
			if err := txn.Set(op.dataKey, op.score); err != nil {
				log.Printf("ZAdd: Failed to set data key: %v", err)
				return err
			}
			if err := txn.Set(op.indexKey, nil); err != nil {
				log.Printf("ZAdd: Failed to set index key: %v", err)
				return err
			}
		}

		log.Printf("ZAdd: Successfully added %d members to %s, new card: %d", len(members), zSetName, meta.Card)
		return nil
	})
}

// ZRangeByScore 获取分数范围内的成员
func (s *BoltreonStore) ZRangeByScore(zSetName string, minScore, maxScore float64, offset, count int) ([]ZSetMember, error) {
	var results []ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex)) // e.g., "zset:myset:index:"
		opts.Prefix = prefix
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := append(prefix, encodeScore(minScore)...)
		current := 0
		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			keyParts := bytes.Split(key[len(prefixKeySortedSetBytes):], []byte(":"))
			if len(keyParts) != 5 {
				log.Printf("ZRangeByScore: Invalid key format: %s", key)
				continue
			}
			scoreBytes := keyParts[2]
			member := string(keyParts[3])
			if len(scoreBytes) != 8 {
				log.Printf("ZRangeByScore: Invalid score bytes length: %d", len(scoreBytes))
				continue
			}
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
		log.Printf("ZRangeByScore: Retrieved %d members from %s", len(results), zSetName)
		return nil
	})
	return results, err
}

// ZRem 删除成员
func (s *BoltreonStore) ZRem(zSetName, member string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		dataKey := sortedSetKeyMember(zSetName, member)
		item, err := txn.Get(dataKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			log.Printf("ZRem: Failed to get data key %s: %v", dataKey, err)
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
				log.Printf("ZRem: Failed to decode meta: %v", err)
				return err
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			log.Printf("ZRem: Failed to get meta: %v", err)
			return err
		}

		var scoreBytes []byte
		err = item.Value(func(val []byte) error {
			scoreBytes = val
			return nil
		})
		if err != nil {
			log.Printf("ZRem: Failed to get score for %s: %v", member, err)
			return err
		}
		score := decodeScore(scoreBytes)

		if err := txn.Delete(dataKey); err != nil {
			log.Printf("ZRem: Failed to delete data key: %v", err)
			return err
		}

		indexKey := sortedSetKeyIndex(zSetName, score, member, meta.Version)
		if err := txn.Delete(indexKey); err != nil {
			log.Printf("ZRem: Failed to delete index key: %v", err)
			return err
		}

		// 更新元数据
		meta.Card--
		if meta.Card <= 0 {
			if err := txn.Delete(metaKey); err != nil {
				log.Printf("ZRem: Failed to delete meta: %v", err)
				return err
			}
			log.Printf("ZRem: Deleted %s from %s, set empty", member, zSetName)
			return nil
		}
		if err := txn.Set(metaKey, encodeMeta(meta)); err != nil {
			log.Printf("ZRem: Failed to set meta: %v", err)
			return err
		}

		log.Printf("ZRem: Successfully removed %s from %s, new card: %d", member, zSetName, meta.Card)
		return nil
	})
}

// ZScore 获取成员分数
func (s *BoltreonStore) ZScore(zSetName, member string) (float64, bool, error) {
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
	if err != nil {
		log.Printf("ZScore: Failed to get score for %s: %v", member, err)
	}
	return score, true, err
}

// ZRange 获取指定排名范围的成员
func (s *BoltreonStore) ZRange(zSetName string, start, stop int64) ([]*ZSetMember, error) {
	var results []*ZSetMember
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		//prefix := []byte(zSetName + sortedSetIndex) // e.g., "myset:index:"
		//opts.Prefix = prefix
		prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zSetName+sortedSetIndex)) // e.g., "zset:myset:index:"
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
				log.Printf("ZRange: Failed to decode meta: %v", err)
				return err
			}
			totalCount = meta.Card
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			log.Printf("ZRange: Failed to get meta: %v", err)
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
		if start > stop || totalCount == 0 {
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

			parts := bytes.Split(key[len(prefixKeySortedSetBytes):], []byte(UnderScore))
			if len(parts) < 2 {
				log.Printf("ZRange: Invalid key format: %s", key)
				continue
			}
			member := string(parts[1][:len(parts[1])-4]) // 去掉版本号
			scoreBytes := key[len(prefixKeySortedSetBytes)+len(zSetName)+len(sortedSetIndex) : len(key)-len(UnderScore+member)-4]
			if len(scoreBytes) != 8 {
				log.Printf("ZRange: Invalid score bytes length: %d", len(scoreBytes))
				continue
			}
			score := decodeScore(scoreBytes)

			results = append(results, &ZSetMember{Member: member, Score: score})
			currentIndex++
		}
		log.Printf("ZRange: Retrieved %d members from %s", len(results), zSetName)
		return nil
	})
	return results, err
}

// ZSetDel 删除整个排序集
func (s *BoltreonStore) ZSetDel(zSetName string) error {
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
				log.Printf("ZSetDel: Failed to delete data key: %v", err)
				return err
			}
		}

		// 删除索引键
		it = txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(indexPrefix); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				log.Printf("ZSetDel: Failed to delete index key: %v", err)
				return err
			}
		}

		// 删除元数据和类型键
		if err := txn.Delete(sortedSetKeyMeta(zSetName)); err != nil {
			log.Printf("ZSetDel: Failed to delete meta: %v", err)
			return err
		}
		if err := txn.Delete(TypeOfKeyGet(zSetName)); err != nil {
			log.Printf("ZSetDel: Failed to delete type key: %v", err)
			return err
		}

		log.Printf("ZSetDel: Successfully deleted set %s", zSetName)
		return nil
	})
}

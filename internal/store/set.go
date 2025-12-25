package store

import (
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/Boltreon/internal/helper"
)

// setKey 方法用于生成存储在 Badger 数据库中的键
func (s *BoltreonStore) setKey(key []byte, parts ...string) []byte {
	base := []string{"SET", string(key)}
	base = append(base, parts...)
	return []byte(strings.Join(base, ":"))
}

// SAdd 实现 Redis SADD 命令
func (s *BoltreonStore) SAdd(key []byte, members ...[]byte) (int, error) {
	added := 0
	err := s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(TypeOfKeyGet(string(key)), []byte(KeyTypeSet)); err != nil {
			return err
		}
		countKey := s.setKey(key, "count")
		var count uint64

		// 获取当前计数器值
		item, err := txn.Get(countKey)
		if err != badger.ErrKeyNotFound {
			if err != nil {
				return err
			}
			countBytes, _ := item.ValueCopy(nil)
			count = helper.BytesToUint64(countBytes)
		}

		for _, member := range members {
			memberStr := string(member)
			memberKey := s.setKey(key, "member", memberStr)

			// 检查成员是否存在
			if _, err := txn.Get(memberKey); err == badger.ErrKeyNotFound {
				// 新成员：写入成员键并增加计数器
				if err := txn.Set(memberKey, []byte{}); err != nil {
					return err
				}
				count++
				added++
			}
		}

		// 更新计数器
		if added > 0 {
			return txn.Set(countKey, helper.Uint64ToBytes(count))
		}
		return nil
	})
	return added, err
}

// SRem 实现 Redis SREM 命令
func (s *BoltreonStore) SRem(key []byte, members ...[]byte) (int, error) {
	removed := 0
	err := s.db.Update(func(txn *badger.Txn) error {
		countKey := s.setKey(key, "count")
		var count uint64

		// 获取当前计数器值
		item, err := txn.Get(countKey)
		if !errors.Is(err, badger.ErrKeyNotFound) {
			if err != nil {
				return err
			}
			countBytes, _ := item.ValueCopy(nil)
			count = helper.BytesToUint64(countBytes)
		}

		for _, member := range members {
			memberStr := string(member)
			memberKey := s.setKey(key, "member", memberStr)

			// 检查成员是否存在
			if err := txn.Delete(memberKey); err == nil {
				count--
				removed++
			}
		}

		// 更新计数器（即使计数器为0也要保留）
		return txn.Set(countKey, helper.Uint64ToBytes(count))
	})
	return removed, err
}

// SCard 实现 Redis SCARD 命令
func (s *BoltreonStore) SCard(key []byte) (uint64, error) {
	var count uint64
	err := s.db.View(func(txn *badger.Txn) error {
		countKey := s.setKey(key, "count")
		item, err := txn.Get(countKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		countBytes, _ := item.ValueCopy(nil)
		count = helper.BytesToUint64(countBytes)
		return nil
	})
	return count, err
}

// SIsMember 实现 Redis SISMEMBER 命令
func (s *BoltreonStore) SIsMember(key []byte, member []byte) (bool, error) {
	exists := false
	err := s.db.View(func(txn *badger.Txn) error {
		memberKey := s.setKey(key, "member", string(member))
		_, err := txn.Get(memberKey)
		if err == nil {
			exists = true
		}
		return err
	})
	return exists, err
}

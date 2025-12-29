package store

import (
	"errors"
	"math/rand"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/Boltreon/internal/helper"
)

// setKey 方法用于生成存储在 Badger 数据库中的键
func (s *BoltreonStore) setKey(key string, parts ...string) string {
	base := []string{KeyTypeSet, key}
	base = append(base, parts...)
	return strings.Join(base, ":")
}

// SAdd 实现 Redis SADD 命令
func (s *BoltreonStore) SAdd(key string, members ...string) (int, error) {
	added := 0
	err := s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeSet)); err != nil {
			return err
		}
		countKey := s.setKey(key, "count")
		var count uint64

		// 获取当前计数器值
		item, err := txn.Get([]byte(countKey))
		if err != badger.ErrKeyNotFound {
			if err != nil {
				return err
			}
			countBytes, _ := item.ValueCopy(nil)
			count = helper.BytesToUint64(countBytes)
		}

		for _, member := range members {
			memberKey := s.setKey(key, "member", member)

			// 检查成员是否存在
			if _, err := txn.Get([]byte(memberKey)); err == badger.ErrKeyNotFound {
				// 新成员：写入成员键并增加计数器
				if err := txn.Set([]byte(memberKey), []byte{}); err != nil {
					return err
				}
				count++
				added++
			}
		}

		// 更新计数器
		if added > 0 {
			return txn.Set([]byte(countKey), helper.Uint64ToBytes(count))
		}
		return nil
	})
	return added, err
}

// SRem 实现 Redis SREM 命令
func (s *BoltreonStore) SRem(key string, members ...string) (int, error) {
	removed := 0
	err := s.db.Update(func(txn *badger.Txn) error {
		countKey := s.setKey(key, "count")
		var count uint64

		// 获取当前计数器值
		item, err := txn.Get([]byte(countKey))
		if !errors.Is(err, badger.ErrKeyNotFound) {
			if err != nil {
				return err
			}
			countBytes, _ := item.ValueCopy(nil)
			count = helper.BytesToUint64(countBytes)
		}

		for _, member := range members {
			memberKey := s.setKey(key, "member", member)

			// 检查成员是否存在
			if _, err := txn.Get([]byte(memberKey)); err == nil {
				if err := txn.Delete([]byte(memberKey)); err != nil {
					return err
				}
				count--
				removed++
			}
		}

		// 更新计数器（即使计数器为0也要保留）
		if removed > 0 {
			return txn.Set([]byte(countKey), helper.Uint64ToBytes(count))
		}
		return nil
	})
	return removed, err
}

// SCard 实现 Redis SCARD 命令
func (s *BoltreonStore) SCard(key string) (uint64, error) {
	var count uint64
	err := s.db.View(func(txn *badger.Txn) error {
		countKey := s.setKey(key, "count")
		item, err := txn.Get([]byte(countKey))
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
func (s *BoltreonStore) SIsMember(key string, member string) (bool, error) {
	exists := false
	err := s.db.View(func(txn *badger.Txn) error {
		memberKey := s.setKey(key, "member", member)
		_, err := txn.Get([]byte(memberKey))
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

// getAllMembers 获取集合中的所有成员
func (s *BoltreonStore) getAllMembers(txn *badger.Txn, key string) ([]string, error) {
	var members []string
	prefix := s.setKey(key, "member")
	prefixBytes := []byte(prefix + ":")
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	for iter.Seek(prefixBytes); iter.ValidForPrefix(prefixBytes); iter.Next() {
		k := iter.Item().Key()
		kStr := string(k)
		// 提取成员名：SET:key:member:memberName
		// 移除前缀，获取成员名
		if strings.HasPrefix(kStr, prefix+":") {
			member := kStr[len(prefix)+1:] // 跳过 "SET:key:member:" 前缀
			members = append(members, member)
		}
	}
	return members, nil
}

// SMembers 实现 Redis SMEMBERS 命令
func (s *BoltreonStore) SMembers(key string) ([]string, error) {
	var members []string
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		members, err = s.getAllMembers(txn, key)
		return err
	})
	return members, err
}

// SPop 实现 Redis SPOP 命令，随机弹出并删除一个成员
func (s *BoltreonStore) SPop(key string) (string, error) {
	var member string
	err := s.db.Update(func(txn *badger.Txn) error {
		members, err := s.getAllMembers(txn, key)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return nil // 集合为空
		}

		// 随机选择一个成员
		rand.Seed(time.Now().UnixNano())
		index := rand.Intn(len(members))
		member = members[index]

		// 删除成员
		memberKey := s.setKey(key, "member", member)
		if err := txn.Delete([]byte(memberKey)); err != nil {
			return err
		}

		// 更新计数器
		countKey := s.setKey(key, "count")
		item, err := txn.Get([]byte(countKey))
		if err == nil {
			countBytes, _ := item.ValueCopy(nil)
			count := helper.BytesToUint64(countBytes)
			if count > 0 {
				count--
				return txn.Set([]byte(countKey), helper.Uint64ToBytes(count))
			}
		}
		return nil
	})
	return member, err
}

// SPopN 实现 Redis SPOP 命令（带count参数），随机弹出并删除多个成员
func (s *BoltreonStore) SPopN(key string, count int) ([]string, error) {
	var members []string
	err := s.db.Update(func(txn *badger.Txn) error {
		allMembers, err := s.getAllMembers(txn, key)
		if err != nil {
			return err
		}
		if len(allMembers) == 0 {
			return nil
		}

		// 限制count不超过集合大小
		if count > len(allMembers) {
			count = len(allMembers)
		}
		if count < 0 {
			count = 0
		}

		// 随机选择成员
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(allMembers), func(i, j int) {
			allMembers[i], allMembers[j] = allMembers[j], allMembers[i]
		})

		// 删除选中的成员
		for i := 0; i < count; i++ {
			member := allMembers[i]
			memberKey := s.setKey(key, "member", member)
			if err := txn.Delete([]byte(memberKey)); err != nil {
				return err
			}
			members = append(members, member)
		}

		// 更新计数器
		if count > 0 {
			countKey := s.setKey(key, "count")
			item, err := txn.Get([]byte(countKey))
			if err == nil {
				countBytes, _ := item.ValueCopy(nil)
				currentCount := helper.BytesToUint64(countBytes)
				if currentCount >= uint64(count) {
					currentCount -= uint64(count)
					return txn.Set([]byte(countKey), helper.Uint64ToBytes(currentCount))
				}
			}
		}
		return nil
	})
	return members, err
}

// SRandMember 实现 Redis SRANDMEMBER 命令，随机获取一个成员（不删除）
func (s *BoltreonStore) SRandMember(key string) (string, error) {
	var member string
	err := s.db.View(func(txn *badger.Txn) error {
		members, err := s.getAllMembers(txn, key)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return nil
		}

		// 随机选择一个成员
		rand.Seed(time.Now().UnixNano())
		index := rand.Intn(len(members))
		member = members[index]
		return nil
	})
	return member, err
}

// SRandMemberN 实现 Redis SRANDMEMBER 命令（带count参数），随机获取多个成员（不删除）
func (s *BoltreonStore) SRandMemberN(key string, count int) ([]string, error) {
	var members []string
	err := s.db.View(func(txn *badger.Txn) error {
		allMembers, err := s.getAllMembers(txn, key)
		if err != nil {
			return err
		}
		if len(allMembers) == 0 {
			return nil
		}

		// 如果count为负数，允许重复
		if count < 0 {
			count = -count
			rand.Seed(time.Now().UnixNano())
			for i := 0; i < count; i++ {
				index := rand.Intn(len(allMembers))
				members = append(members, allMembers[index])
			}
		} else {
			// 限制count不超过集合大小
			if count > len(allMembers) {
				count = len(allMembers)
			}
			// 随机选择不重复的成员
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(allMembers), func(i, j int) {
				allMembers[i], allMembers[j] = allMembers[j], allMembers[i]
			})
			members = allMembers[:count]
		}
		return nil
	})
	return members, err
}

// SMove 实现 Redis SMOVE 命令，将成员从源集合移动到目标集合
func (s *BoltreonStore) SMove(source, destination, member string) (bool, error) {
	moved := false
	err := s.db.Update(func(txn *badger.Txn) error {
		// 检查成员是否在源集合中
		sourceMemberKey := s.setKey(source, "member", member)
		_, err := txn.Get([]byte(sourceMemberKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 成员不存在，返回false
		}
		if err != nil {
			return err
		}

		// 检查成员是否已在目标集合中
		destMemberKey := s.setKey(destination, "member", member)
		_, err = txn.Get([]byte(destMemberKey))
		alreadyInDest := err == nil

		// 从源集合删除
		if err := txn.Delete([]byte(sourceMemberKey)); err != nil {
			return err
		}

		// 更新源集合计数器
		sourceCountKey := s.setKey(source, "count")
		item, err := txn.Get([]byte(sourceCountKey))
		if err == nil {
			countBytes, _ := item.ValueCopy(nil)
			count := helper.BytesToUint64(countBytes)
			if count > 0 {
				count--
				if err := txn.Set([]byte(sourceCountKey), helper.Uint64ToBytes(count)); err != nil {
					return err
				}
			}
		}

		// 如果成员不在目标集合中，添加到目标集合
		if !alreadyInDest {
			if err := txn.Set(TypeOfKeyGet(destination), []byte(KeyTypeSet)); err != nil {
				return err
			}
			if err := txn.Set([]byte(destMemberKey), []byte{}); err != nil {
				return err
			}

			// 更新目标集合计数器
			destCountKey := s.setKey(destination, "count")
			item, err = txn.Get([]byte(destCountKey))
			var destCount uint64
			if err == nil {
				countBytes, _ := item.ValueCopy(nil)
				destCount = helper.BytesToUint64(countBytes)
			}
			destCount++
			if err := txn.Set([]byte(destCountKey), helper.Uint64ToBytes(destCount)); err != nil {
				return err
			}
		}

		moved = true
		return nil
	})
	return moved, err
}

// SInter 实现 Redis SINTER 命令，计算多个集合的交集
func (s *BoltreonStore) SInter(keys ...string) ([]string, error) {
	var result []string
	err := s.db.View(func(txn *badger.Txn) error {
		if len(keys) == 0 {
			return nil
		}

		// 获取第一个集合的所有成员
		firstMembers, err := s.getAllMembers(txn, keys[0])
		if err != nil {
			return err
		}
		if len(firstMembers) == 0 {
			return nil
		}

		// 检查每个成员是否在所有其他集合中
		for _, member := range firstMembers {
			inAll := true
			for i := 1; i < len(keys); i++ {
				memberKey := s.setKey(keys[i], "member", member)
				_, err := txn.Get([]byte(memberKey))
				if errors.Is(err, badger.ErrKeyNotFound) {
					inAll = false
					break
				}
				if err != nil {
					return err
				}
			}
			if inAll {
				result = append(result, member)
			}
		}
		return nil
	})
	return result, err
}

// SUnion 实现 Redis SUNION 命令，计算多个集合的并集
func (s *BoltreonStore) SUnion(keys ...string) ([]string, error) {
	var result []string
	seen := make(map[string]bool)
	err := s.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			members, err := s.getAllMembers(txn, key)
			if err != nil {
				return err
			}
			for _, member := range members {
				if !seen[member] {
					seen[member] = true
					result = append(result, member)
				}
			}
		}
		return nil
	})
	return result, err
}

// SDiff 实现 Redis SDIFF 命令，计算第一个集合与其他集合的差集
func (s *BoltreonStore) SDiff(keys ...string) ([]string, error) {
	var result []string
	err := s.db.View(func(txn *badger.Txn) error {
		if len(keys) == 0 {
			return nil
		}

		// 获取第一个集合的所有成员
		firstMembers, err := s.getAllMembers(txn, keys[0])
		if err != nil {
			return err
		}

		// 构建其他集合的成员集合
		otherMembers := make(map[string]bool)
		for i := 1; i < len(keys); i++ {
			members, err := s.getAllMembers(txn, keys[i])
			if err != nil {
				return err
			}
			for _, member := range members {
				otherMembers[member] = true
			}
		}

		// 找出只在第一个集合中的成员
		for _, member := range firstMembers {
			if !otherMembers[member] {
				result = append(result, member)
			}
		}
		return nil
	})
	return result, err
}

// SInterStore 实现 Redis SINTERSTORE 命令，计算交集并存储到目标集合
func (s *BoltreonStore) SInterStore(destination string, keys ...string) (int, error) {
	var count int
	err := s.db.Update(func(txn *badger.Txn) error {
		// 在事务中计算交集
		var result []string
		if len(keys) > 0 {
			// 获取第一个集合的所有成员
			firstMembers, err := s.getAllMembers(txn, keys[0])
			if err != nil {
				return err
			}

			// 检查每个成员是否在所有其他集合中
			for _, member := range firstMembers {
				inAll := true
				for i := 1; i < len(keys); i++ {
					memberKey := s.setKey(keys[i], "member", member)
					_, err := txn.Get([]byte(memberKey))
					if errors.Is(err, badger.ErrKeyNotFound) {
						inAll = false
						break
					}
					if err != nil {
						return err
					}
				}
				if inAll {
					result = append(result, member)
				}
			}
		}

		// 删除目标集合的现有成员
		existingMembers, err := s.getAllMembers(txn, destination)
		if err == nil {
			for _, member := range existingMembers {
				memberKey := s.setKey(destination, "member", member)
				txn.Delete([]byte(memberKey))
			}
		}

		// 添加交集结果到目标集合
		if len(result) > 0 {
			if err := txn.Set(TypeOfKeyGet(destination), []byte(KeyTypeSet)); err != nil {
				return err
			}
			for _, member := range result {
				memberKey := s.setKey(destination, "member", member)
				if err := txn.Set([]byte(memberKey), []byte{}); err != nil {
					return err
				}
			}
		}

		// 更新计数器
		count = len(result)
		countKey := s.setKey(destination, "count")
		return txn.Set([]byte(countKey), helper.Uint64ToBytes(uint64(count)))
	})
	return count, err
}

// SUnionStore 实现 Redis SUNIONSTORE 命令，计算并集并存储到目标集合
func (s *BoltreonStore) SUnionStore(destination string, keys ...string) (int, error) {
	var count int
	err := s.db.Update(func(txn *badger.Txn) error {
		// 在事务中计算并集
		var result []string
		seen := make(map[string]bool)
		for _, key := range keys {
			members, err := s.getAllMembers(txn, key)
			if err != nil {
				return err
			}
			for _, member := range members {
				if !seen[member] {
					seen[member] = true
					result = append(result, member)
				}
			}
		}

		// 删除目标集合的现有成员
		existingMembers, err := s.getAllMembers(txn, destination)
		if err == nil {
			for _, member := range existingMembers {
				memberKey := s.setKey(destination, "member", member)
				txn.Delete([]byte(memberKey))
			}
		}

		// 添加并集结果到目标集合
		if len(result) > 0 {
			if err := txn.Set(TypeOfKeyGet(destination), []byte(KeyTypeSet)); err != nil {
				return err
			}
			for _, member := range result {
				memberKey := s.setKey(destination, "member", member)
				if err := txn.Set([]byte(memberKey), []byte{}); err != nil {
					return err
				}
			}
		}

		// 更新计数器
		count = len(result)
		countKey := s.setKey(destination, "count")
		return txn.Set([]byte(countKey), helper.Uint64ToBytes(uint64(count)))
	})
	return count, err
}

// SDiffStore 实现 Redis SDIFFSTORE 命令，计算差集并存储到目标集合
func (s *BoltreonStore) SDiffStore(destination string, keys ...string) (int, error) {
	var count int
	err := s.db.Update(func(txn *badger.Txn) error {
		// 在事务中计算差集
		var result []string
		if len(keys) > 0 {
			// 获取第一个集合的所有成员
			firstMembers, err := s.getAllMembers(txn, keys[0])
			if err != nil {
				return err
			}

			// 构建其他集合的成员集合
			otherMembers := make(map[string]bool)
			for i := 1; i < len(keys); i++ {
				members, err := s.getAllMembers(txn, keys[i])
				if err != nil {
					return err
				}
				for _, member := range members {
					otherMembers[member] = true
				}
			}

			// 找出只在第一个集合中的成员
			for _, member := range firstMembers {
				if !otherMembers[member] {
					result = append(result, member)
				}
			}
		}

		// 删除目标集合的现有成员
		existingMembers, err := s.getAllMembers(txn, destination)
		if err == nil {
			for _, member := range existingMembers {
				memberKey := s.setKey(destination, "member", member)
				txn.Delete([]byte(memberKey))
			}
		}

		// 添加差集结果到目标集合
		if len(result) > 0 {
			if err := txn.Set(TypeOfKeyGet(destination), []byte(KeyTypeSet)); err != nil {
				return err
			}
			for _, member := range result {
				memberKey := s.setKey(destination, "member", member)
				if err := txn.Set([]byte(memberKey), []byte{}); err != nil {
					return err
				}
			}
		}

		// 更新计数器
		count = len(result)
		countKey := s.setKey(destination, "count")
		return txn.Set([]byte(countKey), helper.Uint64ToBytes(uint64(count)))
	})
	return count, err
}

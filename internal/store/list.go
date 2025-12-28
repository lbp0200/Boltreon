package store

import (
	"fmt"
	"strings"

	"github.com/lbp0200/Boltreon/internal/helper"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

// ListNode 结构体定义了链表节点的结构
// ID 是节点的唯一标识符，使用字符串存储
// Value 是节点存储的数据，以字节切片形式存储
// Prev 是指向前一个节点的 ID，使用字符串存储
// Next 是指向后一个节点的 ID，使用字符串存储
type ListNode struct {
	ID    string
	Value []byte // 节点存储的值，以字节切片形式表示
	Prev  string // 指向前一个节点的 ID
	Next  string // 指向后一个节点的 ID
}

// listKey 方法用于生成存储在 Badger 数据库中的键
// key 是链表的主键，以字节切片形式传入
// parts 是可变参数，用于拼接更多的键信息
// 返回一个字节切片，作为存储在数据库中的完整键
func (s *BoltreonStore) listKey(key string, parts ...string) string {
	return fmt.Sprintf("%s:%s:%s", KeyTypeList, key, strings.Join(parts, ":"))
}

// listLength 方法用于获取链表的长度
// key 是链表的主键，以字节切片形式传入
// 返回链表的长度（无符号 64 位整数）和可能出现的错误
func (s *BoltreonStore) listLength(key string) (uint64, error) {
	var length uint64
	errView := s.db.View(func(txn *badger.Txn) error {
		// 获取长度
		// 通过 listKey 方法生成存储长度信息的键
		lengthItem, err := txn.Get([]byte(s.listKey(key, "length")))
		if err != nil {
			return err
		}
		// 从数据库中获取长度值，并将其复制到 lengthVal 中
		lengthVal, _ := lengthItem.ValueCopy(nil)
		// 将字节切片形式的长度值转换为无符号 64 位整数
		length = helper.BytesToUint64(lengthVal)
		return nil
	})
	return length, errView
}

// store/badger_store.go
func (s *BoltreonStore) listCreate(key string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// 初始化链表元数据
		lengthKey := s.listKey(key, "length")
		if err := txn.Set([]byte(lengthKey), helper.Uint64ToBytes(0)); err != nil {
			return err
		}
		startKey := s.listKey(key, "start")
		if err := txn.Set([]byte(startKey), []byte{}); err != nil {
			return err
		}
		endKey := s.listKey(key, "end")
		return txn.Set([]byte(endKey), []byte{})
	})
}

func (s *BoltreonStore) listGetMeta(keyRedis string) (length uint64, start, end string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		// 获取长度
		lengthItem, errGet := txn.Get([]byte(s.listKey(keyRedis, "length")))
		if errGet != nil {
			return errGet
		}
		lengthVal, errValueCopy := lengthItem.ValueCopy(nil)
		if errValueCopy != nil {
			return errValueCopy
		}
		length = helper.BytesToUint64(lengthVal)

		// 获取起始节点
		startItem, errStart := txn.Get([]byte(s.listKey(keyRedis, "start")))
		if errStart == nil {
			startVal, _ := startItem.ValueCopy(nil)
			start = string(startVal)
		}

		// 获取结束节点
		endItem, errEnd := txn.Get([]byte(s.listKey(keyRedis, "end")))
		if errEnd == nil {
			endVal, _ := endItem.ValueCopy(nil)
			end = string(endVal)
		}
		return nil
	})
	return
}

func (s *BoltreonStore) listUpdateMeta(txn *badger.Txn, key string, length uint64, start, end string) error {
	// 更新长度
	if err := txn.Set([]byte(s.listKey(key, "length")), helper.Uint64ToBytes(length)); err != nil {
		return err
	}
	// 更新起始节点
	if err := txn.Set([]byte(s.listKey(key, "start")), []byte(start)); err != nil {
		return err
	}
	// 更新结束节点
	return txn.Set([]byte(s.listKey(key, "end")), []byte(end))
}

func (s *BoltreonStore) createNode(txn *badger.Txn, key string, value []byte) (string, error) {
	nodeID := uuid.New().String()
	nodeKey := s.listKey(key, nodeID)
	if err := txn.Set([]byte(nodeKey), value); err != nil {
		return "", err
	}
	return nodeID, nil
}

func (s *BoltreonStore) linkNodes(txn *badger.Txn, key string, prevID, nextID string) error {
	// 更新前节点的next指针
	if prevID != "" {
		prevNextKey := s.listKey(key, prevID, "next")
		if err := txn.Set([]byte(prevNextKey), []byte(nextID)); err != nil {
			return err
		}
	}
	// 更新后节点的prev指针
	if nextID != "" {
		nextPrevKey := s.listKey(key, nextID, "prev")
		return txn.Set([]byte(nextPrevKey), []byte(prevID))
	}
	return nil
}

// LPush Redis LPUSH 实现
func (s *BoltreonStore) LPush(key string, values ...string) (isSuccess int, err error) {
	err = s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeList)); err != nil {
			return err
		}
		length, start, end, _ := s.listGetMeta(key)
		for _, value := range values {
			// 创建新节点
			nodeID, err := s.createNode(txn, key, []byte(value))
			if err != nil {
				return err
			}

			// 链接节点
			if length == 0 { // 空链表
				start = nodeID
				end = nodeID
				if err := s.linkNodes(txn, key, nodeID, nodeID); err != nil {
					return err
				}
			} else {
				// 链接新节点和原头节点
				if err := s.linkNodes(txn, key, nodeID, start); err != nil {
					return err
				}
				// 更新原头节点的prev指针
				if err := txn.Set([]byte(s.listKey(key, start, "prev")), []byte(nodeID)); err != nil {
					return err
				}
				start = nodeID
			}
			length++
		}

		// 更新元数据
		err := s.listUpdateMeta(txn, key, length, start, end)
		if err != nil {
			return err
		}
		isSuccess = 1
		return nil
	})
	return isSuccess, err
}

// RPOP 实现
func (s *BoltreonStore) RPop(key string) (string, error) {
	var value string
	err := s.db.Update(func(txn *badger.Txn) error {
		length, start, end, err := s.listGetMeta(key)
		if length == 0 {
			return nil
		}

		// 获取尾节点值
		endNodeKey := s.listKey(key, end)
		item, err := txn.Get([]byte(endNodeKey))
		if err != nil {
			return err
		}
		valueBytes, _ := item.ValueCopy(nil)
		value = string(valueBytes)

		// 获取新的尾节点
		newEndKey := s.listKey(key, end, "prev")
		item, err = txn.Get([]byte(newEndKey))
		if err != nil {
			return err
		}

		newEndVal, _ := item.ValueCopy(nil)
		value = string(newEndVal)
		newEnd := string(newEndVal)

		// 更新链表关系
		if length == 1 {
			start = ""
			newEnd = ""
		} else {
			// 断开旧尾节点连接
			if err := s.linkNodes(txn, key, newEnd, start); err != nil {
				return err
			}
		}

		// 删除旧节点数据
		if err := txn.Delete([]byte(endNodeKey)); err != nil {
			return err
		}
		txn.Delete([]byte(s.listKey(key, end, "prev")))
		txn.Delete([]byte(s.listKey(key, end, "next")))

		// 更新元数据
		return s.listUpdateMeta(txn, key, length-1, start, newEnd)
	})
	return value, err
}

// LLEN 实现
func (s *BoltreonStore) LLen(key string) (uint64, error) {
	length, err := s.listLength(key)
	return length, err
}

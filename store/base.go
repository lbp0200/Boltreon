package store

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

func (s *BadgerStore) Del(key string) error {
	// TODO 需要完善，多个key，返回删除的数量
	bKey := []byte(key)
	bTypeKey := TypeOfKeyGet(key)
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(bTypeKey)
		if err != nil {
			return err
		}
		valCopy, errValueCopy := item.ValueCopy(nil)
		if errValueCopy != nil {
			return errValueCopy
		}
		switch string(valCopy) {
		case KeyTypeString:
			// 处理 type1 的删除逻辑
			return s.DelString(key)
		case KeyTypeList:
			// 处理 type2 的删除逻辑
			return txn.Delete(bKey)
		case KeyTypeHash:
			// 处理 type3 的删除逻辑
			return txn.Delete(bKey)
		case KeyTypeSet:
			// 处理 type4 的删除逻辑
			return txn.Delete(bKey)
		case KeyTypeSortedSet:
			// 处理 type5 的删除逻辑
			return txn.Delete(bKey)
		default:
			// 默认的删除逻辑
			return txn.Delete(bKey)
		}
	})
}

func (s *BadgerStore) DelString(key string) error {
	logFuncTag := "BadgerStoreDelString"
	bKey := []byte(key)
	badgerTypeKey := TypeOfKeyGet(key)
	badgerValueKey := keyBadgerGet(prefixKeyStringBytes, bKey)
	return s.db.Update(func(txn *badger.Txn) error {
		errDel := txn.Delete(badgerTypeKey)
		if errDel != nil {
			return fmt.Errorf("%s,Del Badger Type Key:%v", logFuncTag, errDel)
		}
		errDel = txn.Delete(badgerValueKey)
		if errDel != nil {
			return fmt.Errorf("%s,Del Badger Value Key:%v", logFuncTag, errDel)
		}
		return nil
	})
}

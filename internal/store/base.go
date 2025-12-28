package store

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

func (s *BoltreonStore) Del(key string) error {
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
			if err := deleteByPrefix(txn, []byte(fmt.Sprintf("%s%s", prefixKeySortedSetBytes, key))); err != nil {
				return err
			}
			return txn.Delete(typeKey)
		default:
			return txn.Delete(typeKey)
		}
	})
}

func (s *BoltreonStore) DelString(key string) error {
	logFuncTag := "BoltreonStoreDelString"
	bKey := []byte(key)
	badgerTypeKey := TypeOfKeyGet(key)
	badgerValueKey := s.stringKey(string(bKey))
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

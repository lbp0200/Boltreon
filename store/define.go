package store

import (
	"github.com/dgraph-io/badger/v4"
)

const (
	UnderScore       = "_"
	KeyTypeString    = "STRING"
	KeyTypeList      = "LIST"
	KeyTypeHash      = "HASH"
	KeyTypeSet       = "SET"
	KeyTypeSortedSet = "SORTEDSET"

	sortedSetIndex = "_INDEX_"
	sortedSetData  = "_DATA_"
)

var (
	prefixKeyTypeBytes      = []byte("TYPE_")
	prefixKeyStringBytes    = []byte("STRING_")
	prefixKeyListBytes      = []byte("LIST_")
	prefixKeyHashBytes      = []byte("HASH_")
	prefixKeySetBytes       = []byte("SET_")
	prefixKeySortedSetBytes = []byte("SORTEDSET_")
)

type BadgerStore struct {
	db *badger.DB
}

func NewBadgerStore(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerStore{db: db}, nil
}

func (s *BadgerStore) Close() error {
	return s.db.Close()
}

// TypeOfKeyGet 用于生成存储类型的键
func TypeOfKeyGet(strKey string) []byte {
	bKey := []byte(strKey)
	bKey = append(prefixKeyTypeBytes, bKey...)
	return bKey
}

// keyBadgerGet 用于生成存储键的键
func keyBadgerGet(bType, bKey []byte) []byte {
	bKey = append(bType, bKey...)
	return bKey
}

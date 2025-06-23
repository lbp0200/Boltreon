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

	sortedSetIndex = "_INDEX|"
	sortedSetData  = "_DATA_"
)

var (
	keyTypeBytes    = []byte("TYPE_")
	prefixKeyString = []byte("STRING_")
	prefixKeyList   = []byte("LIST_")
	prefixKeyHash   = []byte("HASH_")
	prefixKeySet    = []byte("SET_")
	prefixKeyZSet   = []byte("SORTEDSET_")
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

func TypeKeyGet(strKey string) []byte {
	bKey := []byte(strKey)
	bKey = append(keyTypeBytes, bKey...)
	return bKey
}

func keyBadgerGet(bType, bKey []byte) []byte {
	bKey = append(bType, bKey...)
	return bKey
}

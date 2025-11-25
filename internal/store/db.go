package store

import "github.com/dgraph-io/badger/v4"

type DB struct {
	db *badger.DB
}

func Open(path string) (*DB, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

func (d *DB) Set(key, value []byte) error {
	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (d *DB) Get(key []byte) ([]byte, error) {
	var val []byte
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			val = append(val[:0], v...)
			return nil
		})
	})
	return val, err
}

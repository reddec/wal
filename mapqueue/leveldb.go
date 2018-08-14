package mapqueue

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type leveldbMap struct {
	db *leveldb.DB
}

func (bdp *leveldbMap) Put(key []byte, value []byte) error {
	return bdp.db.Put(key, value, nil)
}

func (bdp *leveldbMap) Get(key []byte) ([]byte, error) {
	return bdp.db.Get(key, nil)
}

func (bdp *leveldbMap) Del(key []byte) error {
	return bdp.db.Delete(key, nil)
}

func (bdp *leveldbMap) Keys(handler func(key []byte) error) error {
	it := bdp.db.NewIterator(nil, nil)
	defer it.Release()
	if it.Error() != nil {
		return it.Error()
	}
	for it.Next() {
		if it.Error() != nil {
			return it.Error()
		}
		err := handler(it.Key())
		if err != nil {
			return err
		}
	}
	return nil
}
func (bdp *leveldbMap) Close() error { return bdp.db.Close() }

// New storage, base on go-leveldb store
func NewLevelDbMap(location string) (ClosableMap, error) {
	db, err := leveldb.OpenFile(location, nil)
	if err != nil {
		return nil, err
	}
	return &leveldbMap{db: db}, nil
}

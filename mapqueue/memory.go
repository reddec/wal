package mapqueue

import "os"

type memoryMap struct {
	db map[string][]byte
}

func (bdp *memoryMap) Put(key []byte, value []byte) error {
	if bdp.db == nil {
		bdp.db = make(map[string][]byte)
	}
	k := string(key)
	cp := make([]byte, len(value))
	copy(cp, value)
	bdp.db[k] = cp
	return nil
}

func (bdp *memoryMap) Get(key []byte) ([]byte, error) {
	k := string(key)
	value, ok := bdp.db[k]
	if !ok {
		return nil, os.ErrNotExist
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	return value, nil
}

func (bdp *memoryMap) Del(key []byte) error {
	k := string(key)
	delete(bdp.db, k)
	return nil
}

func (bdp *memoryMap) Keys(handler func(key []byte) error) error {
	for k := range bdp.db {
		err := handler([]byte(k))
		if err != nil {
			return err
		}
	}
	return nil
}

// New in-memory storage, based on Go concurrent map. For each Add and Get new copy of key and data will be made.
// The storage is not thread-safe by it self, but due to the guarantees from Queue it's ok
func NewMemoryMap() Map {
	return &memoryMap{}
}

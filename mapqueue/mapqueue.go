package mapqueue

import (
	"github.com/pkg/errors"
	"github.com/reddec/storages"
	"math"
	"strconv"
	"sync"
)

// The error occurred after access to empty queue
var ErrEmpty = errors.New("queue is empty")

// Queue with map-based storage. Thread safe
type Queue struct {
	onCreated Notification
	storage   storages.Storage
	lock      sync.RWMutex
	readId    int64
	writeId   int64
}

// Get notifications manager for new items event
func (q *Queue) OnCreated() *Notification { return &q.onCreated }

// Check is queue empty
func (q *Queue) Empty() bool { return q.readId >= q.writeId }

// Size of queue
func (q *Queue) Size() int64 { return q.writeId - q.readId }

// Put data to the tail of queue
func (q *Queue) Put(data []byte) error {
	q.lock.Lock()
	id := strconv.FormatInt(q.writeId, 10)
	err := q.storage.Put([]byte(id), data)
	if err != nil {
		q.lock.Unlock()
		return err
	}
	q.writeId++
	q.lock.Unlock()
	q.onCreated.notify()
	return nil
}

// Put string to the tail of a queue
func (q *Queue) PutString(data string) error { return q.Put([]byte(data)) }

// Head value of queue
func (q *Queue) Head() ([]byte, error) {
	if q.Empty() {
		return nil, ErrEmpty
	}
	q.lock.RLock()
	defer q.lock.RUnlock()
	id := strconv.FormatInt(q.readId, 10)
	return q.storage.Get([]byte(id))
}

// Get value as string from head
func (q *Queue) HeadString() (string, error) {
	v, err := q.Head()
	return string(v), err
}

// Remove head item from queue
func (q *Queue) Remove() error {
	if q.Empty() {
		return nil
	}
	q.lock.Lock()
	defer q.lock.Unlock()
	id := strconv.FormatInt(q.readId, 10)
	err := q.storage.Del([]byte(id))
	if err != nil {
		return err
	}

	q.readId++
	return nil
}

func NewMapQueue(storage storages.Storage) (*Queue, error) {
	var minVal int64 = math.MaxInt64
	var maxVal int64 = math.MinInt64
	var empty = true
	err := storage.Keys(func(key []byte) error {
		id, err := strconv.ParseInt(string(key), 10, 64)
		if err != nil {
			return err
		}
		if id < minVal {
			minVal = id
		}
		if id > maxVal {
			maxVal = id
		}
		empty = false
		return nil
	})
	if err != nil {
		return nil, err
	}
	if empty {
		minVal = 0
		maxVal = 0
	} else {
		maxVal++ // point to next cell for writing
	}
	return &Queue{storage: storage, writeId: maxVal, readId: minVal}, nil
}

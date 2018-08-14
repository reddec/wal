package mapqueue

import (
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestNewLevelDbMap(t *testing.T) {
	const file = "db"
	defer os.RemoveAll(file)
	storage, err := NewLevelDbMap(file)
	if err != nil {
		t.Error("open storage:", err)
		return
	}
	defer storage.Close()

	queue, err := NewMapQueue(storage)
	if err != nil {
		t.Error("create queue:", err)
		return
	}

	t.Log("queue size:", queue.Size())

	err = queue.PutString("hello 1")
	if err != nil {
		t.Error("put 1:", err)
		return
	}

	err = queue.PutString("hello 2")
	if err != nil {
		t.Error("put 2:", err)
		return
	}

	val, err := queue.HeadString()
	if err != nil {
		t.Error("get 1:", err)
		return
	}
	if val != "hello 1" {
		t.Error("invalid value for 1:", val)
		return
	}

	err = queue.Remove()
	if err != nil {
		t.Error("failed to remove:", err)
		return
	}

	val, err = queue.HeadString()
	if err != nil {
		t.Error("get 2:", err)
		return
	}
	if val != "hello 2" {
		t.Error("invalid value for 2:", val)
		return
	}

	var spam []byte
	for i := 0; i < 1024; i++ {
		spam = append(spam, byte(int('A')+rand.Intn(25)))
	}
	val = string(spam)

	const num = 100000
	begin := time.Now()
	for i := 0; i < num; i++ {
		err = queue.PutString(val)
		if err != nil {
			t.Error("put massive", err)
			return
		}

	}

	end := time.Now()

	tps := num / (float64((end.Sub(begin))) / float64(time.Second))
	t.Log("TPS (write only):", tps)

	begin = time.Now()
	for i := 0; i < num; i++ {
		err = queue.PutString(val)
		if err != nil {
			t.Error("put massive", err)
			return
		}
		_, err = queue.HeadString()
		if err != nil {
			t.Error("get massive", err)
			return
		}
		err = queue.Remove()
		if err != nil {
			t.Error("remove massive", err)
			return
		}
	}
	end = time.Now()

	tps = num / (float64((end.Sub(begin))) / float64(time.Second))
	t.Log("TPS (write/read/commit):", tps)

}

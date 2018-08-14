package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/reddec/wal/mapqueue"
)

func ExampleNew_InMemory() {
	// prepare storage
	storage := mapqueue.NewMemoryMap()
	// nothing to fail in in-memory queue, so error is suppressed
	queue, _ := mapqueue.NewMapQueue(storage)
	// for testing we should control stream
	ctx, stop := context.WithCancel(context.Background())
	// setup attempts for test
	attempts := 2
	// create new stream, setup context and handler and start
	stream := New(queue).Context(ctx).Process(func(ctx context.Context, data []byte) error {
		if attempts == 0 {
			fmt.Println("done")
			stop()
			return nil
		}
		fmt.Println("attempts left", attempts)
		attempts--
		return errors.New("again")
	}).Start()

	// push test message (ignore error due to in-memory map)
	queue.PutString("test")

	<-stream.Done()
	// Output:
	// attempts left 2
	// attempts left 1
	// done
}

func ExampleNew_Persistent() {
	// prepare storage
	storage, err := mapqueue.NewLevelDbMap("./db")
	// for test reason, all errors are panicing. Don't do it in production code!
	if err != nil {
		panic(err)
	}
	// release resources on exit
	defer storage.Close()
	// if storage is corrupted, error may appear
	queue, err := mapqueue.NewMapQueue(storage)
	if err != nil {
		panic(err)
	}

	testFinished := make(chan struct{})

	// create new stream, handler and then start
	stream := New(queue).Process(func(ctx context.Context, data []byte) error {
		fmt.Println("message got", string(data))
		// here usually goes sending code or any other that may produce error
		close(testFinished) // for test only
		return nil
	}).Start()

	// push test message
	err = queue.PutString("test")
	if err != nil {
		panic(err)
	}

	// wait when our handler will be touched
	<-testFinished
	// shutdown stream
	stream.Stop()
	// Output:
	// message got test
}

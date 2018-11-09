# Persistent queue and streaming

[![](https://godoc.org/github.com/reddec/wal?status.svg)](https://godoc.org/github.com/reddec/wal)


[API documentation](https://godoc.org/github.com/reddec/wal)

The main goal of the project is provide simple and convenient way of making local durable queue in applications.

Idea is that if application has an local queue, the transport of messages can be anyone.

![diag](https://user-images.githubusercontent.com/6597086/44100830-7a648f9e-a018-11e8-93da-7bba5e4bab3d.png)

Built-in [storages](https://github.com/reddec/storages): in-memory, leveldb and else...

Built-in processor:

* HTTP client - http client for multiple endpoints with different delivery modes (everyone, at least one)

Built-in strategy:

* Repeat-with-delay - adds delay before new attempt if error appeared after processor
* Ignore - ignore any errors

## Basic usage

See godoc

```go

import (
    "github.com/reddec/wal/mapqueue"
    "github.com/reddec/wal/stream"
    "context"
)

func start(globalCtx context.Context) error {
    // prepare storage
	storage, err := mapqueue.NewLevelDbMap("./db")
	if err != nil {
		return error
	}
	// release resources on exit
	defer storage.Close()
	// if storage is corrupted, error may appear
	queue, err := mapqueue.NewMapQueue(storage)
	if err != nil {
		return error
	}

	// create new stream, handler and then start
	sendStream := stream.New(queue).
    		Context(globalCtx).
    		StdLog("[stream] ").
    		Process(func(ctx context.Context, data []byte) error {
    			// todo: here usually goes sending code or any other that may produce error
    			return nil
    		}).Start()

    // wait while will finish
    return <-sendStream.Done()
}
```

## HTTP delivery

See cmd

Supports modes:

* Everyone - will fail if even one request failed
* At-least-one - will pass if even one request was successful
* At-most-one - will randomize urls and try one-by-one till first successful request, otherwise failed

```go
import (
    "github.com/reddec/wal/mapqueue"
    "github.com/reddec/wal/stream"
    "github.com/reddec/wal/processor"
    "github.com/reddec/storages/leveldbstorage"
    "context"
)

func start(globalCtx context.Context) error {
    // prepare storage
	storage, err := leveldbstorage.New("./db")
	if err != nil {
		return error
	}
	// release resources on exit
	defer storage.Close()

	// if storage is corrupted, error may appear
	queue, err := mapqueue.NewMapQueue(storage)
	if err != nil {
		return error
	}

	// setup destinations
	output := processor.NewHttpClient("http://example.com/", "http://serve.org/").Build()

	// create new stream, handler and then start
	sendStream := stream.New(queue).
    		Context(globalCtx).
    		StdLog("[stream] ").
    		Handle(output).Start()
    defer sendStream.Stop()

    // todo: do some work
    for i:=0; i<100; i++{
        err = queue.PutString("hello world")
        if err!= nil {
            return err
        }
    }
    // ...

    return nil
}
```

# CLI generators


## typedqueue

Generates typed queue.

Usage:

    Usage of typedqueue:
      -out string
            Output file (default: <type name>_storage.go)
      -package string
            Output package (default: same as in input file)
      -type string
            Type name to wrap


Embedded usage example:

```go

type Sample struct {
    // ...
}

//go:generate typedqueue -type Sample

```

will produce (methods body omitted)

```go

// Typed queue for Sample
type SampleQueue struct {
	queue *mapqueue.Queue
}

// Creates new queue for Sample
func NewSampleQueue(queue *mapqueue.Queue) *SampleQueue {}

// Put single Sample encoded in JSON into queue
func (cs *SampleQueue) Put(item *Sample) error {}

// Peek single Sample from queue and decode data as JSON
func (cs *SampleQueue) Head() (*Sample, error) {}

// Base (underline) queue
func (cs *SampleQueue) Base() *mapqueue.Queue {}

```
package stream

import (
	"context"
	"github.com/reddec/wal/mapqueue"
	"github.com/reddec/wal/strategy"
	"io/ioutil"
	"log"
	"os"
	"time"
)

type ConsumerFunc func(ctx context.Context, num int, data []byte) error

type StreamConfig struct {
	queue    *mapqueue.Queue
	handlers []StreamHandler
	strategy strategy.FinishStrategy
	logger   Logger
	ctx      context.Context
}

type StreamHandler func(ctx context.Context, data []byte) error

func New(queue *mapqueue.Queue) *StreamConfig {
	return &StreamConfig{
		queue:    queue,
		ctx:      context.Background(),
		strategy: strategy.Delay(5*time.Second, 3*time.Second),
		logger:   log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

func (sc *StreamConfig) Logger(logger Logger) *StreamConfig {
	sc.logger = logger
	return sc
}

func (sc *StreamConfig) Context(ctx context.Context) *StreamConfig {
	sc.ctx = ctx
	return sc
}

func (sc *StreamConfig) StdLog(prefix string) *StreamConfig {
	return sc.Logger(log.New(os.Stderr, prefix, log.LstdFlags))
}

func (sc *StreamConfig) Process(handler StreamHandler) *StreamConfig {
	sc.handlers = append(sc.handlers, handler)
	return sc
}

func (sc *StreamConfig) Strategy(strategy strategy.FinishStrategy) *StreamConfig {
	sc.strategy = strategy
	return sc
}

func (sc *StreamConfig) Start() *Stream {
	child, stop := context.WithCancel(sc.ctx)
	stream := &Stream{cfg: *sc, done: make(chan error, 1), stop: stop}
	stream.start(child)
	return stream
}

type Stream struct {
	cfg  StreamConfig
	stop func()
	done chan error
}

func (s *Stream) Done() <-chan error {
	return s.done
}

func (s *Stream) Stop() {
	s.stop()
	<-s.done
}

func (s *Stream) start(ctx context.Context) {
	// run once!
	go func() {
		defer close(s.done)
		s.done <- s.run(ctx)
	}()
}

func (s *Stream) run(ctx context.Context) error {
	sub := s.cfg.queue.OnCreated().Subscribe()
	defer sub.Close()
LOOP:
	for {
		processed, err := s.processNotification(ctx)
		if err != nil {
			return err
		}
		if !processed {
			select {
			case <-ctx.Done():
				break LOOP
			case <-sub.Wait():

			}
		}
	}
	return nil
}

func (s *Stream) processNotification(ctx context.Context) (bool, error) {
	var handlerErr error
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:

		}
		if len(s.cfg.handlers) == 0 {
			return false, nil
		}
		if s.cfg.queue.Empty() {
			return false, nil
		}
		data, err := s.cfg.queue.Head()
		if err != nil {
			s.cfg.logger.Println("failed get head from queue:", err)
			return false, err
		}

		for i, h := range s.cfg.handlers {
			handlerErr = h(ctx, data)
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			default:

			}
			if handlerErr != nil {
				s.cfg.logger.Println("handler", i, ":", handlerErr, "(queue size", s.cfg.queue.Size(), ")")
				break
			}
		}
		if s.cfg.strategy != nil {
			handlerErr = s.cfg.strategy.Done(ctx, handlerErr)
		}
		if handlerErr != nil {
			continue
		}
		err = s.cfg.queue.Remove()
		if err != nil {
			s.cfg.logger.Println("failed commit:", err)
			return false, err
		}
		break
	}
	return true, nil
}

type Logger interface {
	Println(...interface{})
}

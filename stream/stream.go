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

// Stream configuration builder
type StreamConfig struct {
	queue    *mapqueue.Queue
	handlers []StreamHandlerFunc
	strategy strategy.FinishStrategy
	logger   Logger
	ctx      context.Context
}

// Function that processing package
type StreamHandlerFunc func(ctx context.Context, data []byte) error

type StreamHandler interface {
	// Handle incoming message
	Handle(ctx context.Context, data []byte) error
}

// New stream builder. Builder should not be used after final method (Start()).
// Default parameters is: delay strategy (5s retry and 3s jitter), no logging and background context
func New(queue *mapqueue.Queue) *StreamConfig {
	return &StreamConfig{
		queue:    queue,
		ctx:      context.Background(),
		strategy: strategy.Delay(5*time.Second, 3*time.Second),
		logger:   log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

// Set logger for stream
func (sc *StreamConfig) Logger(logger Logger) *StreamConfig {
	sc.logger = logger
	return sc
}

// Set default Golang logger to STDERR with defined prefix
func (sc *StreamConfig) StdLog(prefix string) *StreamConfig {
	return sc.Logger(log.New(os.Stderr, prefix, log.LstdFlags))
}

// Set context for stream. By default - background context
func (sc *StreamConfig) Context(ctx context.Context) *StreamConfig {
	sc.ctx = ctx
	return sc
}

// Set processor. Multiple processor will be invoked sequentially as defined if no error occurred.
func (sc *StreamConfig) Process(handler StreamHandlerFunc) *StreamConfig {
	sc.handlers = append(sc.handlers, handler)
	return sc
}

// Set processor object. Multiple processor will be invoked sequentially as defined if no error occurred.
func (sc *StreamConfig) Handle(handler StreamHandler) *StreamConfig {
	return sc.Process(handler.Handle)
}

// Set finalizing strategy. By default - delay (5s retry on retry with 3s jitter). Can be nil.
// If strategy returns nil, message is committed otherwise repeated without delay.
func (sc *StreamConfig) Strategy(strategy strategy.FinishStrategy) *StreamConfig {
	sc.strategy = strategy
	return sc
}

// Initialize and start stream. Builder should be no modified after calling this method
func (sc *StreamConfig) Start() *Stream {
	child, stop := context.WithCancel(sc.ctx)
	stream := &Stream{cfg: *sc, done: make(chan error, 1), stop: stop}
	stream.start(child)
	return stream
}

// Processing stream
type Stream struct {
	cfg  StreamConfig
	stop func()
	done chan error
}

// Done channel. Once finished, channel will be closed
func (s *Stream) Done() <-chan error {
	return s.done
}

// Stop stream and wait for finish. Can be called multiple times
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

// General logger interface
type Logger interface {
	// Print items in line
	Println(...interface{})
}

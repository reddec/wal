package main

import (
	"bytes"
	"context"
	"github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	"github.com/reddec/wal/mapqueue"
	"github.com/reddec/wal/stream"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

type HttpStream struct {
	Delay     time.Duration `yaml:"delay"             long:"delay"   env:"DELAY"             description:"delay between attempts" default:"5s"`
	Jitter    time.Duration `yaml:"jitter"            long:"jitter"  env:"JITTER"            description:"jitter for delay" default:"2s"`
	URLs      []string      `yaml:"urls"    short:"u" long:"url"     env:"URL" env-delim:"," description:"target urls"`
	Timeout   time.Duration `yaml:"timeout" short:"t" long:"timeout" env:"TIMEOUT"           description:"connection timeout" default:"20s"`
	Method    string        `yaml:"method"  short:"m" long:"method"  env:"METHOD"            description:"HTTP method for request" default:"POST"`
	Success   int           `yaml:"success" short:"s" long:"success" env:"SUCCESS"           description:"HTTP success code" default:"200"`
	Bind      string        `yaml:"bind"    short:"b" long:"bind"    env:"BIND"              description:"Binding address" default:"localhost:9876"`
	QueueFile string        `yaml:"file"    short:"q" long:"queue"   env:"QUEUE"             description:"queue file name" default:"queue.dat"`
	client    *http.Client
}

func (st *HttpStream) dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, st.Timeout)
}

func (st *HttpStream) sendData(ctx context.Context, block []byte) error {
	child, cancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	var errs = make([]error, len(st.URLs))
	for i, url := range st.URLs {
		wg.Add(1)
		go func(i int, url string) {
			defer wg.Done()
			err := st.requestUrl(child, url, block)
			if err != nil {
				errs[i] = err
				cancel()
			}
		}(i, url)
	}
	wg.Wait()
	cancel()

	var errMessages []string
	for _, err := range errs {
		if err != nil {
			errMessages = append(errMessages, err.Error())
		}
	}
	if errMessages != nil {
		return errors.New(strings.Join(errMessages, "; "))
	}

	return nil
}

func (st *HttpStream) requestUrl(ctx context.Context, url string, block []byte) error {
	req, err := http.NewRequest(st.Method, url, bytes.NewBuffer(block))
	if err != nil {
		panic(err)
	}
	req = req.WithContext(ctx)
	req.ContentLength = int64(len(block))

	res, err := st.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(ioutil.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != st.Success {
		return errors.Errorf("non-success code: %v %v", res.StatusCode, res.Status)
	}
	return nil
}

func signalContext() context.Context {
	parent := context.Background()
	ctx, closer := context.WithCancel(parent)
	go func() {
		c := make(chan os.Signal, 2)
		signal.Notify(c, os.Kill, os.Interrupt)
		for range c {
			closer()
			break
		}
	}()
	return ctx
}

func main() {
	st := &HttpStream{}
	_, err := flags.Parse(st)
	if err != nil {
		os.Exit(1)
	}
	log.SetPrefix("[main] ")
	transport := &http.Transport{
		Dial: st.dialTimeout,
	}
	st.client = &http.Client{
		Transport: transport,
	}

	storage, err := mapqueue.NewLevelDbMap(st.QueueFile)
	if err != nil {
		panic(err)
	}

	defer storage.Close()
	queue, err := mapqueue.NewMapQueue(storage)

	if err != nil {
		panic(queue)
	}

	ctx, cancel := context.WithCancel(signalContext())

	str := stream.New(queue).Context(ctx).StdLog("[stream] ").Process(st.sendData).Strategy(stream.Delay(st.Delay, st.Jitter)).Start()

	serverDone := make(chan error, 1)
	srv := http.Server{Addr: st.Bind}
	mux := http.NewServeMux()
	srv.Handler = mux

	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		data, err := ioutil.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		err = queue.Put(data)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})

	log.Println("server available on", st.Bind)
	go func() { serverDone <- srv.ListenAndServe(); close(serverDone) }()

	select {
	case <-ctx.Done():
		log.Println("application interrupted by signal")
	case <-str.Done():
		log.Println("stream processor stopped")
	case err := <-serverDone:
		log.Println("http server stopped. err:", err)
	}

	cancel()
	ctxShutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	srv.Shutdown(ctxShutdown)
	<-serverDone
	<-ctx.Done()
	<-str.Done()
	log.Println("finished")
}

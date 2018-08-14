package main

import (
	"context"
	"github.com/jessevdk/go-flags"
	"github.com/reddec/wal/mapqueue"
	"github.com/reddec/wal/processor"
	"github.com/reddec/wal/strategy"
	"github.com/reddec/wal/stream"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	output := processor.NewHttpClient(st.URLs...).Timeout(st.Timeout).Method(st.Method).Success(st.Success).Build()

	str := stream.New(queue).Context(ctx).StdLog("[stream] ").Handle(output).Strategy(strategy.Delay(st.Delay, st.Jitter)).Start()

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

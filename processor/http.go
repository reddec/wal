package processor

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
	"github.com/reddec/wal/stream"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Delivery mode for HTTP client
type HttpClientMode int

const (
	// Must delivery at least to one url
	AtLeastOne HttpClientMode = 1
	// Must delivery to all urls
	Everyone = 2
	// Must delivery only once
	AtMostOnce = 3
)

// HTTP client configuration builder
type HttpProcessorConfig struct {
	urls              []string
	connectionTimeout time.Duration
	method            string
	success           int
	mode              HttpClientMode
	customClient      *http.Client
}

// Create new http client with default mode EVERYONE, connection timeout 20s, expected success code 200 (OK) and
// request method POST
func NewHttpClient(urls ...string) *HttpProcessorConfig {
	return &HttpProcessorConfig{
		urls:              urls,
		mode:              Everyone,
		connectionTimeout: 20 * time.Second,
		success:           http.StatusOK,
		method:            http.MethodPost,
	}
}

// Add urls to request list
func (htpc *HttpProcessorConfig) Url(url ...string) *HttpProcessorConfig {
	htpc.urls = append(htpc.urls, url...)
	return htpc
}

// Connection timeout. By default 20s. Request timeout handles by context
func (htpc *HttpProcessorConfig) Timeout(connectionTimeout time.Duration) *HttpProcessorConfig {
	htpc.connectionTimeout = connectionTimeout
	return htpc
}

// Request method. By default is POST
func (htpc *HttpProcessorConfig) Method(httpMethod string) *HttpProcessorConfig {
	htpc.method = httpMethod
	return htpc
}

// Expected success response code. By default 200 (HTTP OK)
func (htpc *HttpProcessorConfig) Success(httpCode int) *HttpProcessorConfig {
	htpc.success = httpCode
	return htpc
}

// Delivery mode. By default EVERYONE (if at least one failed, send will be repeated)
func (htpc *HttpProcessorConfig) Mode(deliveryMode HttpClientMode) *HttpProcessorConfig {
	htpc.mode = deliveryMode
	return htpc
}

// Custom client. If defined connection timeout is ignored
func (htpc *HttpProcessorConfig) Client(httpClient *http.Client) *HttpProcessorConfig {
	htpc.customClient = httpClient
	return htpc
}

// Build HTTP client handler for stream
func (htpc *HttpProcessorConfig) Build() stream.StreamHandler {
	client := htpc.customClient
	if client == nil {
		transport := &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				return net.DialTimeout(network, addr, htpc.connectionTimeout)
			},
		}
		client = &http.Client{
			Transport: transport,
		}
	}
	return &httpProcessor{cfg: *htpc, client: client}
}

type httpProcessor struct {
	client *http.Client
	cfg    HttpProcessorConfig
}

func (htp *httpProcessor) Handle(ctx context.Context, data []byte) error {
	if htp.cfg.mode == AtMostOnce {
		return htp.shuffleSequentialSend(ctx, data)
	} else {
		return htp.massiveSend(ctx, data)
	}
}

func (htp *httpProcessor) shuffleSequentialSend(ctx context.Context, data []byte) error {
	var errMessages []string
	cp := make([]string, len(htp.cfg.urls))
	copy(cp, htp.cfg.urls)
	rand.Shuffle(len(cp), func(i, j int) {
		cp[i], cp[j] = cp[j], cp[i]
	})
	for _, url := range cp {
		err := htp.requestUrl(ctx, url, data)
		if err != nil {
			errMessages = append(errMessages, err.Error())
		} else {
			return nil
		}
	}
	return errors.New(strings.Join(errMessages, "; "))
}

func (htp *httpProcessor) massiveSend(ctx context.Context, data []byte) error {
	child, cancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	var errs = make([]error, len(htp.cfg.urls))
	for i, url := range htp.cfg.urls {
		wg.Add(1)
		go func(i int, url string) {
			defer wg.Done()
			err := htp.requestUrl(child, url, data)
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
	if errMessages == nil {
		return nil
	}
	switch htp.cfg.mode {
	case AtLeastOne:
		if len(errMessages) != len(errs) {
			// at least one
			return nil
		}
	}
	return errors.New(strings.Join(errMessages, "; "))
}
func (htp *httpProcessor) requestUrl(ctx context.Context, url string, block []byte) error {
	req, err := http.NewRequest(htp.cfg.method, url, bytes.NewBuffer(block))
	if err != nil {
		panic(err)
	}
	req = req.WithContext(ctx)
	req.ContentLength = int64(len(block))

	res, err := htp.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(ioutil.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != htp.cfg.success {
		return errors.Errorf("%v: non-success code: %v %v", url, res.StatusCode, res.Status)
	}
	return nil
}

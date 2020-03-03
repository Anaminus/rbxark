package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/robloxapi/rbxdump/histlog"
	"golang.org/x/time/rate"
)

// sanitizeBaseURL ensure that a given URL is a base URL.
func sanitizeBaseURL(u string) string {
	return strings.TrimRight(u, "/")
}

func buildFileURL(server, hash, file string) string {
	return sanitizeBaseURL(server) + "/" + hash + "-" + file
}

type job struct {
	req    *http.Request
	finish chan result
}

type result struct {
	resp *http.Response
	err  error
}

// Fetcher is used to make HTTP requests.
type Fetcher struct {
	config  *Config
	client  *http.Client
	limiter *rate.Limiter
	request chan job
}

func NewFetcher(config *Config, client *http.Client, workers int) *Fetcher {
	if client == nil {
		client = http.DefaultClient
	}
	if workers <= 0 {
		workers = 32
	}
	state := Fetcher{
		config:  config,
		client:  client,
		limiter: rate.NewLimiter(rate.Limit(config.RateLimit), 1),
		request: make(chan job, workers),
	}
	for i := 0; i < workers; i++ {
		go state.spawnWorker()
	}
	return &state
}

func (f *Fetcher) spawnWorker() {
	for job := range f.request {
		if err := f.limiter.Wait(job.req.Context()); err != nil {
			job.finish <- result{resp: nil, err: err}
			continue
		}
		resp, err := f.client.Do(job.req)
		job.finish <- result{resp: resp, err: err}
	}
}

// Client returns the underlying client used to make requests.
func (f *Fetcher) Client() *http.Client {
	return f.client
}

// Do makes an HTTP request through the fetchers's client and rate limiter.
func (f *Fetcher) Do(req *http.Request) (resp *http.Response, err error) {
	finish := make(chan result)
	f.request <- job{req: req, finish: finish}
	result := <-finish
	return result.resp, result.err
}

func (f *Fetcher) FetchDeployHistory(ctx context.Context, server string) (stream histlog.Stream, err error) {
	file := f.config.GetDeployHistory()
	url := sanitizeBaseURL(server) + "/" + file
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := f.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", url, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: status %s", url, resp.Status)
	}
	var buf bytes.Buffer
	_, err = buf.ReadFrom(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("%s: read response: %w", url, err)
	}
	stream = histlog.Lex(buf.Bytes())
	return stream, nil
}

func (f *Fetcher) FetchHeaders(ctx context.Context, server, hash, file string) (status int, headers http.Header, err error) {
	url := buildFileURL(server, hash, file)
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := f.Do(req)
	if err != nil {
		return 0, nil, err
	}
	resp.Body.Close()
	return resp.StatusCode, resp.Header, nil
}

func (f *Fetcher) FetchFile(ctx context.Context, w io.Writer, server, hash, file string) (err error) {
	url := buildFileURL(server, hash, file)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	resp, err := f.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return fmt.Errorf("%s: status %s", url, resp.Status)
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

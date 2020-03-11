package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/anaminus/rbxark/objects"
	"github.com/robloxapi/rbxdump/histlog"
	"golang.org/x/time/rate"
)

type HashChecker struct {
	h map[string]struct{}
	m sync.Mutex
}

// Check returns whether the given hash is in the map. If it isn't, the hash is
// added to the map.
func (h *HashChecker) Check(hash string) bool {
	if h == nil {
		return false
	}
	h.m.Lock()
	defer h.m.Unlock()
	if h.h == nil {
		h.h = map[string]struct{}{}
	}
	_, ok := h.h[hash]
	if ok {
		return true
	}
	h.h[hash] = struct{}{}
	return false
}

type job struct {
	req    *http.Request
	finish chan<- RequestResult
}

type RequestResult struct {
	Resp *http.Response
	Err  error
}

type chanRequestResult <-chan RequestResult

func (ch chanRequestResult) Get() (resp *http.Response, err error) {
	result := <-ch
	return result.Resp, result.Err
}

// Fetcher is used to make HTTP requests.
type Fetcher struct {
	client  *http.Client
	limiter *rate.Limiter
	request chan job
	workers int
}

func NewFetcher(client *http.Client, workers int, rateLimit float64) *Fetcher {
	if client == nil {
		client = http.DefaultClient
	}

	if workers <= 0 {
		workers = 32
	}
	var rl rate.Limit
	if rateLimit < 0 {
		rl = rate.Inf
	} else {
		rl = rate.Limit(rateLimit)
	}
	state := Fetcher{
		client:  client,
		limiter: rate.NewLimiter(rl, 1),
		request: make(chan job, workers),
		workers: workers,
	}
	for i := 0; i < workers; i++ {
		go state.spawnWorker()
	}
	return &state
}

func (f *Fetcher) Workers() int {
	return f.workers
}

func (f *Fetcher) spawnWorker() {
	for job := range f.request {
		if err := f.limiter.Wait(job.req.Context()); err != nil {
			job.finish <- RequestResult{Resp: nil, Err: err}
			continue
		}
		resp, err := f.client.Do(job.req)
		job.finish <- RequestResult{Resp: resp, Err: err}
	}
}

// Client returns the underlying client used to make requests.
func (f *Fetcher) Client() *http.Client {
	return f.client
}

// Do makes an HTTP request through the fetchers's client and rate limiter.
func (f *Fetcher) Do(req *http.Request) (resp *http.Response, err error) {
	finish := make(chan RequestResult)
	f.request <- job{req: req, finish: finish}
	result := <-finish
	return result.Resp, result.Err
}

// FetchDeployHistory retrieves and parses a history log from the given server.
func (f *Fetcher) FetchDeployHistory(ctx context.Context, url string) (stream histlog.Stream, err error) {
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

// FetchContent fetches information about a file from url. If w is not nil, the
// content of the file is written to it. Otherwise, just the headers of the
// response are returned.
func (f *Fetcher) FetchContent(ctx context.Context, url string, objpath string, hashes *HashChecker, w io.Writer) (status int, headers http.Header, err error) {
	method := "GET"
	if w == nil {
		method = "HEAD"
	}
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("make request: %w", err)
	}
	resp, err := f.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("do request: %w", err)
	}
	if w == nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return resp.StatusCode, resp.Header, nil
	}
	if hash := objects.HashFromETag(resp.Header.Get("etag")); hash != "" {
		if hashes.Check(hash) {
			// A file with the same hash is already being downloaded; skip.
			resp.Body.Close()
			return resp.StatusCode, resp.Header, nil
		}
		if objpath != "" {
			if objects.Exists(objpath, hash) {
				// The hash was found in the cache; download can be skipped.
				resp.Body.Close()
				return resp.StatusCode, resp.Header, nil
			}
		}
	}
	if _, err = io.Copy(w, resp.Body); err != nil {
		return 0, nil, fmt.Errorf("%s: write file: %w", url, err)
	}
	return resp.StatusCode, resp.Header, nil
}

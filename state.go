package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/robloxapi/rbxdump/histlog"
)

// sanitizeBaseURL ensure that a given URL is a base URL.
func sanitizeBaseURL(u string) string {
	return strings.TrimRight(u, "/")
}

type State struct {
	Client *http.Client
	Config *Config
}

func (s *State) GetClient() *http.Client {
	if s.Client == nil {
		return http.DefaultClient
	}
	return s.Client
}

func (s *State) FetchDeployHistory(ctx context.Context, server string) (stream histlog.Stream, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	client := s.GetClient()
	file := s.Config.GetDeployHistory()
	url := sanitizeBaseURL(server) + "/" + file
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
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

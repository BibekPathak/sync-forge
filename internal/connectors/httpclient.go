package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Client is a small HTTP client shared by provider connectors. It paces
// requests through an optional token bucket and classifies non-2xx responses
// into typed connector errors (rate limit, auth, transient, permanent) so the
// retry engine can act appropriately.
type Client struct {
	BaseURL    string
	Token      string
	HTTP       *http.Client
	UserAgent  string
	HeaderAuth string // "Bearer" or "Token"; empty means no auth header
	Limit      *Limiter
}

func NewClient(baseURL, token string, timeout time.Duration) *Client {
	return &Client{
		BaseURL:   baseURL,
		Token:     token,
		HTTP:      &http.Client{Timeout: timeout},
		UserAgent: "syncforge/0.1",
	}
}

// NewRateLimitedClient builds a client that paces requests to perMinute
// requests using a token bucket. Mirrors the provider's documented API limit.
func NewRateLimitedClient(baseURL, token string, timeout time.Duration, perMinute int) *Client {
	c := NewClient(baseURL, token, timeout)
	if perMinute > 0 {
		c.Limit = NewLimiter(perMinute)
	}
	return c
}

// Do performs a request against the provider API. out, if non-nil, is the JSON
// destination for a 2xx response body.
func (c *Client) Do(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return NewError(ErrSchema, "marshal request body", err)
		}
		rdr = bytes.NewReader(b)
	}

	// Client-side rate limiting: preempt provider-side limits by pacing
	// requests through the per-connector token bucket.
	if err := c.Limit.Wait(ctx); err != nil {
		return NewError(ErrUnknown, "rate limit wait: context cancelled", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rdr)
	if err != nil {
		return NewError(ErrUnknown, "build request", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	if c.HeaderAuth != "" && c.Token != "" {
		req.Header.Set("Authorization", c.HeaderAuth+" "+c.Token)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return NewError(ErrTransient, "http request", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out != nil {
			dec := json.NewDecoder(io.LimitReader(resp.Body, 8<<20))
			if err := dec.Decode(out); err != nil {
				return NewError(ErrSchema, "decode response body", err)
			}
		}
		return nil
	}

	return classifyError(resp)
}

func classifyError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	msg := string(body)
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		ra := time.Duration(0)
		if v := resp.Header.Get("Retry-After"); v != "" {
			if sec, err := strconv.Atoi(v); err == nil {
				ra = time.Duration(sec) * time.Second
			}
		}
		return &Error{Kind: ErrRateLimited, Message: "provider rate limited: " + msg, RetryAfter: ra}
	case http.StatusUnauthorized, http.StatusForbidden:
		return NewError(ErrAuth, "provider auth failed: "+msg, nil)
	case http.StatusNotFound:
		return NewError(ErrNotFound, "resource not found: "+msg, nil)
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return NewError(ErrSchema, "provider rejected request: "+msg, nil)
	case http.StatusConflict:
		return NewError(ErrConflict, "provider reported conflict: "+msg, nil)
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return NewError(ErrTransient, "provider transient error: "+msg, nil)
	default:
		return NewError(ErrUnknown, fmt.Sprintf("unexpected status %d: %s", resp.StatusCode, msg), nil)
	}
}

// Package dav is a minimal WebDAV client for Nextcloud (PROPFIND + GET only).
package dav

import (
	"context"
	"crypto/tls"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ErrAuth is returned on HTTP 401.
var ErrAuth = errors.New("authentication failed (401): check NC_USER and use an APP PASSWORD (not your login password or a TOTP code)")

// StatusError is an unexpected HTTP status.
type StatusError struct {
	Method string
	Path   string
	Code   int
	Status string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s %q: unexpected status %s", e.Method, e.Path, e.Status)
}

// Entry describes one remote file or directory. Path is relative to the sync
// root, without leading or trailing slash ("" is the root itself).
type Entry struct {
	Path      string `json:"p"`
	IsDir     bool   `json:"d,omitempty"`
	ETag      string `json:"e"`
	Size      int64  `json:"s"` // -1 if the server did not report it
	MTime     int64  `json:"m,omitempty"`
	Checksum  string `json:"c,omitempty"` // raw oc:checksums, e.g. "SHA1:abc.. MD5:def.."
	FileID    string `json:"i,omitempty"`
	Perms     string `json:"pm,omitempty"`
	Encrypted bool   `json:"enc,omitempty"`
}

// Options configure the client.
type Options struct {
	BaseURL     string // https://cloud.example.com
	User        string
	Password    string
	RemotePath  string // optional sub-folder
	Conns       int    // expected parallelism (sizes the idle connection pool)
	IdleTimeout time.Duration
	InsecureTLS bool
}

// Client talks to /remote.php/dav/files/<user>/.
type Client struct {
	hc       *http.Client
	root     string // full URL of the sync root, no trailing slash
	rootPath string // decoded URL path of the sync root
	user, pw string
	idle     time.Duration
	backoff  time.Duration
	attempts int
}

// New builds a client.
func New(o Options) (*Client, error) {
	if o.Conns < 1 {
		o.Conns = 4
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = 60 * time.Second
	}
	root := strings.TrimRight(o.BaseURL, "/") + "/remote.php/dav/files/" + url.PathEscape(o.User)
	if o.RemotePath != "" {
		root += "/" + escapePath(o.RemotePath)
	}
	ru, err := url.Parse(root)
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          o.Conns * 2,
		MaxIdleConnsPerHost:   o.Conns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
	if o.InsecureTLS {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit opt-in
	}
	return &Client{
		hc:       &http.Client{Transport: tr}, // no global timeout (large files); idle timeout is used instead
		root:     root,
		rootPath: ru.Path,
		user:     o.User,
		pw:       o.Password,
		idle:     o.IdleTimeout,
		backoff:  time.Second,
		attempts: 4,
	}, nil
}

func escapePath(rel string) string {
	segs := strings.Split(rel, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

func (c *Client) urlFor(rel string) string {
	if rel == "" {
		return c.root + "/"
	}
	return c.root + "/" + escapePath(rel)
}

// idleBody cancels the request if a single Read blocks for longer than idle.
type idleBody struct {
	io.ReadCloser
	timer  *time.Timer
	cancel context.CancelFunc
	idle   time.Duration
}

func (b *idleBody) Read(p []byte) (int, error) {
	b.timer.Reset(b.idle)
	n, err := b.ReadCloser.Read(p)
	b.timer.Stop()
	return n, err
}

func (b *idleBody) Close() error {
	b.timer.Stop()
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

func retryAfter(resp *http.Response) time.Duration {
	if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
		if s > 60 {
			s = 60
		}
		return time.Duration(s) * time.Second
	}
	return 0
}

// do performs a request, retrying on network errors, 429 and 502-504.
func (c *Client) do(ctx context.Context, method, rel string, hdr map[string]string, body string) (*http.Response, error) {
	var lastErr error
	delay := c.backoff
	for attempt := 1; attempt <= c.attempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
		}
		rctx, cancel := context.WithCancel(ctx)
		timer := time.AfterFunc(c.idle, cancel) // covers connect + waiting for headers
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		req, err := http.NewRequestWithContext(rctx, method, c.urlFor(rel), rd)
		if err != nil {
			timer.Stop()
			cancel()
			return nil, err
		}
		req.SetBasicAuth(c.user, c.pw)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := c.hc.Do(req)
		timer.Stop()
		if err != nil {
			cancel()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
			continue
		}
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			resp.Body.Close()
			cancel()
			return nil, ErrAuth
		case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			lastErr = fmt.Errorf("%s %q: %s", method, rel, resp.Status)
			if ra := retryAfter(resp); ra > delay {
				delay = ra
			}
			resp.Body.Close()
			cancel()
			continue
		}
		resp.Body = &idleBody{ReadCloser: resp.Body, timer: timer, cancel: cancel, idle: c.idle}
		return resp, nil
	}
	return nil, fmt.Errorf("%s %q failed after %d attempts: %w", method, rel, c.attempts, lastErr)
}

const propfindBody = `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns"><d:prop>
<d:getetag/><d:getcontentlength/><d:getlastmodified/><d:resourcetype/>
<oc:checksums/><oc:fileid/><oc:permissions/><nc:is-encrypted/>
</d:prop></d:propfind>`

type multistatus struct {
	Responses []struct {
		Href      string `xml:"href"`
		Propstats []struct {
			Status string `xml:"status"`
			Prop   struct {
				ETag         string `xml:"getetag"`
				Length       string `xml:"getcontentlength"`
				Modified     string `xml:"getlastmodified"`
				ResourceType struct {
					Collection *struct{} `xml:"collection"`
				} `xml:"resourcetype"`
				Checksums string `xml:"checksums>checksum"`
				FileID    string `xml:"fileid"`
				Perms     string `xml:"permissions"`
				Encrypted string `xml:"is-encrypted"`
			} `xml:"prop"`
		} `xml:"propstat"`
	} `xml:"response"`
}

func statusOK(s string) bool {
	f := strings.Fields(s)
	return len(f) >= 2 && f[1] == "200"
}

// relFromHref converts a multistatus href into a root-relative decoded path.
func (c *Client) relFromHref(href string) (string, bool) {
	u, err := url.Parse(href)
	if err != nil {
		return "", false
	}
	p := strings.Trim(u.Path, "/")
	base := strings.Trim(c.rootPath, "/")
	switch {
	case p == base:
		return "", true
	case strings.HasPrefix(p, base+"/"):
		return p[len(base)+1:], true
	}
	return "", false
}

// List runs PROPFIND with depth 0 or 1. The entry for rel itself is included
// (Path == rel).
func (c *Client) List(ctx context.Context, rel string, depth int) ([]Entry, error) {
	resp, err := c.do(ctx, "PROPFIND", rel, map[string]string{
		"Depth":        strconv.Itoa(depth),
		"Content-Type": "application/xml; charset=utf-8",
	}, propfindBody)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMultiStatus {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, &StatusError{"PROPFIND", rel, resp.StatusCode, resp.Status}
	}
	var ms multistatus
	if err := xml.NewDecoder(resp.Body).Decode(&ms); err != nil {
		return nil, fmt.Errorf("PROPFIND %q: bad XML: %w", rel, err)
	}
	out := make([]Entry, 0, len(ms.Responses))
	for _, r := range ms.Responses {
		p, ok := c.relFromHref(r.Href)
		if !ok {
			continue
		}
		e := Entry{Path: p, Size: -1}
		got := false
		for _, ps := range r.Propstats {
			if !statusOK(ps.Status) {
				continue
			}
			got = true
			pr := ps.Prop
			if t := strings.TrimSpace(pr.ETag); t != "" {
				e.ETag = strings.Trim(strings.TrimPrefix(t, "W/"), `"`)
			}
			if pr.ResourceType.Collection != nil {
				e.IsDir = true
			}
			if n, err := strconv.ParseInt(strings.TrimSpace(pr.Length), 10, 64); err == nil {
				e.Size = n
			}
			if t, err := http.ParseTime(strings.TrimSpace(pr.Modified)); err == nil {
				e.MTime = t.Unix()
			}
			if s := strings.TrimSpace(pr.Checksums); s != "" {
				e.Checksum = s
			}
			if s := strings.TrimSpace(pr.FileID); s != "" {
				e.FileID = s
			}
			if s := strings.TrimSpace(pr.Perms); s != "" {
				e.Perms = s
			}
			if strings.TrimSpace(pr.Encrypted) == "1" {
				e.Encrypted = true
			}
		}
		if got {
			out = append(out, e)
		}
	}
	return out, nil
}

// Get starts a download. The caller must Close the returned body.
func (c *Client) Get(ctx context.Context, rel string) (io.ReadCloser, error) {
	resp, err := c.do(ctx, "GET", rel, nil, "")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, &StatusError{"GET", rel, resp.StatusCode, resp.Status}
	}
	return resp.Body, nil
}

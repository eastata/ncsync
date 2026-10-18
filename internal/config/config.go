// Package config loads the service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime settings.
type Config struct {
	URL        string // Nextcloud base URL, e.g. https://cloud.example.com (no trailing slash)
	User       string // login name
	Password   string // APP PASSWORD (works with TOTP/2FA enabled)
	RemotePath string // optional sub-folder to mirror, "" = whole account

	Dest      string // local mirror directory
	StatePath string // state file (keep it on the same disk as Dest)

	Poll        time.Duration // interval between root-etag checks
	IdleTimeout time.Duration // abort a request if no bytes flow for this long
	Downloads   int           // parallel file downloads
	Listers     int           // parallel PROPFIND requests
	MemLimitMB  int64         // soft Go memory limit, 0 = off

	Once        bool // run a single pass and exit
	Delete      bool // mirror remote deletions locally
	SkipE2EE    bool // skip end-to-end encrypted folders
	Verify      bool // verify downloads against server checksums when available
	InsecureTLS bool // skip TLS verification (testing only!)

	LogLevel slog.Level
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// Load reads and validates configuration from the environment.
func Load() (*Config, error) {
	var errs []error
	bad := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	getInt := func(key string, def, min, max int) int {
		v := env(key, strconv.Itoa(def))
		n, err := strconv.Atoi(v)
		if err != nil || n < min || n > max {
			bad("%s must be an integer in [%d,%d], got %q", key, min, max, v)
			return def
		}
		return n
	}
	getBool := func(key string, def bool) bool {
		v := env(key, strconv.FormatBool(def))
		b, err := strconv.ParseBool(v)
		if err != nil {
			bad("%s must be true/false, got %q", key, v)
			return def
		}
		return b
	}
	getDur := func(key, def string) time.Duration {
		v := env(key, def)
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			bad("%s must be a positive duration like 30s or 5m, got %q", key, v)
			d, _ = time.ParseDuration(def)
		}
		return d
	}

	c := &Config{
		URL:        strings.TrimRight(env("NC_URL", ""), "/"),
		User:       env("NC_USER", ""),
		Password:   os.Getenv("NC_APP_PASSWORD"), // not trimmed: keep exactly as given
		RemotePath: strings.Trim(env("NC_REMOTE_PATH", ""), "/"),
		Dest:       env("NC_DEST", "/data"),
		StatePath:  env("NC_STATE", "/state/ncsync.state"),
	}
	c.Poll = getDur("NC_POLL", "60s")
	c.IdleTimeout = getDur("NC_IDLE_TIMEOUT", "60s")
	c.Downloads = getInt("NC_DOWNLOADS", 6, 1, 64)
	c.Listers = getInt("NC_LISTERS", 4, 1, 32)
	c.MemLimitMB = int64(getInt("NC_MEMLIMIT_MB", 256, 0, 1<<20))
	c.Once = getBool("NC_ONCE", false)
	c.Delete = getBool("NC_DELETE", true)
	c.SkipE2EE = getBool("NC_SKIP_E2EE", true)
	c.Verify = getBool("NC_VERIFY", true)
	c.InsecureTLS = getBool("NC_INSECURE_TLS", false)

	if err := c.LogLevel.UnmarshalText([]byte(env("NC_LOG_LEVEL", "info"))); err != nil {
		bad("NC_LOG_LEVEL must be debug/info/warn/error")
	}

	if c.URL == "" {
		bad("NC_URL is required (e.g. https://cloud.example.com)")
	} else if u, err := url.Parse(c.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		bad("NC_URL must be a valid http(s) URL, got %q", c.URL)
	}
	if c.User == "" {
		bad("NC_USER is required")
	}
	if c.Password == "" {
		bad("NC_APP_PASSWORD is required (create one in Nextcloud: Settings > Security)")
	}
	return c, errors.Join(errs...)
}

// String is a log-safe description (never includes the password).
func (c *Config) String() string {
	return fmt.Sprintf("url=%s user=%s remote=/%s dest=%s state=%s poll=%s downloads=%d listers=%d delete=%t skipE2EE=%t verify=%t once=%t",
		c.URL, c.User, c.RemotePath, c.Dest, c.StatePath, c.Poll, c.Downloads, c.Listers, c.Delete, c.SkipE2EE, c.Verify, c.Once)
}

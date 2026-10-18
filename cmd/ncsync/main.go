// Command ncsync mirrors a Nextcloud account to a local directory (one-way).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"ncsync/internal/config"
	"ncsync/internal/dav"
	"ncsync/internal/state"
	"ncsync/internal/syncer"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:\n"+err.Error())
		os.Exit(2)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(logger)
	if cfg.MemLimitMB > 0 {
		debug.SetMemoryLimit(cfg.MemLimitMB << 20)
	}
	logger.Info("starting ncsync", "config", cfg.String())
	os.Exit(run(cfg, logger))
}

func run(cfg *config.Config, logger *slog.Logger) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := dav.New(dav.Options{
		BaseURL:     cfg.URL,
		User:        cfg.User,
		Password:    cfg.Password,
		RemotePath:  cfg.RemotePath,
		Conns:       cfg.Downloads + cfg.Listers,
		IdleTimeout: cfg.IdleTimeout,
		InsecureTLS: cfg.InsecureTLS,
	})
	if err != nil {
		logger.Error("client setup failed", "err", err)
		return 1
	}
	st, err := state.Open(cfg.StatePath)
	if err != nil {
		logger.Error("cannot open state", "err", err)
		return 1
	}
	defer st.Close()

	s := syncer.New(cfg, client, st, logger)
	first := true
	for {
		err := s.Run(ctx)
		switch {
		case err == nil:
		case ctx.Err() != nil:
			logger.Info("shutting down")
			return 0
		case errors.Is(err, dav.ErrAuth) && (first || cfg.Once):
			logger.Error("login rejected", "err", err)
			return 1
		default:
			logger.Error("sync pass failed", "err", err)
		}
		first = false
		if cfg.Once {
			if err != nil {
				return 1
			}
			return 0
		}
		select {
		case <-ctx.Done():
			logger.Info("shutting down")
			return 0
		case <-time.After(cfg.Poll):
		}
	}
}

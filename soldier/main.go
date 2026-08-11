package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig()

	soldierID := cfg.SoldierID
	if soldierID == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		soldierID = "soldier-" + host
	}
	log.Info("soldier starting", "soldier_id", soldierID, "pool_size", cfg.WorkerPoolSize)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rabbit := NewRabbit(cfg.RabbitURL, log)
	if err := rabbit.Connect(ctx); err != nil {
		log.Error("rabbitmq connect failed", "err", err)
		os.Exit(1)
	}
	defer rabbit.Close()

	tokens := NewTokenManager(cfg.CommanderURL, cfg.SoldierSecret, soldierID, cfg.TokenRefreshSkew, log)

	for {
		if ctx.Err() != nil {
			return
		}
		_, _, err := tokens.GetValidToken(ctx)
		if err == nil {
			break
		}
		log.Warn("waiting for commander token endpoint", "err", err)
		time.Sleep(2 * time.Second)
	}

	pool := NewWorkerPool(cfg.WorkerPoolSize, cfg.MinDelay, cfg.MaxDelay, soldierID, tokens, rabbit, log)
	pool.Start(ctx)

	deliveries, err := rabbit.ConsumeOrders(cfg.WorkerPoolSize)
	if err != nil {
		log.Error("consume orders failed", "err", err)
		os.Exit(1)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					stop()
					return
				}
				pool.Submit(d)
			}
		}
	}()

	<-ctx.Done()
	log.Info("shutting down soldier", "soldier_id", soldierID)

	done := make(chan struct{})
	go func() {
		pool.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		log.Warn("shutdown timeout waiting for workers")
	}
}

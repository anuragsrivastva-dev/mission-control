package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := openMySQL(ctx, cfg, log)
	if err != nil {
		log.Error("mysql connect failed", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	rabbit := NewRabbit(cfg.RabbitURL, log)
	if err := rabbit.Connect(ctx); err != nil {
		log.Error("rabbitmq connect failed", "err", err)
		os.Exit(1)
	}
	defer rabbit.Close()

	missions := NewMissionStore(db)
	tokens := NewTokenStore(db, cfg.SoldierSecret, cfg.TokenTTL, log)
	h := &Handlers{missions: missions, tokens: tokens, rabbit: rabbit, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.Health)
	mux.HandleFunc("/missions", h.CreateMission)
	mux.HandleFunc("/missions/", h.GetMission)
	mux.HandleFunc("/auth/token", h.IssueToken)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("commander listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server failed", "err", err)
			stop()
		}
	}()

	go func() {
		if err := rabbit.ConsumeStatus(ctx, h.HandleStatus); err != nil {
			log.Error("status consumer stopped", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down commander")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func openMySQL(ctx context.Context, cfg Config, log *slog.Logger) (*sql.DB, error) {
	var last error
	for i := 0; i < 30; i++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		db, err := sql.Open("mysql", cfg.MySQLDSN)
		if err != nil {
			last = err
			time.Sleep(2 * time.Second)
			continue
		}
		db.SetMaxOpenConns(cfg.MySQLMaxOpenConns)
		db.SetMaxIdleConns(cfg.MySQLMaxIdleConns)
		db.SetConnMaxLifetime(30 * time.Minute)

		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err != nil {
			last = err
			_ = db.Close()
			log.Warn("mysql ping failed, retrying", "err", err, "attempt", i+1)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Info("connected to mysql")
		return db, nil
	}
	return nil, fmt.Errorf("mysql connect failed: %w", last)
}

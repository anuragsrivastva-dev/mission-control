package main

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	RabbitURL        string
	CommanderURL     string
	SoldierID        string
	SoldierSecret    string
	WorkerPoolSize   int
	MinDelay         time.Duration
	MaxDelay         time.Duration
	TokenRefreshSkew time.Duration
}

func loadConfig() Config {
	minSec := envInt("MISSION_MIN_DELAY_SECONDS", 5)
	maxSec := envInt("MISSION_MAX_DELAY_SECONDS", 15)
	if maxSec < minSec {
		maxSec = minSec
	}
	return Config{
		RabbitURL:        envOr("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		CommanderURL:     envOr("COMMANDER_URL", "http://localhost:8080"),
		SoldierID:        os.Getenv("SOLDIER_ID"),
		SoldierSecret:    envOr("SOLDIER_SHARED_SECRET", "change-me-soldier-secret"),
		WorkerPoolSize:   envInt("WORKER_POOL_SIZE", 4),
		MinDelay:         time.Duration(minSec) * time.Second,
		MaxDelay:         time.Duration(maxSec) * time.Second,
		TokenRefreshSkew: time.Duration(envInt("TOKEN_REFRESH_SKEW_SECONDS", 5)) * time.Second,
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

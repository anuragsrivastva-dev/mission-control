package main

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port              string
	RabbitURL         string
	MySQLDSN          string
	SoldierSecret     string
	TokenTTL          time.Duration
	MySQLMaxOpenConns int
	MySQLMaxIdleConns int
}

func loadConfig() Config {
	return Config{
		Port:              envOr("PORT", "8080"),
		RabbitURL:         envOr("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		MySQLDSN:          envOr("MYSQL_DSN", "mission:mission@tcp(localhost:3306)/mission_control?parseTime=true"),
		SoldierSecret:     envOr("SOLDIER_SHARED_SECRET", "change-me-soldier-secret"),
		TokenTTL:          time.Duration(envInt("TOKEN_TTL_SECONDS", 30)) * time.Second,
		MySQLMaxOpenConns: envInt("MYSQL_MAX_OPEN_CONNS", 10),
		MySQLMaxIdleConns: envInt("MYSQL_MAX_IDLE_CONNS", 5),
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

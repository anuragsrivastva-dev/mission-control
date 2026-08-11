package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type TokenManager struct {
	commanderURL string
	secret       string
	soldierID    string
	skew         time.Duration
	client       *http.Client
	log          *slog.Logger

	mu         sync.Mutex
	token      string
	tokenID    string
	expiresAt  time.Time
	refreshing bool
	waiters    []chan refreshResult
}

type refreshResult struct {
	token   string
	tokenID string
	err     error
}

type tokenResponse struct {
	Token     string    `json:"token"`
	TokenID   string    `json:"token_id"`
	SoldierID string    `json:"soldier_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

func NewTokenManager(commanderURL, secret, soldierID string, skew time.Duration, log *slog.Logger) *TokenManager {
	return &TokenManager{
		commanderURL: commanderURL,
		secret:       secret,
		soldierID:    soldierID,
		skew:         skew,
		client:       &http.Client{Timeout: 10 * time.Second},
		log:          log,
	}
}

// GetValidToken returns a usable token. Concurrent callers share one refresh.
func (tm *TokenManager) GetValidToken(ctx context.Context) (string, string, error) {
	tm.mu.Lock()
	if tm.token != "" && time.Now().Before(tm.expiresAt.Add(-tm.skew)) {
		tok, id := tm.token, tm.tokenID
		tm.mu.Unlock()
		return tok, id, nil
	}

	if tm.refreshing {
		ch := make(chan refreshResult, 1)
		tm.waiters = append(tm.waiters, ch)
		tm.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", "", ctx.Err()
		case res := <-ch:
			return res.token, res.tokenID, res.err
		}
	}

	tm.refreshing = true
	tm.mu.Unlock()

	token, tokenID, expiresAt, err := tm.doRefresh(ctx)

	tm.mu.Lock()
	tm.refreshing = false
	res := refreshResult{err: err}
	if err == nil {
		tm.token = token
		tm.tokenID = tokenID
		tm.expiresAt = expiresAt
		res.token = token
		res.tokenID = tokenID
	}
	waiters := tm.waiters
	tm.waiters = nil
	tm.mu.Unlock()

	for _, w := range waiters {
		w <- res
	}
	return res.token, res.tokenID, res.err
}

func (tm *TokenManager) doRefresh(ctx context.Context) (string, string, time.Time, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tm.commanderURL+"/auth/token", bytes.NewReader(nil))
	if err != nil {
		return "", "", time.Time{}, err
	}
	req.Header.Set("X-Soldier-Secret", tm.secret)
	req.Header.Set("X-Soldier-ID", tm.soldierID)

	resp, err := tm.client.Do(req)
	if err != nil {
		return "", "", time.Time{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", "", time.Time{}, fmt.Errorf("token refresh failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", "", time.Time{}, err
	}

	tm.log.Info("token refreshed",
		"token_id", tr.TokenID,
		"soldier_id", tm.soldierID,
		"expires_at", tr.ExpiresAt.UTC().Format(time.RFC3339),
	)
	return tr.Token, tr.TokenID, tr.ExpiresAt, nil
}

package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// ErrInvalidToken covers every reason a token is not usable: unknown, expired
// or revoked. The wrapped message says which one for the logs.
var ErrInvalidToken = errors.New("invalid token")

type TokenStore struct {
	db     *sql.DB
	secret string
	ttl    time.Duration
	log    *slog.Logger
}

type IssuedToken struct {
	Token     string    `json:"token"`
	TokenID   string    `json:"token_id"`
	SoldierID string    `json:"soldier_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

type TokenRecord struct {
	TokenID   string
	SoldierID string
	ExpiresAt time.Time
	RevokedAt sql.NullTime
}

func NewTokenStore(db *sql.DB, secret string, ttl time.Duration, log *slog.Logger) *TokenStore {
	return &TokenStore{db: db, secret: secret, ttl: ttl, log: log}
}

func (s *TokenStore) CheckSecret(got string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.secret)) == 1
}

func (s *TokenStore) Issue(ctx context.Context, soldierID string) (*IssuedToken, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(raw)
	tokenID := uuid.NewString()
	hash := hashToken(token)
	now := time.Now().UTC()
	expires := now.Add(s.ttl)

	// Older tokens are left alone so a status message that was already in
	// flight with the previous token is still accepted until it expires.
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO auth_tokens (token_id, soldier_id, token_hash, issued_at, expires_at, revoked_at)
		VALUES (?, ?, ?, ?, ?, NULL)`,
		tokenID, soldierID, hash, now, expires)
	if err != nil {
		return nil, err
	}

	s.log.Info("token issued",
		"token_id", tokenID,
		"soldier_id", soldierID,
		"expires_at", expires.Format(time.RFC3339),
	)

	return &IssuedToken{
		Token:     token,
		TokenID:   tokenID,
		SoldierID: soldierID,
		ExpiresAt: expires,
	}, nil
}

func (s *TokenStore) Validate(ctx context.Context, rawToken, soldierID string) (*TokenRecord, error) {
	if rawToken == "" {
		return nil, fmt.Errorf("%w: no token in message", ErrInvalidToken)
	}
	row := s.db.QueryRowContext(ctx, `
		SELECT token_id, soldier_id, expires_at, revoked_at
		FROM auth_tokens WHERE token_hash = ?`, hashToken(rawToken))

	var rec TokenRecord
	err := row.Scan(&rec.TokenID, &rec.SoldierID, &rec.ExpiresAt, &rec.RevokedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: unknown token", ErrInvalidToken)
	}
	if err != nil {
		return nil, err
	}
	if rec.RevokedAt.Valid {
		return nil, fmt.Errorf("%w: revoked", ErrInvalidToken)
	}
	if time.Now().UTC().After(rec.ExpiresAt) {
		return nil, fmt.Errorf("%w: expired", ErrInvalidToken)
	}
	if soldierID != rec.SoldierID {
		return nil, fmt.Errorf("%w: belongs to %s", ErrInvalidToken, rec.SoldierID)
	}
	return &rec, nil
}

func hashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

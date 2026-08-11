package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

const (
	StatusQueued     = "QUEUED"
	StatusInProgress = "IN_PROGRESS"
	StatusCompleted  = "COMPLETED"
	StatusFailed     = "FAILED"
)

var (
	ErrMissionNotFound   = errors.New("mission not found")
	ErrInvalidTransition = errors.New("invalid status transition")
	ErrUnknownStatus     = errors.New("unknown status")
)

type Mission struct {
	MissionID      string     `json:"mission_id"`
	IdempotencyKey *string    `json:"idempotency_key,omitempty"`
	Description    string     `json:"description"`
	Status         string     `json:"status"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	StartedAt      *time.Time `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at"`
}

type MissionStore struct {
	db *sql.DB
}

func NewMissionStore(db *sql.DB) *MissionStore {
	return &MissionStore{db: db}
}

func (s *MissionStore) GetByID(ctx context.Context, id string) (*Mission, error) {
	return s.scanOne(ctx, `
		SELECT mission_id, idempotency_key, description, status,
		       created_at, updated_at, started_at, completed_at
		FROM missions WHERE mission_id = ?`, id)
}

func (s *MissionStore) GetByIdempotencyKey(ctx context.Context, key string) (*Mission, error) {
	return s.scanOne(ctx, `
		SELECT mission_id, idempotency_key, description, status,
		       created_at, updated_at, started_at, completed_at
		FROM missions WHERE idempotency_key = ?`, key)
}

// CreateNew inserts a mission. created=false means an idempotency key already existed.
func (s *MissionStore) CreateNew(ctx context.Context, description string, idempotencyKey *string) (*Mission, bool, error) {
	if idempotencyKey != nil {
		existing, err := s.GetByIdempotencyKey(ctx, *idempotencyKey)
		if err != nil {
			return nil, false, err
		}
		if existing != nil {
			return existing, false, nil
		}
	}

	now := time.Now().UTC()
	m := &Mission{
		MissionID:      uuid.NewString(),
		IdempotencyKey: idempotencyKey,
		Description:    description,
		Status:         StatusQueued,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO missions (mission_id, idempotency_key, description, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		m.MissionID, nullString(idempotencyKey), m.Description, m.Status, m.CreatedAt, m.UpdatedAt)
	if err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1062 && idempotencyKey != nil {
			existing, getErr := s.GetByIdempotencyKey(ctx, *idempotencyKey)
			if getErr != nil {
				return nil, false, getErr
			}
			return existing, false, nil
		}
		return nil, false, err
	}
	return m, true, nil
}

// ApplyStatus moves a mission to newStatus only if it is in the status the
// transition expects. Applying the same status twice is a no-op so that a
// redelivered message cannot corrupt the mission.
func (s *MissionStore) ApplyStatus(ctx context.Context, missionID, newStatus string) (bool, error) {
	from, ok := expectedPrev(newStatus)
	if !ok {
		return false, fmt.Errorf("%w: %s", ErrUnknownStatus, newStatus)
	}

	now := time.Now().UTC()
	var res sql.Result
	var err error
	if newStatus == StatusInProgress {
		res, err = s.db.ExecContext(ctx, `
			UPDATE missions
			SET status = ?, updated_at = ?, started_at = ?
			WHERE mission_id = ? AND status = ?`,
			newStatus, now, now, missionID, from)
	} else {
		res, err = s.db.ExecContext(ctx, `
			UPDATE missions
			SET status = ?, updated_at = ?, completed_at = ?
			WHERE mission_id = ? AND status = ?`,
			newStatus, now, now, missionID, from)
	}
	if err != nil {
		return false, err
	}
	if n, err := res.RowsAffected(); err != nil {
		return false, err
	} else if n > 0 {
		return true, nil
	}

	// Nothing was updated: either the mission is already in the target status
	// (a duplicate message) or the transition is not allowed.
	current, err := s.GetByID(ctx, missionID)
	if err != nil {
		return false, err
	}
	if current == nil {
		return false, ErrMissionNotFound
	}
	if current.Status == newStatus {
		return false, nil
	}
	return false, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, current.Status, newStatus)
}

func expectedPrev(newStatus string) (string, bool) {
	switch newStatus {
	case StatusInProgress:
		return StatusQueued, true
	case StatusCompleted, StatusFailed:
		return StatusInProgress, true
	default:
		return "", false
	}
}

func (s *MissionStore) scanOne(ctx context.Context, query string, arg string) (*Mission, error) {
	row := s.db.QueryRowContext(ctx, query, arg)
	var m Mission
	var idem sql.NullString
	var started, completed sql.NullTime
	err := row.Scan(&m.MissionID, &idem, &m.Description, &m.Status,
		&m.CreatedAt, &m.UpdatedAt, &started, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if idem.Valid {
		m.IdempotencyKey = &idem.String
	}
	if started.Valid {
		t := started.Time
		m.StartedAt = &t
	}
	if completed.Valid {
		t := completed.Time
		m.CompletedAt = &t
	}
	return &m, nil
}

func nullString(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
)

type Handlers struct {
	missions *MissionStore
	tokens   *TokenStore
	rabbit   *Rabbit
	log      *slog.Logger
}

type createMissionRequest struct {
	Description string `json:"description"`
}

func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handlers) CreateMission(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req createMissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Description) == "" {
		http.Error(w, "description required", http.StatusBadRequest)
		return
	}

	var idemPtr *string
	if idem := strings.TrimSpace(r.Header.Get("Idempotency-Key")); idem != "" {
		idemPtr = &idem
	}

	m, created, err := h.missions.CreateNew(r.Context(), req.Description, idemPtr)
	if err != nil {
		h.log.Error("create mission failed", "err", err)
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	if !created {
		h.log.Info("idempotent mission replay", "mission_id", m.MissionID)
		writeJSON(w, http.StatusAccepted, map[string]string{"mission_id": m.MissionID})
		return
	}

	if err := h.rabbit.PublishOrder(r.Context(), OrderMessage{
		MissionID:   m.MissionID,
		Description: m.Description,
	}); err != nil {
		h.log.Error("publish order failed", "err", err, "mission_id", m.MissionID)
		http.Error(w, "message broker unavailable", http.StatusServiceUnavailable)
		return
	}

	h.log.Info("mission queued", "mission_id", m.MissionID, "status", m.Status)
	writeJSON(w, http.StatusAccepted, map[string]string{"mission_id": m.MissionID})
}

func (h *Handlers) GetMission(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/missions/")
	id = strings.Trim(id, "/")
	if id == "" || strings.Contains(id, "/") {
		http.Error(w, "mission_id required", http.StatusBadRequest)
		return
	}

	m, err := h.missions.GetByID(r.Context(), id)
	if err != nil {
		h.log.Error("get mission failed", "err", err, "mission_id", id)
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	if m == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (h *Handlers) IssueToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.tokens.CheckSecret(r.Header.Get("X-Soldier-Secret")) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	soldierID := strings.TrimSpace(r.Header.Get("X-Soldier-ID"))
	if soldierID == "" {
		http.Error(w, "X-Soldier-ID required", http.StatusBadRequest)
		return
	}

	issued, err := h.tokens.Issue(r.Context(), soldierID)
	if err != nil {
		h.log.Error("issue token failed", "err", err, "soldier_id", soldierID)
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, issued)
}

// HandleStatus applies one status update from the field. It returns an error
// only when the update should be retried later; messages that are rejected on
// purpose are logged and dropped so they are not redelivered forever.
func (h *Handlers) HandleStatus(ctx context.Context, msg StatusMessage) error {
	rec, err := h.tokens.Validate(ctx, msg.Token, msg.SoldierID)
	if err != nil {
		if errors.Is(err, ErrInvalidToken) {
			h.log.Warn("status rejected",
				"err", err,
				"mission_id", msg.MissionID,
				"soldier_id", msg.SoldierID,
				"status", msg.Status,
			)
			return nil
		}
		return err
	}

	changed, err := h.missions.ApplyStatus(ctx, msg.MissionID, msg.Status)
	if err != nil {
		if errors.Is(err, ErrInvalidTransition) ||
			errors.Is(err, ErrMissionNotFound) ||
			errors.Is(err, ErrUnknownStatus) {
			h.log.Warn("status rejected",
				"err", err,
				"mission_id", msg.MissionID,
				"status", msg.Status,
				"token_id", rec.TokenID,
				"soldier_id", msg.SoldierID,
			)
			return nil
		}
		return err
	}

	h.log.Info("status applied",
		"mission_id", msg.MissionID,
		"status", msg.Status,
		"token_id", rec.TokenID,
		"soldier_id", msg.SoldierID,
		"changed", changed,
	)
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

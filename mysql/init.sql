CREATE DATABASE IF NOT EXISTS mission_control;
USE mission_control;

CREATE TABLE IF NOT EXISTS missions (
  mission_id      CHAR(36)     NOT NULL PRIMARY KEY,
  idempotency_key VARCHAR(128) NULL,
  description     TEXT         NOT NULL,
  status          VARCHAR(32)  NOT NULL,
  created_at      DATETIME(3)  NOT NULL,
  updated_at      DATETIME(3)  NOT NULL,
  started_at      DATETIME(3)  NULL,
  completed_at    DATETIME(3)  NULL,
  UNIQUE KEY uq_missions_idempotency_key (idempotency_key),
  INDEX idx_missions_status (status),
  INDEX idx_missions_created_at (created_at)
);

CREATE TABLE IF NOT EXISTS auth_tokens (
  token_id      CHAR(36)     NOT NULL PRIMARY KEY,
  soldier_id    VARCHAR(128) NOT NULL,
  token_hash    CHAR(64)     NOT NULL,
  issued_at     DATETIME(3)  NOT NULL,
  expires_at    DATETIME(3)  NOT NULL,
  revoked_at    DATETIME(3)  NULL,
  UNIQUE KEY uq_token_hash (token_hash),
  INDEX idx_tokens_soldier (soldier_id),
  INDEX idx_tokens_expires (expires_at)
);

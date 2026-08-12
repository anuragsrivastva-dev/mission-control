# Mission Control

Secure, asynchronous command-and-control demo: a **Commander Camp** issues missions, **Soldier** workers execute them, and all field traffic goes through a central **RabbitMQ** hub. Mission and auth state live in **MySQL**. Soldiers never expose a public endpoint.

## Scope and objectives

- Issue missions via HTTP (`202 Accepted`) and track status asynchronously
- Deliver orders and status over durable RabbitMQ queues
- Authenticate Soldiers with short-lived opaque tokens (hash stored in MySQL)
- Rotate tokens gracefully with overlap (no revoke-on-rotate)
- Run everything with Docker Compose; demonstrate with `test_missions.sh`

## Architecture

```
Client / test_missions.sh
        │
        ▼
   Commander (:8080)  ◄──── POST /auth/token (Soldier refresh)
        │        │
        │        └── MySQL (missions, auth_tokens)
        │
        ├── publish orders ──► RabbitMQ ◄── consume orders ── Soldier(s)
        └── consume status ◄── RabbitMQ ◄── publish status ── Soldier(s)
```

| Component | Responsibility |
|-----------|----------------|
| Commander | Stateless API; persist missions; publish orders; validate status; issue tokens |
| Soldier | Consume orders; worker pool; publish status with token; refresh token via Commander |
| RabbitMQ | Durable `orders` / `status` channels; competing consumers; redelivery |
| MySQL | Source of truth for mission state and token hashes |

## Quick start (Docker)

```bash
docker compose up --build -d
curl -s http://localhost:18080/health
./test_missions.sh
```

> Host port **18080** maps to Commander `:8080` inside Compose (avoids clashing with other local apps on 8080). Internally, Soldiers still use `http://commander:8080`.

Scale Soldiers:

```bash
docker compose up -d --scale soldier=3
```

Stop:

```bash
docker compose down
```

### Docker Hub images

Published images:

- https://hub.docker.com/r/anuragsrivastva/mission-commander
- https://hub.docker.com/r/anuragsrivastva/mission-soldier

Rebuild and push:

```bash
docker build -t anuragsrivastva/mission-commander:latest ./commander
docker build -t anuragsrivastva/mission-soldier:latest ./soldier
docker push anuragsrivastva/mission-commander:latest
docker push anuragsrivastva/mission-soldier:latest
```

Run from Hub (no local Go build needed):

```bash
export DOCKERHUB_USER=anuragsrivastva
docker compose -f docker-compose.hub.yml up -d
```

## API documentation

Base URL (local Compose): `http://localhost:18080`

### `GET /health`

```json
{"status":"ok"}
```

### `POST /missions`

Headers:

- `Content-Type: application/json`
- `Idempotency-Key: <optional unique key>` — retries with the same key return the same `mission_id` and do not re-publish

```bash
curl -s -X POST http://localhost:18080/missions \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"description":"Secure the ridge"}'
```

Response `202`:

```json
{"mission_id":"..."}
```

### `GET /missions/{mission_id}`

```bash
curl -s http://localhost:18080/missions/<mission_id>
```

```json
{
  "mission_id": "...",
  "description": "...",
  "status": "IN_PROGRESS",
  "created_at": "...",
  "updated_at": "...",
  "started_at": "...",
  "completed_at": null
}
```

Statuses: `QUEUED` → `IN_PROGRESS` → `COMPLETED` | `FAILED`

### `POST /auth/token`

Used by Soldiers (not for general clients).

Headers:

- `X-Soldier-Secret: <shared secret>`
- `X-Soldier-ID: <soldier id>`

```json
{
  "token": "<opaque>",
  "token_id": "<uuid for logs>",
  "soldier_id": "...",
  "expires_at": "..."
}
```

## Database schema

**missions:** `mission_id`, `idempotency_key` (UNIQUE), `description`, `status`, `created_at`, `updated_at`, `started_at`, `completed_at`

**auth_tokens:** `token_id`, `soldier_id`, `token_hash` (SHA-256, UNIQUE), `issued_at`, `expires_at`, `revoked_at` (unused on normal rotation)

## RabbitMQ topology

| Resource | Name | Notes |
|----------|------|-------|
| Exchange | `orders_exchange` (direct, durable) | routing key `order` |
| Queue | `orders_queue` (durable) | competing Soldier consumers |
| Exchange | `status_exchange` (direct, durable) | routing key `status` |
| Queue | `status_queue` (durable) | competing Commander consumers |

- Persistent message delivery
- Manual acknowledgements
- Soldier prefetch = `WORKER_POOL_SIZE` (backpressure: one Soldier only holds about as many unacked orders as it can run)

## Mission lifecycle and ACK semantics

1. Commander inserts `QUEUED`, publishes order, returns `202`
2. Soldier receives order (unacked)
3. Soldier ensures valid token → publishes `IN_PROGRESS`
4. Simulated execution (`MISSION_MIN/MAX_DELAY_SECONDS`)
5. Soldier publishes `COMPLETED` or `FAILED` (~90% / 10%)
6. **Only then** ACK the order

If the Soldier crashes after final publish but before ACK, RabbitMQ redelivers. Commander applies **idempotent** transitions so duplicate `COMPLETED` does not corrupt state.

### Allowed transitions

- `QUEUED` → `IN_PROGRESS`
- `IN_PROGRESS` → `COMPLETED` | `FAILED`
- Identical terminal updates are no-ops
- Illegal transitions are rejected and logged

## Authentication and token rotation

- Shared secret proves Soldier identity to `POST /auth/token`
- Opaque crypto-random token; **only hash** stored in MySQL
- TTL default **30s** (`TOKEN_TTL_SECONDS`)
- Soldier refreshes before expiry (`TOKEN_REFRESH_SKEW_SECONDS`)
- **TokenManager singleflight:** concurrent workers share one refresh
- **No revoke-on-rotate:** previous token remains valid until natural expiry (avoids in-flight status races)
- Logs use `token_id` / `soldier_id` — **never raw tokens**

## Concurrency model

- Configurable worker pool (`WORKER_POOL_SIZE`, default 4)
- Prefetch matches pool size → fair distribution across scaled Soldiers
- Multiple Soldier replicas compete on `orders_queue`

## Configuration

See [`.env.example`](.env.example). Important variables:

**Commander:** `PORT`, `RABBITMQ_URL`, `MYSQL_DSN`, `SOLDIER_SHARED_SECRET`, `TOKEN_TTL_SECONDS`

**Soldier:** `RABBITMQ_URL`, `COMMANDER_URL`, `SOLDIER_ID` (optional; defaults to `soldier-<hostname>`), `SOLDIER_SHARED_SECRET`, `WORKER_POOL_SIZE`, `MISSION_MIN_DELAY_SECONDS`, `MISSION_MAX_DELAY_SECONDS`

## Design rationale

### Why RabbitMQ (not Kafka)?

Missions are discrete work items with competing consumers and ACK/redelivery — a classic task-queue pattern. Kafka is excellent for durable event logs and replay at high throughput; that is more than this control plane needs and heavier in Compose. This system is **scale-ready** via queue buffering and horizontal Soldier scale; we do **not** claim a measured 100k-mission benchmark.

### Why MySQL (not in-memory / SQLite)?

Mission and token state must survive Commander restarts and be shared by multiple Commander instances. MySQL matches common production backends and the evaluation focus on durable state.

### Why dual-write without an outbox?

v1 inserts then publishes. A rare failure can leave a `QUEUED` row unpublished. A **transactional outbox** is the clear next production step; it is intentionally not implemented here to keep the codebase small and interview-defendable.

## Failure and scale notes

| Scenario | Behavior |
|----------|----------|
| MySQL down | Startup retries; API returns 503; no in-memory fallback |
| RabbitMQ down | Startup retries; publish failures → 503; Soldiers do not ACK unfinished work |
| Status with bad/expired token | Rejected, logged, message discarded (no infinite loop) |
| Many missions | Commander returns quickly; RabbitMQ buffers; scale Soldiers |

## Testing

```bash
./test_missions.sh
```

Covers:

1. Single mission → terminal status  
2. `Idempotency-Key` replay  
3. Concurrent missions (overlap via `IN_PROGRESS` snapshot or timestamps)  
4. Work continues after token TTL with rotation logs  

Requires: Docker, `curl`, and `jq` or `python3`.

## Project layout

```
commander/          Commander Camp service
soldier/            Soldier worker
mysql/init.sql      Schema
docker-compose.yml  Local build + run
docker-compose.hub.yml  Hub image mode
test_missions.sh    End-to-end demo
```

## Known limitations / production evolution

- Dual-write without transactional outbox
- No TLS/mTLS between services
- No metrics/tracing yet
- Expired token rows not purged automatically
- Soldier has no inbound health HTTP port (by design)

## AI usage policy

This project was designed and implemented with assistance from **Cursor** and cross-reviewed with **ChatGPT** for architecture critique. I used AI as a helper for planning and scaffolding. I directed the architecture, reviewed the code for relevance, and verified the running system before submission.

### How AI helped

- Turned the assignment into a concrete architecture (stateless Commander, MySQL, RabbitMQ ACK/prefetch, token overlap, idempotency)
- Accelerated service scaffolding, Docker Compose, and `test_missions.sh`
- Helped iterate design decisions (queue choice, no revoke-on-rotate, `Idempotency-Key`, `slog`, hostname Soldier IDs, no unverified capacity claims)
- Helped reason through a few hard edges: redelivery after crash, concurrent token refresh, and dual-write without an outbox

### What I owned

- Final architecture locks and evaluation goals
- Review of ChatGPT’s gate questions (multi-Commander, multi-Soldier, redelivery, singleflight, MySQL/Rabbit failure, backlog)
- Choosing what not to build in v1 (transactional outbox documented, not implemented)
- Line-by-line relevance review of the final code and docs
- Runtime verification with Docker Compose, `test_missions.sh`, and MySQL checks

### Prompts / responses (summary appendix)

Major conversation beats:

1. **Me:** Full Mission Control assignment; propose a from-scratch layout for Commander, Soldier, message hub, auth rotation, Compose, and a test script that I can own end to end.  
   **AI:** Proposed Go + RabbitMQ + Compose; sketched API/queues/auth options; asked how token refresh should work.

2. **Me:** Confirm RabbitMQ for this control plane; proceed with suggested tools; Soldier refreshes via `POST /auth/token`.  
   **AI:** Summarized queue trade-offs and updated the plan around that auth path (first draft still used in-memory state).

3. **Me:** Before coding, revise for production orientation — MySQL, stateless Commander, hashed tokens, durable queues, ACK rules, idempotency, health, graceful shutdown.  
   **AI:** Full revised architecture plan (schema, topology, lifecycle, Compose layout).

4. **Me (via ChatGPT review):** Answer multi-instance, redelivery, singleflight, MySQL/Rabbit down, and large backlog questions before coding.  
   **AI:** Documented architecture Q&A matching those gates.

5. **Me (via ChatGPT):** Final locks — no revoke-on-rotate; `Idempotency-Key`; `slog`; hostname Soldier ID; document outbox but do not implement; no unverified capacity claims. Then build.  
   **AI:** Updated plan and assisted with implementation against that checklist.

6. **Me:** Validate with Compose health, `test_missions.sh`, DB inspection, code relevance pass, then publish GitHub + Docker Hub.  
   **AI:** Assisted with delivery wiring; I confirmed green tests and reviewed the final tree.

Full chat transcripts remain in Cursor / ChatGPT histories. This README is the submission-facing disclosure.

## License

Prepared as an evaluation / learning project.

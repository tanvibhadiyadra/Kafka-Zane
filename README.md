# QueryGuard – Unified Event-Driven GitHub Impact Analysis

A scalable, multi-tenant, event-driven system that:
1. Receives GitHub App webhook events via **FastAPI**
2. Publishes events to **Kafka**
3. Consumes and processes events in a separate **worker** process
4. Resolves tenants via **Redis → PostgreSQL** cache-aside
5. Runs SQL impact analysis and posts results as GitHub PR comments

---

## Architecture

```
GitHub App ──HTTPS──► ngrok ──► webhook (FastAPI :8000)
                                    │ validate sig
                                    │ onboard tenant (installation.created)
                                    │ publish envelope
                                    ▼
                               Kafka  (github-events)
                                    │
                                    ▼
                              worker (consumer)
                                    │ idempotency check (pr_analyses)
                                    │ tenant resolve (Redis → Postgres)
                                    │ fetch PR SQL files (GitHub API)
                                    │ run impact analysis
                                    │ post GitHub comment
                                    ▼
                             PostgreSQL  (pr_analyses + per-tenant schemas)
                                    │
                              Redis cache (tenant mappings, idempotency)
```

### Process separation
| Container | Role | DB access |
|---|---|---|
| `webhook` | Receive + enqueue events | asyncpg for onboarding only |
| `worker` | Consume + analyse events | SQLAlchemy full read/write |

---

## Project Structure

```
queryguard/
├── backend/
│   ├── app/
│   │   ├── config.py              # All env vars (single source of truth)
│   │   ├── main.py                # FastAPI entry point
│   │   ├── db/
│   │   │   ├── postgres.py        # asyncpg (webhook) + SQLAlchemy (worker)
│   │   │   └── redis_client.py    # async RedisService + sync helpers
│   │   ├── kafka/
│   │   │   ├── producer.py        # confluent-kafka singleton producer
│   │   │   └── consumer.py        # confluent-kafka blocking consumer loop
│   │   ├── models/
│   │   │   └── db_models.py       # SQLAlchemy ORM (Tenant, GitHubInstallation, PRAnalysis)
│   │   ├── services/
│   │   │   ├── tenant_resolver.py # Redis → DB → auto-onboard
│   │   │   └── impact_analysis.py # GitHub PR fetch + analysis + comment
│   │   └── webhook/
│   │       └── handler.py         # HMAC validation + event routing
│   ├── worker_main.py             # Worker entry point
│   ├── Dockerfile.webhook
│   ├── Dockerfile.worker
│   └── requirements.txt
├── scripts/
│   └── init_db.sql                # Schema bootstrap (idempotent)
├── docker-compose.yml
└── .env.example
```

---

## Quick Start

### 1. Prerequisites
- Docker + Docker Compose
- ngrok (`brew install ngrok` / `choco install ngrok`)
- A GitHub App (see below)

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:
- `DATABASE_URL` – Neon connection string or local postgres URL
- `GITHUB_APP_ID` – from GitHub App settings
- `GITHUB_PRIVATE_KEY` – PEM key (newlines as `\n`)
- `GITHUB_WEBHOOK_SECRET` – secret you set in the GitHub App

### 3. Start the stack

```bash
# With external Neon DB (recommended)
docker compose up redis zookeeper kafka kafdrop webhook worker

# With local Postgres
docker compose --profile local-db up
```

Kafka UI (Kafdrop) will be available at `http://localhost:9000`.

### 4. Expose with ngrok

```bash
ngrok http 8000
```

Copy the HTTPS forwarding URL (e.g. `https://abc123.ngrok-free.app`).

### 5. Configure GitHub App webhook

In your GitHub App settings → Webhooks:
- **Webhook URL:** `https://abc123.ngrok-free.app/webhook`
- **Content type:** `application/json`
- **Secret:** same as `GITHUB_WEBHOOK_SECRET` in `.env`
- **Events:** Subscribe to `Pull requests` and `Installations`

### 6. Test

Open a PR in a repo where your GitHub App is installed.  
Watch the worker logs:

```bash
docker compose logs -f worker
```

---

## Event Flow Detail

### Installation (tenant onboarding)

```
GitHub installs App → POST /webhook
  X-GitHub-Event: installation
  action: created
  → webhook creates row in github_installations
  → creates per-tenant PostgreSQL schema (schema_{installation_id})
  → returns 202
```

### Pull Request Event

```
PR opened/reopened/synchronize → POST /webhook
  X-GitHub-Event: pull_request
  → validate HMAC signature
  → build event envelope with idempotency_key
  → publish_event(topic=github-events, key=installation_id, value=envelope)
  → returns 202 immediately (< 2s guaranteed)

Worker picks up envelope:
  → idempotency check  (skip if pr_analyses.status='done' for this key)
  → TenantResolver: Redis → DB → auto-onboard if needed
  → ImpactAnalysisService.run(event):
       1. Create pr_analyses row (status='processing')
       2. Fetch .sql file changes from GitHub via PyGithub
       3. _analyse_file() per file  ← plug your real RAG/LLM logic here
       4. Post formatted comment to GitHub PR
       5. Update pr_analyses row (status='done')
  → commit Kafka offset
```

---

## Plugging in your real analysis logic

Open `backend/app/services/impact_analysis.py` and replace the stub in `_analyse_file()`:

```python
def _analyse_file(self, file_info: dict, installation_id: str) -> dict:
    # REPLACE THIS:
    impact_report = f"[STUB] Analysis for {file_info['filename']}"
    affected_query_ids: list[str] = []

    # WITH YOUR REAL CALLS, e.g.:
    # from app.services.impact_analysis_core import schema_detection_rag
    # result = schema_detection_rag(analysis_input, self._tenant_id, self._db)
    # impact_report = result["impact_analysis"]
    # affected_query_ids = result["affected_query_ids"]
```

---

## Scaling

### Horizontal worker scaling
```bash
docker compose up --scale worker=3
```
Kafka partitions are distributed across worker instances automatically.
All workers share the same consumer group (`queryguard-workers`).

### Topic partitioning
Events are keyed by `installation_id` → same tenant always goes to the same partition → ordered processing per tenant.

---

## Health Check

```bash
curl http://localhost:8000/health
# {"status":"ok","service":"webhook","dependencies":{"postgres":true,"redis":true,"kafka":true}}
```

---

## Dead-Letter Queue

Failed messages (after `KAFKA_MAX_RETRIES` attempts) are published to `github-events-dlq`.  
Each DLQ message includes:
- Original event envelope
- `_dlq_reason` – error message
- `_dlq_timestamp` – Unix timestamp

Replay a DLQ message by re-publishing it to `github-events` after fixing the root cause.

---

## Queue & DLQ Operations Strategy

### 1) Keep queueing lightweight, processing heavy
- Webhook only validates + enqueues to `github-events`.
- Worker does all expensive work and commits offset only after processing completes.
- Topic key is `installation_id` to preserve per-tenant ordering.

### 2) Monitor queue health continuously
Use Kafdrop (`http://localhost:9000`) for topic-level visibility, plus CLI for lag:

```bash
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group queryguard-workers
```

If `LAG` keeps increasing, scale workers:

```bash
docker compose up --scale worker=3
```

### 3) Treat DLQ as a triage queue, not a sink
- Keep DLQ focused on non-transient failures only (schema mismatch, bad payloads, missing tenant state).
- Include enough metadata for fast debugging (`_dlq_reason`, `_dlq_timestamp`, original envelope).
- Alert on DLQ growth so failures are handled quickly.

### 4) Safe replay workflow
1. Identify and fix root cause in code/config/data.
2. Inspect candidate message from `github-events-dlq`.
3. Re-publish to `github-events`.
4. Verify worker success and that idempotency prevents duplicate side effects.

Consume DLQ messages for inspection:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic github-events-dlq --from-beginning
```

### 5) Retention and capacity guardrails
- Keep finite retention (currently 7 days via `KAFKA_LOG_RETENTION_MS`).
- Revisit retention/partitions when throughput increases.
- Periodically verify topic sizes and consumer lag to avoid silent backlog buildup.

---

## Local Development (no Docker)

```bash
cd backend
pip install -r requirements.txt

# Webhook service
uvicorn app.main:app --reload --port 8000

# Worker (separate terminal)
python worker_main.py
```

Set `VERIFY_GITHUB_SIGNATURE=false` and `KAFKA_ENABLED=false` for quick smoke testing.

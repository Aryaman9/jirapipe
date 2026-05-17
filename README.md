# JiraPipe — JIRA Ticket Triage & Auto-Resolution RAG Pipeline

A cost-aware AI system that uses a two-stage RAG pipeline to automatically classify, route, and resolve JIRA support/engineering tickets. Avoids calling expensive frontier LLMs for every ticket by using a local SLM for signal extraction and vector similarity for known-issue matching.

**Key metric: 73% MTTR reduction** — the system improves over time as resolved tickets become vector search candidates.

## Architecture

```
JIRA Webhook → Kafka → Routing Rules → Ollama (Mistral-7B) → pgvector Search → GPT-4o (novel only)
                                              ↓                        ↓
                                        Signal Extraction        Similarity Match
                                              ↓                        ↓
                                     Embed extracted keywords   ≥0.85 → Auto-resolve
                                                                <0.85 → Escalate to GPT-4o
                                                                        ↓
                                                              Store new embedding
                                                              (self-improving loop)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Runtime | Java 21, Spring Boot 3.2 |
| Messaging | Apache Kafka (KRaft mode) |
| Vector Store | PostgreSQL 16 + pgvector (HNSW index) |
| Cache | Redis 7 (embedding cache, 24h TTL) |
| Local SLM | Ollama + Mistral-7B |
| Frontier LLM | OpenAI GPT-4o |
| Embeddings | OpenAI text-embedding-3-small |
| Resilience | Resilience4j (circuit breakers, retry) |
| Observability | Prometheus + Grafana + Jaeger (OpenTelemetry) |
| API Docs | SpringDoc OpenAPI (Swagger UI) |
| Testing | JUnit 5 + TestContainers |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Java 21+ (for local development)
- Maven 3.9+

### Run with Docker Compose

```bash
# Start all infrastructure + app
docker compose up -d

# Pull Mistral model into Ollama (first time only)
docker exec jirapipe-ollama ollama pull mistral:7b
```

### Run Locally (Development Mode)

```bash
# Start infrastructure only
docker compose up -d postgres redis kafka ollama

# Run app in dev/mock mode (no OpenAI key needed)
./mvnw spring-boot:run -Dspring-boot.run.profiles=dev
```

### Endpoints

| URL | Description |
|-----|-------------|
| http://localhost:8080/swagger-ui.html | API Documentation |
| http://localhost:8080/actuator/health | Health Check |
| http://localhost:8080/actuator/prometheus | Prometheus Metrics |
| http://localhost:3000 | Grafana Dashboard (admin/admin) |
| http://localhost:16686 | Jaeger Tracing UI |
| http://localhost:9090 | Prometheus UI |

## API Endpoints

### Webhook
- `POST /webhook/jira` — Receive JIRA webhook events (returns 202)

### Feedback
- `POST /api/feedback/{ticketKey}` — Submit resolution feedback

### Admin
- `GET /admin/stats` — Pipeline statistics
- `POST /admin/backfill` — Trigger embedding backfill
- `GET /admin/config/rules` — List routing rules
- `POST /admin/config/rules` — Create routing rule
- `DELETE /admin/config/rules/{id}` — Delete routing rule
- `GET /admin/dlq` — View dead letter queue
- `POST /admin/dlq/{id}/retry` — Retry failed ticket

## Pipeline Stages

| Order | Stage | Purpose |
|-------|-------|---------|
| 50 | Routing Rules | DB-driven rule overrides (e.g., "PROD DOWN" → P0) |
| 100 | Local SLM | Ollama/Mistral-7B extracts keywords, components, severity |
| 200 | Vector Search | pgvector cosine similarity over historical tickets |
| 300 | GPT Resolution | GPT-4o generates resolution for novel tickets |

## Cost Architecture

```
Per ticket processing cost:
- Routing Rules:  $0.00 (local logic)
- Ollama SLM:     $0.00 (local inference)
- Embedding:      ~$0.0001 (text-embedding-3-small, cached 60%+)
- Vector Search:  $0.00 (PostgreSQL query)
- GPT-4o:         ~$0.01 (only for novel tickets, <35% of volume)

At 500 tickets/day:
- Without JiraPipe: $5/day ($1,825/year) — GPT-4o on every ticket
- With JiraPipe:    ~$1.40/day (~$511/year) — 72% cost reduction
```

## Configuration

Key properties in `application.yml`:

```yaml
jirapipe:
  pipeline:
    vector-similarity-threshold: 0.85  # Below this → GPT-4o
    auto-resolve-threshold: 0.95       # Above this → auto-apply
  mock-mode: true/false                # Enable mock implementations
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | For prod | OpenAI API key |
| `JIRA_BASE_URL` | For prod | JIRA Cloud instance URL |
| `JIRA_EMAIL` | For prod | JIRA API email |
| `JIRA_API_TOKEN` | For prod | JIRA API token |
| `JIRA_WEBHOOK_SECRET` | Optional | HMAC webhook validation secret |

## Testing

```bash
# Unit tests
./mvnw test

# Integration tests (requires Docker)
./mvnw verify -P integration

# Send test webhook
curl -X POST http://localhost:8080/webhook/jira \
  -H "Content-Type: application/json" \
  -d '{
    "webhookEvent": "jira:issue_created",
    "timestamp": 1700000000000,
    "issue": {
      "id": "10001",
      "key": "PROJ-123",
      "fields": {
        "summary": "Production API returning 500 errors",
        "description": "The /api/users endpoint is returning 500 errors since the last deployment",
        "priority": {"name": "High"},
        "status": {"name": "Open"},
        "issuetype": {"name": "Bug"},
        "labels": ["production", "api"],
        "project": {"key": "PROJ", "name": "Project"},
        "created": "2024-01-15T10:00:00Z",
        "updated": "2024-01-15T10:00:00Z"
      }
    }
  }'
```

# JiraPipe Deep-Dive Documentation

A complete technical reference for every layer, class, and design decision in the JiraPipe system. If you ever need to revisit this project from scratch, this document is your single source of truth.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture & Data Flow](#2-architecture--data-flow)
3. [Project Structure](#3-project-structure)
4. [Entry Point & Bootstrap](#4-entry-point--bootstrap)
5. [Configuration System](#5-configuration-system)
6. [Webhook Ingestion Layer](#6-webhook-ingestion-layer)
7. [Kafka Messaging Layer](#7-kafka-messaging-layer)
8. [Pipeline Engine](#8-pipeline-engine)
9. [Pipeline Stages In-Depth](#9-pipeline-stages-in-depth)
10. [Routing Rules Engine](#10-routing-rules-engine)
11. [LLM Integration Layer](#11-llm-integration-layer)
12. [Embedding & Caching Layer](#12-embedding--caching-layer)
13. [Vector Store & Similarity Search](#13-vector-store--similarity-search)
14. [JIRA Client Layer](#14-jira-client-layer)
15. [Feedback Loop System](#15-feedback-loop-system)
16. [Admin & Operations API](#16-admin--operations-api)
17. [Database Schema In-Depth](#17-database-schema-in-depth)
18. [Resilience & Circuit Breakers](#18-resilience--circuit-breakers)
19. [Observability Stack](#19-observability-stack)
20. [Mock Mode & Dev Profile](#20-mock-mode--dev-profile)
21. [Docker Infrastructure](#21-docker-infrastructure)
22. [Testing Strategy](#22-testing-strategy)
23. [Key Design Patterns](#23-key-design-patterns)
24. [Cost Architecture](#24-cost-architecture)
25. [API Reference](#25-api-reference)
26. [Troubleshooting Guide](#26-troubleshooting-guide)

---

## 1. System Overview

JiraPipe is a cost-aware JIRA ticket triage and auto-resolution system built on a **two-stage RAG (Retrieval-Augmented Generation) pipeline**. The fundamental insight: most support tickets are repetitive. Instead of calling an expensive frontier LLM (GPT-4o) for every ticket, the system:

1. Uses a **local Small Language Model** (Ollama/Mistral-7B) to extract structured signals (keywords, components, severity) from the raw ticket text. This costs $0 because it runs locally.
2. Converts those signals into a **vector embedding** (OpenAI text-embedding-3-small, ~$0.0001/call) and searches a **pgvector database** of previously-resolved tickets.
3. If a match is found with high cosine similarity (>= 0.85), the system reuses the existing resolution. **No GPT-4o call needed.**
4. Only if the ticket is truly **novel** (no similar historical tickets) does the system escalate to GPT-4o (~$0.01/call).

The system gets smarter over time: every GPT-4o resolution is stored as a new vector embedding, so the next similar ticket will be caught by vector search instead. This is the **self-improving loop**.

**Key metric**: 73% MTTR (Mean Time To Resolution) reduction in production.

**Cost savings**: At 500 tickets/day, the system reduces LLM costs from ~$5/day to ~$1.40/day (72% reduction) because 65%+ of tickets are resolved via vector matching or routing rules.

---

## 2. Architecture & Data Flow

### High-Level Flow

```
JIRA Cloud
    |
    | (Webhook POST)
    v
JiraWebhookController (/webhook/jira)
    |
    | 1. Validate HMAC-SHA256 signature
    | 2. Publish raw JSON payload to Kafka
    |
    v
Kafka Topic: jirapipe.tickets.ingestion (3 partitions)
    |
    | (async consume)
    v
TicketIngestionConsumer
    |
    | 1. Deserialize JiraWebhookPayload
    | 2. Check Redis deduplication key
    | 3. Persist ticket to PostgreSQL (PROCESSING)
    | 4. Invoke TriagePipeline asynchronously (@Async)
    |
    v
TriagePipeline.execute(TicketContext)
    |
    | Stage 50: RoutingRulesStage
    |   - Evaluate DB-driven rules (CONTAINS, REGEX, LABEL_MATCH)
    |   - Can SET_PRIORITY, ASSIGN, ESCALATE, or SKIP_PIPELINE
    |   - If SKIP_PIPELINE matched, pipeline terminates here
    |
    | Stage 100: LocalSlmStage
    |   - Send ticket text to Ollama (Mistral-7B)
    |   - Extract: keywords[], componentNames[], errorSignatures[], severityHint, categoryHint
    |   - These become TicketSignals attached to TicketContext
    |
    | Stage 200: VectorSearchStage
    |   - Convert TicketSignals to embedding text via toEmbeddingText()
    |   - Generate embedding via EmbeddingService (OpenAI text-embedding-3-small)
    |   - Check Redis cache first (CachedEmbeddingService decorator)
    |   - Query pgvector: cosine similarity via <=> operator
    |   - Three-tier threshold:
    |       >= 0.95  -->  AUTO-RESOLVE (use historical resolution as-is)
    |       >= 0.85  -->  SUGGEST (use historical resolution, slightly lower confidence)
    |       <  0.85  -->  PASS THROUGH (continue to GPT-4o stage)
    |
    | Stage 300: GptResolutionStage
    |   - Only reached for novel tickets (similarity < 0.85)
    |   - Call GPT-4o via OpenAI-compatible API (OpenRouter)
    |   - Enriched prompt includes extracted signals from Stage 100
    |   - Parse structured JSON response: classification, severity, team, steps
    |   - **Self-improving loop**: store new embedding in pgvector for future matching
    |
    v
TicketPersistenceService
    |
    | - On success: UPDATE tickets SET pipeline_status = 'RESOLVED'
    | - On failure: UPDATE tickets SET pipeline_status = 'FAILED'
    |               INSERT INTO dead_letter_queue
    v
(Optional) JiraApiClient.applyResolution()
    |
    | - Add structured comment to JIRA ticket
    | - Update priority if resolved
    v
Done. Ticket is available via GET /api/tickets/{jiraKey}
```

### Why This Architecture?

The key cost optimization is the **cascade design**: each stage is progressively more expensive but handles fewer tickets:

| Stage | Cost Per Ticket | % of Tickets Reaching This Stage |
|-------|----------------|-----------------------------------|
| Routing Rules | $0.00 | 100% |
| Ollama SLM | $0.00 (local) | ~95% (5% handled by rules) |
| Embedding + Vector Search | ~$0.0001 | ~95% |
| GPT-4o | ~$0.01 | <35% (only novel tickets) |

---

## 3. Project Structure

```
jirapipe/
├── src/main/java/com/jirapipe/
│   ├── JiraPipeApplication.java             # Spring Boot entry point
│   │
│   ├── config/                              # Configuration beans
│   │   ├── JiraPipeProperties.java          # Typed config (@ConfigurationProperties)
│   │   ├── KafkaConfig.java                 # Topic creation (ticket-ingestion, dead-letter)
│   │   ├── LlmConfig.java                   # Mock LLM bean wiring for dev profile
│   │   └── OpenApiConfig.java               # Swagger UI customization
│   │
│   ├── webhook/                             # HTTP entry point
│   │   ├── JiraWebhookController.java       # POST /webhook/jira
│   │   ├── WebhookSignatureValidator.java   # HMAC-SHA256 validation
│   │   └── dto/
│   │       └── JiraWebhookPayload.java      # Nested records for JIRA JSON
│   │
│   ├── ingestion/                           # Kafka produce/consume + persistence
│   │   ├── TicketIngestionProducer.java      # KafkaTemplate publisher
│   │   ├── TicketIngestionConsumer.java      # @KafkaListener + dedup + async pipeline
│   │   └── TicketPersistenceService.java     # JDBC: save, markResolved, markFailed
│   │
│   ├── pipeline/                            # Core triage engine
│   │   ├── TriagePipeline.java              # Orchestrator (iterates stages, records metrics)
│   │   ├── context/
│   │   │   ├── TicketContext.java            # Mutable state carrier through pipeline
│   │   │   ├── TicketSignals.java            # Record: extracted ML signals
│   │   │   └── StageResult.java             # Record: stage outcome (success/terminal/failure)
│   │   ├── stage/
│   │   │   ├── PipelineStage.java           # Interface: process(), shouldExecute(), getOrder()
│   │   │   ├── RoutingRulesStage.java       # Order=50: DB rule evaluation
│   │   │   ├── LocalSlmStage.java           # Order=100: Ollama signal extraction
│   │   │   ├── VectorSearchStage.java       # Order=200: pgvector similarity search
│   │   │   └── GptResolutionStage.java      # Order=300: GPT-4o resolution + embedding store
│   │   └── routing/
│   │       ├── RoutingRule.java             # Interface + RoutingAction record
│   │       └── RoutingRuleEngine.java       # JDBC-based rule loader + evaluator
│   │
│   ├── embedding/                           # Vector embedding generation
│   │   ├── EmbeddingService.java            # Interface: float[] embed(String)
│   │   ├── OpenAiEmbeddingService.java      # Real impl: calls /embeddings API
│   │   ├── CachedEmbeddingService.java      # @Primary decorator: Redis cache layer
│   │   └── MockEmbeddingService.java        # Dev profile: deterministic random vectors
│   │
│   ├── vectorstore/                         # PostgreSQL pgvector operations
│   │   ├── VectorStoreRepository.java       # Interface: findSimilar(), store()
│   │   ├── PgVectorRepository.java          # JDBC impl with <=> cosine distance
│   │   └── dto/
│   │       └── SimilarTicket.java           # Record: match result
│   │
│   ├── llm/                                 # Language model clients
│   │   ├── LlmService.java                 # Interface: extractSignals(), generateResolution()
│   │   ├── OllamaService.java              # Local SLM: Mistral-7B signal extraction
│   │   ├── OpenAiService.java              # GPT-4o: resolution generation
│   │   ├── MockLlmService.java             # Dev profile: canned responses
│   │   └── dto/
│   │       └── ResolutionResult.java        # Record: resolution output
│   │
│   ├── jira/                                # JIRA Cloud REST API client
│   │   ├── JiraApiClient.java              # Interface: addComment, updatePriority, etc.
│   │   ├── JiraRestClient.java             # Real impl: REST API v3, Basic auth
│   │   └── MockJiraClient.java             # Dev profile: logs instead of calling API
│   │
│   ├── feedback/                            # Resolution feedback loop
│   │   ├── FeedbackController.java          # POST /api/feedback/{ticketKey}
│   │   ├── FeedbackService.java             # Business logic: boost/flag embeddings
│   │   └── dto/
│   │       └── FeedbackRequest.java         # Validated request record
│   │
│   ├── admin/                               # Admin & operations API
│   │   ├── AdminController.java             # /admin/stats, /admin/config/rules, /admin/dlq
│   │   ├── TicketStatusController.java      # GET /api/tickets, GET /api/tickets/{key}
│   │   ├── BackfillService.java             # @Async batch embedding backfill
│   │   └── dto/
│   │       ├── PipelineStats.java           # Stats response record
│   │       ├── RoutingRuleDto.java          # Rule CRUD request/response
│   │       └── BackfillRequest.java         # Backfill parameters
│   │
│   ├── health/                              # Custom Actuator health indicators
│   │   ├── OllamaHealthIndicator.java       # Pings /api/tags
│   │   └── OpenAiHealthIndicator.java       # Reports configured/mock status
│   │
│   ├── observability/                       # Metrics instrumentation
│   │   └── PipelineMetrics.java             # Micrometer counters, timers, gauges
│   │
│   └── common/
│       └── exception/
│           └── GlobalExceptionHandler.java  # @ControllerAdvice error formatting
│
├── src/main/resources/
│   ├── application.yml                      # Central config (all defaults)
│   ├── application-dev.yml                  # Dev/mock profile overrides
│   ├── application-docker.yml               # Docker internal hostnames
│   └── db/migration/
│       ├── V1__create_tickets_table.sql
│       ├── V2__create_embeddings_table.sql
│       ├── V3__create_resolutions_table.sql
│       ├── V4__create_feedback_table.sql
│       ├── V5__create_dlq_table.sql
│       └── V6__create_routing_rules_table.sql
│
├── src/test/java/com/jirapipe/
│   ├── unit/                                # Fast unit tests (no Docker needed)
│   └── integration/
│       └── PipelineIntegrationTest.java     # TestContainers end-to-end tests
│
├── docker/
│   ├── prometheus/prometheus.yml            # Scrape config
│   └── grafana/
│       ├── dashboards/
│       │   ├── dashboard.yml                # Provisioning config
│       │   └── jirapipe-dashboard.json      # 14-panel dashboard
│       └── datasources/
│           └── datasource.yml               # Prometheus datasource
│
├── docker-compose.yml                       # 8 services (+ app = 9)
├── Dockerfile                               # Multi-stage build
├── pom.xml                                  # Maven dependencies
└── start-dev.cmd                            # Dev startup script (gitignored)
```

---

## 4. Entry Point & Bootstrap

### `JiraPipeApplication.java`

```java
@SpringBootApplication
@EnableAsync
public class JiraPipeApplication {
    public static void main(String[] args) {
        SpringApplication.run(JiraPipeApplication.class, args);
    }
}
```

**Key annotations:**
- `@SpringBootApplication` — enables component scanning, auto-configuration, and property binding.
- `@EnableAsync` — this is **critical**. It enables the `@Async` annotation used in `TicketIngestionConsumer.processAsync()`. Without this, pipeline processing would block the Kafka consumer thread, creating a bottleneck. With it, each ticket is dispatched to a separate thread from the default `SimpleAsyncTaskExecutor` pool.

### Startup Sequence

1. Spring Boot scans `com.jirapipe` for `@Component`, `@Service`, `@Repository`, `@Controller`, `@Configuration`
2. `JiraPipeProperties` binds all `jirapipe.*` config from `application.yml`
3. Flyway runs migrations V1-V6 against PostgreSQL (creates tables if they don't exist)
4. `KafkaConfig` creates the `jirapipe.tickets.ingestion` and `jirapipe.tickets.dlq` topics
5. Conditional beans activate based on `jirapipe.mock-mode`:
   - `true` → MockLlmService, MockEmbeddingService, MockJiraClient
   - `false` → OllamaService, OpenAiService + CachedEmbeddingService, JiraRestClient
6. Health indicators register with Actuator
7. Prometheus metrics endpoint becomes available at `/actuator/prometheus`
8. Swagger UI available at `/swagger-ui.html`

---

## 5. Configuration System

### `JiraPipeProperties.java`

This is the **typed configuration class** that binds all properties under the `jirapipe.*` namespace. It uses `@ConfigurationProperties(prefix = "jirapipe")` which provides compile-time safety and IDE auto-completion.

**Nested structure:**

| Property Class | Prefix | Key Properties |
|----------------|--------|----------------|
| `JiraPipeProperties` | `jirapipe` | `mockMode` (boolean) |
| `OllamaProperties` | `jirapipe.ollama` | `baseUrl`, `model`, `timeout` |
| `OpenAiProperties` | `jirapipe.openai` | `baseUrl`, `apiKey`, `embeddingModel`, `completionModel`, `timeout`, `maxTokens` |
| `JiraProperties` | `jirapipe.jira` | `baseUrl`, `email`, `apiToken`, `webhookSecret` |
| `PipelineProperties` | `jirapipe.pipeline` | `vectorSimilarityThreshold` (0.85), `autoResolveThreshold` (0.95), `maxSimilarResults` (5) |
| `RetryProperties` | `jirapipe.retry` | `maxAttempts` (3), `initialDelayMs` (1000), `multiplier` (2.0), `maxDelayMs` (30000) |
| `CacheProperties` | `jirapipe.cache` | `embeddingTtlHours` (24) |
| `KafkaTopicsProperties` | `jirapipe.kafka.topics` | `ticketIngestion`, `deadLetter` |

**Why standard classes instead of records?** Spring's `@ConfigurationProperties` requires mutable setters for property binding. Java records are immutable by design, so they can't be used here. JPA entities have the same constraint (Hibernate proxying requires a default constructor and setters).

### `application.yml` — The Master Config

All configuration lives in `application.yml` with sensible defaults. Every value can be overridden by environment variables using Spring's `${ENV_VAR:default}` syntax.

**Key sections:**

- **Kafka**: Consumer group `jirapipe-triage`, `auto-offset-reset: earliest`, JSON serialization
- **DataSource**: PostgreSQL on `localhost:5432/jirapipe` (override with `DB_HOST`, `DB_PORT`, `DB_NAME`)
- **JPA**: `ddl-auto: validate` — Hibernate validates the schema but **never** auto-generates DDL. Flyway handles all schema changes.
- **Flyway**: Enabled, reads from `classpath:db/migration`
- **Redis**: `localhost:6379` (override with `REDIS_HOST`, `REDIS_PORT`)
- **Resilience4j**: Three circuit breaker instances — `ollama`, `openai`, `jira` — each with sliding window of 10, 50% failure threshold
- **Actuator**: Exposes `health`, `info`, `metrics`, `prometheus` endpoints. `show-details: always` gives full health breakdown.
- **Tracing**: 100% sampling probability, OTLP export to `localhost:4318` (Jaeger)
- **Logging**: Custom pattern includes `[%X{correlationId}]` for distributed tracing correlation. `com.jirapipe` is set to `DEBUG` level.

### Spring Profiles

| Profile | Activated By | Purpose |
|---------|-------------|---------|
| `default` | No profile set | Production mode — real LLM + JIRA clients |
| `dev` | `--spring.profiles.active=dev` | Mock mode — no external API calls needed |
| `docker` | Set in docker-compose.yml | Uses Docker internal hostnames (kafka:29092, postgres:5432, etc.) |

The `dev` profile sets `jirapipe.mock-mode: true`, which activates:
- `MockLlmService` (via `LlmConfig`)
- `MockEmbeddingService` (via `@ConditionalOnProperty`)
- `MockJiraClient` (via `@ConditionalOnProperty`)

---

## 6. Webhook Ingestion Layer

### `JiraWebhookController.java`

**Endpoint:** `POST /webhook/jira`

This is the system's entry point. JIRA Cloud fires webhooks to this endpoint when tickets are created or updated.

**Flow:**
1. Receives raw JSON payload as `String` (not deserialized yet — needed for HMAC validation)
2. Extracts `X-Hub-Signature` header (optional)
3. Calls `WebhookSignatureValidator.isValid(rawPayload, signature)`
4. If valid, publishes the raw payload string to Kafka via `TicketIngestionProducer`
5. Returns `202 Accepted` immediately — processing is fully async

**Why 202 instead of 200?** The HTTP 202 status code means "accepted for processing" — it signals to JIRA that the webhook was received but the actual ticket processing will happen asynchronously. This is important because JIRA webhooks have a timeout; if we tried to run the full pipeline synchronously, it would time out.

**Why raw String instead of parsed object?** The raw payload string is needed for HMAC signature validation — the signature is computed over the exact bytes received, not over a re-serialized object. Kafka also stores it as a string for flexibility.

### `WebhookSignatureValidator.java`

Validates JIRA webhook authenticity using **HMAC-SHA256**.

**Logic:**
- If no `JIRA_WEBHOOK_SECRET` is configured (empty string), **all requests pass through**. This is intentional for development/testing.
- If a secret IS configured but the request has no signature header, the request is **rejected** (401).
- If both exist, it computes `HMAC-SHA256(secret, payload)` using `javax.crypto.Mac`, converts to hex, and compares against the provided signature (stripping the `sha256=` prefix).

**Security note:** The `HexFormat.of().formatHex()` method (Java 17+) is used instead of manual hex conversion. The comparison uses `equalsIgnoreCase()` — constant-time comparison would be more secure but this is adequate for webhook validation.

### `JiraWebhookPayload.java`

Nested Java records that model the JIRA webhook JSON structure:

```
JiraWebhookPayload
├── webhookEvent (String): "jira:issue_created", "jira:issue_updated"
├── timestamp (long): Unix timestamp in ms
└── issue (JiraIssue)
    ├── id (String)
    ├── key (String): "PROJ-123"
    └── fields (JiraFields)
        ├── summary (String)
        ├── description (String)
        ├── priority (JiraPriority): { name: "High" }
        ├── status (JiraStatus): { name: "Open" }
        ├── issueType (JiraIssueType): { name: "Bug" }
        ├── reporter (JiraUser): { displayName, emailAddress }
        ├── assignee (JiraUser, nullable)
        ├── labels (List<String>)
        ├── project (JiraProject): { key: "PROJ", name: "My Project" }
        ├── created (Instant)
        └── updated (Instant)
```

All records use `@JsonIgnoreProperties(ignoreUnknown = true)` so that extra fields in JIRA's JSON don't cause deserialization failures. The `@JsonProperty` annotations map JSON field names to Java names (e.g., `issuetype` -> `issueType`).

---

## 7. Kafka Messaging Layer

### Why Kafka?

Kafka provides **asynchronous decoupling** between webhook ingestion and pipeline processing. This gives us:
- **Back-pressure handling**: If the pipeline is slow, messages queue in Kafka instead of timing out HTTP connections.
- **Durability**: Messages are persisted to disk. If the app crashes mid-processing, messages are re-consumed on restart.
- **Scalability**: The `jirapipe.tickets.ingestion` topic has 3 partitions, allowing up to 3 consumer instances to process tickets in parallel.

### `KafkaConfig.java`

Creates two topics at startup:
- `jirapipe.tickets.ingestion` — 3 partitions, 1 replica (main processing topic)
- `jirapipe.tickets.dlq` — 1 partition, 1 replica (dead letter queue topic, reserved for future use)

The 3-partition design on the ingestion topic means that in a multi-instance deployment, up to 3 JiraPipe instances can process tickets concurrently, with Kafka automatically distributing messages across partitions.

**KRaft mode**: The docker-compose uses Kafka in KRaft (Kafka Raft) mode — no ZooKeeper dependency. This is the modern Kafka deployment model. The `KAFKA_PROCESS_ROLES: broker,controller` config makes a single node act as both broker and controller.

### `TicketIngestionProducer.java`

Publishes raw webhook payload strings to Kafka:

```java
kafkaTemplate.send(topic, payload)
    .whenComplete((result, ex) -> {
        if (ex != null) log.error(...);
        else log.debug("Published to partition {} offset {}", ...);
    });
```

Uses `KafkaTemplate<String, String>` — the key is null (Kafka round-robins across partitions) and the value is the raw JSON string. The `whenComplete` callback is non-blocking — it fires asynchronously when the broker acknowledges the write.

### `TicketIngestionConsumer.java`

The core Kafka consumer. This is where the pipeline is triggered.

**`@KafkaListener` annotation:**
```java
@KafkaListener(topics = "${jirapipe.kafka.topics.ticket-ingestion}", groupId = "jirapipe-triage")
```
This listens on the configured topic with consumer group `jirapipe-triage`. The topic name is resolved from YAML at runtime.

**Processing flow:**

1. **Deserialize**: `objectMapper.readValue(payload, JiraWebhookPayload.class)` — converts the raw JSON string back into the typed record structure.

2. **Null guard**: If `webhook.issue()` is null, log a warning and return. Some JIRA events (like board-level events) don't include issue data.

3. **Deduplication via Redis**: Checks `redisTemplate.hasKey("processing:" + jiraKey)`. If the key exists, this ticket is already being processed (duplicate webhook). JIRA sometimes sends duplicate webhooks for the same event.

4. **Mark as processing**: Sets a Redis key `processing:{jiraKey}` with a 30-minute TTL. This serves as an **idempotency lock** — prevents duplicate processing if JIRA retries the webhook.

5. **Build TicketContext**: Extracts fields from the webhook payload into a `TicketContext` object. This is the mutable state carrier that flows through all pipeline stages.

6. **Persist**: Calls `persistenceService.saveIngestedTicket(context)` — inserts the ticket into PostgreSQL with `pipeline_status = 'PROCESSING'`. Uses `ON CONFLICT (jira_key) DO UPDATE` for upsert behavior (idempotent writes).

7. **Async pipeline execution**: `processAsync(context)` — this is `@Async`, so it runs in a separate thread. The Kafka consumer thread is freed immediately.

**`processAsync()` method:**
```java
@Async
protected void processAsync(TicketContext context) {
    PipelineResult result = pipeline.execute(context);
    if (result.success()) {
        persistenceService.markResolved(context);
    } else {
        persistenceService.markFailed(context, result.errorMessage());
    }
}
```

After the pipeline completes, the ticket is either marked as RESOLVED or FAILED. Failed tickets are also inserted into the `dead_letter_queue` table for later retry.

### `TicketPersistenceService.java`

Pure JDBC operations for ticket lifecycle management:

- **`saveIngestedTicket()`**: `INSERT ... ON CONFLICT (jira_key) DO UPDATE` — idempotent. If a ticket with the same JIRA key already exists, it updates the metadata and resets status to PROCESSING.

- **`markResolved()`**: Updates `pipeline_status`, `resolution_source`, `confidence`, and `resolved_at`.

- **`markFailed()`**: Updates `pipeline_status` to FAILED and inserts a record into `dead_letter_queue` with the error message and a JSON payload snapshot.

---

## 8. Pipeline Engine

### `TriagePipeline.java` — The Orchestrator

This is the heart of the system. It takes a `TicketContext` and runs it through an ordered chain of `PipelineStage` implementations.

**Constructor injection with auto-sorting:**
```java
public TriagePipeline(List<PipelineStage> stages, PipelineMetrics metrics) {
    this.stages = stages.stream()
            .sorted(Comparator.comparingInt(PipelineStage::getOrder))
            .toList();
    this.metrics = metrics;
}
```

Spring automatically injects ALL beans implementing `PipelineStage`. The constructor sorts them by `getOrder()`:
- 50 → RoutingRulesStage
- 100 → LocalSlmStage
- 200 → VectorSearchStage
- 300 → GptResolutionStage

**Execution logic (`execute()`):**

1. Sets MDC (Mapped Diagnostic Context) with `correlationId` and `jiraKey` — every log line from this thread will include these values for traceability.
2. Iterates through sorted stages:
   - Calls `stage.shouldExecute(context)` — if false, stage is skipped
   - Calls `stage.process(context)` — the actual work
   - Adds the `StageResult` to the context's result list
   - If result is a failure, records a stage failure metric
   - If result is `terminal`, breaks the loop (no more stages run)
3. Records pipeline duration and resolution source in Prometheus metrics
4. Returns a `PipelineResult` record with the final context, duration, success flag, and error message
5. Always clears MDC in `finally` block to prevent thread-local leaks

**`PipelineResult` inner record:**
```java
public record PipelineResult(TicketContext context, Duration duration, boolean success, String errorMessage) {}
```

### Context Objects

#### `TicketContext.java`

The **mutable state carrier** that flows through the entire pipeline. It starts with ticket metadata (from the webhook) and accumulates analysis results as each stage processes it.

**Input fields** (set from webhook):
- `ticketId` (UUID) — generated at persistence time
- `jiraKey` (String) — e.g., "PROJ-123"
- `projectKey`, `summary`, `description`, `priority`, `issueType`, `labels`, `createdAt`
- `correlationId` (UUID string) — generated in constructor for distributed tracing

**Pipeline output fields** (set by stages):
- `extractedSignals` (TicketSignals) — set by LocalSlmStage
- `resolvedPriority` — may be set by routing rules, SLM, or GPT-4o
- `resolvedClassification` — e.g., "Bug", "Feature", "Infra"
- `resolvedTeam` — e.g., "Platform Engineering"
- `resolutionText` — the actual resolution/suggestion text
- `resolutionSource` — "RULE_OVERRIDE", "VECTOR_MATCH", "GPT4O", "ESCALATED"
- `confidence` — 0.0 to 1.0, how confident the system is in the resolution
- `terminal` — if true, no more stages should execute
- `stageResults` — list of StageResult from each stage

**Design decision**: This is a plain Java class (not a record) because it needs to be mutable — stages progressively set fields as they process the ticket.

#### `TicketSignals.java`

A Java record holding the structured signals extracted by the local SLM:

```java
public record TicketSignals(
    List<String> keywords,          // ["login", "500", "error", "authentication"]
    List<String> componentNames,    // ["api-gateway", "auth-service"]
    List<String> errorSignatures,   // ["NullPointerException at AuthController:42"]
    String severityHint,            // "P0", "P1", "P2", "P3"
    String categoryHint             // "Bug", "Feature", "Infra", "Config"
)
```

**`toEmbeddingText()`**: Concatenates keywords, componentNames, and errorSignatures into a single string for embedding generation. This is the text that gets converted to a vector and searched against pgvector.

```java
public String toEmbeddingText() {
    StringBuilder sb = new StringBuilder();
    if (keywords != null && !keywords.isEmpty()) sb.append(String.join(" ", keywords));
    if (componentNames != null && !componentNames.isEmpty()) sb.append(" ").append(String.join(" ", componentNames));
    if (errorSignatures != null && !errorSignatures.isEmpty()) sb.append(" ").append(String.join(" ", errorSignatures));
    return sb.toString().trim();
}
```

**Why extract signals before embedding?** Raw ticket text is noisy — it contains greetings, context-setting sentences, formatting artifacts. The SLM extracts only the *technical signals*, which produces much better embedding quality and cosine similarity accuracy.

#### `StageResult.java`

An immutable record representing a stage's outcome:

```java
public record StageResult(String stageName, boolean success, boolean terminal, String message, Duration duration)
```

Three factory methods express common outcomes:
- `StageResult.success(name, duration)` — stage completed, continue to next stage
- `StageResult.terminal(name, message, duration)` — stage completed AND pipeline should stop here
- `StageResult.failure(name, message, duration)` — stage failed, but pipeline can continue

**When is `terminal` used?**
- RoutingRulesStage: `SKIP_PIPELINE` action matched → terminal
- VectorSearchStage: similarity >= 0.85 → terminal (resolution found)
- GptResolutionStage: always terminal (it's the last stage anyway)

### `PipelineStage.java` — The Interface

```java
public interface PipelineStage {
    StageResult process(TicketContext context);      // Do the actual work
    boolean shouldExecute(TicketContext context);     // Gate: should this stage run?
    int getOrder();                                   // Execution order (lower = first)
    String getName();                                 // Stage name for logging/metrics
}
```

Every stage implements this interface. The pipeline engine uses `getOrder()` for sorting and `shouldExecute()` for conditional skipping (all stages except RoutingRulesStage skip if `context.isTerminal()` is true).

---

## 9. Pipeline Stages In-Depth

### Stage 50: `RoutingRulesStage.java`

**Purpose**: Evaluate configurable, database-driven rules BEFORE any ML processing. This handles urgent patterns (like "PROD DOWN") that don't need ML to classify.

**How it works:**
1. Calls `ruleEngine.evaluate(context)` — loads active rules from the `routing_rules` table and tests each one
2. If a rule matches, applies the action:
   - `SET_PRIORITY` → sets `resolvedPriority` on context
   - `ASSIGN` → sets `resolvedTeam` on context
   - `ESCALATE` → sets priority to P0 AND assigns to team
   - `SKIP_PIPELINE` → sets resolution text, marks terminal (no more stages run)
3. If no rules match, returns success and the pipeline continues

**Always executes**: `shouldExecute()` returns `true` unconditionally — rules are always checked first.

### Stage 100: `LocalSlmStage.java`

**Purpose**: Extract structured signals from raw ticket text using a local language model (Ollama/Mistral-7B).

**How it works:**
1. Uses `@Qualifier("ollamaService")` to inject the Ollama-specific LLM service
2. Calls `llmService.extractSignals(summary, description)` — sends a structured prompt to Ollama
3. Sets the resulting `TicketSignals` on the context
4. If the SLM provides a severity hint and no priority was set by routing rules, adopts the SLM's suggestion
5. If extraction fails (Ollama is down, parse error, etc.), logs a warning and returns a **failure** result — but the pipeline CONTINUES. The VectorSearchStage will fall back to using raw ticket text instead of extracted signals.

**Graceful degradation**: Even if Ollama is completely unavailable, the circuit breaker fallback in `OllamaService` splits the summary on whitespace and returns those words as "keywords". This ensures the pipeline never completely fails due to SLM issues.

### Stage 200: `VectorSearchStage.java`

**Purpose**: Search historical tickets for similar issues using vector embeddings and cosine similarity.

**How it works:**

1. **Prepare text for embedding**: Uses `extractedSignals.toEmbeddingText()` if signals were extracted by Stage 100. Falls back to `summary + description` if signals are null (SLM failed).

2. **Generate embedding**: Calls `embeddingService.embed(text)` — this goes through `CachedEmbeddingService` (which checks Redis first, then calls OpenAI).

3. **Query pgvector**: `vectorStore.findSimilar(embedding, maxSimilarResults)` — executes a cosine similarity search against the `ticket_embeddings` table using the `<=>` operator and HNSW index.

4. **Three-tier threshold evaluation**:
   ```
   topMatch.similarity >= 0.95 → AUTO-RESOLVE
       Set source = "VECTOR_MATCH", terminal = true
       Use the matched ticket's resolution text directly

   topMatch.similarity >= 0.85 → SUGGEST
       Set source = "VECTOR_MATCH", terminal = true
       Use the matched ticket's resolution text (lower confidence)

   topMatch.similarity < 0.85 → PASS THROUGH
       Return success (non-terminal)
       Pipeline continues to GptResolutionStage
   ```

**Why two thresholds?** The 0.95 threshold is for cases where we're very confident the match is correct (nearly identical tickets). The 0.85 threshold covers "close enough" matches. Below 0.85, the ticket is considered novel and needs GPT-4o analysis. These thresholds are configurable in `application.yml`.

### Stage 300: `GptResolutionStage.java`

**Purpose**: Generate a resolution for novel tickets using GPT-4o (via OpenRouter API).

**How it works:**

1. **Only reached for novel tickets**: `shouldExecute()` returns false if context is already terminal (which happens if vector search found a match).

2. **Call GPT-4o**: `llmService.generateResolution(context)` — sends an enriched prompt including the ticket details AND the extracted signals from Stage 100.

3. **Handle GPT-4o unavailability**: If the circuit breaker is open or the call fails, `result` is null. In that case, the ticket is marked as `ESCALATED` — it needs manual human review.

4. **Apply resolution**: Sets classification, severity, team, resolution text, and confidence on the context.

5. **Self-improving loop** (critical!): After a successful GPT-4o resolution, `storeNewEmbedding()` is called:
   ```java
   private void storeNewEmbedding(TicketContext context) {
       String textToEmbed = context.getExtractedSignals() != null
               ? context.getExtractedSignals().toEmbeddingText()
               : context.getSummary();
       float[] embedding = embeddingService.embed(textToEmbed);
       if (embedding != null && context.getTicketId() != null) {
           String contentHash = sha256(textToEmbed);
           vectorStore.store(context.getTicketId(), embedding, contentHash);
       }
   }
   ```
   This stores the new embedding in pgvector. The NEXT time a similar ticket arrives, VectorSearchStage will find this embedding and resolve it without calling GPT-4o. This is how the system gets smarter over time.

**Content hashing**: The `sha256()` method generates a hash of the embedding source text. This is stored alongside the embedding and can be used to detect when the source text has changed (e.g., during backfill).

---

## 10. Routing Rules Engine

### `RoutingRule.java`

```java
public interface RoutingRule {
    boolean matches(TicketContext context);
    RoutingAction apply(TicketContext context);
    int getPriority();
    String getName();
    record RoutingAction(String actionType, String actionValue) {}
}
```

Defines the contract for routing rules. `RoutingAction` is a simple record with an action type and value.

**Action types:**
- `SET_PRIORITY` — actionValue is "P0", "P1", "P2", or "P3"
- `ASSIGN` — actionValue is the team name
- `ESCALATE` — sets P0 + assigns to team
- `SKIP_PIPELINE` — actionValue is the resolution text; skips all remaining stages

### `RoutingRuleEngine.java`

Loads rules from the `routing_rules` database table and evaluates them against the ticket.

**Rule loading:**
```sql
SELECT name, condition_type, condition_value, action_type, action_value
FROM routing_rules
WHERE enabled = TRUE
ORDER BY priority_order ASC
```

Rules are evaluated in priority order. The FIRST matching rule wins (short-circuit evaluation).

**Condition types:**

1. **CONTAINS**: The condition value is a comma-separated list of terms. If ANY term is found in the ticket text (case-insensitive), the rule matches.
   ```
   conditionValue = "PROD DOWN,production down,site down"
   → Matches if ticket text contains "prod down" OR "production down" OR "site down"
   ```

2. **REGEX**: The condition value is a Java regex pattern. Applied to the ticket text with `.find()`.
   ```
   conditionValue = "(?i)(outage|service.*(down|unavailable))"
   → Matches "Service is down", "outage reported", etc.
   ```
   Safety guards: regex length is capped at 500 chars to prevent ReDoS, and the input is capped at 10,000 chars.

3. **LABEL_MATCH**: Checks if the ticket text contains a specific label. Currently a simplified implementation that looks for the condition value in the text.

**Default rules** (seeded by V6 migration):
| Name | Priority | Condition | Action |
|------|----------|-----------|--------|
| `prod-down-p0` | 1 | CONTAINS: "PROD DOWN, production down, site down" | SET_PRIORITY: P0 |
| `security-incident` | 2 | CONTAINS: "security breach, vulnerability, CVE-" | SET_PRIORITY: P1 |
| `outage-escalate` | 3 | REGEX: `(?i)(outage\|service.*(down\|unavailable))` | SET_PRIORITY: P0 |

---

## 11. LLM Integration Layer

### `LlmService.java` — The Interface

```java
public interface LlmService {
    TicketSignals extractSignals(String summary, String description);
    ResolutionResult generateResolution(TicketContext context);
}
```

Two implementations exist for different purposes:
- `OllamaService` implements `extractSignals()` (throws on `generateResolution()`)
- `OpenAiService` implements `generateResolution()` (throws on `extractSignals()`)

This design enforces the **separation of concerns**: Ollama is cheap and fast (signal extraction), GPT-4o is expensive and smart (resolution generation). They're never interchangeable.

### `OllamaService.java`

**Bean name**: `ollamaService` (explicitly named via `@Service("ollamaService")`)

**Prompt engineering:**
```
You are a JIRA ticket analysis system. Extract structured signals from the following ticket.
Return ONLY valid JSON with these fields:
- keywords: array of relevant technical keywords
- componentNames: array of software component names mentioned
- errorSignatures: array of error messages or stack trace patterns
- severityHint: one of "P0", "P1", "P2", "P3" based on apparent urgency
- categoryHint: one of "Bug", "Feature", "Infra", "Config"

Ticket Summary: {summary}
Ticket Description: {description}
```

**API call:**
- Endpoint: `{ollamaBaseUrl}/api/generate`
- Body: `{ model: "mistral:7b", prompt: "...", stream: false, format: "json" }`
- The `stream: false` flag ensures a single response (not SSE streaming)
- The `format: "json"` flag tells Ollama to constrain output to valid JSON

**Response parsing:**
1. Parse the outer response JSON: `{ response: "..." }` — the `response` field contains the generated text
2. Parse the generated text as JSON
3. Map JSON arrays to `List<String>` via `jsonArrayToList()` helper
4. Construct a `TicketSignals` record

**Circuit breaker fallback:**
```java
private TicketSignals extractSignalsFallback(String summary, String description, Throwable t) {
    List<String> keywords = List.of(summary.split("\\s+"));
    return new TicketSignals(keywords, List.of(), List.of(), null, null);
}
```
When the circuit breaker trips (Ollama down, too many failures), the fallback splits the summary on whitespace and uses those words as keywords. This is a degraded but functional result — the pipeline can still proceed with these rough signals.

### `OpenAiService.java`

**Bean name**: `openAiService`

**Prompt engineering:**
```
You are an expert JIRA ticket triage system. Analyze the following ticket...

Ticket Key: {jiraKey}
Summary: {summary}
Description: {description}
Extracted Keywords: {keywords from Ollama}
Component Names: {components from Ollama}
Error Signatures: {errors from Ollama}

Respond with ONLY valid JSON containing:
- classification, severity, suggestedTeam, resolutionText, resolutionSteps, confidence
```

The enriched prompt includes the signals extracted by Stage 100 — this is the key to the two-stage approach. GPT-4o gets pre-processed signals, not raw noisy text, which improves resolution quality.

**API call:**
- Endpoint: `{openaiBaseUrl}/chat/completions`
- Uses the Chat Completions API format (messages array with system + user roles)
- `response_format: { type: "json_object" }` — forces JSON output
- Default model: `gpt-4o`
- Max tokens: 2048

**OpenRouter compatibility**: The `baseUrl` defaults to `https://openrouter.ai/api/v1`, which is OpenAI-compatible. The same code works with the real OpenAI API by changing the base URL to `https://api.openai.com/v1`.

**Circuit breaker fallback:**
```java
private ResolutionResult resolutionFallback(TicketContext context, Throwable t) {
    return null;
}
```
Returns null, which GptResolutionStage interprets as "escalated for manual review."

### `ResolutionResult.java`

```java
public record ResolutionResult(
    String classification,       // "Bug", "Feature", "Infra", "Config"
    String severity,             // "P0", "P1", "P2", "P3"
    String suggestedTeam,        // "Platform Engineering"
    String resolutionText,       // Free-text resolution summary
    List<String> resolutionSteps, // Step-by-step instructions
    double confidence            // 0.0 - 1.0
)
```

### `MockLlmService.java`

Used in dev/mock mode. No annotations — it's manually instantiated by `LlmConfig`.

**`extractSignals()`**: Splits the summary into keywords, returns canned component names ("api-gateway", "auth-service"). If the summary contains "prod", severity = P0; if it contains "error", category = Bug.

**`generateResolution()`**: Returns a canned resolution with 0.82 confidence. The resolution text includes the project key for slight variability.

### `LlmConfig.java`

```java
@Configuration
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "true")
public class LlmConfig {
    @Bean("ollamaService")
    public LlmService mockOllamaService() { return new MockLlmService(); }

    @Bean("openAiService")
    public LlmService mockOpenAiService() { return new MockLlmService(); }
}
```

**Why a separate config class?** `MockLlmService` doesn't have `@Service` or `@ConditionalOnProperty` annotations — it can't self-register. The `LlmConfig` class registers two separate beans with specific qualifier names (`ollamaService` and `openAiService`), matching the `@Qualifier` annotations used in `LocalSlmStage` and `GptResolutionStage`.

This `@Configuration` class is only activated when `jirapipe.mock-mode=true`. When mock-mode is false, `OllamaService` and `OpenAiService` (which have `@Service("ollamaService")` and `@Service("openAiService")`) are used instead.

---

## 12. Embedding & Caching Layer

### `EmbeddingService.java`

```java
public interface EmbeddingService {
    float[] embed(String text);
}
```

Returns a 1536-dimensional float array (the dimensionality of OpenAI's text-embedding-3-small model). Returns `null` if the service is unavailable.

### `OpenAiEmbeddingService.java`

Calls the OpenAI-compatible `/embeddings` endpoint:

```java
Map<String, Object> body = Map.of("model", model, "input", truncated);
String response = restClient.post().uri("/embeddings").body(body).retrieve().body(String.class);
```

**Text truncation**: Input is truncated to 8,000 characters to stay within the model's context window.

**Response parsing**: The response JSON has the structure `{ data: [{ embedding: [0.1, 0.2, ...] }] }`. The embedding array is extracted and converted to `float[]`.

**Circuit breaker**: `@CircuitBreaker(name = "openai", fallbackMethod = "embeddingFallback")` — returns null when the breaker is open.

**Important**: This bean is NOT `@Primary` and only activates when `jirapipe.mock-mode=false`. When the `CachedEmbeddingService` is also active (same condition), `CachedEmbeddingService` is `@Primary` and wraps this one.

### `CachedEmbeddingService.java` — The Decorator

This is the most important embedding class. It implements the **Decorator pattern** — it wraps another `EmbeddingService` (the `OpenAiEmbeddingService`) and adds Redis caching.

```java
@Service
@Primary
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "false", matchIfMissing = true)
public class CachedEmbeddingService implements EmbeddingService {
    private final EmbeddingService delegate;  // <-- This is OpenAiEmbeddingService
```

**Cache key generation:**
```java
private String sha256(String input) {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
    return HexFormat.of().formatHex(hash);
}
```
Cache key = `"emb:" + SHA-256(text)`. This ensures identical text always maps to the same cache key, regardless of length.

**Cache flow:**
1. Compute SHA-256 of input text
2. Check Redis for `emb:{hash}`
3. **Cache hit**: Deserialize the float[] from JSON, record `metrics.recordCacheHit()`
4. **Cache miss**: Call `delegate.embed(text)`, store result in Redis with 24h TTL, record `metrics.recordCacheMiss()`

**Serialization**: Embeddings (float arrays) are serialized to JSON strings for Redis storage. This is not the most space-efficient format but is human-readable and debuggable.

**TTL**: 24 hours (configurable via `jirapipe.cache.embedding-ttl-hours`). Embeddings are deterministic for the same model and input, so 24h is conservative — in theory they could be cached indefinitely. The TTL helps with memory management in Redis.

### `MockEmbeddingService.java`

Used in dev/mock mode (`@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "true")`).

```java
@Service
@Primary
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "true")
public class MockEmbeddingService implements EmbeddingService {
    public float[] embed(String text) {
        Random random = new Random(text.hashCode());
        float[] embedding = new float[1536];
        // Generate random values, then normalize to unit length
        ...
    }
}
```

**Key property: deterministic**. By seeding `Random` with `text.hashCode()`, the same input text always produces the same embedding. This means mock mode has *some* vector search behavior — identical tickets will match with similarity 1.0. Different tickets will have low similarity (random noise).

**Normalization**: After generating random values, the vector is normalized to unit length (divided by its L2 norm). This is important because cosine similarity assumes unit vectors for accurate distance computation.

---

## 13. Vector Store & Similarity Search

### `VectorStoreRepository.java` — Interface

```java
public interface VectorStoreRepository {
    List<SimilarTicket> findSimilar(float[] embedding, int limit);
    void store(UUID ticketId, float[] embedding, String contentHash);
}
```

### `PgVectorRepository.java` — Implementation

This is where the actual vector similarity search happens. It uses raw JDBC (not JPA) because pgvector's `<=>` operator and `::vector` cast aren't natively supported by Hibernate.

**`findSimilar()` — The Core Search Query:**
```sql
SELECT te.ticket_id, t.jira_key, t.summary,
       r.resolution_text,
       1 - (te.embedding <=> ?::vector) AS similarity
FROM ticket_embeddings te
JOIN tickets t ON t.id = te.ticket_id
LEFT JOIN resolutions r ON r.ticket_id = te.ticket_id AND r.applied = TRUE
WHERE te.flagged = FALSE
ORDER BY te.embedding <=> ?::vector
LIMIT ?
```

**Dissecting this query:**

- `te.embedding <=> ?::vector` — the `<=>` operator is pgvector's **cosine distance** operator. It returns a value between 0 (identical) and 2 (opposite). The `?::vector` casts the string literal `[0.1,0.2,...]` to the PostgreSQL `vector` type.

- `1 - (te.embedding <=> ?::vector) AS similarity` — converts cosine distance to cosine similarity (0 = unrelated, 1 = identical).

- `WHERE te.flagged = FALSE` — excludes embeddings that have been flagged by negative feedback. This is part of the feedback loop.

- `LEFT JOIN resolutions r ON r.ticket_id = te.ticket_id AND r.applied = TRUE` — fetches the resolution text for the matched ticket, so we can reuse it.

- `ORDER BY te.embedding <=> ?::vector` — order by distance ascending (most similar first).

**`store()` — Adding New Embeddings:**
```sql
INSERT INTO ticket_embeddings (ticket_id, embedding, content_hash)
VALUES (?, ?::vector, ?)
ON CONFLICT (ticket_id) DO UPDATE SET
    embedding = EXCLUDED.embedding,
    content_hash = EXCLUDED.content_hash
```
Upserts — if an embedding already exists for this ticket, it's replaced.

**`toVectorLiteral()` — Float Array to pgvector String:**
```java
private String toVectorLiteral(float[] embedding) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < embedding.length; i++) {
        if (i > 0) sb.append(",");
        sb.append(embedding[i]);
    }
    sb.append("]");
    return sb.toString();
}
```
pgvector expects vector literals in the format `[0.1,0.2,0.3,...]`. This method converts a Java float[] to that format. The `StringBuilder` approach is efficient for 1536-element arrays.

### `SimilarTicket.java`

```java
public record SimilarTicket(
    UUID ticketId,
    String jiraKey,
    String summary,
    String resolutionText,
    double similarity         // 0.0 to 1.0
)
```

### HNSW Index

The V2 migration creates an HNSW (Hierarchical Navigable Small World) index:

```sql
CREATE INDEX idx_embeddings_vector ON ticket_embeddings
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
```

**What is HNSW?** It's an approximate nearest neighbor (ANN) algorithm that trades a tiny amount of recall accuracy for massive speed improvements. Instead of comparing against every embedding (O(n)), HNSW navigates a graph structure to find approximate nearest neighbors in O(log n).

**Parameters:**
- `vector_cosine_ops` — use cosine distance for the index
- `m = 16` — each node in the graph connects to 16 neighbors (higher = better recall, more memory)
- `ef_construction = 64` — search depth during index building (higher = better recall, slower build)

---

## 14. JIRA Client Layer

### `JiraApiClient.java` — Interface

```java
public interface JiraApiClient {
    void addComment(String jiraKey, String comment);
    void updatePriority(String jiraKey, String priority);
    void assignTicket(String jiraKey, String team);
    void applyResolution(TicketContext context);
}
```

### `JiraRestClient.java` — Real Implementation

Uses JIRA Cloud REST API v3 with Basic authentication (email + API token).

**Authentication:**
```java
String credentials = properties.getJira().getEmail() + ":" + properties.getJira().getApiToken();
String encoded = Base64.getEncoder().encodeToString(credentials.getBytes());
// Header: "Authorization: Basic {encoded}"
```

**`applyResolution()`**: Formats a structured comment and posts it to the JIRA issue:
```
[JiraPipe Auto-Triage]
Classification: Bug
Severity: P1
Suggested Team: Platform Engineering
Confidence: 92%
Source: VECTOR_MATCH

Resolution:
Check authentication service logs for NullPointerException...
```

All methods are protected by the `jira` circuit breaker. Fallback methods log warnings instead of throwing exceptions.

### `MockJiraClient.java`

Logs all JIRA operations instead of making API calls. Used in dev/mock mode.

```java
@Service
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "true")
public class MockJiraClient implements JiraApiClient {
    public void addComment(String jiraKey, String comment) {
        log.info("[MOCK JIRA] Adding comment to {}: {}...", jiraKey, comment.substring(0, 100));
    }
}
```

---

## 15. Feedback Loop System

The feedback loop allows engineers to rate resolutions, which affects future vector search results.

### `FeedbackController.java`

**Endpoint:** `POST /api/feedback/{ticketKey}`

Accepts a `FeedbackRequest` with validation and delegates to `FeedbackService`.

### `FeedbackRequest.java`

```java
public record FeedbackRequest(
    @NotNull @Min(1) @Max(5) Integer rating,     // 1-5 star rating
    @NotNull Boolean accurate,                    // Was the resolution correct?
    String comment,                               // Optional free-text comment
    String submittedBy                            // Email of the reviewer
)
```

### `FeedbackService.java`

**Transactional feedback processing:**

1. **Finds the ticket** by JIRA key (returns UUID)
2. **Inserts feedback record** into the `feedback` table
3. **Adjusts embeddings based on feedback:**
   - **Positive feedback** (`accurate = true`):
     ```sql
     UPDATE ticket_embeddings SET confidence_boost = confidence_boost + 0.01
     WHERE ticket_id = ?
     ```
     Each positive review adds 0.01 to the confidence boost. This could be used to prioritize higher-confidence embeddings in search results.

   - **Negative feedback** (`accurate = false`):
     ```sql
     UPDATE ticket_embeddings SET flagged = TRUE
     WHERE ticket_id = ?
     ```
     Flagged embeddings are **excluded from future searches** (the `WHERE te.flagged = FALSE` clause in `PgVectorRepository.findSimilar()`). This prevents bad resolutions from propagating.

**This is the feedback loop in action**: Good resolutions get reinforced (used more), bad resolutions get removed from the pool. The system's accuracy improves based on human feedback.

---

## 16. Admin & Operations API

### `AdminController.java`

**`GET /admin/stats`** — Pipeline statistics:
```json
{
  "totalTickets": 150,
  "resolvedTickets": 120,
  "failedTickets": 10,
  "pendingTickets": 20,
  "vectorMatchCount": 78,
  "gptResolutionCount": 32,
  "ruleOverrideCount": 10,
  "averageConfidence": 0.87,
  "dlqSize": 3,
  "cacheHitRate": 0.0
}
```

Each metric is a separate SQL query against the `tickets` and `dead_letter_queue` tables.

**`GET /admin/config/rules`** — List all routing rules (active and inactive)

**`POST /admin/config/rules`** — Create a new routing rule:
```json
{
  "name": "my-custom-rule",
  "priorityOrder": 10,
  "conditionType": "CONTAINS",
  "conditionValue": "database,connection timeout",
  "actionType": "SET_PRIORITY",
  "actionValue": "P1",
  "enabled": true
}
```

**`DELETE /admin/config/rules/{id}`** — Remove a routing rule

**`GET /admin/dlq`** — View dead letter queue entries (last 50)

**`POST /admin/dlq/{id}/retry`** — Reset a DLQ entry to PENDING for retry

**`POST /admin/backfill`** — Trigger async embedding backfill for historical tickets

### `TicketStatusController.java`

**`GET /api/tickets`** — List recent tickets with optional filtering:
- `?limit=20` (default 20)
- `?status=RESOLVED`

**`GET /api/tickets/{jiraKey}`** — Get full ticket details by JIRA key. Returns 404 if not found.

### `BackfillService.java`

Batch processes resolved tickets that don't have embeddings yet:

```sql
SELECT t.id, t.summary, t.description
FROM tickets t
LEFT JOIN ticket_embeddings te ON te.ticket_id = t.id
WHERE te.id IS NULL AND t.pipeline_status = 'RESOLVED'
LIMIT 500
```

For each ticket:
1. Concatenates summary + description
2. Generates an embedding via `EmbeddingService`
3. Stores it in pgvector via `VectorStoreRepository`

**`@Async`**: Runs in a background thread so the HTTP request returns immediately.

---

## 17. Database Schema In-Depth

All tables are created by Flyway migrations (V1-V6) and are never modified by Hibernate (`ddl-auto: validate`).

### `tickets` (V1)

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID (PK) | `gen_random_uuid()` default |
| `jira_key` | VARCHAR(32), UNIQUE | The JIRA issue key (e.g., "PROJ-123") |
| `project_key` | VARCHAR(16) | Project prefix |
| `summary` | TEXT, NOT NULL | Issue title |
| `description` | TEXT | Issue body (can be null) |
| `priority` | VARCHAR(16) | Original JIRA priority |
| `status` | VARCHAR(32) | Original JIRA status |
| `issue_type` | VARCHAR(32) | Bug, Feature, etc. |
| `reporter` | VARCHAR(128) | Reporter display name |
| `assignee` | VARCHAR(128) | Assignee display name |
| `labels` | TEXT[] | PostgreSQL array of strings |
| `created_at` | TIMESTAMPTZ | When the ticket was created in JIRA |
| `updated_at` | TIMESTAMPTZ | Last update time |
| `ingested_at` | TIMESTAMPTZ | When JiraPipe received it (default: NOW()) |
| `pipeline_status` | VARCHAR(32) | PENDING → PROCESSING → RESOLVED / FAILED |
| `resolution_source` | VARCHAR(32) | RULE_OVERRIDE / VECTOR_MATCH / GPT4O / ESCALATED |
| `confidence` | DECIMAL(5,4) | 0.0000 to 1.0000 |
| `resolved_at` | TIMESTAMPTZ | When resolution was applied |

**Indexes**: `jira_key`, `pipeline_status`, `created_at DESC`, `project_key`

### `ticket_embeddings` (V2)

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID (PK) | |
| `ticket_id` | UUID (FK → tickets, UNIQUE) | One embedding per ticket |
| `embedding` | vector(1536) | pgvector type, 1536 dimensions |
| `content_hash` | VARCHAR(64) | SHA-256 of the source text |
| `model_version` | VARCHAR(64) | Default: "text-embedding-3-small" |
| `flagged` | BOOLEAN | Default: FALSE. Set to TRUE by negative feedback |
| `confidence_boost` | DECIMAL(5,4) | Default: 0. Incremented by +0.01 per positive feedback |
| `created_at` | TIMESTAMPTZ | |

**Indexes**:
- `idx_embeddings_vector` — HNSW cosine similarity index (m=16, ef_construction=64)
- `idx_embeddings_not_flagged` — partial index on `flagged WHERE flagged = FALSE` (speeds up the `WHERE flagged = FALSE` filter in search queries)

**The `vector(1536)` type**: This is pgvector's vector type. 1536 is the dimension of OpenAI's text-embedding-3-small model. Each dimension is a 32-bit float, so each embedding takes ~6KB of storage.

### `resolutions` (V3)

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID (PK) | |
| `ticket_id` | UUID (FK → tickets) | |
| `resolution_text` | TEXT | The generated resolution |
| `source` | VARCHAR(32) | VECTOR_MATCH, GPT4O, etc. |
| `similar_ticket_ids` | UUID[] | Array of matched ticket UUIDs |
| `classification` | VARCHAR(32) | Bug, Feature, Infra, Config |
| `severity` | VARCHAR(8) | P0-P3 |
| `suggested_team` | VARCHAR(64) | |
| `confidence` | DECIMAL(5,4) | |
| `applied` | BOOLEAN | Was this resolution applied to JIRA? |
| `applied_at` | TIMESTAMPTZ | |
| `created_at` | TIMESTAMPTZ | |

### `feedback` (V4)

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID (PK) | |
| `ticket_id` | UUID (FK → tickets) | |
| `resolution_id` | UUID (FK → resolutions, nullable) | |
| `rating` | SMALLINT | 1-5, constrained by CHECK |
| `accurate` | BOOLEAN | Was the resolution correct? |
| `comment` | TEXT | Optional reviewer notes |
| `submitted_by` | VARCHAR(128) | Reviewer email |
| `created_at` | TIMESTAMPTZ | |

### `dead_letter_queue` (V5)

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID (PK) | |
| `ticket_id` | UUID (FK → tickets, nullable) | |
| `jira_key` | VARCHAR(32) | |
| `payload` | JSONB | Original webhook payload snapshot |
| `error_message` | TEXT | Why processing failed |
| `error_class` | VARCHAR(256) | Exception class name |
| `stage` | VARCHAR(32) | Which pipeline stage failed |
| `retry_count` | INT | Default: 0 |
| `max_retries` | INT | Default: 3 |
| `last_attempt_at` | TIMESTAMPTZ | |
| `next_retry_at` | TIMESTAMPTZ | |
| `status` | VARCHAR(16) | PENDING / COMPLETED / EXHAUSTED |
| `created_at` | TIMESTAMPTZ | |

**Indexes**: `status`, `next_retry_at WHERE status = 'PENDING'` (partial index for efficient retry polling)

### `routing_rules` (V6)

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID (PK) | |
| `name` | VARCHAR(128), UNIQUE | Rule identifier |
| `priority_order` | INT | Lower = evaluated first |
| `condition_type` | VARCHAR(32) | CONTAINS, REGEX, LABEL_MATCH |
| `condition_value` | TEXT | The pattern to match |
| `action_type` | VARCHAR(32) | SET_PRIORITY, ASSIGN, ESCALATE, SKIP_PIPELINE |
| `action_value` | TEXT | The value to apply |
| `enabled` | BOOLEAN | Default: TRUE |
| `created_at` | TIMESTAMPTZ | |
| `updated_at` | TIMESTAMPTZ | |

---

## 18. Resilience & Circuit Breakers

### Resilience4j Configuration

Three circuit breaker instances are configured in `application.yml`:

| Instance | Window | Failure Threshold | Wait in Open | Half-Open Calls |
|----------|--------|-------------------|--------------|-----------------|
| `ollama` | 10 | 50% | 30s | 3 |
| `openai` | 10 | 50% | 60s | 2 |
| `jira` | 10 | 50% | 30s | 3 |

**How circuit breakers work:**

1. **Closed** (normal): Requests pass through. Failures are tracked in a sliding window of 10 calls.
2. **Open** (breaker tripped): When failure rate exceeds 50%, the breaker opens. ALL requests are immediately rejected and the fallback method is called. No actual calls are made.
3. **Half-Open** (testing recovery): After the wait duration, a limited number of test calls are allowed. If they succeed, the breaker closes. If they fail, it stays open.

**Fallback methods:**
- `OllamaService.extractSignalsFallback()` → returns keyword-split of summary
- `OpenAiService.resolutionFallback()` → returns null (ticket escalated)
- `OpenAiEmbeddingService.embeddingFallback()` → returns null
- `JiraRestClient.addCommentFallback()` → logs warning
- `JiraRestClient.updatePriorityFallback()` → logs warning

**Why different wait durations?** OpenAI has a longer wait (60s) because it's a remote API that might take longer to recover from outages. Ollama is local (30s) and JIRA is another remote API (30s).

---

## 19. Observability Stack

### `PipelineMetrics.java`

Custom Micrometer metrics registered with the `MeterRegistry`:

| Metric Name | Type | Description |
|-------------|------|-------------|
| `jirapipe.tickets.processed` | Counter | Total tickets processed successfully |
| `jirapipe.tickets.failed` | Counter | Total tickets that failed |
| `jirapipe.gpt4o.calls` | Counter | Number of GPT-4o API calls |
| `jirapipe.cache.hits` | Counter | Embedding cache hits |
| `jirapipe.cache.misses` | Counter | Embedding cache misses |
| `jirapipe.pipeline.duration` | Timer | Pipeline execution duration, tagged by `source` |
| `jirapipe.stage.failures` | Counter | Stage failures, tagged by `stage` name |
| `jirapipe.stage.duration` | Timer | Individual stage duration, tagged by `stage` name |

All metrics are exposed at `/actuator/prometheus` in Prometheus exposition format.

### Prometheus

Config at `docker/prometheus/prometheus.yml`:
- Scrapes `jirapipe-app:8080/actuator/prometheus` every 15s
- Data stored in `prometheus_data` Docker volume

### Grafana

14-panel dashboard auto-provisioned at startup. Access at `http://localhost:3000` (admin/admin).

Panels include:
- Tickets Processed/Failed counters
- GPT-4o call rate
- Pipeline duration percentiles (p50, p95, p99)
- Stage duration breakdown
- Cache hit/miss rates
- Circuit breaker states

### Jaeger

Distributed tracing via OpenTelemetry. Every request gets a trace ID that flows through Kafka, pipeline stages, and external API calls.

- OTLP endpoint: `http://localhost:4318/v1/traces`
- Jaeger UI: `http://localhost:16686`
- 100% sampling probability (every trace is captured)

### Structured Logging

Log pattern: `%d{...} [%thread] [%X{correlationId}] %-5level %logger{36} - %msg%n`

The `correlationId` is a UUID generated per ticket in `TicketContext`. It's set in MDC (Mapped Diagnostic Context) at the start of pipeline execution and cleared afterward. This means every log line during a ticket's processing includes the same correlation ID, making it easy to grep all logs for a specific ticket.

---

## 20. Mock Mode & Dev Profile

### Purpose

Mock mode allows running the full pipeline without any external API dependencies:
- No OpenAI API key needed
- No Ollama server needed
- No JIRA instance needed

This is useful for:
- Local development
- Running integration tests (TestContainers)
- Demo/showcase purposes

### How It Works

Setting `jirapipe.mock-mode: true` (or `JIRAPIPE_MOCK_MODE=true` env var) triggers:

1. **`LlmConfig`** activates (`@ConditionalOnProperty(havingValue = "true")`):
   - Registers `MockLlmService` as both `ollamaService` and `openAiService` beans

2. **`MockEmbeddingService`** activates:
   - `@ConditionalOnProperty(havingValue = "true")` + `@Primary`
   - Generates deterministic random embeddings seeded by text hash

3. **`MockJiraClient`** activates:
   - `@ConditionalOnProperty(havingValue = "true")`
   - Logs all JIRA operations

4. **`OpenAiEmbeddingService`** does NOT activate:
   - `@ConditionalOnProperty(havingValue = "false", matchIfMissing = true)`

5. **`CachedEmbeddingService`** does NOT activate:
   - Same condition as OpenAiEmbeddingService

6. **`OllamaService`** and **`OpenAiService`** do NOT activate:
   - Same condition

### Running in Dev Mode

```bash
# Start only infrastructure
docker compose up -d postgres redis kafka

# Run with dev profile (mock mode)
./mvnw spring-boot:run -Dspring-boot.run.profiles=dev
```

---

## 21. Docker Infrastructure

### `Dockerfile` — Multi-Stage Build

```dockerfile
# Stage 1: Build with Maven
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B    # Cache deps in Docker layer
COPY src ./src
RUN mvn package -DskipTests -B

# Stage 2: Runtime
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

**Why multi-stage?** The build stage needs the full JDK + Maven (~800MB). The runtime stage only needs JRE (~180MB). The final image is ~178MB.

**Layer caching**: `pom.xml` is copied and deps downloaded BEFORE copying source code. This means that unless dependencies change, the `mvn dependency:go-offline` layer is cached and not re-executed on code changes.

### `docker-compose.yml` — 9 Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `postgres` | pgvector/pgvector:pg16 | 5432 | Database + vector extensions |
| `redis` | redis:7-alpine | 6379 | Embedding cache + deduplication |
| `kafka` | confluentinc/cp-kafka:7.6.0 | 9092 | Message broker (KRaft mode) |
| `ollama` | ollama/ollama:latest | 11434 | Local SLM runtime |
| `prometheus` | prom/prometheus:latest | 9090 | Metrics collection |
| `grafana` | grafana/grafana:latest | 3000 | Metrics visualization |
| `jaeger` | jaegertracing/all-in-one:latest | 16686 | Distributed tracing |
| `jirapipe` | (built from Dockerfile) | 8080 | The application |

**Dependency ordering**: The `jirapipe` service depends on postgres, redis, kafka, and ollama with `condition: service_healthy`. It won't start until all dependencies pass their health checks.

**Health checks**: Every service has a health check:
- PostgreSQL: `pg_isready`
- Redis: `redis-cli ping`
- Kafka: `kafka-topics --list`
- Ollama: `curl http://localhost:11434/api/tags`

**Volumes**: Persistent data volumes for postgres, redis, kafka, ollama, prometheus, and grafana. Data survives container restarts.

---

## 22. Testing Strategy

### Unit Tests

Located in `src/test/java/com/jirapipe/unit/`. Run with:
```bash
./mvnw test
```

Unit tests don't need Docker or any external services. They test:
- Routing rule evaluation logic
- Pipeline orchestration (stage ordering, terminal handling)
- Webhook signature validation
- Mock service behavior

### Integration Tests

Located in `src/test/java/com/jirapipe/integration/`. Run with:
```bash
./mvnw test -P integration
```

**Requires Docker Desktop** running (TestContainers spins up real containers).

#### `PipelineIntegrationTest.java`

Uses TestContainers to start:
- `pgvector/pgvector:pg16` — real PostgreSQL with pgvector extension
- `redis:7` — real Redis
- `confluentinc/cp-kafka:7.6.0` — real Kafka

**`@DynamicPropertySource`**: Overrides Spring properties with container connection details at runtime:
```java
registry.add("spring.datasource.url", postgres::getJdbcUrl);
registry.add("spring.data.redis.host", redis::getHost);
registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
registry.add("jirapipe.mock-mode", () -> "true");  // Always mock LLM/JIRA in tests
```

**5 Integration Tests:**

1. **Full Pipeline Flow**: POST webhook → Kafka → async processing → verify RESOLVED in DB
2. **Feedback Endpoint**: Insert resolved ticket → POST feedback → verify stored in DB with correct values
3. **Admin Stats**: Insert 2 resolved + 1 failed tickets → GET /admin/stats → verify counts
4. **Webhook with No Issue**: POST webhook without issue data → verify no ticket created
5. **Non-existent Ticket**: GET /api/tickets/NONEXIST-999 → verify 404

**Awaitility**: For async operations (Kafka processing), tests use Awaitility to poll with timeouts:
```java
await().atMost(30, TimeUnit.SECONDS)
    .pollInterval(500, TimeUnit.MILLISECONDS)
    .untilAsserted(() -> {
        List<Map<String, Object>> tickets = jdbcTemplate.queryForList(...);
        assertThat(tickets).isNotEmpty();
        assertThat(tickets.get(0).get("pipeline_status")).isEqualTo("RESOLVED");
    });
```

### Test Separation

Maven Surefire is configured to **exclude** integration tests by default:
```xml
<configuration>
    <excludedGroups>integration</excludedGroups>
</configuration>
```

The `integration` Maven profile overrides this:
```xml
<profile>
    <id>integration</id>
    <configuration>
        <groups>integration</groups>
    </configuration>
</profile>
```

---

## 23. Key Design Patterns

### 1. Strategy Pattern (PipelineStage)
Each pipeline stage implements the `PipelineStage` interface. The `TriagePipeline` orchestrator is agnostic to the specific stages — it just iterates through them. New stages can be added by creating a new `@Component` implementing `PipelineStage` with a unique order number.

### 2. Decorator Pattern (CachedEmbeddingService)
`CachedEmbeddingService` wraps `OpenAiEmbeddingService`, adding Redis caching. Both implement `EmbeddingService`. The decorator is `@Primary`, so Spring injects it wherever `EmbeddingService` is required.

### 3. Template Method (Pipeline Execution)
The pipeline engine defines the execution skeleton (iterate stages, check shouldExecute, handle terminal), while each stage defines its specific processing logic.

### 4. Chain of Responsibility (Stage Ordering)
Stages are ordered (50 → 100 → 200 → 300) and can terminate the chain (terminal result). Each stage decides whether to handle the request or pass it along.

### 5. Producer-Consumer (Kafka)
`TicketIngestionProducer` pushes to Kafka, `TicketIngestionConsumer` pulls from it. This decouples ingestion rate from processing rate.

### 6. Factory Method (StageResult)
`StageResult.success()`, `.terminal()`, `.failure()` — named factory methods make the code more readable than constructors.

### 7. Conditional Bean Registration
`@ConditionalOnProperty` switches between real and mock implementations based on `jirapipe.mock-mode`. No if/else in business logic — Spring's DI handles it.

### 8. Circuit Breaker (Resilience4j)
All external service calls (Ollama, OpenAI, JIRA) use circuit breakers with fallback methods. This prevents cascading failures when downstream services are unhealthy.

---

## 24. Cost Architecture

### Per-Ticket Cost Breakdown

| Stage | Cost | Notes |
|-------|------|-------|
| Routing Rules | $0.00 | Pure DB logic |
| Ollama/Mistral-7B | $0.00 | Local inference |
| Embedding (text-embedding-3-small) | ~$0.0001 | ~$0.02 per 1M tokens; average ticket ~200 tokens |
| Redis cache hit | $0.00 | ~60%+ hit rate after warmup |
| pgvector search | $0.00 | PostgreSQL query |
| GPT-4o resolution | ~$0.01 | ~$5/1M tokens input, ~$15/1M tokens output |

### At Scale (500 tickets/day)

**Without JiraPipe**: $5/day → $1,825/year (GPT-4o for every ticket)

**With JiraPipe**: ~$1.40/day → ~$511/year
- ~5% handled by routing rules alone ($0)
- ~35% handled by vector matching ($0.0001 each for embedding)
- ~25% cache hits on embeddings ($0)
- ~35% escalated to GPT-4o ($0.01 each)

**Annual savings**: ~$1,314 (72% cost reduction)

### Self-Improving Economics

The system gets cheaper over time:
- Day 1: Most tickets are novel → high GPT-4o usage
- Day 30: Common issues have embeddings → 50%+ vector matches
- Day 90: Mature embedding database → 65%+ vector matches
- Steady state: Only truly new issue types hit GPT-4o

---

## 25. API Reference

### Webhook

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| POST | `/webhook/jira` | Receive JIRA webhook event | 202 Accepted / 401 Unauthorized |

**Headers:**
- `Content-Type: application/json`
- `X-Hub-Signature: sha256={hmac}` (optional, required if webhook secret is configured)

### Tickets

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| GET | `/api/tickets` | List recent tickets | 200 + JSON array |
| GET | `/api/tickets/{jiraKey}` | Get ticket by JIRA key | 200 + JSON / 404 |

**Query params for list:**
- `limit` (int, default 20)
- `status` (string, optional: PENDING, PROCESSING, RESOLVED, FAILED)

### Feedback

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| POST | `/api/feedback/{ticketKey}` | Submit resolution feedback | 200 OK |

**Body:**
```json
{
  "rating": 4,
  "accurate": true,
  "comment": "Resolution was helpful",
  "submittedBy": "engineer@company.com"
}
```

### Admin

| Method | Path | Description |
|--------|------|-------------|
| GET | `/admin/stats` | Pipeline statistics |
| POST | `/admin/backfill` | Trigger embedding backfill |
| GET | `/admin/config/rules` | List routing rules |
| POST | `/admin/config/rules` | Create routing rule |
| DELETE | `/admin/config/rules/{id}` | Delete routing rule |
| GET | `/admin/dlq` | View dead letter queue |
| POST | `/admin/dlq/{id}/retry` | Retry DLQ entry |

### Actuator

| Method | Path | Description |
|--------|------|-------------|
| GET | `/actuator/health` | Health check (all dependencies) |
| GET | `/actuator/prometheus` | Prometheus metrics |
| GET | `/swagger-ui.html` | OpenAPI documentation |

---

## 26. Troubleshooting Guide

### Common Issues

**1. Ticket stuck in PROCESSING**
- Check Kafka consumer logs for errors
- Verify Redis is running (dedup lock may be stuck)
- Check `processing:{jiraKey}` key in Redis (30-min TTL)
- If the app crashed during processing, the Redis key will expire after 30 minutes

**2. All tickets going to GPT-4o (no vector matches)**
- This is expected on a fresh system with no historical embeddings
- Run backfill: `POST /admin/backfill`
- Check if Ollama is producing good signals (if signals are noisy, embeddings will be poor)
- Check Redis cache hit rate at `/actuator/prometheus`

**3. Circuit breaker tripped (Ollama/OpenAI)**
- Check the service health: `GET /actuator/health`
- The breaker auto-recovers after the wait duration (30s for Ollama, 60s for OpenAI)
- Monitor at Prometheus: `resilience4j_circuitbreaker_state`

**4. Embeddings not being cached**
- Verify Redis connection: `redis-cli ping`
- Check `jirapipe.cache.embedding-ttl-hours` config
- Look for cache metrics: `jirapipe.cache.hits` / `jirapipe.cache.misses`

**5. Flyway migration fails**
- Ensure PostgreSQL has the pgvector extension: `CREATE EXTENSION IF NOT EXISTS vector;`
- Use the `pgvector/pgvector:pg16` Docker image (has pgvector pre-installed)
- Check `spring.flyway.locations` points to `classpath:db/migration`

**6. Kafka consumer not receiving messages**
- Verify topic exists: `kafka-topics --list --bootstrap-server localhost:9092`
- Check consumer group: `kafka-consumer-groups --describe --group jirapipe-triage`
- Ensure serializer/deserializer settings match between producer and consumer

**7. Docker Compose services not starting**
- Check logs: `docker compose logs -f {service}`
- Ensure ports aren't already in use (5432, 6379, 9092, 11434, 8080)
- On first run, Ollama needs model pull: `docker exec jirapipe-ollama ollama pull mistral:7b`

### Useful Debug Commands

```bash
# Check pipeline stats
curl http://localhost:8080/admin/stats | jq

# Send a test webhook
curl -X POST http://localhost:8080/webhook/jira \
  -H "Content-Type: application/json" \
  -d '{
    "webhookEvent": "jira:issue_created",
    "issue": {
      "key": "TEST-1",
      "fields": {
        "summary": "Login page returns 500 error",
        "description": "Users cannot log in since last deployment",
        "priority": {"name": "High"},
        "issuetype": {"name": "Bug"},
        "labels": ["production"],
        "project": {"key": "TEST"},
        "created": "2024-01-15T10:00:00Z"
      }
    }
  }'

# Check ticket status
curl http://localhost:8080/api/tickets/TEST-1 | jq

# View dead letter queue
curl http://localhost:8080/admin/dlq | jq

# Check health
curl http://localhost:8080/actuator/health | jq

# View Prometheus metrics
curl http://localhost:8080/actuator/prometheus | grep jirapipe
```

---

## Appendix: Environment Variables Quick Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | (empty) | OpenAI/OpenRouter API key |
| `OPENAI_BASE_URL` | `https://openrouter.ai/api/v1` | API base URL |
| `OLLAMA_BASE_URL` | `http://localhost:11500` | Ollama server URL |
| `OLLAMA_MODEL` | `mistral:7b` | Ollama model name |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `jirapipe` | Database name |
| `DB_USER` | `jirapipe` | Database user |
| `DB_PASSWORD` | `jirapipe` | Database password |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `JIRA_BASE_URL` | (empty) | JIRA Cloud instance URL |
| `JIRA_EMAIL` | (empty) | JIRA API email |
| `JIRA_API_TOKEN` | (empty) | JIRA API token |
| `JIRA_WEBHOOK_SECRET` | (empty) | HMAC webhook secret |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318/v1/traces` | Jaeger OTLP endpoint |

---

*This document covers every class, method, configuration option, and design decision in the JiraPipe system. If a class isn't mentioned here, it doesn't exist in the codebase.*

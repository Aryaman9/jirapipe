CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE ticket_embeddings (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id       UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    embedding       vector(1536),
    content_hash    VARCHAR(64) NOT NULL,
    model_version   VARCHAR(64) NOT NULL DEFAULT 'text-embedding-3-small',
    flagged         BOOLEAN DEFAULT FALSE,
    confidence_boost DECIMAL(5,4) DEFAULT 0,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(ticket_id)
);

CREATE INDEX idx_embeddings_vector ON ticket_embeddings
    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

CREATE INDEX idx_embeddings_not_flagged ON ticket_embeddings(flagged) WHERE flagged = FALSE;

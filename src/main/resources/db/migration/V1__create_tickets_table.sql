CREATE TABLE tickets (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    jira_key        VARCHAR(32) NOT NULL UNIQUE,
    project_key     VARCHAR(16) NOT NULL,
    summary         TEXT NOT NULL,
    description     TEXT,
    priority        VARCHAR(16),
    status          VARCHAR(32),
    issue_type      VARCHAR(32),
    reporter        VARCHAR(128),
    assignee        VARCHAR(128),
    labels          TEXT[],
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    pipeline_status VARCHAR(32) DEFAULT 'PENDING',
    resolution_source VARCHAR(32),
    confidence      DECIMAL(5,4),
    resolved_at     TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_tickets_jira_key ON tickets(jira_key);
CREATE INDEX idx_tickets_status ON tickets(pipeline_status);
CREATE INDEX idx_tickets_created ON tickets(created_at DESC);
CREATE INDEX idx_tickets_project ON tickets(project_key);

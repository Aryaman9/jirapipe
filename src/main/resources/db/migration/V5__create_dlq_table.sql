CREATE TABLE dead_letter_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id       UUID REFERENCES tickets(id),
    jira_key        VARCHAR(32),
    payload         JSONB NOT NULL,
    error_message   TEXT NOT NULL,
    error_class     VARCHAR(256),
    stage           VARCHAR(32),
    retry_count     INT DEFAULT 0,
    max_retries     INT DEFAULT 3,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    next_retry_at   TIMESTAMP WITH TIME ZONE,
    status          VARCHAR(16) DEFAULT 'PENDING',
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_dlq_status ON dead_letter_queue(status);
CREATE INDEX idx_dlq_next_retry ON dead_letter_queue(next_retry_at) WHERE status = 'PENDING';

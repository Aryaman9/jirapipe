CREATE TABLE resolutions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id       UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    resolution_text TEXT NOT NULL,
    source          VARCHAR(32) NOT NULL,
    similar_ticket_ids UUID[],
    classification  VARCHAR(32),
    severity        VARCHAR(8),
    suggested_team  VARCHAR(64),
    confidence      DECIMAL(5,4) NOT NULL,
    applied         BOOLEAN DEFAULT FALSE,
    applied_at      TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_resolutions_ticket ON resolutions(ticket_id);
CREATE INDEX idx_resolutions_source ON resolutions(source);

CREATE TABLE feedback (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id       UUID NOT NULL REFERENCES tickets(id),
    resolution_id   UUID REFERENCES resolutions(id),
    rating          SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    accurate        BOOLEAN NOT NULL,
    comment         TEXT,
    submitted_by    VARCHAR(128),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_feedback_ticket ON feedback(ticket_id);
CREATE INDEX idx_feedback_rating ON feedback(rating);

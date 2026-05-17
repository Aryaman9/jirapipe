CREATE TABLE routing_rules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(128) NOT NULL UNIQUE,
    priority_order  INT NOT NULL,
    condition_type  VARCHAR(32) NOT NULL,
    condition_value TEXT NOT NULL,
    action_type     VARCHAR(32) NOT NULL,
    action_value    TEXT NOT NULL,
    enabled         BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_routing_rules_order ON routing_rules(priority_order) WHERE enabled = TRUE;

INSERT INTO routing_rules (name, priority_order, condition_type, condition_value, action_type, action_value)
VALUES
    ('prod-down-p0', 1, 'CONTAINS', 'PROD DOWN,production down,site down', 'SET_PRIORITY', 'P0'),
    ('security-incident', 2, 'CONTAINS', 'security breach,vulnerability,CVE-', 'SET_PRIORITY', 'P1'),
    ('outage-escalate', 3, 'REGEX', '(?i)(outage|service.*(down|unavailable))', 'SET_PRIORITY', 'P0');

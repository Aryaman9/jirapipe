package com.jirapipe.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jirapipe.pipeline.context.TicketContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
public class TicketPersistenceService {

    private static final Logger log = LoggerFactory.getLogger(TicketPersistenceService.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public TicketPersistenceService(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public void saveIngestedTicket(TicketContext context) {
        UUID id = UUID.randomUUID();
        context.setTicketId(id);

        jdbcTemplate.update("""
            INSERT INTO tickets (id, jira_key, project_key, summary, description, priority,
                                 issue_type, labels, created_at, updated_at, pipeline_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PROCESSING')
            ON CONFLICT (jira_key) DO UPDATE SET
                summary = EXCLUDED.summary,
                description = EXCLUDED.description,
                priority = EXCLUDED.priority,
                updated_at = NOW(),
                pipeline_status = 'PROCESSING'
            """,
                id, context.getJiraKey(), context.getProjectKey(),
                context.getSummary(), context.getDescription(), context.getPriority(),
                context.getIssueType(),
                context.getLabels() != null ? context.getLabels().toArray(new String[0]) : new String[0],
                context.getCreatedAt() != null ? Timestamp.from(context.getCreatedAt()) : Timestamp.from(Instant.now()),
                Timestamp.from(Instant.now()));

        log.debug("Persisted ticket {} with ID {}", context.getJiraKey(), id);
    }

    public void markResolved(TicketContext context) {
        jdbcTemplate.update("""
            UPDATE tickets SET pipeline_status = 'RESOLVED',
                               resolution_source = ?,
                               confidence = ?,
                               resolved_at = NOW(),
                               updated_at = NOW()
            WHERE jira_key = ?
            """,
                context.getResolutionSource(),
                context.getConfidence(),
                context.getJiraKey());
    }

    public void markFailed(TicketContext context, String errorMessage) {
        jdbcTemplate.update("""
            UPDATE tickets SET pipeline_status = 'FAILED', updated_at = NOW()
            WHERE jira_key = ?
            """, context.getJiraKey());

        jdbcTemplate.update("""
            INSERT INTO dead_letter_queue (ticket_id, jira_key, payload, error_message, status)
            VALUES (?, ?, ?::jsonb, ?, 'PENDING')
            """,
                context.getTicketId(), context.getJiraKey(),
                toJsonPayload(context),
                errorMessage);
    }

    private String toJsonPayload(TicketContext context) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "summary", context.getSummary() != null ? context.getSummary() : "",
                    "jiraKey", context.getJiraKey() != null ? context.getJiraKey() : ""
            ));
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }
}

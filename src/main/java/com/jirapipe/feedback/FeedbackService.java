package com.jirapipe.feedback;

import com.jirapipe.feedback.dto.FeedbackRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class FeedbackService {

    private static final Logger log = LoggerFactory.getLogger(FeedbackService.class);

    private final JdbcTemplate jdbcTemplate;

    public FeedbackService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public void submitFeedback(String ticketJiraKey, FeedbackRequest request) {
        UUID ticketId = findTicketId(ticketJiraKey);
        if (ticketId == null) {
            throw new IllegalArgumentException("Ticket not found: " + ticketJiraKey);
        }

        jdbcTemplate.update("""
            INSERT INTO feedback (ticket_id, rating, accurate, comment, submitted_by)
            VALUES (?, ?, ?, ?, ?)
            """, ticketId, request.rating(), request.accurate(), request.comment(), request.submittedBy());

        if (request.accurate()) {
            jdbcTemplate.update("""
                UPDATE ticket_embeddings SET confidence_boost = confidence_boost + 0.01
                WHERE ticket_id = ?
                """, ticketId);
            log.info("Positive feedback for {}, boosting confidence", ticketJiraKey);
        } else {
            jdbcTemplate.update("""
                UPDATE ticket_embeddings SET flagged = TRUE
                WHERE ticket_id = ?
                """, ticketId);
            log.info("Negative feedback for {}, flagging embedding", ticketJiraKey);
        }
    }

    private UUID findTicketId(String jiraKey) {
        return jdbcTemplate.query(
                "SELECT id FROM tickets WHERE jira_key = ?",
                (rs, rowNum) -> UUID.fromString(rs.getString("id")),
                jiraKey
        ).stream().findFirst().orElse(null);
    }
}

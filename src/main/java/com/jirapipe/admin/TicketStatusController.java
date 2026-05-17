package com.jirapipe.admin;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/tickets")
@Tag(name = "Tickets", description = "Ticket status and query endpoints")
public class TicketStatusController {

    private final JdbcTemplate jdbcTemplate;

    public TicketStatusController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/{jiraKey}")
    @Operation(summary = "Get ticket status by JIRA key")
    public ResponseEntity<Map<String, Object>> getTicketStatus(@PathVariable String jiraKey) {
        List<Map<String, Object>> results = jdbcTemplate.queryForList("""
            SELECT t.jira_key, t.summary, t.priority, t.pipeline_status,
                   t.resolution_source, t.confidence, t.resolved_at,
                   t.created_at, t.ingested_at
            FROM tickets t WHERE t.jira_key = ?
            """, jiraKey);

        if (results.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(results.get(0));
    }

    @GetMapping
    @Operation(summary = "List recent tickets")
    public List<Map<String, Object>> listTickets(
            @RequestParam(defaultValue = "20") int limit,
            @RequestParam(required = false) String status) {

        if (status != null) {
            return jdbcTemplate.queryForList("""
                SELECT jira_key, summary, priority, pipeline_status,
                       resolution_source, confidence, created_at
                FROM tickets WHERE pipeline_status = ?
                ORDER BY created_at DESC LIMIT ?
                """, status, limit);
        }

        return jdbcTemplate.queryForList("""
            SELECT jira_key, summary, priority, pipeline_status,
                   resolution_source, confidence, created_at
            FROM tickets ORDER BY created_at DESC LIMIT ?
            """, limit);
    }
}

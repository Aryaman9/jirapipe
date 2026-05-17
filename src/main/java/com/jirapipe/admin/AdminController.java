package com.jirapipe.admin;

import com.jirapipe.admin.dto.PipelineStats;
import com.jirapipe.admin.dto.RoutingRuleDto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import java.util.concurrent.CompletableFuture;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/admin")
@Tag(name = "Admin", description = "Administration and monitoring endpoints")
public class AdminController {

    private final JdbcTemplate jdbcTemplate;
    private final BackfillService backfillService;

    public AdminController(JdbcTemplate jdbcTemplate, BackfillService backfillService) {
        this.jdbcTemplate = jdbcTemplate;
        this.backfillService = backfillService;
    }

    @PostMapping("/backfill")
    @Operation(summary = "Trigger embedding backfill for historical tickets")
    public ResponseEntity<Map<String, String>> triggerBackfill() {
        backfillService.backfillEmbeddings();
        return ResponseEntity.accepted().body(Map.of("status", "Backfill started"));
    }

    @GetMapping("/stats")
    @Operation(summary = "Get pipeline statistics")
    public PipelineStats getStats() {
        long total = countByStatus(null);
        long resolved = countByStatus("RESOLVED");
        long failed = countByStatus("FAILED");
        long pending = countByStatus("PENDING") + countByStatus("PROCESSING");
        long vectorMatch = countBySource("VECTOR_MATCH");
        long gpt = countBySource("GPT4O");
        long ruleOverride = countBySource("RULE_OVERRIDE");
        double avgConfidence = getAverageConfidence();
        long dlq = countDlq();

        return new PipelineStats(total, resolved, failed, pending,
                vectorMatch, gpt, ruleOverride, avgConfidence, dlq, 0.0);
    }

    @GetMapping("/config/rules")
    @Operation(summary = "List all routing rules")
    public List<RoutingRuleDto> listRules() {
        return jdbcTemplate.query("""
            SELECT id, name, priority_order, condition_type, condition_value,
                   action_type, action_value, enabled
            FROM routing_rules ORDER BY priority_order
            """, (rs, rowNum) -> new RoutingRuleDto(
                UUID.fromString(rs.getString("id")),
                rs.getString("name"),
                rs.getInt("priority_order"),
                rs.getString("condition_type"),
                rs.getString("condition_value"),
                rs.getString("action_type"),
                rs.getString("action_value"),
                rs.getBoolean("enabled")
        ));
    }

    @PostMapping("/config/rules")
    @Operation(summary = "Create a routing rule")
    public ResponseEntity<Void> createRule(@Valid @RequestBody RoutingRuleDto rule) {
        jdbcTemplate.update("""
            INSERT INTO routing_rules (name, priority_order, condition_type, condition_value, action_type, action_value, enabled)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rule.name(), rule.priorityOrder(), rule.conditionType(),
                rule.conditionValue(), rule.actionType(), rule.actionValue(),
                rule.enabled() != null ? rule.enabled() : true);
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/config/rules/{id}")
    @Operation(summary = "Delete a routing rule")
    public ResponseEntity<Void> deleteRule(@PathVariable UUID id) {
        jdbcTemplate.update("DELETE FROM routing_rules WHERE id = ?", id);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/dlq")
    @Operation(summary = "View dead letter queue entries")
    public List<Map<String, Object>> getDlq() {
        return jdbcTemplate.queryForList("""
            SELECT id, jira_key, error_message, retry_count, status, created_at
            FROM dead_letter_queue
            ORDER BY created_at DESC
            LIMIT 50
            """);
    }

    @PostMapping("/dlq/{id}/retry")
    @Operation(summary = "Retry a dead letter queue entry")
    public ResponseEntity<Void> retryDlq(@PathVariable UUID id) {
        jdbcTemplate.update("""
            UPDATE dead_letter_queue SET status = 'PENDING', retry_count = retry_count + 1,
                                         next_retry_at = NOW()
            WHERE id = ?
            """, id);
        return ResponseEntity.ok().build();
    }

    private long countByStatus(String status) {
        if (status == null) {
            return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tickets", Long.class);
        }
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tickets WHERE pipeline_status = ?", Long.class, status);
    }

    private long countBySource(String source) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tickets WHERE resolution_source = ?", Long.class, source);
    }

    private double getAverageConfidence() {
        Double avg = jdbcTemplate.queryForObject(
                "SELECT AVG(confidence) FROM tickets WHERE confidence IS NOT NULL", Double.class);
        return avg != null ? avg : 0.0;
    }

    private long countDlq() {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dead_letter_queue WHERE status = 'PENDING'", Long.class);
    }
}

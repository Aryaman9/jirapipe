package com.jirapipe.pipeline.routing;

import com.jirapipe.pipeline.context.TicketContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

@Component
public class RoutingRuleEngine {

    private static final Logger log = LoggerFactory.getLogger(RoutingRuleEngine.class);

    private final JdbcTemplate jdbcTemplate;

    public RoutingRuleEngine(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<RoutingRule.RoutingAction> evaluate(TicketContext context) {
        List<RuleDefinition> rules = loadActiveRules();
        String ticketText = (context.getSummary() + " " +
                (context.getDescription() != null ? context.getDescription() : "")).toLowerCase();

        for (RuleDefinition rule : rules) {
            if (matches(rule, ticketText)) {
                log.info("Routing rule '{}' matched for ticket {}", rule.name(), context.getJiraKey());
                return Optional.of(new RoutingRule.RoutingAction(rule.actionType(), rule.actionValue()));
            }
        }

        return Optional.empty();
    }

    private boolean matches(RuleDefinition rule, String ticketText) {
        return switch (rule.conditionType()) {
            case "CONTAINS" -> {
                String[] terms = rule.conditionValue().split(",");
                for (String term : terms) {
                    if (ticketText.contains(term.trim().toLowerCase())) {
                        yield true;
                    }
                }
                yield false;
            }
            case "REGEX" -> {
                try {
                    if (rule.conditionValue().length() > 500) {
                        log.warn("Regex too long in rule '{}', skipping", rule.name());
                        yield false;
                    }
                    yield Pattern.compile(rule.conditionValue())
                            .matcher(ticketText.substring(0, Math.min(ticketText.length(), 10000)))
                            .find();
                } catch (Exception e) {
                    log.warn("Invalid regex in rule '{}': {}", rule.name(), e.getMessage());
                    yield false;
                }
            }
            case "LABEL_MATCH" -> {
                if (ticketText.contains("[labels:")) {
                    yield ticketText.contains(rule.conditionValue().toLowerCase());
                }
                yield false;
            }
            default -> false;
        };
    }

    private List<RuleDefinition> loadActiveRules() {
        return jdbcTemplate.query("""
            SELECT name, condition_type, condition_value, action_type, action_value
            FROM routing_rules
            WHERE enabled = TRUE
            ORDER BY priority_order ASC
            """,
                (rs, rowNum) -> new RuleDefinition(
                        rs.getString("name"),
                        rs.getString("condition_type"),
                        rs.getString("condition_value"),
                        rs.getString("action_type"),
                        rs.getString("action_value")
                ));
    }

    private record RuleDefinition(String name, String conditionType, String conditionValue,
                                   String actionType, String actionValue) {}
}

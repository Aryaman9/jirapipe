package com.jirapipe.pipeline.stage;

import com.jirapipe.pipeline.context.StageResult;
import com.jirapipe.pipeline.context.TicketContext;
import com.jirapipe.pipeline.routing.RoutingRule;
import com.jirapipe.pipeline.routing.RoutingRuleEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

@Component
public class RoutingRulesStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(RoutingRulesStage.class);

    private final RoutingRuleEngine ruleEngine;

    public RoutingRulesStage(RoutingRuleEngine ruleEngine) {
        this.ruleEngine = ruleEngine;
    }

    @Override
    public StageResult process(TicketContext context) {
        Instant start = Instant.now();

        Optional<RoutingRule.RoutingAction> action = ruleEngine.evaluate(context);

        if (action.isPresent()) {
            applyAction(context, action.get());
            log.info("Rule applied for {}: {} = {}",
                    context.getJiraKey(), action.get().actionType(), action.get().actionValue());

            if ("SKIP_PIPELINE".equals(action.get().actionType())) {
                context.setResolutionSource("RULE_OVERRIDE");
                context.setTerminal(true);
                return StageResult.terminal(getName(), "Pipeline skipped by routing rule",
                        Duration.between(start, Instant.now()));
            }
        }

        return StageResult.success(getName(), Duration.between(start, Instant.now()));
    }

    private void applyAction(TicketContext context, RoutingRule.RoutingAction action) {
        switch (action.actionType()) {
            case "SET_PRIORITY" -> context.setResolvedPriority(action.actionValue());
            case "ASSIGN" -> context.setResolvedTeam(action.actionValue());
            case "ESCALATE" -> {
                context.setResolvedPriority("P0");
                context.setResolvedTeam(action.actionValue());
            }
            case "SKIP_PIPELINE" -> context.setResolutionText("Handled by routing rule: " + action.actionValue());
        }
    }

    @Override
    public boolean shouldExecute(TicketContext context) {
        return true;
    }

    @Override
    public int getOrder() {
        return 50;
    }

    @Override
    public String getName() {
        return "ROUTING_RULES";
    }
}

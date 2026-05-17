package com.jirapipe.unit;

import com.jirapipe.pipeline.context.TicketContext;
import com.jirapipe.pipeline.routing.RoutingRule;
import com.jirapipe.pipeline.stage.RoutingRulesStage;
import com.jirapipe.pipeline.routing.RoutingRuleEngine;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RoutingRuleEngineTest {

    @Test
    void ticketContextBuildsCorrectly() {
        TicketContext context = new TicketContext();
        context.setJiraKey("PROJ-123");
        context.setSummary("PROD DOWN - API gateway not responding");
        context.setDescription("All requests returning 503");
        context.setProjectKey("PROJ");

        assertNotNull(context.getCorrelationId());
        assertEquals("PROJ-123", context.getJiraKey());
        assertFalse(context.isTerminal());
    }

    @Test
    void routingActionRecordWorks() {
        RoutingRule.RoutingAction action = new RoutingRule.RoutingAction("SET_PRIORITY", "P0");
        assertEquals("SET_PRIORITY", action.actionType());
        assertEquals("P0", action.actionValue());
    }
}

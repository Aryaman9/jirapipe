package com.jirapipe.pipeline.routing;

import com.jirapipe.pipeline.context.TicketContext;

public interface RoutingRule {

    boolean matches(TicketContext context);

    RoutingAction apply(TicketContext context);

    int getPriority();

    String getName();

    record RoutingAction(String actionType, String actionValue) {}
}

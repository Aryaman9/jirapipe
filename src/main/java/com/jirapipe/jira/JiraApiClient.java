package com.jirapipe.jira;

import com.jirapipe.pipeline.context.TicketContext;

public interface JiraApiClient {

    void addComment(String jiraKey, String comment);

    void updatePriority(String jiraKey, String priority);

    void assignTicket(String jiraKey, String team);

    void applyResolution(TicketContext context);
}

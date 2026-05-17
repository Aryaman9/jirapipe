package com.jirapipe.jira;

import com.jirapipe.pipeline.context.TicketContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "true")
public class MockJiraClient implements JiraApiClient {

    private static final Logger log = LoggerFactory.getLogger(MockJiraClient.class);

    @Override
    public void addComment(String jiraKey, String comment) {
        log.info("[MOCK JIRA] Adding comment to {}: {}", jiraKey, comment.substring(0, Math.min(100, comment.length())));
    }

    @Override
    public void updatePriority(String jiraKey, String priority) {
        log.info("[MOCK JIRA] Updating priority of {} to {}", jiraKey, priority);
    }

    @Override
    public void assignTicket(String jiraKey, String team) {
        log.info("[MOCK JIRA] Assigning {} to team: {}", jiraKey, team);
    }

    @Override
    public void applyResolution(TicketContext context) {
        log.info("[MOCK JIRA] Applying resolution to {}: source={}, confidence={}, classification={}",
                context.getJiraKey(), context.getResolutionSource(),
                context.getConfidence(), context.getResolvedClassification());
    }
}

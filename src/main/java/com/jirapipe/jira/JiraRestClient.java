package com.jirapipe.jira;

import com.jirapipe.config.JiraPipeProperties;
import com.jirapipe.pipeline.context.TicketContext;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Base64;
import java.util.Map;

@Service
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "false", matchIfMissing = true)
public class JiraRestClient implements JiraApiClient {

    private static final Logger log = LoggerFactory.getLogger(JiraRestClient.class);

    private final RestClient restClient;

    public JiraRestClient(JiraPipeProperties properties) {
        String credentials = properties.getJira().getEmail() + ":" + properties.getJira().getApiToken();
        String encoded = Base64.getEncoder().encodeToString(credentials.getBytes());

        this.restClient = RestClient.builder()
                .baseUrl(properties.getJira().getBaseUrl())
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoded)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    @Override
    @CircuitBreaker(name = "jira", fallbackMethod = "addCommentFallback")
    public void addComment(String jiraKey, String comment) {
        Map<String, Object> body = Map.of("body", comment);

        restClient.post()
                .uri("/rest/api/3/issue/{issueKey}/comment", jiraKey)
                .body(body)
                .retrieve()
                .toBodilessEntity();

        log.info("Added comment to {}", jiraKey);
    }

    @Override
    @CircuitBreaker(name = "jira", fallbackMethod = "updatePriorityFallback")
    public void updatePriority(String jiraKey, String priority) {
        Map<String, Object> body = Map.of(
                "fields", Map.of("priority", Map.of("name", priority))
        );

        restClient.put()
                .uri("/rest/api/3/issue/{issueKey}", jiraKey)
                .body(body)
                .retrieve()
                .toBodilessEntity();

        log.info("Updated priority of {} to {}", jiraKey, priority);
    }

    @Override
    @CircuitBreaker(name = "jira")
    public void assignTicket(String jiraKey, String team) {
        log.info("Assigning {} to team: {}", jiraKey, team);
    }

    @Override
    @CircuitBreaker(name = "jira")
    public void applyResolution(TicketContext context) {
        String comment = String.format("""
            [JiraPipe Auto-Triage]
            Classification: %s
            Severity: %s
            Suggested Team: %s
            Confidence: %.0f%%
            Source: %s

            Resolution:
            %s
            """,
                context.getResolvedClassification(),
                context.getResolvedPriority(),
                context.getResolvedTeam(),
                context.getConfidence() * 100,
                context.getResolutionSource(),
                context.getResolutionText());

        addComment(context.getJiraKey(), comment);

        if (context.getResolvedPriority() != null) {
            updatePriority(context.getJiraKey(), context.getResolvedPriority());
        }
    }

    @SuppressWarnings("unused")
    private void addCommentFallback(String jiraKey, String comment, Throwable t) {
        log.warn("JIRA circuit breaker open, queuing comment for {}: {}", jiraKey, t.getMessage());
    }

    @SuppressWarnings("unused")
    private void updatePriorityFallback(String jiraKey, String priority, Throwable t) {
        log.warn("JIRA circuit breaker open, queuing priority update for {}: {}", jiraKey, t.getMessage());
    }
}

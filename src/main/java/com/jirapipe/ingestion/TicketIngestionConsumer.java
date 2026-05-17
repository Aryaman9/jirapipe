package com.jirapipe.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jirapipe.config.JiraPipeProperties;
import com.jirapipe.pipeline.TriagePipeline;
import com.jirapipe.pipeline.context.TicketContext;
import com.jirapipe.webhook.dto.JiraWebhookPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Component
public class TicketIngestionConsumer {

    private static final Logger log = LoggerFactory.getLogger(TicketIngestionConsumer.class);

    private final ObjectMapper objectMapper;
    private final TriagePipeline pipeline;
    private final StringRedisTemplate redisTemplate;
    private final TicketPersistenceService persistenceService;

    public TicketIngestionConsumer(ObjectMapper objectMapper,
                                   TriagePipeline pipeline,
                                   StringRedisTemplate redisTemplate,
                                   TicketPersistenceService persistenceService) {
        this.objectMapper = objectMapper;
        this.pipeline = pipeline;
        this.redisTemplate = redisTemplate;
        this.persistenceService = persistenceService;
    }

    @KafkaListener(topics = "${jirapipe.kafka.topics.ticket-ingestion}", groupId = "jirapipe-triage")
    public void consume(String payload) {
        try {
            JiraWebhookPayload webhook = objectMapper.readValue(payload, JiraWebhookPayload.class);

            if (webhook.issue() == null) {
                log.warn("Received webhook without issue data, skipping");
                return;
            }

            String jiraKey = webhook.issue().key();

            if (isDuplicate(jiraKey)) {
                log.info("Duplicate ticket {} detected, skipping", jiraKey);
                return;
            }

            markAsProcessing(jiraKey);
            TicketContext context = buildContext(webhook);
            persistenceService.saveIngestedTicket(context);

            processAsync(context);

        } catch (Exception e) {
            log.error("Failed to process ingested ticket: {}", e.getMessage(), e);
        }
    }

    @Async
    protected void processAsync(TicketContext context) {
        TriagePipeline.PipelineResult result = pipeline.execute(context);
        if (result.success()) {
            persistenceService.markResolved(context);
        } else {
            persistenceService.markFailed(context, result.errorMessage());
        }
    }

    private boolean isDuplicate(String jiraKey) {
        return Boolean.TRUE.equals(redisTemplate.hasKey("processing:" + jiraKey));
    }

    private void markAsProcessing(String jiraKey) {
        redisTemplate.opsForValue().set("processing:" + jiraKey, "1", Duration.ofMinutes(30));
    }

    private TicketContext buildContext(JiraWebhookPayload webhook) {
        var issue = webhook.issue();
        var fields = issue.fields();

        TicketContext context = new TicketContext();
        context.setJiraKey(issue.key());
        context.setProjectKey(fields.project() != null ? fields.project().key() : "UNKNOWN");
        context.setSummary(fields.summary());
        context.setDescription(fields.description());
        context.setPriority(fields.priority() != null ? fields.priority().name() : null);
        context.setIssueType(fields.issueType() != null ? fields.issueType().name() : null);
        context.setLabels(fields.labels() != null ? fields.labels() : List.of());
        context.setCreatedAt(fields.created());
        return context;
    }
}

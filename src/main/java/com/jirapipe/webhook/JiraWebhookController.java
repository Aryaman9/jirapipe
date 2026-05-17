package com.jirapipe.webhook;

import com.jirapipe.ingestion.TicketIngestionProducer;
import com.jirapipe.webhook.dto.JiraWebhookPayload;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/webhook")
@Tag(name = "Webhook", description = "JIRA webhook ingestion endpoint")
public class JiraWebhookController {

    private static final Logger log = LoggerFactory.getLogger(JiraWebhookController.class);

    private final WebhookSignatureValidator signatureValidator;
    private final TicketIngestionProducer producer;

    public JiraWebhookController(WebhookSignatureValidator signatureValidator,
                                  TicketIngestionProducer producer) {
        this.signatureValidator = signatureValidator;
        this.producer = producer;
    }

    @PostMapping("/jira")
    @Operation(summary = "Receive JIRA webhook event", description = "Validates webhook signature and publishes ticket to Kafka for async processing")
    @ApiResponse(responseCode = "202", description = "Event accepted for processing")
    @ApiResponse(responseCode = "401", description = "Invalid webhook signature")
    @ApiResponse(responseCode = "400", description = "Invalid payload")
    public ResponseEntity<Void> receiveWebhook(
            @RequestBody String rawPayload,
            @RequestHeader(value = "X-Hub-Signature", required = false) String signature) {

        if (!signatureValidator.isValid(rawPayload, signature)) {
            log.warn("Invalid webhook signature received");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        try {
            producer.publish(rawPayload);
            return ResponseEntity.accepted().build();
        } catch (Exception e) {
            log.error("Failed to publish webhook payload to Kafka: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}

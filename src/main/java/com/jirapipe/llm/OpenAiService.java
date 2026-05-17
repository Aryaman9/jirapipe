package com.jirapipe.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jirapipe.config.JiraPipeProperties;
import com.jirapipe.llm.dto.ResolutionResult;
import com.jirapipe.pipeline.context.TicketContext;
import com.jirapipe.pipeline.context.TicketSignals;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service("openAiService")
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "false", matchIfMissing = true)
public class OpenAiService implements LlmService {

    private static final Logger log = LoggerFactory.getLogger(OpenAiService.class);

    private final RestClient restClient;
    private final ObjectMapper objectMapper;
    private final String model;
    private final int maxTokens;

    private static final String RESOLUTION_PROMPT = """
        You are an expert JIRA ticket triage system. Analyze the following ticket and provide a resolution recommendation.

        Ticket Key: %s
        Summary: %s
        Description: %s
        Extracted Keywords: %s
        Component Names: %s
        Error Signatures: %s

        Respond with ONLY valid JSON containing:
        - classification: one of "Bug", "Feature", "Infra", "Config"
        - severity: one of "P0", "P1", "P2", "P3"
        - suggestedTeam: the team best suited to handle this
        - resolutionText: a concise resolution summary
        - resolutionSteps: array of step-by-step resolution instructions
        - confidence: your confidence score between 0.0 and 1.0

        JSON Response:
        """;

    public OpenAiService(JiraPipeProperties properties, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.model = properties.getOpenai().getCompletionModel();
        this.maxTokens = properties.getOpenai().getMaxTokens();
        this.restClient = RestClient.builder()
                .baseUrl(properties.getOpenai().getBaseUrl())
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + properties.getOpenai().getApiKey())
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    @Override
    public TicketSignals extractSignals(String summary, String description) {
        throw new UnsupportedOperationException("OpenAI is used for resolution only, use Ollama for extraction");
    }

    @Override
    @CircuitBreaker(name = "openai", fallbackMethod = "resolutionFallback")
    public ResolutionResult generateResolution(TicketContext context) {
        TicketSignals signals = context.getExtractedSignals();
        String prompt = String.format(RESOLUTION_PROMPT,
                context.getJiraKey(),
                context.getSummary(),
                context.getDescription() != null ? context.getDescription() : "N/A",
                signals != null ? String.join(", ", signals.keywords()) : "N/A",
                signals != null ? String.join(", ", signals.componentNames()) : "N/A",
                signals != null ? String.join(", ", signals.errorSignatures()) : "N/A"
        );

        Map<String, Object> body = Map.of(
                "model", model,
                "messages", List.of(
                        Map.of("role", "system", "content", "You are a JIRA ticket triage expert. Always respond with valid JSON."),
                        Map.of("role", "user", "content", prompt)
                ),
                "max_tokens", maxTokens,
                "response_format", Map.of("type", "json_object")
        );

        String response = restClient.post()
                .uri("/chat/completions")
                .body(body)
                .retrieve()
                .body(String.class);

        try {
            JsonNode root = objectMapper.readTree(response);
            String content = root.path("choices").get(0).path("message").path("content").asText();
            JsonNode result = objectMapper.readTree(content);

            return new ResolutionResult(
                    result.path("classification").asText("Bug"),
                    result.path("severity").asText("P2"),
                    result.path("suggestedTeam").asText("Engineering"),
                    result.path("resolutionText").asText(""),
                    jsonArrayToList(result.path("resolutionSteps")),
                    result.path("confidence").asDouble(0.7)
            );
        } catch (Exception e) {
            log.error("Failed to parse GPT-4o response: {}", e.getMessage());
            throw new RuntimeException("GPT-4o parse failure", e);
        }
    }

    @SuppressWarnings("unused")
    private ResolutionResult resolutionFallback(TicketContext context, Throwable t) {
        log.warn("OpenAI circuit breaker open for resolution: {}", t.getMessage());
        return null;
    }

    private List<String> jsonArrayToList(JsonNode arrayNode) {
        List<String> list = new ArrayList<>();
        if (arrayNode != null && arrayNode.isArray()) {
            for (JsonNode node : arrayNode) {
                list.add(node.asText());
            }
        }
        return list;
    }
}

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
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service("ollamaService")
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "false", matchIfMissing = true)
public class OllamaService implements LlmService {

    private static final Logger log = LoggerFactory.getLogger(OllamaService.class);

    private final RestClient restClient;
    private final ObjectMapper objectMapper;
    private final String model;

    private static final String EXTRACTION_PROMPT = """
        You are a JIRA ticket analysis system. Extract structured signals from the following ticket.
        Return ONLY valid JSON with these fields:
        - keywords: array of relevant technical keywords
        - componentNames: array of software component names mentioned
        - errorSignatures: array of error messages or stack trace patterns
        - severityHint: one of "P0", "P1", "P2", "P3" based on apparent urgency
        - categoryHint: one of "Bug", "Feature", "Infra", "Config"

        Ticket Summary: %s
        Ticket Description: %s

        JSON Response:
        """;

    public OllamaService(JiraPipeProperties properties, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.model = properties.getOllama().getModel();
        this.restClient = RestClient.builder()
                .baseUrl(properties.getOllama().getBaseUrl())
                .build();
    }

    @Override
    @CircuitBreaker(name = "ollama", fallbackMethod = "extractSignalsFallback")
    public TicketSignals extractSignals(String summary, String description) {
        String prompt = String.format(EXTRACTION_PROMPT, summary, description != null ? description : "N/A");

        Map<String, Object> body = Map.of(
                "model", model,
                "prompt", prompt,
                "stream", false,
                "format", "json"
        );

        String response = restClient.post()
                .uri("/api/generate")
                .body(body)
                .retrieve()
                .body(String.class);

        try {
            JsonNode root = objectMapper.readTree(response);
            String generatedText = root.path("response").asText();
            JsonNode signals = objectMapper.readTree(generatedText);

            return new TicketSignals(
                    jsonArrayToList(signals.path("keywords")),
                    jsonArrayToList(signals.path("componentNames")),
                    jsonArrayToList(signals.path("errorSignatures")),
                    signals.path("severityHint").asText(null),
                    signals.path("categoryHint").asText(null)
            );
        } catch (Exception e) {
            log.error("Failed to parse Ollama response: {}", e.getMessage());
            throw new RuntimeException("Ollama parse failure", e);
        }
    }

    @Override
    public ResolutionResult generateResolution(TicketContext context) {
        throw new UnsupportedOperationException("Ollama is used for extraction only, not resolution");
    }

    @SuppressWarnings("unused")
    private TicketSignals extractSignalsFallback(String summary, String description, Throwable t) {
        log.warn("Ollama circuit breaker open, returning raw text signals: {}", t.getMessage());
        List<String> keywords = List.of(summary.split("\\s+"));
        return new TicketSignals(keywords, List.of(), List.of(), null, null);
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

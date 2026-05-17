package com.jirapipe.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jirapipe.config.JiraPipeProperties;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Map;

@Service
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "false", matchIfMissing = true)
public class OpenAiEmbeddingService implements EmbeddingService {

    private static final Logger log = LoggerFactory.getLogger(OpenAiEmbeddingService.class);

    private final RestClient restClient;
    private final ObjectMapper objectMapper;
    private final String model;

    public OpenAiEmbeddingService(JiraPipeProperties properties, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.model = properties.getOpenai().getEmbeddingModel();
        this.restClient = RestClient.builder()
                .baseUrl("https://api.openai.com/v1")
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + properties.getOpenai().getApiKey())
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    @Override
    @CircuitBreaker(name = "openai", fallbackMethod = "embeddingFallback")
    public float[] embed(String text) {
        String truncated = text.length() > 8000 ? text.substring(0, 8000) : text;

        Map<String, Object> body = Map.of(
                "model", model,
                "input", truncated
        );

        String response = restClient.post()
                .uri("/embeddings")
                .body(body)
                .retrieve()
                .body(String.class);

        try {
            JsonNode root = objectMapper.readTree(response);
            JsonNode embeddingArray = root.path("data").get(0).path("embedding");
            float[] embedding = new float[embeddingArray.size()];
            for (int i = 0; i < embeddingArray.size(); i++) {
                embedding[i] = (float) embeddingArray.get(i).asDouble();
            }
            return embedding;
        } catch (Exception e) {
            log.error("Failed to parse embedding response: {}", e.getMessage());
            throw new RuntimeException("Embedding parse failure", e);
        }
    }

    @SuppressWarnings("unused")
    private float[] embeddingFallback(String text, Throwable t) {
        log.warn("OpenAI embedding circuit breaker open, returning null: {}", t.getMessage());
        return null;
    }
}

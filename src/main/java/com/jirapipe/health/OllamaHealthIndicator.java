package com.jirapipe.health;

import com.jirapipe.config.JiraPipeProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class OllamaHealthIndicator implements HealthIndicator {

    private final RestClient restClient;

    public OllamaHealthIndicator(JiraPipeProperties properties) {
        this.restClient = RestClient.builder()
                .baseUrl(properties.getOllama().getBaseUrl())
                .build();
    }

    @Override
    public Health health() {
        try {
            String response = restClient.get()
                    .uri("/api/tags")
                    .retrieve()
                    .body(String.class);
            return Health.up().withDetail("models", response != null ? "available" : "none").build();
        } catch (Exception e) {
            return Health.down().withDetail("error", e.getMessage()).build();
        }
    }
}

package com.jirapipe.health;

import com.jirapipe.config.JiraPipeProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class OpenAiHealthIndicator implements HealthIndicator {

    private final JiraPipeProperties properties;

    public OpenAiHealthIndicator(JiraPipeProperties properties) {
        this.properties = properties;
    }

    @Override
    public Health health() {
        String apiKey = properties.getOpenai().getApiKey();
        if (apiKey == null || apiKey.isBlank() || apiKey.startsWith("sk-mock")) {
            return Health.up().withDetail("mode", "mock/unconfigured").build();
        }
        return Health.up().withDetail("mode", "configured").build();
    }
}

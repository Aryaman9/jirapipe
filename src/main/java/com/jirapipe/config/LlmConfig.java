package com.jirapipe.config;

import com.jirapipe.llm.LlmService;
import com.jirapipe.llm.MockLlmService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "true")
public class LlmConfig {

    @Bean("ollamaService")
    public LlmService mockOllamaService() {
        return new MockLlmService();
    }

    @Bean("openAiService")
    public LlmService mockOpenAiService() {
        return new MockLlmService();
    }
}

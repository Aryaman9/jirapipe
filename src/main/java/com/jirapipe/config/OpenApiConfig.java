package com.jirapipe.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI jirapipeOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("JiraPipe - JIRA Ticket Triage RAG Pipeline")
                        .description("Cost-aware AI system using a two-stage RAG pipeline to automatically classify, route, and resolve JIRA tickets. Uses local SLM (Ollama) for signal extraction, pgvector for similarity search, and GPT-4o only for novel tickets.")
                        .version("0.1.0")
                        .contact(new Contact()
                                .name("JiraPipe")
                                .url("https://github.com/jirapipe")));
    }
}

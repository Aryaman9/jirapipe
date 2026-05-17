package com.jirapipe.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "jirapipe")
public class JiraPipeProperties {

    private boolean mockMode = false;
    private OllamaProperties ollama = new OllamaProperties();
    private OpenAiProperties openai = new OpenAiProperties();
    private JiraProperties jira = new JiraProperties();
    private PipelineProperties pipeline = new PipelineProperties();
    private RetryProperties retry = new RetryProperties();
    private CacheProperties cache = new CacheProperties();
    private KafkaTopicsProperties kafka = new KafkaTopicsProperties();

    public boolean isMockMode() { return mockMode; }
    public void setMockMode(boolean mockMode) { this.mockMode = mockMode; }
    public OllamaProperties getOllama() { return ollama; }
    public void setOllama(OllamaProperties ollama) { this.ollama = ollama; }
    public OpenAiProperties getOpenai() { return openai; }
    public void setOpenai(OpenAiProperties openai) { this.openai = openai; }
    public JiraProperties getJira() { return jira; }
    public void setJira(JiraProperties jira) { this.jira = jira; }
    public PipelineProperties getPipeline() { return pipeline; }
    public void setPipeline(PipelineProperties pipeline) { this.pipeline = pipeline; }
    public RetryProperties getRetry() { return retry; }
    public void setRetry(RetryProperties retry) { this.retry = retry; }
    public CacheProperties getCache() { return cache; }
    public void setCache(CacheProperties cache) { this.cache = cache; }
    public KafkaTopicsProperties getKafka() { return kafka; }
    public void setKafka(KafkaTopicsProperties kafka) { this.kafka = kafka; }

    public static class OllamaProperties {
        private String baseUrl = "http://localhost:11434";
        private String model = "mistral:7b";
        private String timeout = "30s";

        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }
        public String getModel() { return model; }
        public void setModel(String model) { this.model = model; }
        public String getTimeout() { return timeout; }
        public void setTimeout(String timeout) { this.timeout = timeout; }
    }

    public static class OpenAiProperties {
        private String apiKey = "";
        private String embeddingModel = "text-embedding-3-small";
        private String completionModel = "gpt-4o";
        private String timeout = "60s";
        private int maxTokens = 2048;

        public String getApiKey() { return apiKey; }
        public void setApiKey(String apiKey) { this.apiKey = apiKey; }
        public String getEmbeddingModel() { return embeddingModel; }
        public void setEmbeddingModel(String embeddingModel) { this.embeddingModel = embeddingModel; }
        public String getCompletionModel() { return completionModel; }
        public void setCompletionModel(String completionModel) { this.completionModel = completionModel; }
        public String getTimeout() { return timeout; }
        public void setTimeout(String timeout) { this.timeout = timeout; }
        public int getMaxTokens() { return maxTokens; }
        public void setMaxTokens(int maxTokens) { this.maxTokens = maxTokens; }
    }

    public static class JiraProperties {
        private String baseUrl = "";
        private String email = "";
        private String apiToken = "";
        private String webhookSecret = "";

        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
        public String getApiToken() { return apiToken; }
        public void setApiToken(String apiToken) { this.apiToken = apiToken; }
        public String getWebhookSecret() { return webhookSecret; }
        public void setWebhookSecret(String webhookSecret) { this.webhookSecret = webhookSecret; }
    }

    public static class PipelineProperties {
        private double vectorSimilarityThreshold = 0.85;
        private double autoResolveThreshold = 0.95;
        private int maxSimilarResults = 5;

        public double getVectorSimilarityThreshold() { return vectorSimilarityThreshold; }
        public void setVectorSimilarityThreshold(double v) { this.vectorSimilarityThreshold = v; }
        public double getAutoResolveThreshold() { return autoResolveThreshold; }
        public void setAutoResolveThreshold(double v) { this.autoResolveThreshold = v; }
        public int getMaxSimilarResults() { return maxSimilarResults; }
        public void setMaxSimilarResults(int v) { this.maxSimilarResults = v; }
    }

    public static class RetryProperties {
        private int maxAttempts = 3;
        private long initialDelayMs = 1000;
        private double multiplier = 2.0;
        private long maxDelayMs = 30000;

        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int v) { this.maxAttempts = v; }
        public long getInitialDelayMs() { return initialDelayMs; }
        public void setInitialDelayMs(long v) { this.initialDelayMs = v; }
        public double getMultiplier() { return multiplier; }
        public void setMultiplier(double v) { this.multiplier = v; }
        public long getMaxDelayMs() { return maxDelayMs; }
        public void setMaxDelayMs(long v) { this.maxDelayMs = v; }
    }

    public static class CacheProperties {
        private int embeddingTtlHours = 24;

        public int getEmbeddingTtlHours() { return embeddingTtlHours; }
        public void setEmbeddingTtlHours(int v) { this.embeddingTtlHours = v; }
    }

    public static class KafkaTopicsProperties {
        private TopicsProperties topics = new TopicsProperties();

        public TopicsProperties getTopics() { return topics; }
        public void setTopics(TopicsProperties topics) { this.topics = topics; }

        public static class TopicsProperties {
            private String ticketIngestion = "jirapipe.tickets.ingestion";
            private String deadLetter = "jirapipe.tickets.dlq";

            public String getTicketIngestion() { return ticketIngestion; }
            public void setTicketIngestion(String v) { this.ticketIngestion = v; }
            public String getDeadLetter() { return deadLetter; }
            public void setDeadLetter(String v) { this.deadLetter = v; }
        }
    }
}

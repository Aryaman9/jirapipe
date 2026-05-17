package com.jirapipe.embedding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jirapipe.config.JiraPipeProperties;
import com.jirapipe.observability.PipelineMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;

@Service
@Primary
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "false", matchIfMissing = true)
public class CachedEmbeddingService implements EmbeddingService {

    private static final Logger log = LoggerFactory.getLogger(CachedEmbeddingService.class);
    private static final String CACHE_PREFIX = "emb:";

    private final EmbeddingService delegate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final PipelineMetrics metrics;
    private final Duration ttl;

    public CachedEmbeddingService(EmbeddingService delegate,
                                   StringRedisTemplate redisTemplate,
                                   ObjectMapper objectMapper,
                                   PipelineMetrics metrics,
                                   JiraPipeProperties properties) {
        this.delegate = delegate;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.ttl = Duration.ofHours(properties.getCache().getEmbeddingTtlHours());
    }

    @Override
    public float[] embed(String text) {
        String key = CACHE_PREFIX + sha256(text);

        String cached = redisTemplate.opsForValue().get(key);
        if (cached != null) {
            metrics.recordCacheHit();
            log.debug("Cache hit for embedding key {}", key);
            return deserialize(cached);
        }

        metrics.recordCacheMiss();
        float[] embedding = delegate.embed(text);
        if (embedding != null) {
            redisTemplate.opsForValue().set(key, serialize(embedding), ttl);
        }
        return embedding;
    }

    private String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private String serialize(float[] embedding) {
        try {
            return objectMapper.writeValueAsString(embedding);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private float[] deserialize(String json) {
        try {
            return objectMapper.readValue(json, float[].class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}

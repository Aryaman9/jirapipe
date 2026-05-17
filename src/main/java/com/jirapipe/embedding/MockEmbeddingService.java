package com.jirapipe.embedding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
@Primary
@ConditionalOnProperty(name = "jirapipe.mock-mode", havingValue = "true")
public class MockEmbeddingService implements EmbeddingService {

    private static final Logger log = LoggerFactory.getLogger(MockEmbeddingService.class);
    private static final int DIMENSION = 1536;

    @Override
    public float[] embed(String text) {
        log.debug("Mock embedding for text: {}...", text.substring(0, Math.min(50, text.length())));
        Random random = new Random(text.hashCode());
        float[] embedding = new float[DIMENSION];
        float norm = 0;
        for (int i = 0; i < DIMENSION; i++) {
            embedding[i] = random.nextFloat() * 2 - 1;
            norm += embedding[i] * embedding[i];
        }
        norm = (float) Math.sqrt(norm);
        for (int i = 0; i < DIMENSION; i++) {
            embedding[i] /= norm;
        }
        return embedding;
    }
}

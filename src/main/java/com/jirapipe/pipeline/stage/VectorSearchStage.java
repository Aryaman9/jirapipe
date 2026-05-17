package com.jirapipe.pipeline.stage;

import com.jirapipe.config.JiraPipeProperties;
import com.jirapipe.embedding.EmbeddingService;
import com.jirapipe.pipeline.context.StageResult;
import com.jirapipe.pipeline.context.TicketContext;
import com.jirapipe.vectorstore.VectorStoreRepository;
import com.jirapipe.vectorstore.dto.SimilarTicket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Component
public class VectorSearchStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(VectorSearchStage.class);

    private final EmbeddingService embeddingService;
    private final VectorStoreRepository vectorStore;
    private final JiraPipeProperties properties;

    public VectorSearchStage(EmbeddingService embeddingService,
                             VectorStoreRepository vectorStore,
                             JiraPipeProperties properties) {
        this.embeddingService = embeddingService;
        this.vectorStore = vectorStore;
        this.properties = properties;
    }

    @Override
    public StageResult process(TicketContext context) {
        Instant start = Instant.now();

        String textToEmbed = context.getExtractedSignals() != null
                ? context.getExtractedSignals().toEmbeddingText()
                : context.getSummary() + " " + (context.getDescription() != null ? context.getDescription() : "");

        float[] embedding = embeddingService.embed(textToEmbed);
        if (embedding == null) {
            return StageResult.failure(getName(), "Embedding service unavailable", Duration.between(start, Instant.now()));
        }

        List<SimilarTicket> matches = vectorStore.findSimilar(embedding, properties.getPipeline().getMaxSimilarResults());

        if (matches.isEmpty()) {
            log.info("No similar tickets found for {}", context.getJiraKey());
            return StageResult.success(getName(), Duration.between(start, Instant.now()));
        }

        SimilarTicket topMatch = matches.get(0);
        double threshold = properties.getPipeline().getVectorSimilarityThreshold();
        double autoResolve = properties.getPipeline().getAutoResolveThreshold();

        log.info("Top match for {}: {} with similarity {}", context.getJiraKey(), topMatch.jiraKey(), topMatch.similarity());

        if (topMatch.similarity() >= autoResolve) {
            context.setResolutionSource("VECTOR_MATCH");
            context.setResolutionText(topMatch.resolutionText());
            context.setConfidence(topMatch.similarity());
            context.setTerminal(true);
            return StageResult.terminal(getName(),
                    "Auto-resolved via vector match (similarity=" + topMatch.similarity() + ")",
                    Duration.between(start, Instant.now()));
        }

        if (topMatch.similarity() >= threshold) {
            context.setResolutionSource("VECTOR_MATCH");
            context.setResolutionText(topMatch.resolutionText());
            context.setConfidence(topMatch.similarity());
            context.setTerminal(true);
            return StageResult.terminal(getName(),
                    "Suggested resolution via vector match (similarity=" + topMatch.similarity() + ")",
                    Duration.between(start, Instant.now()));
        }

        return StageResult.success(getName(), Duration.between(start, Instant.now()));
    }

    @Override
    public boolean shouldExecute(TicketContext context) {
        return !context.isTerminal();
    }

    @Override
    public int getOrder() {
        return 200;
    }

    @Override
    public String getName() {
        return "VECTOR_SEARCH";
    }
}

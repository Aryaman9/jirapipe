package com.jirapipe.pipeline.stage;

import com.jirapipe.embedding.EmbeddingService;
import com.jirapipe.llm.LlmService;
import com.jirapipe.llm.dto.ResolutionResult;
import com.jirapipe.pipeline.context.StageResult;
import com.jirapipe.pipeline.context.TicketContext;
import com.jirapipe.vectorstore.VectorStoreRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;

@Component
public class GptResolutionStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(GptResolutionStage.class);

    private final LlmService llmService;
    private final VectorStoreRepository vectorStore;
    private final EmbeddingService embeddingService;

    public GptResolutionStage(@Qualifier("openAiService") LlmService llmService,
                              VectorStoreRepository vectorStore,
                              EmbeddingService embeddingService) {
        this.llmService = llmService;
        this.vectorStore = vectorStore;
        this.embeddingService = embeddingService;
    }

    @Override
    public StageResult process(TicketContext context) {
        Instant start = Instant.now();

        ResolutionResult result = llmService.generateResolution(context);

        if (result == null) {
            context.setResolutionSource("ESCALATED");
            context.setTerminal(true);
            return StageResult.terminal(getName(), "GPT-4o unavailable, escalated for manual review",
                    Duration.between(start, Instant.now()));
        }

        context.setResolvedClassification(result.classification());
        context.setResolvedPriority(result.severity());
        context.setResolvedTeam(result.suggestedTeam());
        context.setResolutionText(result.resolutionText());
        context.setResolutionSource("GPT4O");
        context.setConfidence(result.confidence());
        context.setTerminal(true);

        storeNewEmbedding(context);

        log.info("GPT-4o resolved {} as {} {} (confidence: {})",
                context.getJiraKey(), result.classification(), result.severity(), result.confidence());

        return StageResult.terminal(getName(), "Resolved by GPT-4o", Duration.between(start, Instant.now()));
    }

    private void storeNewEmbedding(TicketContext context) {
        try {
            String textToEmbed = context.getExtractedSignals() != null
                    ? context.getExtractedSignals().toEmbeddingText()
                    : context.getSummary();

            float[] embedding = embeddingService.embed(textToEmbed);
            if (embedding != null && context.getTicketId() != null) {
                String contentHash = sha256(textToEmbed);
                vectorStore.store(context.getTicketId(), embedding, contentHash);
                log.debug("Stored new embedding for novel ticket {}", context.getJiraKey());
            }
        } catch (Exception e) {
            log.warn("Failed to store embedding for {}: {}", context.getJiraKey(), e.getMessage());
        }
    }

    private String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            return "unknown";
        }
    }

    @Override
    public boolean shouldExecute(TicketContext context) {
        return !context.isTerminal();
    }

    @Override
    public int getOrder() {
        return 300;
    }

    @Override
    public String getName() {
        return "GPT_RESOLUTION";
    }
}

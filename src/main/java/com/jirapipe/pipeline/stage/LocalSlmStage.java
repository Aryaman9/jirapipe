package com.jirapipe.pipeline.stage;

import com.jirapipe.llm.LlmService;
import com.jirapipe.pipeline.context.StageResult;
import com.jirapipe.pipeline.context.TicketContext;
import com.jirapipe.pipeline.context.TicketSignals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

@Component
public class LocalSlmStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(LocalSlmStage.class);

    private final LlmService llmService;

    public LocalSlmStage(@Qualifier("ollamaService") LlmService llmService) {
        this.llmService = llmService;
    }

    @Override
    public StageResult process(TicketContext context) {
        Instant start = Instant.now();

        try {
            TicketSignals signals = llmService.extractSignals(context.getSummary(), context.getDescription());
            context.setExtractedSignals(signals);

            if (signals.severityHint() != null && context.getResolvedPriority() == null) {
                context.setResolvedPriority(signals.severityHint());
            }
            if (signals.categoryHint() != null) {
                context.setResolvedClassification(signals.categoryHint());
            }

            log.info("SLM extracted {} keywords, {} components for {}",
                    signals.keywords().size(), signals.componentNames().size(), context.getJiraKey());

            return StageResult.success(getName(), Duration.between(start, Instant.now()));

        } catch (Exception e) {
            log.warn("SLM extraction failed for {}, continuing with raw text: {}", context.getJiraKey(), e.getMessage());
            return StageResult.failure(getName(), e.getMessage(), Duration.between(start, Instant.now()));
        }
    }

    @Override
    public boolean shouldExecute(TicketContext context) {
        return !context.isTerminal();
    }

    @Override
    public int getOrder() {
        return 100;
    }

    @Override
    public String getName() {
        return "LOCAL_SLM";
    }
}

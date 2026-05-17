package com.jirapipe.pipeline;

import com.jirapipe.pipeline.context.StageResult;
import com.jirapipe.pipeline.context.TicketContext;
import com.jirapipe.pipeline.stage.PipelineStage;
import com.jirapipe.observability.PipelineMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

@Component
public class TriagePipeline {

    private static final Logger log = LoggerFactory.getLogger(TriagePipeline.class);

    private final List<PipelineStage> stages;
    private final PipelineMetrics metrics;

    public TriagePipeline(List<PipelineStage> stages, PipelineMetrics metrics) {
        this.stages = stages.stream()
                .sorted(Comparator.comparingInt(PipelineStage::getOrder))
                .toList();
        this.metrics = metrics;
    }

    public PipelineResult execute(TicketContext context) {
        MDC.put("correlationId", context.getCorrelationId());
        MDC.put("jiraKey", context.getJiraKey());

        Instant pipelineStart = Instant.now();
        log.info("Starting pipeline for ticket {}", context.getJiraKey());

        try {
            for (PipelineStage stage : stages) {
                if (!stage.shouldExecute(context)) {
                    log.debug("Skipping stage {} for ticket {}", stage.getName(), context.getJiraKey());
                    continue;
                }

                log.info("Executing stage {} for ticket {}", stage.getName(), context.getJiraKey());
                StageResult result = stage.process(context);
                context.addStageResult(result);

                if (!result.success()) {
                    log.warn("Stage {} failed for ticket {}: {}", stage.getName(), context.getJiraKey(), result.message());
                    metrics.recordStageFailure(stage.getName());
                }

                if (result.terminal()) {
                    log.info("Pipeline terminated at stage {} for ticket {}: {}",
                            stage.getName(), context.getJiraKey(), result.message());
                    break;
                }
            }

            Duration totalDuration = Duration.between(pipelineStart, Instant.now());
            metrics.recordPipelineExecution(context.getResolutionSource(), totalDuration);

            log.info("Pipeline completed for ticket {} in {}ms. Source: {}, Confidence: {}",
                    context.getJiraKey(), totalDuration.toMillis(),
                    context.getResolutionSource(), context.getConfidence());

            return new PipelineResult(context, totalDuration, true, null);

        } catch (Exception e) {
            Duration totalDuration = Duration.between(pipelineStart, Instant.now());
            log.error("Pipeline failed for ticket {}: {}", context.getJiraKey(), e.getMessage(), e);
            metrics.recordPipelineFailure();
            return new PipelineResult(context, totalDuration, false, e.getMessage());

        } finally {
            MDC.clear();
        }
    }

    public record PipelineResult(
        TicketContext context,
        Duration duration,
        boolean success,
        String errorMessage
    ) {}
}

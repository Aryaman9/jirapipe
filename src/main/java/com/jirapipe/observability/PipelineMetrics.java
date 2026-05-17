package com.jirapipe.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class PipelineMetrics {

    private final MeterRegistry registry;
    private final Counter ticketsProcessed;
    private final Counter ticketsFailed;
    private final Counter gptCalls;
    private final Counter cacheHits;
    private final Counter cacheMisses;

    public PipelineMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.ticketsProcessed = Counter.builder("jirapipe.tickets.processed")
                .description("Total tickets processed")
                .register(registry);
        this.ticketsFailed = Counter.builder("jirapipe.tickets.failed")
                .description("Total tickets that failed processing")
                .register(registry);
        this.gptCalls = Counter.builder("jirapipe.gpt4o.calls")
                .description("Number of GPT-4o API calls made")
                .register(registry);
        this.cacheHits = Counter.builder("jirapipe.cache.hits")
                .description("Embedding cache hits")
                .register(registry);
        this.cacheMisses = Counter.builder("jirapipe.cache.misses")
                .description("Embedding cache misses")
                .register(registry);
    }

    public void recordPipelineExecution(String resolutionSource, Duration duration) {
        ticketsProcessed.increment();
        Timer.builder("jirapipe.pipeline.duration")
                .tag("source", resolutionSource != null ? resolutionSource : "unknown")
                .register(registry)
                .record(duration);
        if ("GPT4O".equals(resolutionSource)) {
            gptCalls.increment();
        }
    }

    public void recordPipelineFailure() {
        ticketsFailed.increment();
    }

    public void recordStageFailure(String stageName) {
        registry.counter("jirapipe.stage.failures", "stage", stageName).increment();
    }

    public void recordCacheHit() {
        cacheHits.increment();
    }

    public void recordCacheMiss() {
        cacheMisses.increment();
    }

    public void recordStageLatency(String stageName, Duration duration) {
        registry.timer("jirapipe.stage.duration", "stage", stageName).record(duration);
    }
}

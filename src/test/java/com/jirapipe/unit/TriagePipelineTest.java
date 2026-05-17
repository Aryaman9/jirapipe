package com.jirapipe.unit;

import com.jirapipe.observability.PipelineMetrics;
import com.jirapipe.pipeline.TriagePipeline;
import com.jirapipe.pipeline.context.StageResult;
import com.jirapipe.pipeline.context.TicketContext;
import com.jirapipe.pipeline.stage.PipelineStage;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TriagePipelineTest {

    @Test
    void executesStagesInOrder() {
        PipelineMetrics metrics = new PipelineMetrics(new SimpleMeterRegistry());
        StringBuilder executionOrder = new StringBuilder();

        PipelineStage stage1 = new TestStage("A", 100, executionOrder, false);
        PipelineStage stage2 = new TestStage("B", 200, executionOrder, false);
        PipelineStage stage3 = new TestStage("C", 300, executionOrder, false);

        TriagePipeline pipeline = new TriagePipeline(List.of(stage3, stage1, stage2), metrics);
        TicketContext context = new TicketContext();
        context.setJiraKey("TEST-1");
        context.setSummary("Test");

        TriagePipeline.PipelineResult result = pipeline.execute(context);

        assertTrue(result.success());
        assertEquals("ABC", executionOrder.toString());
    }

    @Test
    void stopsAtTerminalStage() {
        PipelineMetrics metrics = new PipelineMetrics(new SimpleMeterRegistry());
        StringBuilder executionOrder = new StringBuilder();

        PipelineStage stage1 = new TestStage("A", 100, executionOrder, false);
        PipelineStage stage2 = new TestStage("B", 200, executionOrder, true);
        PipelineStage stage3 = new TestStage("C", 300, executionOrder, false);

        TriagePipeline pipeline = new TriagePipeline(List.of(stage1, stage2, stage3), metrics);
        TicketContext context = new TicketContext();
        context.setJiraKey("TEST-2");
        context.setSummary("Test");

        pipeline.execute(context);

        assertEquals("AB", executionOrder.toString());
    }

    private static class TestStage implements PipelineStage {
        private final String name;
        private final int order;
        private final StringBuilder tracker;
        private final boolean terminal;

        TestStage(String name, int order, StringBuilder tracker, boolean terminal) {
            this.name = name;
            this.order = order;
            this.tracker = tracker;
            this.terminal = terminal;
        }

        @Override
        public StageResult process(TicketContext context) {
            tracker.append(name);
            if (terminal) {
                return StageResult.terminal(name, "done", Duration.ZERO);
            }
            return StageResult.success(name, Duration.ZERO);
        }

        @Override
        public boolean shouldExecute(TicketContext context) { return true; }

        @Override
        public int getOrder() { return order; }

        @Override
        public String getName() { return name; }
    }
}

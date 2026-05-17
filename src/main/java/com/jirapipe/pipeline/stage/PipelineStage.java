package com.jirapipe.pipeline.stage;

import com.jirapipe.pipeline.context.StageResult;
import com.jirapipe.pipeline.context.TicketContext;

public interface PipelineStage {

    StageResult process(TicketContext context);

    boolean shouldExecute(TicketContext context);

    int getOrder();

    String getName();
}

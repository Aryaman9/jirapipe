package com.jirapipe.llm;

import com.jirapipe.llm.dto.ResolutionResult;
import com.jirapipe.pipeline.context.TicketContext;
import com.jirapipe.pipeline.context.TicketSignals;

public interface LlmService {

    TicketSignals extractSignals(String summary, String description);

    ResolutionResult generateResolution(TicketContext context);
}

package com.jirapipe.llm;

import com.jirapipe.llm.dto.ResolutionResult;
import com.jirapipe.pipeline.context.TicketContext;
import com.jirapipe.pipeline.context.TicketSignals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MockLlmService implements LlmService {

    private static final Logger log = LoggerFactory.getLogger(MockLlmService.class);

    @Override
    public TicketSignals extractSignals(String summary, String description) {
        log.debug("Mock SLM extracting signals from: {}", summary);

        List<String> keywords = List.of(summary.toLowerCase().split("\\s+"));
        List<String> components = List.of("api-gateway", "auth-service");
        String severity = summary.toLowerCase().contains("prod") ? "P0" : "P2";
        String category = summary.toLowerCase().contains("error") ? "Bug" : "Feature";

        return new TicketSignals(keywords, components, List.of(), severity, category);
    }

    @Override
    public ResolutionResult generateResolution(TicketContext context) {
        log.debug("Mock GPT-4o generating resolution for: {}", context.getJiraKey());

        return new ResolutionResult(
                "Bug",
                "P2",
                "Platform Engineering",
                "Mock resolution: Investigate the reported issue in " + context.getProjectKey() + " project. Check recent deployments and configuration changes.",
                List.of(
                        "1. Check recent deployment logs",
                        "2. Review configuration changes in the last 24h",
                        "3. Verify service health endpoints",
                        "4. Escalate to on-call if P0/P1"
                ),
                0.82
        );
    }
}

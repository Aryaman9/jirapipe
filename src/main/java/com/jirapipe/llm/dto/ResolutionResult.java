package com.jirapipe.llm.dto;

import java.util.List;

public record ResolutionResult(
    String classification,
    String severity,
    String suggestedTeam,
    String resolutionText,
    List<String> resolutionSteps,
    double confidence
) {}

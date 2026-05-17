package com.jirapipe.admin.dto;

public record PipelineStats(
    long totalTickets,
    long resolvedTickets,
    long failedTickets,
    long pendingTickets,
    long vectorMatchCount,
    long gptResolutionCount,
    long ruleOverrideCount,
    double averageConfidence,
    long dlqSize,
    double cacheHitRate
) {}

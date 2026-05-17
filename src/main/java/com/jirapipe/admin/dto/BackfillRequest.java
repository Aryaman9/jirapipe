package com.jirapipe.admin.dto;

public record BackfillRequest(
    int batchSize,
    boolean resolvedOnly
) {
    public BackfillRequest {
        if (batchSize <= 0) batchSize = 500;
    }
}

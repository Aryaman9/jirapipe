package com.jirapipe.pipeline.context;

import java.time.Duration;

public record StageResult(
    String stageName,
    boolean success,
    boolean terminal,
    String message,
    Duration duration
) {
    public static StageResult success(String stageName, Duration duration) {
        return new StageResult(stageName, true, false, null, duration);
    }

    public static StageResult terminal(String stageName, String message, Duration duration) {
        return new StageResult(stageName, true, true, message, duration);
    }

    public static StageResult failure(String stageName, String message, Duration duration) {
        return new StageResult(stageName, false, false, message, duration);
    }
}

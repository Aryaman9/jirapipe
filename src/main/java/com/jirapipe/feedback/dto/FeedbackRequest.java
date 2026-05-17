package com.jirapipe.feedback.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record FeedbackRequest(
    @NotNull @Min(1) @Max(5) Integer rating,
    @NotNull Boolean accurate,
    String comment,
    String submittedBy
) {}

package com.jirapipe.admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record RoutingRuleDto(
    UUID id,
    @NotBlank String name,
    @NotNull Integer priorityOrder,
    @NotBlank String conditionType,
    @NotBlank String conditionValue,
    @NotBlank String actionType,
    @NotBlank String actionValue,
    Boolean enabled
) {}

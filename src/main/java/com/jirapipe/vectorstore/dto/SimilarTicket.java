package com.jirapipe.vectorstore.dto;

import java.util.UUID;

public record SimilarTicket(
    UUID ticketId,
    String jiraKey,
    String summary,
    String resolutionText,
    double similarity
) {}

package com.jirapipe.pipeline.context;

import java.util.List;

public record TicketSignals(
    List<String> keywords,
    List<String> componentNames,
    List<String> errorSignatures,
    String severityHint,
    String categoryHint
) {
    public String toEmbeddingText() {
        StringBuilder sb = new StringBuilder();
        if (keywords != null && !keywords.isEmpty()) {
            sb.append(String.join(" ", keywords));
        }
        if (componentNames != null && !componentNames.isEmpty()) {
            sb.append(" ").append(String.join(" ", componentNames));
        }
        if (errorSignatures != null && !errorSignatures.isEmpty()) {
            sb.append(" ").append(String.join(" ", errorSignatures));
        }
        return sb.toString().trim();
    }
}

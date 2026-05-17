package com.jirapipe.pipeline.context;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TicketContext {

    private UUID ticketId;
    private String jiraKey;
    private String projectKey;
    private String summary;
    private String description;
    private String priority;
    private String issueType;
    private List<String> labels;
    private Instant createdAt;
    private String correlationId;

    private TicketSignals extractedSignals;
    private String resolvedPriority;
    private String resolvedClassification;
    private String resolvedTeam;
    private String resolutionText;
    private String resolutionSource;
    private double confidence;
    private boolean terminal;
    private List<StageResult> stageResults = new ArrayList<>();

    public TicketContext() {
        this.correlationId = UUID.randomUUID().toString();
    }

    public void addStageResult(StageResult result) {
        this.stageResults.add(result);
    }

    // Getters and setters
    public UUID getTicketId() { return ticketId; }
    public void setTicketId(UUID ticketId) { this.ticketId = ticketId; }
    public String getJiraKey() { return jiraKey; }
    public void setJiraKey(String jiraKey) { this.jiraKey = jiraKey; }
    public String getProjectKey() { return projectKey; }
    public void setProjectKey(String projectKey) { this.projectKey = projectKey; }
    public String getSummary() { return summary; }
    public void setSummary(String summary) { this.summary = summary; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getPriority() { return priority; }
    public void setPriority(String priority) { this.priority = priority; }
    public String getIssueType() { return issueType; }
    public void setIssueType(String issueType) { this.issueType = issueType; }
    public List<String> getLabels() { return labels; }
    public void setLabels(List<String> labels) { this.labels = labels; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public TicketSignals getExtractedSignals() { return extractedSignals; }
    public void setExtractedSignals(TicketSignals extractedSignals) { this.extractedSignals = extractedSignals; }
    public String getResolvedPriority() { return resolvedPriority; }
    public void setResolvedPriority(String resolvedPriority) { this.resolvedPriority = resolvedPriority; }
    public String getResolvedClassification() { return resolvedClassification; }
    public void setResolvedClassification(String resolvedClassification) { this.resolvedClassification = resolvedClassification; }
    public String getResolvedTeam() { return resolvedTeam; }
    public void setResolvedTeam(String resolvedTeam) { this.resolvedTeam = resolvedTeam; }
    public String getResolutionText() { return resolutionText; }
    public void setResolutionText(String resolutionText) { this.resolutionText = resolutionText; }
    public String getResolutionSource() { return resolutionSource; }
    public void setResolutionSource(String resolutionSource) { this.resolutionSource = resolutionSource; }
    public double getConfidence() { return confidence; }
    public void setConfidence(double confidence) { this.confidence = confidence; }
    public boolean isTerminal() { return terminal; }
    public void setTerminal(boolean terminal) { this.terminal = terminal; }
    public List<StageResult> getStageResults() { return stageResults; }
}

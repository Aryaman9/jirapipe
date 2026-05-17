package com.jirapipe.webhook.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record JiraWebhookPayload(
    @JsonProperty("webhookEvent") String webhookEvent,
    @JsonProperty("timestamp") long timestamp,
    @JsonProperty("issue") JiraIssue issue
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record JiraIssue(
        @JsonProperty("id") String id,
        @JsonProperty("key") String key,
        @JsonProperty("fields") JiraFields fields
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record JiraFields(
        @JsonProperty("summary") String summary,
        @JsonProperty("description") String description,
        @JsonProperty("priority") JiraPriority priority,
        @JsonProperty("status") JiraStatus status,
        @JsonProperty("issuetype") JiraIssueType issueType,
        @JsonProperty("reporter") JiraUser reporter,
        @JsonProperty("assignee") JiraUser assignee,
        @JsonProperty("labels") List<String> labels,
        @JsonProperty("project") JiraProject project,
        @JsonProperty("created") Instant created,
        @JsonProperty("updated") Instant updated
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record JiraPriority(@JsonProperty("name") String name) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record JiraStatus(@JsonProperty("name") String name) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record JiraIssueType(@JsonProperty("name") String name) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record JiraUser(
        @JsonProperty("displayName") String displayName,
        @JsonProperty("emailAddress") String emailAddress
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record JiraProject(
        @JsonProperty("key") String key,
        @JsonProperty("name") String name
    ) {}
}

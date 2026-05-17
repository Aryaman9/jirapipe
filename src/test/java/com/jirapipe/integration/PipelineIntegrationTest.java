package com.jirapipe.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Testcontainers
@Tag("integration")
class PipelineIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("pgvector/pgvector:pg16")
                    .asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("jirapipe_test")
            .withUsername("test")
            .withPassword("test");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:7"))
            .withExposedPorts(6379);

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // PostgreSQL
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        // Redis
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));

        // Kafka
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");

        // Use String serializers for test (matches the KafkaTemplate<String, String> in producer)
        registry.add("spring.kafka.producer.value-serializer",
                () -> "org.apache.kafka.common.serialization.StringSerializer");
        registry.add("spring.kafka.consumer.value-deserializer",
                () -> "org.apache.kafka.common.serialization.StringDeserializer");

        // Enable mock mode so no real LLM/JIRA calls are made
        registry.add("jirapipe.mock-mode", () -> "true");

        // Flyway
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");

        // Disable tracing/OTLP for tests
        registry.add("management.tracing.enabled", () -> "false");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void cleanDatabase() {
        jdbcTemplate.execute("DELETE FROM feedback");
        jdbcTemplate.execute("DELETE FROM resolutions");
        jdbcTemplate.execute("DELETE FROM ticket_embeddings");
        jdbcTemplate.execute("DELETE FROM dead_letter_queue");
        jdbcTemplate.execute("DELETE FROM tickets");
    }

    @Test
    @DisplayName("Full pipeline flow: webhook ingestion to ticket resolution")
    void testFullPipelineFlow() throws Exception {
        String jiraKey = "TEST-101";
        String webhookPayload = buildWebhookPayload(jiraKey, "Application login fails with 500 error",
                "Users report they cannot log in. Server returns HTTP 500.");

        // POST webhook - should return 202 Accepted
        mockMvc.perform(post("/webhook/jira")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(webhookPayload))
                .andExpect(status().isAccepted());

        // Wait for async Kafka processing to persist the ticket
        await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    List<Map<String, Object>> tickets = jdbcTemplate.queryForList(
                            "SELECT pipeline_status FROM tickets WHERE jira_key = ?", jiraKey);
                    assertThat(tickets).isNotEmpty();
                    String status = (String) tickets.get(0).get("pipeline_status");
                    assertThat(status).isIn("PROCESSING", "RESOLVED");
                });

        // Wait until ticket is RESOLVED (pipeline completes async)
        await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    List<Map<String, Object>> tickets = jdbcTemplate.queryForList(
                            "SELECT pipeline_status FROM tickets WHERE jira_key = ?", jiraKey);
                    assertThat(tickets).isNotEmpty();
                    assertThat((String) tickets.get(0).get("pipeline_status")).isEqualTo("RESOLVED");
                });

        // Verify ticket can be queried via GET /api/tickets/{key}
        mockMvc.perform(get("/api/tickets/{jiraKey}", jiraKey))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jira_key").value(jiraKey))
                .andExpect(jsonPath("$.pipeline_status").value("RESOLVED"))
                .andExpect(jsonPath("$.resolution_source").exists())
                .andExpect(jsonPath("$.confidence").exists());
    }

    @Test
    @DisplayName("Feedback endpoint: submit and verify feedback is stored")
    void testFeedbackEndpoint() throws Exception {
        // First, insert a resolved ticket directly so we can submit feedback for it
        String jiraKey = "TEST-201";
        insertResolvedTicket(jiraKey);

        // Submit feedback
        String feedbackJson = """
                {
                    "rating": 4,
                    "accurate": true,
                    "comment": "Resolution was helpful and accurate",
                    "submittedBy": "test-user@company.com"
                }
                """;

        mockMvc.perform(post("/api/feedback/{ticketKey}", jiraKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(feedbackJson))
                .andExpect(status().isOk());

        // Verify feedback is stored in the database
        List<Map<String, Object>> feedbackRows = jdbcTemplate.queryForList(
                "SELECT rating, accurate, comment, submitted_by FROM feedback f " +
                        "JOIN tickets t ON f.ticket_id = t.id WHERE t.jira_key = ?", jiraKey);

        assertThat(feedbackRows).hasSize(1);
        Map<String, Object> feedback = feedbackRows.get(0);
        assertThat(((Number) feedback.get("rating")).intValue()).isEqualTo(4);
        assertThat((Boolean) feedback.get("accurate")).isTrue();
        assertThat((String) feedback.get("comment")).isEqualTo("Resolution was helpful and accurate");
        assertThat((String) feedback.get("submitted_by")).isEqualTo("test-user@company.com");
    }

    @Test
    @DisplayName("Admin stats endpoint returns valid pipeline statistics")
    void testAdminStatsEndpoint() throws Exception {
        // Insert some test tickets to have meaningful stats
        insertResolvedTicket("STATS-001");
        insertResolvedTicket("STATS-002");
        insertFailedTicket("STATS-003");

        mockMvc.perform(get("/admin/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalTickets").value(3))
                .andExpect(jsonPath("$.resolvedTickets").value(2))
                .andExpect(jsonPath("$.failedTickets").value(1))
                .andExpect(jsonPath("$.pendingTickets").value(0))
                .andExpect(jsonPath("$.averageConfidence").isNumber())
                .andExpect(jsonPath("$.dlqSize").isNumber());
    }

    @Test
    @DisplayName("Webhook with missing issue returns 202 but ticket is not created")
    void testWebhookWithNoIssue() throws Exception {
        String payload = """
                {
                    "webhookEvent": "jira:issue_created",
                    "timestamp": 1700000000000
                }
                """;

        mockMvc.perform(post("/webhook/jira")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isAccepted());

        // Wait briefly and verify no ticket was created
        Thread.sleep(2000);
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tickets", Long.class);
        assertThat(count).isEqualTo(0L);
    }

    @Test
    @DisplayName("GET /api/tickets/{key} returns 404 for non-existent ticket")
    void testGetNonExistentTicket() throws Exception {
        mockMvc.perform(get("/api/tickets/{jiraKey}", "NONEXIST-999"))
                .andExpect(status().isNotFound());
    }

    // ---- Helper methods ----

    private String buildWebhookPayload(String jiraKey, String summary, String description) {
        return """
                {
                    "webhookEvent": "jira:issue_created",
                    "timestamp": 1700000000000,
                    "issue": {
                        "id": "10001",
                        "key": "%s",
                        "fields": {
                            "summary": "%s",
                            "description": "%s",
                            "priority": {"name": "High"},
                            "status": {"name": "Open"},
                            "issuetype": {"name": "Bug"},
                            "reporter": {
                                "displayName": "Test User",
                                "emailAddress": "test@company.com"
                            },
                            "assignee": null,
                            "labels": ["backend", "authentication"],
                            "project": {
                                "key": "TEST",
                                "name": "Test Project"
                            },
                            "created": "2024-01-15T10:30:00.000Z",
                            "updated": "2024-01-15T10:30:00.000Z"
                        }
                    }
                }
                """.formatted(jiraKey, summary, description);
    }

    private void insertResolvedTicket(String jiraKey) {
        jdbcTemplate.update("""
                INSERT INTO tickets (jira_key, project_key, summary, description, priority,
                                     issue_type, created_at, updated_at, pipeline_status,
                                     resolution_source, confidence, resolved_at)
                VALUES (?, 'TEST', 'Test summary', 'Test description', 'High',
                        'Bug', NOW(), NOW(), 'RESOLVED', 'GPT4O', 0.92, NOW())
                """, jiraKey);
    }

    private void insertFailedTicket(String jiraKey) {
        jdbcTemplate.update("""
                INSERT INTO tickets (jira_key, project_key, summary, description, priority,
                                     issue_type, created_at, updated_at, pipeline_status)
                VALUES (?, 'TEST', 'Failed ticket', 'Description', 'Medium',
                        'Bug', NOW(), NOW(), 'FAILED')
                """, jiraKey);
    }
}
